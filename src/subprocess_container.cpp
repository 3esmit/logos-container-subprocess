#include "subprocess_container.h"

#include <boost/asio/connect_pipe.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/readable_pipe.hpp>
#include <boost/asio/writable_pipe.hpp>
#include <boost/asio/write.hpp>
#include <boost/process/v2/process.hpp>
#include <boost/process/v2/stdio.hpp>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace bp2  = boost::process::v2;
namespace asio = boost::asio;

namespace {

std::shared_ptr<spdlog::logger>& moduleStdoutLogger() {
    static std::shared_ptr<spdlog::logger> logger = []() {
        auto l = spdlog::stdout_color_mt("logos_module_stdout");
        l->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [out] %v");
        return l;
    }();
    return logger;
}

// ---------------------------------------------------------------------------
// Background io_context: one thread, kept alive by a work guard.
// ---------------------------------------------------------------------------

struct IoRuntime {
    asio::io_context ctx;
    asio::executor_work_guard<asio::io_context::executor_type> guard;
    std::thread thread;

    IoRuntime()
        : guard(asio::make_work_guard(ctx))
        , thread([this]() { ctx.run(); })
    {}

    ~IoRuntime();
};

IoRuntime& ioRuntime() {
    static IoRuntime s_runtime;
    return s_runtime;
}

// ---------------------------------------------------------------------------
// ProcessEntry: owns one live child process and its read pipes.
// ---------------------------------------------------------------------------

struct ProcessEntry {
    bp2::process                              process;
    asio::readable_pipe                       out_pipe;
    asio::readable_pipe                       err_pipe;
    // Parent write-end of the child's stdin. The auth token is delivered by
    // writing it here (see sendTokenToProcess): the child inherited the read
    // end as fd 0, so this pipe is private to the parent/child pair —
    // unforgeable, with no predictable filesystem path to squat. Held open
    // from launch until sendToken writes the token and closes it.
    asio::writable_pipe                       in_pipe;
    SubprocessContainer::ProcessCallbacks     callbacks;
    LogosCore::ModuleAddress                   address;
    std::string                               name;
    std::array<char, 4096>                    out_read_buf{};
    std::array<char, 4096>                    err_read_buf{};
    std::string                               out_line_buf;
    std::string                               err_line_buf;
    std::atomic<bool>                         exited{false};
    std::atomic<bool>                         cancelled{false};

    ProcessEntry(bp2::process proc,
                 asio::readable_pipe out_rp, asio::readable_pipe err_rp,
                 asio::writable_pipe in_wp,
                 const LogosCore::ModuleAddress& a,
                 const SubprocessContainer::ProcessCallbacks& cb)
        : process(std::move(proc))
        , out_pipe(std::move(out_rp))
        , err_pipe(std::move(err_rp))
        , in_pipe(std::move(in_wp))
        , address(a)
        , name(a.moduleName)
        , callbacks(cb)
    {}
};

struct IoOperationState {
    std::mutex mutex;
    std::condition_variable completed;
    bool done{false};
};

template<typename Operation>
void runOnIoThreadAndWait(Operation&& operation)
{
    auto state = std::make_shared<IoOperationState>();
    asio::dispatch(ioRuntime().ctx,
                   [state, operation = std::forward<Operation>(operation)]() mutable {
        operation();
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->done = true;
        }
        state->completed.notify_one();
    });

    std::unique_lock<std::mutex> lock(state->mutex);
    state->completed.wait(lock, [&]() { return state->done; });
}

// ---------------------------------------------------------------------------
// Global process registry
//
// Declared at namespace scope (constructed before main), while the
// IoRuntime singleton above is a function-local static (constructed
// lazily on first use, from main). C++ destroys statics in reverse
// order of construction, so at exit ~IoRuntime fires first — tearing
// down the asio::io_context (and its epoll_reactor) — and *then*
// s_processes is destroyed, dropping its shared_ptr<ProcessEntry>s,
// each of which closes asio handles (process / pipes) tied to the
// already-freed reactor. Use-after-free → heap corruption → SIGABRT.
// ~IoRuntime (defined below, out-of-line) handles this by clearing
// s_processes itself while ctx is still alive.
// ---------------------------------------------------------------------------

using ProcessMap = std::unordered_map<LogosCore::ModuleAddress,
                                      std::shared_ptr<ProcessEntry>,
                                      LogosCore::ModuleAddressHash>;
using AddressSet = std::unordered_set<LogosCore::ModuleAddress,
                                      LogosCore::ModuleAddressHash>;

ProcessMap s_processes;
AddressSet s_launchingAddresses;
std::mutex s_processesMutex;

LogosCore::ModuleAddress defaultAddress(const std::string& moduleName)
{
    return {moduleName, {}};
}

std::string addressLabel(const LogosCore::ModuleAddress& address)
{
    if (address.instanceId.empty()) return address.moduleName;
    return address.moduleName + "@" + address.instanceId;
}

// ---------------------------------------------------------------------------

IoRuntime::~IoRuntime() {
    guard.reset();
    ctx.stop();
    if (thread.joinable()) {
        // Common case: destructor fires from the main thread at process
        // exit, ctx.run() returned cleanly, just join.
        //
        // Pathological case: destructor fires from *this very thread*.
        // Happens when an asio handler running on `thread` calls
        // exit() (e.g. the onFinished callback below crash-aborts the
        // process). exit() triggers static destruction in the calling
        // thread; that's us. join() on yourself is EDEADLK and would
        // throw a std::system_error → uncaught → terminate() → SIGABRT,
        // masking the real crash that triggered the exit() in the first
        // place. Detach instead: the OS reaps the thread on process
        // exit, no observable difference vs join in this single-process
        // scenario.
        if (thread.get_id() == std::this_thread::get_id()) {
            thread.detach();
        } else {
            thread.join();
        }
    }

    // Tear down ProcessEntries while ctx (and its epoll_reactor) is
    // still alive. See the static-destruction-order note on s_processes
    // above. Doing this from ~IoRuntime instead of relying on the
    // implicit reverse order of static destruction guarantees that
    // every io_object_impl::~io_object_impl() (which calls
    // reactor.deregister_descriptor) runs against a live reactor.
    {
        std::lock_guard<std::mutex> lock(s_processesMutex);
        s_processes.clear();
        s_launchingAddresses.clear();
    }
}

// ---------------------------------------------------------------------------
// Async read loop
// ---------------------------------------------------------------------------

void scheduleRead(std::shared_ptr<ProcessEntry> entry, bool isStderr);

void handleRead(std::shared_ptr<ProcessEntry> entry, bool isStderr,
                const boost::system::error_code& ec, std::size_t n)
{
    auto& buf      = isStderr ? entry->err_read_buf : entry->out_read_buf;
    auto& line_buf = isStderr ? entry->err_line_buf : entry->out_line_buf;

    if (n > 0) {
        // Everything already in line_buf was scanned for '\n' on previous
        // reads and contained none (the loop below consumes through every
        // newline and erase() drops the consumed prefix), so resume the
        // search at the old end instead of rescanning from offset 0. Without
        // this, a newline-free stream from a child re-scans the whole growing
        // buffer on every 4 KB read — O(N^2) CPU that pins the shared io
        // thread supervising all modules (F-014).
        const std::size_t search_start = line_buf.size();
        line_buf.append(buf.data(), n);

        std::size_t pos = 0, nl, search = search_start;
        while ((nl = line_buf.find('\n', search)) != std::string::npos) {
            std::string line = line_buf.substr(pos, nl - pos);
            if (!line.empty() && line.back() == '\r') line.pop_back();
            if (!line.empty() && entry->callbacks.onOutput)
                entry->callbacks.onOutput(entry->name, line, isStderr);
            pos = nl + 1;
            search = pos;
        }
        line_buf.erase(0, pos);

        // Bound the unterminated remainder. The child runs partially-trusted
        // module code; one that emits a long newline-free stream (or a single
        // multi-GB write) would otherwise grow line_buf without limit, pinning
        // host memory until the OS OOM-kills the trusted parent and every
        // module it supervises. Once the buffered prefix reaches the cap,
        // force-flush it as a line and reset so memory stays bounded — a
        // module must not be able to take the host down this way (F-014).
        if (line_buf.size() >= SubprocessContainer::kMaxOutputLineBytes) {
            if (entry->callbacks.onOutput)
                entry->callbacks.onOutput(entry->name, line_buf, isStderr);
            line_buf.clear();
        }
    }

    if (!ec) {
        scheduleRead(std::move(entry), isStderr);
    } else {
        if (!line_buf.empty() && entry->callbacks.onOutput) {
            if (line_buf.back() == '\r') line_buf.pop_back();
            if (!line_buf.empty())
                entry->callbacks.onOutput(entry->name, line_buf, isStderr);
        }
        line_buf.clear();
    }
}

void scheduleRead(std::shared_ptr<ProcessEntry> entry, bool isStderr) {
    auto* e = entry.get();
    auto& pipe = isStderr ? e->err_pipe : e->out_pipe;
    auto& buf  = isStderr ? e->err_read_buf : e->out_read_buf;
    pipe.async_read_some(
        asio::buffer(buf),
        [entry = std::move(entry), isStderr](const boost::system::error_code& ec, std::size_t n) mutable {
            handleRead(std::move(entry), isStderr, ec, n);
        });
}

// ---------------------------------------------------------------------------
// Async wait
// ---------------------------------------------------------------------------

void scheduleWait(std::shared_ptr<ProcessEntry> entry) {
    auto* e = entry.get();
    e->process.async_wait(
        [entry = std::move(entry)](const boost::system::error_code& /*ec*/, int raw_status) mutable {
            entry->exited.store(true);

            if (!entry->cancelled.load() && entry->callbacks.onFinished) {
                bool crashed = false;
                int exit_code = raw_status;
#if defined(WIFEXITED)
                if (WIFSIGNALED(raw_status)) {
                    crashed    = true;
                    exit_code  = WTERMSIG(raw_status);
                } else if (WIFEXITED(raw_status)) {
                    exit_code = WEXITSTATUS(raw_status);
                }
#endif
                entry->callbacks.onFinished(entry->name, exit_code, crashed);
            }

            // Keep this exact entry registered until its completion callback
            // returns. A same-address launch during the callback must be
            // rejected rather than letting an old lifecycle notification mark
            // the replacement runtime as unloaded. Manual termination has
            // already removed the entry and set cancelled, so it reaches here
            // without a callback and this compare-and-erase becomes a no-op.
            std::lock_guard<std::mutex> lock(s_processesMutex);
            const auto current = s_processes.find(entry->address);
            if (current != s_processes.end() && current->second == entry)
                s_processes.erase(current);
        });
}

// ---------------------------------------------------------------------------
// Synchronous kill
// ---------------------------------------------------------------------------

void syncKill(std::shared_ptr<ProcessEntry> entry) {
    if (!entry) return;

    entry->cancelled.store(true);

    // Pipe initiation, cancellation, and completion all run through the one
    // io_context thread. Closing a pipe directly from a lifecycle caller can
    // otherwise race the queued first async_read and corrupt Asio state.
    runOnIoThreadAndWait([entry]() {
        boost::system::error_code ec;
        entry->out_pipe.close(ec);
        entry->err_pipe.close(ec);
        // Close the stdin write end too: if we kill the child before a token
        // was delivered, this gives it EOF on fd 0 so a blocking token read
        // returns instead of hanging until the wait deadline.
        entry->in_pipe.close(ec);
        entry->process.request_exit(ec);
    });

    // A lifecycle callback runs on the same io_context thread as
    // async_wait. Returning lets that wait handler observe the exit; blocking
    // here would prevent it from ever running.
    if (ioRuntime().thread.get_id() == std::this_thread::get_id()) return;

    auto wait = [&](std::chrono::milliseconds budget) -> bool {
        auto deadline = std::chrono::steady_clock::now() + budget;
        while (!entry->exited.load()) {
            if (std::chrono::steady_clock::now() >= deadline) return false;
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        return true;
    };

    if (!wait(std::chrono::seconds(5))) {
        spdlog::warn("Process did not terminate gracefully, killing: {}",
                     addressLabel(entry->address));
        runOnIoThreadAndWait([entry]() {
            boost::system::error_code ec;
            entry->process.terminate(ec);
        });
        if (!wait(std::chrono::seconds(2))) {
            spdlog::error("Process did not respond to SIGKILL: {}",
                          addressLabel(entry->address));
        }
    }
}

bool startProcessAtAddress(const LogosCore::ModuleAddress& address,
                           const std::string& executable,
                           const std::vector<std::string>& arguments,
                           const SubprocessContainer::ProcessCallbacks& callbacks)
{
    if (!address.isValid()) {
        spdlog::error("Refusing process with invalid module address: {}",
                      addressLabel(address));
        return false;
    }

    IoRuntime& rt = ioRuntime();

    // Reserve the address before spawning so concurrent launches cannot bind
    // two children to one runtime identity. Do not hold a process-global
    // mutex across Boost.Process: POSIX launch may fork, and a child forked
    // while another thread owns that mutex can deadlock before exec.
    {
        std::lock_guard<std::mutex> lock(s_processesMutex);
        const auto existing = s_processes.find(address);
        if (s_launchingAddresses.count(address) > 0
            || (existing != s_processes.end() && existing->second)) {
            spdlog::error("A module process is already running for {}",
                          addressLabel(address));
            return false;
        }
        s_launchingAddresses.insert(address);
    }

    auto releaseReservation = [&]() {
        std::lock_guard<std::mutex> lock(s_processesMutex);
        s_launchingAddresses.erase(address);
    };

    boost::system::error_code ec;
    asio::readable_pipe out_rpipe(rt.ctx), err_rpipe(rt.ctx);
    asio::writable_pipe out_wpipe(rt.ctx), err_wpipe(rt.ctx);
    asio::readable_pipe in_rpipe(rt.ctx);
    asio::writable_pipe in_wpipe(rt.ctx);

    asio::connect_pipe(out_rpipe, out_wpipe, ec);
    if (ec) {
        releaseReservation();
        spdlog::error("Failed to create stdout pipe for {}: {}",
                      addressLabel(address), ec.message());
        return false;
    }
    asio::connect_pipe(err_rpipe, err_wpipe, ec);
    if (ec) {
        releaseReservation();
        spdlog::error("Failed to create stderr pipe for {}: {}",
                      addressLabel(address), ec.message());
        return false;
    }
    asio::connect_pipe(in_rpipe, in_wpipe, ec);
    if (ec) {
        releaseReservation();
        spdlog::error("Failed to create stdin pipe for {}: {}",
                      addressLabel(address), ec.message());
        return false;
    }

    bp2::process_stdio pstdio;
    pstdio.in = in_rpipe;
    pstdio.out = out_wpipe;
    pstdio.err = err_wpipe;

    bp2::process process = bp2::default_process_launcher()(
        rt.ctx, ec, executable, arguments, pstdio);

    out_wpipe.close();
    err_wpipe.close();
    in_rpipe.close();

    if (ec) {
        releaseReservation();
        spdlog::error("Failed to start process for {}: {}",
                      addressLabel(address), ec.message());
        return false;
    }

    auto entry = std::make_shared<ProcessEntry>(
        std::move(process), std::move(out_rpipe), std::move(err_rpipe),
        std::move(in_wpipe), address, callbacks);
    bool accepted = false;
    {
        std::lock_guard<std::mutex> lock(s_processesMutex);
        if (s_launchingAddresses.erase(address) > 0) {
            const auto current = s_processes.find(address);
            if (current == s_processes.end() || !current->second) {
                s_processes[address] = entry;
                accepted = true;
            }
        }
    }

    if (!accepted) entry->cancelled.store(true);
    asio::post(rt.ctx, [entry]() {
        scheduleRead(entry, /*isStderr=*/false);
        scheduleRead(entry, /*isStderr=*/true);
        scheduleWait(entry);
    });

    if (accepted) return true;

    syncKill(entry);
    return false;
}

bool sendTokenToAddress(const LogosCore::ModuleAddress& address,
                        const std::string& token)
{
    std::shared_ptr<ProcessEntry> entry;
    {
        std::lock_guard<std::mutex> lock(s_processesMutex);
        const auto current = s_processes.find(address);
        if (current != s_processes.end()) entry = current->second;
    }

    if (!entry) {
        spdlog::error("No process entry to deliver token to for: {}",
                      addressLabel(address));
        return false;
    }

    std::string payload = token;
    payload.push_back('\n');

    boost::system::error_code ec;
    runOnIoThreadAndWait([entry, payload = std::move(payload), &ec]() {
        boost::asio::write(entry->in_pipe, boost::asio::buffer(payload), ec);

        // The child receives exactly one newline-delimited token, then EOF.
        // The pipe is private to this exact parent/child address pair.
        boost::system::error_code closeEc;
        entry->in_pipe.close(closeEc);
    });

    if (!ec) return true;

    spdlog::error("Failed to write token to stdin pipe for {}: {}",
                  addressLabel(address), ec.message());
    std::shared_ptr<ProcessEntry> dead;
    {
        std::lock_guard<std::mutex> lock(s_processesMutex);
        const auto current = s_processes.find(address);
        if (current != s_processes.end() && current->second == entry) {
            dead = current->second;
            s_processes.erase(current);
        }
    }
    syncKill(dead);
    return false;
}

bool terminateAddress(const LogosCore::ModuleAddress& address)
{
    std::shared_ptr<ProcessEntry> entry;
    bool launchCancelled = false;
    {
        std::lock_guard<std::mutex> lock(s_processesMutex);
        const auto current = s_processes.find(address);
        launchCancelled = s_launchingAddresses.erase(address) > 0;
        if (current != s_processes.end()) {
            entry = current->second;
            s_processes.erase(current);
        }
    }
    if (!entry && !launchCancelled) return false;
    syncKill(entry);
    return true;
}

bool hasAddress(const LogosCore::ModuleAddress& address)
{
    std::lock_guard<std::mutex> lock(s_processesMutex);
    return s_processes.count(address) > 0;
}

int64_t processIdForAddress(const LogosCore::ModuleAddress& address)
{
    std::lock_guard<std::mutex> lock(s_processesMutex);
    const auto current = s_processes.find(address);
    if (current == s_processes.end() || !current->second) return -1;
    return static_cast<int64_t>(current->second->process.id());
}

std::unordered_map<LogosCore::ModuleAddress, int64_t, LogosCore::ModuleAddressHash>
allProcessIdsByAddress()
{
    std::lock_guard<std::mutex> lock(s_processesMutex);
    std::unordered_map<LogosCore::ModuleAddress, int64_t, LogosCore::ModuleAddressHash> result;
    for (const auto& [address, entry] : s_processes) {
        if (entry) result.emplace(address, static_cast<int64_t>(entry->process.id()));
    }
    return result;
}

void terminateAllAddresses()
{
    ProcessMap snapshot;
    {
        std::lock_guard<std::mutex> lock(s_processesMutex);
        if (s_processes.empty() && s_launchingAddresses.empty()) return;
        snapshot.swap(s_processes);
        s_launchingAddresses.clear();
    }
    for (auto& [address, entry] : snapshot) syncKill(entry);
}

} // anonymous namespace

// ===========================================================================
// ModuleContainer interface
// ===========================================================================

bool SubprocessContainer::canHandle(const LogosCore::ModuleDescriptor& /*desc*/) const
{
    return true;
}

bool SubprocessContainer::launch(const LogosCore::ModuleDescriptor& desc,
                                  const std::string& hostBinary,
                                  const std::vector<std::string>& args,
                                  std::function<void(const std::string&)> onTerminated,
                                  LogosCore::LoadedModuleHandle& out)
{
    if (!desc.instanceId.empty()) {
        spdlog::error("Scoped module {} must use the instance-aware container API",
                      addressLabel(desc.address()));
        return false;
    }

    return launchInstance(
        desc, hostBinary, args,
        [onTerminated](const LogosCore::ModuleAddress& address) {
            if (onTerminated) onTerminated(address.moduleName);
        },
        out);
}

bool SubprocessContainer::sendToken(const std::string& name, const std::string& token)
{
    return sendTokenToInstance(defaultAddress(name), token);
}

void SubprocessContainer::terminate(const std::string& name)
{
    static_cast<void>(terminateInstance(defaultAddress(name)));
}

void SubprocessContainer::terminateAll()
{
    terminateAllAddresses();
}

bool SubprocessContainer::hasModule(const std::string& name) const
{
    return hasInstance(defaultAddress(name));
}

std::optional<int64_t> SubprocessContainer::pid(const std::string& name) const
{
    return instancePid(defaultAddress(name));
}

std::unordered_map<std::string, int64_t> SubprocessContainer::getAllPids() const
{
    return getAllProcessIds();
}

bool SubprocessContainer::launchInstance(
    const LogosCore::ModuleDescriptor& desc,
    const std::string& hostBinary,
    const std::vector<std::string>& args,
    std::function<void(const LogosCore::ModuleAddress&)> onTerminated,
    LogosCore::LoadedModuleHandle& out)
{
    const LogosCore::ModuleAddress address = desc.address();
    if (!address.isValid()) {
        spdlog::error("Refusing module with invalid runtime address: {}",
                      addressLabel(address));
        return false;
    }

    ProcessCallbacks callbacks;
    callbacks.onFinished = [onTerminated, address](const std::string&, int,
                                                    bool crashed) {
        if (crashed)
            spdlog::critical("Module process crashed: {}", addressLabel(address));
        if (onTerminated) onTerminated(address);
    };
    callbacks.onError = [onTerminated, address](const std::string&, bool crashed) {
        if (crashed)
            spdlog::critical("Module process crashed: {}", addressLabel(address));
        if (onTerminated) onTerminated(address);
    };
    callbacks.onOutput = [label = addressLabel(address)](
                             const std::string&, const std::string& line,
                             bool isStderr) {
        if (!isStderr) {
            moduleStdoutLogger()->info("[{}] {}", label, line);
            return;
        }
        auto contains = [&](std::initializer_list<const char*> keywords) {
            for (const char* keyword : keywords) {
                if (line.find(keyword) != std::string::npos) return true;
            }
            return false;
        };
        if (contains({"Critical:", "CRITICAL:", "Fatal:", "FATAL:"}))
            spdlog::critical("[{}] {}", label, line);
        else if (contains({"Error:", "ERROR:", "FAILED:"}))
            spdlog::error("[{}] {}", label, line);
        else if (contains({"Warning:", "WARNING:"}))
            spdlog::warn("[{}] {}", label, line);
        else if (contains({"Debug:", "DEBUG:"}))
            spdlog::debug("[{}] {}", label, line);
        else if (contains({"Trace:", "TRACE:"}))
            spdlog::trace("[{}] {}", label, line);
        else
            spdlog::info("[{}] {}", label, line);
    };

    std::vector<std::string> launchArgs = args;
    launchArgs.push_back("--token-source");
    launchArgs.push_back("stdin");

    if (!startProcessAtAddress(address, hostBinary, launchArgs, callbacks))
        return false;

    out.name = address.moduleName;
    out.instanceId = address.instanceId;
    out.pid = processIdForAddress(address);
    return true;
}

bool SubprocessContainer::sendTokenToInstance(const LogosCore::ModuleAddress& address,
                                               const std::string& token)
{
    if (!address.isValid()) return false;
    return sendTokenToAddress(address, token);
}

bool SubprocessContainer::terminateInstance(const LogosCore::ModuleAddress& address)
{
    if (!address.isValid()) return false;
    return terminateAddress(address);
}

bool SubprocessContainer::hasInstance(const LogosCore::ModuleAddress& address) const
{
    return address.isValid() && hasAddress(address);
}

std::optional<int64_t> SubprocessContainer::instancePid(
    const LogosCore::ModuleAddress& address) const
{
    if (!address.isValid()) return std::nullopt;
    const int64_t processId = processIdForAddress(address);
    if (processId < 0) return std::nullopt;
    return processId;
}

std::unordered_map<LogosCore::ModuleAddress, int64_t, LogosCore::ModuleAddressHash>
SubprocessContainer::getAllInstancePids() const
{
    return allProcessIdsByAddress();
}

// ===========================================================================
// Static process management API
// ===========================================================================

bool SubprocessContainer::startProcess(const std::string& name, const std::string& executable,
                                        const std::vector<std::string>& arguments,
                                        const ProcessCallbacks& callbacks)
{
    return startProcessAtAddress(defaultAddress(name), executable, arguments,
                                 callbacks);
}

bool SubprocessContainer::sendTokenToProcess(const std::string& name,
                                              const std::string& token,
                                              int /*max_wait_ms*/)
{
    return sendTokenToAddress(defaultAddress(name), token);
}

void SubprocessContainer::terminateProcess(const std::string& name)
{
    static_cast<void>(terminateAddress(defaultAddress(name)));
}

void SubprocessContainer::terminateAllProcesses()
{
    terminateAllAddresses();
}

bool SubprocessContainer::hasProcess(const std::string& name)
{
    return hasAddress(defaultAddress(name));
}

int64_t SubprocessContainer::getProcessId(const std::string& name)
{
    return processIdForAddress(defaultAddress(name));
}

std::unordered_map<std::string, int64_t> SubprocessContainer::getAllProcessIds()
{
    std::unordered_map<std::string, int64_t> result;
    for (const auto& [address, processId] : allProcessIdsByAddress()) {
        if (address.isDefaultInstance()) result.emplace(address.moduleName, processId);
    }
    return result;
}

void SubprocessContainer::clearAll()
{
    terminateAllAddresses();
}

void SubprocessContainer::registerProcess(const std::string& name)
{
    const LogosCore::ModuleAddress address = defaultAddress(name);
    if (!address.isValid()) return;
    std::lock_guard<std::mutex> lock(s_processesMutex);
    if (!s_processes.count(address))
        s_processes[address] = nullptr;
}
