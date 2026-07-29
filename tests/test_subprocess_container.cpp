// =============================================================================
// Tests for SubprocessContainer — the ModuleContainer implementation that
// manages child processes via Boost.Process v2.
//
// Verifies the container interface methods (launch, sendToken, terminate,
// hasModule, pid, getAllPids) independently from any ModuleFormatLoader.
// =============================================================================
#include <gtest/gtest.h>
#include "subprocess_container.h"
#include <algorithm>
#include <atomic>
#include <cstddef>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

class SubprocessContainerTest : public ::testing::Test {
protected:
    SubprocessContainer container;

    void SetUp() override { SubprocessContainer::clearAll(); }
    void TearDown() override { SubprocessContainer::clearAll(); }
};

// Launch a fake module host under `name` that stays alive ~5s. It must tolerate
// the extra CLI args the container appends in launch() (e.g. --token-source
// stdin): /bin/sleep can't be used directly because it rejects unknown options,
// so we run a shell that execs sleep and ignores the trailing args. exec keeps
// the pid stable (the shell becomes sleep), matching what the pid assertions
// expect.
static bool launchFakeModule(SubprocessContainer& c, const char* name,
                             LogosCore::LoadedModuleHandle& handle) {
    LogosCore::ModuleDescriptor desc;
    desc.name = name;
    return c.launch(desc, "/bin/sh", {"-c", "exec sleep 5"}, nullptr, handle);
}

static LogosCore::ModuleDescriptor scopedDescriptor(const char* instanceId) {
    LogosCore::ModuleDescriptor desc;
    desc.name = "lez_indexer_module";
    desc.instanceId = instanceId;
    return desc;
}

static bool launchFakeInstance(SubprocessContainer& c,
                               const LogosCore::ModuleDescriptor& desc,
                               LogosCore::LoadedModuleHandle& handle,
                               std::function<void(const LogosCore::ModuleAddress&)> onTerminated = {}) {
    return c.launchInstance(desc, "/bin/sh", {"-c", "exec sleep 5"},
                            std::move(onTerminated), handle);
}

// ---------------------------------------------------------------------------
// canHandle: subprocess container accepts any module descriptor
// ---------------------------------------------------------------------------

TEST_F(SubprocessContainerTest, CanHandle_AcceptsAnyDescriptor) {
    LogosCore::ModuleDescriptor desc;
    desc.format = "qt-plugin";
    EXPECT_TRUE(container.canHandle(desc));

    desc.format = "wasm";
    EXPECT_TRUE(container.canHandle(desc));

    desc.format = "";
    EXPECT_TRUE(container.canHandle(desc));
}

TEST_F(SubprocessContainerTest, Id_ReturnsSubprocess) {
    EXPECT_EQ(container.id(), "subprocess");
}

// ---------------------------------------------------------------------------
// launch: spawns a real child and populates the handle
// ---------------------------------------------------------------------------

TEST_F(SubprocessContainerTest, Launch_StartsProcessAndPopulatesHandle) {
    LogosCore::LoadedModuleHandle handle;
    bool ok = launchFakeModule(container, "launch_test", handle);
    EXPECT_TRUE(ok);
    EXPECT_EQ(handle.name, "launch_test");
    EXPECT_GT(handle.pid, 0);
}

TEST_F(SubprocessContainerTest, Launch_HasModuleReturnsTrueAfterLaunch) {
    LogosCore::LoadedModuleHandle handle;
    launchFakeModule(container, "has_mod", handle);
    EXPECT_TRUE(container.hasModule("has_mod"));
}

TEST_F(SubprocessContainerTest, Launch_PidReturnsPidAfterLaunch) {
    LogosCore::LoadedModuleHandle handle;
    launchFakeModule(container, "pid_mod", handle);
    auto pid = container.pid("pid_mod");
    ASSERT_TRUE(pid.has_value());
    EXPECT_GT(*pid, 0);
    EXPECT_EQ(*pid, handle.pid);
}

TEST_F(SubprocessContainerTest, Launch_FailsForNonexistentBinary) {
    LogosCore::ModuleDescriptor desc;
    desc.name = "bad_launch";
    LogosCore::LoadedModuleHandle handle;

    bool ok = container.launch(desc, "/nonexistent/binary", {}, nullptr, handle);
    EXPECT_FALSE(ok);
}

// ---------------------------------------------------------------------------
// terminate
// ---------------------------------------------------------------------------

TEST_F(SubprocessContainerTest, Terminate_RemovesModule) {
    LogosCore::LoadedModuleHandle handle;
    launchFakeModule(container, "term_mod", handle);
    ASSERT_TRUE(container.hasModule("term_mod"));

    container.terminate("term_mod");
    EXPECT_FALSE(container.hasModule("term_mod"));
}

TEST_F(SubprocessContainerTest, TerminateAll_RemovesAllModules) {
    LogosCore::LoadedModuleHandle h1;
    launchFakeModule(container, "ta_1", h1);
    LogosCore::LoadedModuleHandle h2;
    launchFakeModule(container, "ta_2", h2);

    container.terminateAll();

    EXPECT_FALSE(container.hasModule("ta_1"));
    EXPECT_FALSE(container.hasModule("ta_2"));
}

// ---------------------------------------------------------------------------
// getAllPids
// ---------------------------------------------------------------------------

TEST_F(SubprocessContainerTest, GetAllPids_ReturnsAllRunningPids) {
    LogosCore::LoadedModuleHandle h1;
    launchFakeModule(container, "gp_a", h1);
    LogosCore::LoadedModuleHandle h2;
    launchFakeModule(container, "gp_b", h2);

    auto pids = container.getAllPids();
    EXPECT_EQ(pids.size(), 2u);
    EXPECT_GT(pids.at("gp_a"), 0);
    EXPECT_GT(pids.at("gp_b"), 0);
    EXPECT_NE(pids.at("gp_a"), pids.at("gp_b"));
}

TEST_F(SubprocessContainerTest, StartProcess_PromotesDefaultPlaceholder) {
    SubprocessContainer::registerProcess("placeholder_start");
    ASSERT_TRUE(SubprocessContainer::hasProcess("placeholder_start"));
    ASSERT_TRUE(SubprocessContainer::startProcess(
        "placeholder_start", "/bin/sh", {"-c", "exec sleep 5"}, {}));

    EXPECT_GT(SubprocessContainer::getProcessId("placeholder_start"), 0);
}

TEST_F(SubprocessContainerTest, Terminate_RemovesDefaultPlaceholder) {
    SubprocessContainer::registerProcess("placeholder_terminate");
    ASSERT_TRUE(SubprocessContainer::hasProcess("placeholder_terminate"));

    container.terminate("placeholder_terminate");

    EXPECT_FALSE(SubprocessContainer::hasProcess("placeholder_terminate"));
}

// ---------------------------------------------------------------------------
// Scoped instances: same logical module, separate process identities
// ---------------------------------------------------------------------------

TEST_F(SubprocessContainerTest, LaunchInstance_KeepsSameNameZonesIndependent) {
    const LogosCore::ModuleDescriptor lez = scopedDescriptor("zone_0101");
    const LogosCore::ModuleDescriptor paradox = scopedDescriptor("zone_8888");
    LogosCore::LoadedModuleHandle lezHandle;
    LogosCore::LoadedModuleHandle paradoxHandle;

    ASSERT_TRUE(launchFakeInstance(container, lez, lezHandle));
    ASSERT_TRUE(launchFakeInstance(container, paradox, paradoxHandle));

    EXPECT_EQ(lezHandle.address(), lez.address());
    EXPECT_EQ(paradoxHandle.address(), paradox.address());
    EXPECT_NE(lezHandle.pid, paradoxHandle.pid);
    EXPECT_TRUE(container.hasInstance(lez.address()));
    EXPECT_TRUE(container.hasInstance(paradox.address()));
    EXPECT_FALSE(container.hasModule(lez.name));

    const auto all = container.getAllInstancePids();
    EXPECT_EQ(all.size(), 2u);
    EXPECT_EQ(all.at(lez.address()), lezHandle.pid);
    EXPECT_EQ(all.at(paradox.address()), paradoxHandle.pid);
    EXPECT_TRUE(container.getAllPids().empty());

    ASSERT_TRUE(container.terminateInstance(lez.address()));
    EXPECT_FALSE(container.hasInstance(lez.address()));
    EXPECT_TRUE(container.hasInstance(paradox.address()));
}

TEST_F(SubprocessContainerTest, LaunchInstance_RejectsDuplicateAddressWithoutReplacement) {
    const LogosCore::ModuleDescriptor descriptor = scopedDescriptor("zone_0101");
    LogosCore::LoadedModuleHandle first;
    LogosCore::LoadedModuleHandle duplicate;

    ASSERT_TRUE(launchFakeInstance(container, descriptor, first));
    EXPECT_FALSE(launchFakeInstance(container, descriptor, duplicate));
    EXPECT_TRUE(container.hasInstance(descriptor.address()));
    EXPECT_EQ(container.instancePid(descriptor.address()),
              std::optional<int64_t>{first.pid});
    EXPECT_EQ(container.getAllInstancePids().size(), 1u);
}

TEST_F(SubprocessContainerTest, LaunchInstance_ConcurrentDuplicateStartsOnlyOneProcess) {
    const LogosCore::ModuleDescriptor descriptor = scopedDescriptor("zone_0101");
    std::mutex mutex;
    std::condition_variable condition;
    unsigned int ready = 0;
    bool start = false;
    bool results[2] = {false, false};

    const auto launch = [&](unsigned int index) {
        {
            std::unique_lock<std::mutex> lock(mutex);
            ++ready;
            condition.notify_all();
            condition.wait(lock, [&]() { return start; });
        }

        LogosCore::LoadedModuleHandle handle;
        results[index] = launchFakeInstance(container, descriptor, handle);
    };

    std::thread first(launch, 0);
    std::thread second(launch, 1);
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(5), [&]() {
            return ready == 2;
        }));
        start = true;
    }
    condition.notify_all();
    first.join();
    second.join();

    EXPECT_NE(results[0], results[1]);
    EXPECT_TRUE(container.hasInstance(descriptor.address()));
    EXPECT_EQ(container.getAllInstancePids().size(), 1u);
}

TEST_F(SubprocessContainerTest, LaunchInstance_RoutesTokensToMatchingInstance) {
    const LogosCore::ModuleDescriptor lez = scopedDescriptor("zone_0101");
    const LogosCore::ModuleDescriptor paradox = scopedDescriptor("zone_8888");
    LogosCore::LoadedModuleHandle lezHandle;
    LogosCore::LoadedModuleHandle paradoxHandle;
    std::atomic<unsigned int> unexpectedTerminations{0};
    const auto onTerminated = [&](const LogosCore::ModuleAddress&) {
        unexpectedTerminations.fetch_add(1);
    };

    ASSERT_TRUE(container.launchInstance(
        lez, "/bin/sh",
        {"-c", "read token; test \"$token\" = \"lez-token\" || exit 91; exec sleep 5"},
        onTerminated, lezHandle));
    ASSERT_TRUE(container.launchInstance(
        paradox, "/bin/sh",
        {"-c", "read token; test \"$token\" = \"paradox-token\" || exit 92; exec sleep 5"},
        onTerminated, paradoxHandle));

    ASSERT_TRUE(container.sendTokenToInstance(paradox.address(), "paradox-token"));
    ASSERT_TRUE(container.sendTokenToInstance(lez.address(), "lez-token"));

    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    EXPECT_EQ(unexpectedTerminations.load(), 0u);
    EXPECT_TRUE(container.hasInstance(lez.address()));
    EXPECT_TRUE(container.hasInstance(paradox.address()));
}

TEST_F(SubprocessContainerTest, LaunchInstance_CrashRemovesOnlyMatchingAddress) {
    const LogosCore::ModuleDescriptor crashing = scopedDescriptor("zone_0101");
    const LogosCore::ModuleDescriptor surviving = scopedDescriptor("zone_8888");
    LogosCore::LoadedModuleHandle crashingHandle;
    LogosCore::LoadedModuleHandle survivingHandle;
    std::mutex mutex;
    std::condition_variable condition;
    std::vector<LogosCore::ModuleAddress> terminated;
    const auto onTerminated = [&](const LogosCore::ModuleAddress& address) {
        std::lock_guard<std::mutex> lock(mutex);
        terminated.push_back(address);
        condition.notify_all();
    };

    ASSERT_TRUE(container.launchInstance(
        crashing, "/bin/sh", {"-c", "kill -SEGV $$"}, onTerminated, crashingHandle));
    ASSERT_TRUE(launchFakeInstance(container, surviving, survivingHandle, onTerminated));

    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(5), [&]() {
        return !terminated.empty();
    }));
    ASSERT_EQ(terminated.size(), 1u);
    EXPECT_EQ(terminated.front(), crashing.address());

    lock.unlock();
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (container.hasInstance(crashing.address())
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_FALSE(container.hasInstance(crashing.address()));
    EXPECT_TRUE(container.hasInstance(surviving.address()));
}

TEST_F(SubprocessContainerTest, LaunchInstance_RelaunchDoesNotLoseNewProcess) {
    const LogosCore::ModuleDescriptor descriptor = scopedDescriptor("zone_0101");
    LogosCore::LoadedModuleHandle first;
    LogosCore::LoadedModuleHandle second;

    ASSERT_TRUE(launchFakeInstance(container, descriptor, first));
    ASSERT_TRUE(container.terminateInstance(descriptor.address()));
    ASSERT_TRUE(launchFakeInstance(container, descriptor, second));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(container.hasInstance(descriptor.address()));
    EXPECT_EQ(container.instancePid(descriptor.address()),
              std::optional<int64_t>{second.pid});
}

TEST_F(SubprocessContainerTest, LaunchInstance_CompletionCannotRaceReplacement) {
    const LogosCore::ModuleDescriptor descriptor = scopedDescriptor("zone_0101");
    LogosCore::LoadedModuleHandle exiting;
    LogosCore::LoadedModuleHandle replacement;
    std::mutex mutex;
    std::condition_variable condition;
    bool callbackEntered = false;
    bool allowCallbackReturn = false;

    ASSERT_TRUE(container.launchInstance(
        descriptor, "/bin/sh", {"-c", "exit 0"},
        [&](const LogosCore::ModuleAddress&) {
            std::unique_lock<std::mutex> lock(mutex);
            callbackEntered = true;
            condition.notify_all();
            condition.wait(lock, [&]() { return allowCallbackReturn; });
        },
        exiting));

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(5), [&]() {
            return callbackEntered;
        }));
    }

    EXPECT_FALSE(launchFakeInstance(container, descriptor, replacement));

    {
        std::lock_guard<std::mutex> lock(mutex);
        allowCallbackReturn = true;
    }
    condition.notify_all();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (container.hasInstance(descriptor.address())
           && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_FALSE(container.hasInstance(descriptor.address()));
    EXPECT_TRUE(launchFakeInstance(container, descriptor, replacement));
}

TEST_F(SubprocessContainerTest, LaunchInstance_RejectsInvalidAddress) {
    LogosCore::ModuleDescriptor descriptor = scopedDescriptor("zone/0101");
    LogosCore::LoadedModuleHandle handle;

    EXPECT_FALSE(launchFakeInstance(container, descriptor, handle));
    EXPECT_FALSE(container.hasInstance(descriptor.address()));
}

// ---------------------------------------------------------------------------
// onOutput callback via launch
// ---------------------------------------------------------------------------

TEST_F(SubprocessContainerTest, Launch_OutputCallbackReceivesStdoutAndStderr) {
    std::mutex mtx;
    std::condition_variable cv;
    std::vector<std::pair<std::string, bool>> lines;
    std::atomic<bool> terminated{false};

    LogosCore::ModuleDescriptor desc;
    desc.name = "output_test";
    LogosCore::LoadedModuleHandle handle;

    // We can't easily capture onOutput through the container interface alone
    // since launch() sets its own callbacks. Instead, use the low-level API.
    SubprocessContainer::ProcessCallbacks cb;
    cb.onOutput = [&](const std::string&, const std::string& line, bool isStderr) {
        std::lock_guard<std::mutex> lock(mtx);
        lines.emplace_back(line, isStderr);
        cv.notify_all();
    };
    cb.onFinished = [&](const std::string&, int, bool) {
        terminated.store(true);
        cv.notify_all();
    };

    ASSERT_TRUE(SubprocessContainer::startProcess(
        "output_test", "/bin/sh", {"-c", "echo out-line; echo err-line >&2"}, cb));

    std::unique_lock<std::mutex> lock(mtx);
    cv.wait_for(lock, std::chrono::seconds(5),
                [&]() { return terminated.load() && lines.size() >= 2; });

    ASSERT_GE(lines.size(), 2u);
    bool saw_stdout = false, saw_stderr = false;
    for (auto& [line, isStderr] : lines) {
        if (!isStderr && line == "out-line") saw_stdout = true;
        if ( isStderr && line == "err-line") saw_stderr = true;
    }
    EXPECT_TRUE(saw_stdout);
    EXPECT_TRUE(saw_stderr);
}

// ---------------------------------------------------------------------------
// Output relay is bounded — a module cannot OOM/stall the host via stdout
// ---------------------------------------------------------------------------
//
// F-014: the parent (trusted host) relays each child's stdout/stderr to
// onOutput line-by-line, buffering bytes until a '\n' arrives. A
// partially-trusted module that emits a long *newline-free* stream (or one
// giant write) would, without a cap, grow the per-stream line buffer without
// bound — pinning host memory until the OS OOM-kills basecamp/logoscore and
// every module it supervises — while each 4 KB read re-scanned the whole
// accumulated buffer for a newline (O(N^2) CPU on the single shared io
// thread). Process isolation exists precisely so a module fault cannot take
// the host down; an unbounded relay buffer breaks that guarantee.
//
// This test drives a child that writes ~5 MB to stdout with NO newline, then
// closes it. The contract: the host must surface that output in bounded
// pieces (each <= kMaxOutputLineBytes plus at most one read chunk), never as a
// single unbounded line. Against the pre-fix code the entire payload is
// accumulated and emitted as one ~5 MB line, which fails the per-piece bound.

TEST_F(SubprocessContainerTest, Launch_BoundsUnterminatedOutputLine) {
    // The child writes a fixed number of newline-free bytes via dd. Skip
    // cleanly if /dev/zero isn't available (some constrained sandboxes).
    if (access("/dev/zero", R_OK) != 0)
        GTEST_SKIP() << "/dev/zero not available";

    // ~5 MB of newline-free output: several times the 1 MiB per-line cap, and
    // deliberately not an exact multiple of it so the final EOF flush carries
    // a non-trivial remainder too.
    constexpr std::size_t kTotalBytes = 5'000'000;

    std::mutex mtx;
    std::condition_variable cv;
    std::size_t received_total = 0;
    std::size_t max_piece = 0;
    std::size_t piece_count = 0;
    std::atomic<bool> terminated{false};

    SubprocessContainer::ProcessCallbacks cb;
    cb.onOutput = [&](const std::string&, const std::string& line, bool isStderr) {
        if (isStderr) return; // dd's own summary is silenced, but be defensive
        std::lock_guard<std::mutex> lock(mtx);
        received_total += line.size();
        max_piece = std::max(max_piece, line.size());
        ++piece_count;
        cv.notify_all();
    };
    cb.onFinished = [&](const std::string&, int, bool) {
        terminated.store(true);
        cv.notify_all();
    };

    // bs=1000000 count=5 => exactly 5,000,000 NUL bytes on stdout, no newline.
    // dd's transfer summary goes to stderr, which we discard.
    ASSERT_TRUE(SubprocessContainer::startProcess(
        "oom_relay_test", "/bin/sh",
        {"-c", "dd if=/dev/zero bs=1000000 count=5 2>/dev/null"}, cb));

    {
        std::unique_lock<std::mutex> lock(mtx);
        // Wait until every byte has been relayed (the meaningful condition);
        // onFinished may fire slightly before the final pipe read drains.
        bool ok = cv.wait_for(lock, std::chrono::seconds(15),
                              [&]() { return received_total >= kTotalBytes; });
        ASSERT_TRUE(ok) << "host relayed only " << received_total << " of "
                        << kTotalBytes << " bytes before timing out";
    }

    std::lock_guard<std::mutex> lock(mtx);

    // No data lost or duplicated: every byte the child wrote is surfaced.
    EXPECT_EQ(received_total, kTotalBytes);

    // The core invariant: a single newline-free stream is delivered in bounded
    // pieces, not one unbounded line. A flush is triggered once the buffer
    // reaches the cap, after appending at most one read chunk (4096 bytes), so
    // no emitted piece may exceed cap + one chunk.
    EXPECT_LE(max_piece, SubprocessContainer::kMaxOutputLineBytes + 4096u)
        << "host buffered a " << max_piece << "-byte line without a newline; "
           "the per-stream relay buffer is unbounded (F-014) — a module can "
           "OOM/stall the trusted host through stdout.";

    // ~5 MB at a 1 MiB cap must split into several pieces. Pre-fix this is
    // exactly one (everything accumulated, emitted once at EOF).
    EXPECT_GT(piece_count, 1u)
        << "expected the capped relay to split a 5 MB newline-free stream into "
           "multiple bounded pieces, got " << piece_count;
}

// ---------------------------------------------------------------------------
// Crash detection — the kernel of crash isolation
// ---------------------------------------------------------------------------
//
// When a child dies on a signal, the container must surface it via
// onFinished(name, exit_code, crashed=true) and tear down its own
// bookkeeping. Every observer above (composite_module_loader → module_manager's
// onTerminated → registry.markUnloaded → daemon → CLI) hangs off this
// callback — if it stops firing with crashed=true, isolation breaks
// silently. The end-to-end test in logos-logoscore-cli covers the full
// stack at ~13s; this one pins the mechanism at its source in ~50ms.

TEST_F(SubprocessContainerTest, Launch_OnFinishedReportsCrashedOnSignal) {
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> fired{false};
    std::string gotName;
    int gotExitCode = -1;
    bool gotCrashed = false;

    SubprocessContainer::ProcessCallbacks cb;
    cb.onFinished = [&](const std::string& n, int code, bool crashed) {
        {
            std::lock_guard<std::mutex> lock(mtx);
            gotName = n;
            gotExitCode = code;
            gotCrashed = crashed;
        }
        fired.store(true);
        cv.notify_all();
    };

    // `kill -SEGV $$` makes the shell signal itself — a portable way to
    // exercise WIFSIGNALED without depending on /bin/kill -s SEGV syntax
    // or shipping a custom helper binary.
    ASSERT_TRUE(SubprocessContainer::startProcess(
        "crash_test", "/bin/sh", {"-c", "kill -SEGV $$"}, cb));

    std::unique_lock<std::mutex> lock(mtx);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5),
                            [&]() { return fired.load(); }))
        << "onFinished never fired for a signaled child";

    EXPECT_EQ(gotName, "crash_test");
    EXPECT_TRUE(gotCrashed)
        << "container must report crashed=true on signal-exit (WIFSIGNALED). "
           "Without this, the markUnloaded propagation up the stack stalls "
           "and a crashed module looks 'loaded' forever.";
    EXPECT_EQ(gotExitCode, SIGSEGV)
        << "exit_code must carry the signal number (WTERMSIG), not the "
           "raw waitpid status. Got " << gotExitCode << ".";

    // Bookkeeping side of the contract: once onFinished has fired the
    // container must no longer claim the module exists. This is what
    // feeds upstream observers via their own ProcessEntry cleanup.
    EXPECT_FALSE(container.hasModule("crash_test"));
    EXPECT_FALSE(container.pid("crash_test").has_value());
}

// ---------------------------------------------------------------------------
// Token delivery over the child's stdin pipe
//
// sendToken/sendTokenToProcess writes the auth token to the child's stdin (a
// private inherited pipe, set up in startProcess) followed by a newline, then
// closes the write end. The child reads its token from fd 0. This replaced the
// old predictable-socket handoff; here we prove the token actually arrives by
// spawning a shell that reads one line from stdin and echoes it back on
// stdout, which the container relays to onOutput.
// ---------------------------------------------------------------------------

TEST_F(SubprocessContainerTest, SendToken_DeliversTokenOverStdin) {
    std::mutex mtx;
    std::condition_variable cv;
    std::string received;
    std::atomic<bool> got{false};

    SubprocessContainer::ProcessCallbacks cb;
    cb.onOutput = [&](const std::string&, const std::string& line, bool isStderr) {
        if (isStderr) return;
        std::lock_guard<std::mutex> lock(mtx);
        if (line.rfind("GOT:", 0) == 0) {       // line starts with "GOT:"
            received = line.substr(4);
            got.store(true);
            cv.notify_all();
        }
    };

    // `read tok` consumes exactly the one newline-terminated line the parent
    // writes; the child then echoes it so the test can observe what arrived.
    ASSERT_TRUE(SubprocessContainer::startProcess(
        "tok_stdin", "/bin/sh", {"-c", "read tok; echo \"GOT:$tok\""}, cb));

    const std::string secret = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
    EXPECT_TRUE(SubprocessContainer::sendTokenToProcess("tok_stdin", secret));

    std::unique_lock<std::mutex> lock(mtx);
    ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] { return got.load(); }))
        << "child never echoed a token read from stdin";
    EXPECT_EQ(received, secret)
        << "token delivered over stdin did not match what the parent sent";
}

TEST_F(SubprocessContainerTest, SendToken_FailsForUnknownProcess) {
    // No entry registered for this name → nothing to write the token to.
    EXPECT_FALSE(SubprocessContainer::sendTokenToProcess("no_such_module", "tok"));
}

// ---------------------------------------------------------------------------
// pid/hasModule for unknown names
// ---------------------------------------------------------------------------

TEST_F(SubprocessContainerTest, Pid_ReturnsNulloptForUnknown) {
    EXPECT_FALSE(container.pid("nonexistent").has_value());
}

TEST_F(SubprocessContainerTest, HasModule_ReturnsFalseForUnknown) {
    EXPECT_FALSE(container.hasModule("nonexistent"));
}

TEST_F(SubprocessContainerTest, Terminate_NoopForUnknown) {
    container.terminate("nonexistent");
    SUCCEED();
}
