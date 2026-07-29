#ifndef SUBPROCESS_CONTAINER_H
#define SUBPROCESS_CONTAINER_H

#include <logos_container/module_container.h>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

class SubprocessContainer : public LogosCore::InstanceAwareModuleContainer {
public:
    struct ProcessCallbacks {
        std::function<void(const std::string& name, int exitCode, bool crashed)> onFinished;
        std::function<void(const std::string& name, bool crashed)> onError;
        std::function<void(const std::string& name, const std::string& line, bool isStderr)> onOutput;
    };

    // Upper bound on how many bytes of a single newline-free output line the
    // host will buffer per stream before force-flushing it to onOutput. The
    // child host runs partially-trusted module code; a malicious or buggy
    // module can emit an unbounded newline-free stream on stdout/stderr. The
    // parent (basecamp/logoscore) relays that output line-by-line, so without
    // a cap the per-stream line buffer grows without limit (OOM-killing the
    // trusted host and every module it supervises) while each 4 KB read
    // re-scans the whole accumulated buffer for a newline — O(N^2) CPU that
    // pins the single shared io thread. This cap bounds both: memory is held
    // to ~kMaxOutputLineBytes per stream and the buffered prefix is flushed
    // and reset once it is reached, so process isolation holds even for a
    // module that never emits a newline (F-014).
    static constexpr std::size_t kMaxOutputLineBytes = 1u * 1024u * 1024u; // 1 MiB

    // -- ModuleContainer interface --
    std::string id() const override { return "subprocess"; }
    bool canHandle(const LogosCore::ModuleDescriptor& desc) const override;
    bool launch(const LogosCore::ModuleDescriptor& desc,
                const std::string& hostBinary,
                const std::vector<std::string>& args,
                std::function<void(const std::string&)> onTerminated,
                LogosCore::LoadedModuleHandle& out) override;
    bool sendToken(const std::string& name, const std::string& token) override;
    void terminate(const std::string& name) override;
    void terminateAll() override;
    bool hasModule(const std::string& name) const override;
    std::optional<int64_t> pid(const std::string& name) const override;
    std::unordered_map<std::string, int64_t> getAllPids() const override;

    bool launchInstance(const LogosCore::ModuleDescriptor& desc,
                        const std::string& hostBinary,
                        const std::vector<std::string>& args,
                        std::function<void(const LogosCore::ModuleAddress&)> onTerminated,
                        LogosCore::LoadedModuleHandle& out) override;
    bool sendTokenToInstance(const LogosCore::ModuleAddress& address,
                             const std::string& token) override;
    bool terminateInstance(const LogosCore::ModuleAddress& address) override;
    bool hasInstance(const LogosCore::ModuleAddress& address) const override;
    std::optional<int64_t> instancePid(const LogosCore::ModuleAddress& address) const override;
    std::unordered_map<LogosCore::ModuleAddress, int64_t, LogosCore::ModuleAddressHash>
    getAllInstancePids() const override;

    // -- Low-level static process management (used by tests / qt_test_adapter) --
    static bool startProcess(const std::string& name, const std::string& executable,
                             const std::vector<std::string>& arguments, const ProcessCallbacks& callbacks);
    static bool sendTokenToProcess(const std::string& name,
                                    const std::string& token,
                                    int max_wait_ms = 5000);
    static void terminateProcess(const std::string& name);
    static void terminateAllProcesses();
    static bool hasProcess(const std::string& name);
    static int64_t getProcessId(const std::string& name);
    static std::unordered_map<std::string, int64_t> getAllProcessIds();
    static void clearAll();
    static void registerProcess(const std::string& name);
};

#endif // SUBPROCESS_CONTAINER_H
