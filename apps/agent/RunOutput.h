#pragma once

#include "RunAgent.h"

#include "edge/status/JsonlProtocol.h"
#include "edge/status/StatusSnapshot.h"

#include <string>
#include <memory>

namespace edge {

// 在主线程创建和销毁。
// 必须覆盖完整运行过程及其错误处理。
// 不拥有业务线程、数据库或业务事件队列。
class RunOutput {
public:
    RunOutput(
        const AgentConfig& config,
        const AgentRunOptions& options
    );

    ~RunOutput() noexcept;

    RunOutput(const RunOutput&) = delete;
    RunOutput& operator=(const RunOutput&) = delete;

    void ready() noexcept;

    void record(
        const SensorEvent& event,
        JsonlRecordDisposition disposition
    ) noexcept;

    void status(const StatusSnapshot& snapshot) noexcept;

    void stopped(
        const StatusSnapshot& snapshot,
        int exit_code
    ) noexcept;

    void decorate(StatusSnapshot& snapshot) const;

    void pump() noexcept;

    // 上传报告已经收集、工作线程已经结束后调用。
    // 最多给控制台 200ms 收尾；不等待业务上传。
    void finish() noexcept;

    [[nodiscard]]
    const std::string& runId() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// 输出组件初始化失败时使用的单次、非阻塞诊断。
// 不走 iostream，避免回到同步输出路径。
void reportRunOutputSetupError(const char* message) noexcept;

} // namespace edge