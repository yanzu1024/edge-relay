#pragma once

#include <chrono>
#include <cstddef>
#include <optional>

namespace edge {

struct UploadConfig;

class UploadTrigger {
public:
    using Clock = std::chrono::steady_clock;

    // 只读取 batch_size 和 flush_interval_ms。
    explicit UploadTrigger(const UploadConfig& config);

    // ready_count 必须是当前已到期、可领取的记录数量。
    // 返回 true 后，由调用方执行一次上传协调。
    bool shouldUpload(
        std::size_t ready_count,
        Clock::time_point now
    );

    // 一次上传协调正常返回后调用。
    void onCycleFinished() noexcept;

    // 读取当前聚合截止时间，不修改计时。
    // 应先调用 shouldUpload 更新当前状态。
    std::optional<Clock::time_point> nextDeadline() const noexcept
    {
        return deadline_;
    }

private:
    std::size_t batch_size_ = 0;
    Clock::duration flush_interval_{};

    std::optional<Clock::time_point> deadline_;
};

} // namespace edge