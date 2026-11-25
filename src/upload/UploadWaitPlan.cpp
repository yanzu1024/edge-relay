#include "edge/upload/UploadWaitPlan.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>

namespace edge {

namespace {

// 重试时间使用 system_clock 保存。
// 定期复查，避免系统时间调整后仍睡到原来的长截止时间。
//
// 这个值只限制单次等待，不修改数据库中的重试时间。
constexpr std::int64_t kWallClockRecheckMs = 1000;

UploadTrigger::Clock::time_point boundedDeadline(
    UploadTrigger::Clock::time_point now,
    std::int64_t delay_ms
)
{
    using Clock = UploadTrigger::Clock;

    const auto delay = std::chrono::duration_cast<
        Clock::duration
    >(
        std::chrono::milliseconds(delay_ms)
    );

    const auto maximum = Clock::time_point::max();

    if (now > maximum - delay) {
        return maximum;
    }

    return now + delay;
}

} // namespace

UploadWaitPlan makeUploadWaitPlan(
    UploadTrigger& trigger,
    std::size_t ready_count,
    std::optional<std::int64_t> next_future_at_ms,
    std::int64_t wall_now_ms,
    UploadTrigger::Clock::time_point steady_now
)
{
    if (wall_now_ms <= 0) {
        throw std::invalid_argument(
            "Upload wait plan requires a positive wall time"
        );
    }

    if (next_future_at_ms.has_value()
        && *next_future_at_ms <= wall_now_ms) {
        throw std::invalid_argument(
            "Upload wait plan requires a strictly future ready time"
        );
    }

    UploadWaitPlan plan;

    if (trigger.shouldUpload(ready_count, steady_now)) {
        plan.upload_now = true;
        return plan;
    }

    // 不足一批时，沿用触发器已经建立的聚合截止。
    // 没有到期记录时，shouldUpload 已清除聚合计时。
    plan.deadline = trigger.nextDeadline();

    if (next_future_at_ms.has_value()) {
        // 两者均为正，且 future > now，减法不会溢出。
        const auto remaining_ms =
            *next_future_at_ms - wall_now_ms;

        // 先限制毫秒数，再转换 steady_clock duration，
        // 避免极大的数据库时间导致转换溢出。
        const auto delay_ms = std::min(
            remaining_ms,
            kWallClockRecheckMs
        );

        const auto retry_deadline = boundedDeadline(
            steady_now,
            delay_ms
        );

        if (!plan.deadline.has_value()
            || retry_deadline < *plan.deadline) {
            plan.deadline = retry_deadline;
        }
    }

    return plan;
}

} // namespace edge