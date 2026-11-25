#include "edge/upload/RetryBackoff.h"

#include <limits>
#include <stdexcept>

#include "edge/config/AgentConfig.h"

namespace edge {

RetrySchedule calculateRetrySchedule(
    const UploadConfig& config,
    std::int64_t attempt_count,
    std::int64_t now_ms
)
{
    if (config.retry_initial_ms <= 0
        || config.retry_max_ms <= 0
        || config.retry_initial_ms > config.retry_max_ms) {
        throw std::invalid_argument(
            "Retry intervals must be positive "
            "and initial interval must not exceed maximum"
        );
    }

    if (attempt_count <= 0 || now_ms <= 0) {
        throw std::invalid_argument(
            "Retry calculation requires a positive attempt count "
            "and timestamp"
        );
    }

    std::int32_t delay_ms = config.retry_initial_ms;

    const std::int32_t maximum_delay_ms =
        config.retry_max_ms;

    std::int64_t remaining_doublings = attempt_count - 1;

    while (remaining_doublings > 0
        && delay_ms < maximum_delay_ms) {
        // 判断翻倍是否会达到或超过上限。
        // 使用减法比较，避免先计算 delay_ms * 2。
        if (delay_ms >= maximum_delay_ms - delay_ms) {
            delay_ms = maximum_delay_ms;
            break;
        }

        // 已确认结果小于上限，因此加法不会溢出。
        delay_ms += delay_ms;
        --remaining_doublings;
    }

    const auto delay = static_cast<std::int64_t>(delay_ms);

    const auto maximum_timestamp =
        std::numeric_limits<std::int64_t>::max();

    // 先检查剩余表示空间，再做加法。
    if (delay > maximum_timestamp - now_ms) {
        throw std::overflow_error(
            "Next retry timestamp exceeds int64_t range"
        );
    }

    return RetrySchedule{
        delay_ms,
        now_ms + delay
    };
}

} // namespace edge