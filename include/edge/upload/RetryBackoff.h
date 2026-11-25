#pragma once

#include <cstdint>

namespace edge {

struct UploadConfig;

struct RetrySchedule {
    std::int32_t delay_ms = 0;
    std::int64_t next_attempt_at_ms = 0;
};

// 根据已领取的 attempt_count 计算确定性指数退避。
//
// 只读取配置中的 retry_initial_ms、retry_max_ms。
// 不修改配置，不访问数据库或网络。
//
// 参数非法时抛出 std::invalid_argument。
// 下次时间无法由 int64_t 表示时抛出 std::overflow_error。
RetrySchedule calculateRetrySchedule(
    const UploadConfig& config,
    std::int64_t attempt_count,
    std::int64_t now_ms
);

} // namespace edge