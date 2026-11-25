#pragma once

#include "edge/upload/UploadTrigger.h"

#include <cstddef>
#include <cstdint>
#include <optional>

namespace edge {

struct UploadWaitPlan {
    bool upload_now = false;

    // 没有截止时间时，等待新消息通知或停止。
    std::optional<UploadTrigger::Clock::time_point> deadline;
};

// ready_count 来自本轮 listReady。
// next_future_at_ms 来自本轮 nextReadyAfter。
//
// 两次查询使用同一个 wall_now_ms。
// next_future_at_ms 如果存在，必须严格晚于 wall_now_ms。
//
// 本函数更新 trigger 的聚合状态，但不访问数据库、
// 不执行上传，也不等待。
UploadWaitPlan makeUploadWaitPlan(
    UploadTrigger& trigger,
    std::size_t ready_count,
    std::optional<std::int64_t> next_future_at_ms,
    std::int64_t wall_now_ms,
    UploadTrigger::Clock::time_point steady_now
);

} // namespace edge