#pragma once

#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace edge {

enum class BatchEventStatus {
    Accepted,
    Duplicate,
    Conflict
};

struct BatchEventResult {
    std::string event_id;
    BatchEventStatus status;
};

struct BatchResponse {
    // 固定按请求 ID 的顺序排列。
    std::vector<BatchEventResult> results;
};

struct BatchResponseError {
    std::string message;
};

using BatchResponseParseResult =
    std::variant<BatchResponse, BatchResponseError>;

// 只解析业务响应；调用方必须先确认 HTTP 状态为 200。
//
// requested_ids 必须包含 1..100 个非空、不重复的 ID，
// 否则抛出 std::invalid_argument。
//
// 响应必须覆盖全部请求 ID，且每个 ID 恰好出现一次。
// 任意协议错误都返回 BatchResponseError，不返回部分结果。
//
// 本地内存分配失败等资源异常继续向上传递。
BatchResponseParseResult parseBatchResponse(
    std::string_view body,
    const std::vector<std::string>& requested_ids
);

} // namespace edge