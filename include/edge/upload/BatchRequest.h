#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "edge/domain/EventJson.h"
#include "edge/domain/SensorEvent.h"

namespace edge {

struct BatchRequest {
    std::string body;

    // 与请求中的事件顺序一致，供响应解析器核对。
    std::vector<std::string> event_ids;
};

enum class BatchRequestErrorCode {
    InvalidBatchSize,
    DuplicateEventId,
    EventSerialization,
    RequestTooLarge
};

struct BatchRequestError {
    BatchRequestErrorCode code;

    // 与具体事件有关时填写，下标从 0 开始。
    std::optional<std::size_t> event_index;

    // 序列化失败时保留原始错误信息。
    std::optional<EventJsonError> event_error;

    std::string message;
};

using BatchRequestEncodeResult =
    std::variant<BatchRequest, BatchRequestError>;

// 编码 1..100 条事件。
//
// 前置条件：调用方已完成事件业务校验。
// 本函数检查序列化、批次内身份重复及请求体大小。
//
// 不修改事件，不重新生成 session_id 或 sequence。
// 失败时不返回部分请求。
// 本地内存分配失败等资源异常继续向上传递。
BatchRequestEncodeResult encodeBatchRequest(
    const std::vector<SensorEvent>& events
);

} // namespace edge