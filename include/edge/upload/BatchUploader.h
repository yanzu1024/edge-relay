#pragma once

#include <string>
#include <variant>
#include <vector>

#include "edge/config/AgentConfig.h"
#include "edge/domain/SensorEvent.h"
#include "edge/net/HttpJsonPost.h"
#include "edge/upload/BatchRequest.h"
#include "edge/upload/BatchResponse.h"

namespace edge {

struct BatchHttpError {
    long status_code = 0;
    std::string body;
};

using BatchUploadResult = std::variant<
    BatchResponse,
    BatchRequestError,
    HttpRequestError,
    BatchHttpError,
    BatchResponseError
>;

// 同步执行一次批次上传。
//
// 前置条件：事件已通过业务验证。
// 事件数量不得超过 config.batch_size，且不得超过协议上限。
//
// BatchResponse 表示整份响应合法，其中仍可能包含 Conflict。
// 其他四种结果分别表示编码、请求、HTTP 状态和响应协议错误。
//
// 配置错误、本地资源错误等继续抛出异常。
// 本函数不修改事件，也不负责数据库状态回写。
BatchUploadResult uploadBatch(
    const UploadConfig& config,
    const std::vector<SensorEvent>& events
);

} // namespace edge