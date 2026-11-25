#pragma once

#include <cstddef>
#include <string>
#include <variant>

#include "edge/config/AgentConfig.h"

namespace edge {

inline constexpr std::size_t kMaxHttpRequestBytes =
    2 * 1024 * 1024;

inline constexpr std::size_t kMaxHttpResponseBytes =
    1024 * 1024;

// 只有完整完成传输后，才返回这个类型。
// 这里也包括 HTTP 400、503 等错误状态响应。
struct HttpResponse {
    long status_code = 0;
    std::string body;
};

enum class HttpRequestErrorCode {
    Timeout,
    Transport,
    ResponseTooLarge
};

struct HttpRequestError {
    HttpRequestErrorCode code;
    int curl_code = 0;
    std::string message;
};

using HttpPostResult =
    std::variant<HttpResponse, HttpRequestError>;

// 同步发送一次 JSON POST。
//
// json_body 由调用方提供；本函数不解析其中的 JSON。
// 不自动重试，不自动跟随重定向。
//
// max_response_bytes 可以调小用于测试，不能超过默认上限。
//
// 配置、参数、初始化、选项设置及本地资源错误抛出异常。
// 请求期间的超时、传输失败、响应超限通过结果返回。
HttpPostResult postJson(
    const UploadConfig& config,
    const std::string& json_body,
    std::size_t max_response_bytes = kMaxHttpResponseBytes
);

} // namespace edge