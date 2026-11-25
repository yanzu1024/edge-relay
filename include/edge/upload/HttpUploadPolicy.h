#pragma once

namespace edge {

enum class HttpUploadAction {
    ParseResponse,
    Retry,
    Block
};

// 只用于已经收到的 HTTP 响应状态。
//
// 网络失败、超时等仍根据 HttpRequestError 处理，
// 不把它们转换成某个 HTTP 状态再调用本函数。
//
// 这是本项目的上传策略，不代表通用 HTTP 客户端规则。
HttpUploadAction classifyUploadHttpStatus(
    long status_code
) noexcept;

} // namespace edge