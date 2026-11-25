#include "edge/net/HttpJsonPost.h"

#include <array>
#include <exception>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <utility>

#include <curl/curl.h>

#include "edge/config/ConfigValidator.h"
#include "edge/net/CurlRuntime.h"

namespace edge {

namespace {

using CurlHandle =
    std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>;

using CurlHeaders =
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)>;

struct ResponseBuffer {
    std::string body;
    std::size_t limit = 0;

    bool too_large = false;
    std::exception_ptr exception;
};

void checkCurl(CURLcode code, const char* operation)
{
    if (code != CURLE_OK) {
        throw std::runtime_error(
            std::string{operation}
            + ": "
            + curl_easy_strerror(code)
        );
    }
}

template<typename Value>
void setOption(
    CURL* handle,
    CURLoption option,
    Value value
)
{
    checkCurl(
        curl_easy_setopt(handle, option, value),
        "curl_easy_setopt"
    );
}

void appendHeader(
    CurlHeaders& headers,
    const char* text
)
{
    // 失败时原链表仍然有效，不能直接覆盖原指针。
    auto* updated = curl_slist_append(headers.get(), text);

    if (!updated) {
        throw std::bad_alloc{};
    }

    // updated 管理的仍是完整链表，重新交给唯一所有者。
    (void)headers.release();
    headers.reset(updated);
}

std::size_t receiveBody(
    char* data,
    std::size_t size,
    std::size_t count,
    void* user_data
) noexcept
{
    auto& buffer = *static_cast<ResponseBuffer*>(user_data);

    // 防止 size * count 发生整数溢出。
    if (size != 0
        && count > std::numeric_limits<std::size_t>::max() / size) {
        buffer.too_large = true;
        return 0;
    }

    const auto bytes = size * count;

    if (bytes == 0) {
        return 0;
    }

    // 使用减法检查剩余容量，避免累加溢出。
    if (bytes > buffer.limit - buffer.body.size()) {
        buffer.too_large = true;
        return 0;
    }

    try {
        // curl 提供的是字节块，不保证末尾有 '\0'。
        buffer.body.append(data, bytes);
    }
    catch (...) {
        // 不允许 C++ 异常穿过 C 回调边界。
        // 暂存异常，等 curl_easy_perform 返回后再抛出。
        buffer.exception = std::current_exception();
        return 0;
    }

    return bytes;
}

} // namespace

HttpPostResult postJson(
    const UploadConfig& config,
    const std::string& json_body,
    std::size_t max_response_bytes
)
{
    if (validateUploadConfig(config)) {
        throw std::invalid_argument(
            "Invalid upload configuration."
        );
    }

    if (json_body.empty()
        || json_body.size() > kMaxHttpRequestBytes) {
        throw std::invalid_argument(
            "HTTP request body must contain 1..2097152 bytes."
        );
    }

    if (max_response_bytes == 0
        || max_response_bytes > kMaxHttpResponseBytes) {
        throw std::invalid_argument(
            "HTTP response limit must be 1..1048576 bytes."
        );
    }

    ensureCurlInitialized();

    // 这些对象必须比 curl 请求句柄活得更久。
    ResponseBuffer response;
    response.limit = max_response_bytes;

    std::array<char, CURL_ERROR_SIZE> error_buffer{};

    CurlHeaders headers(
        nullptr,
        &curl_slist_free_all
    );

    appendHeader(headers, "Content-Type: application/json");
    appendHeader(headers, "Accept: application/json");

    // 禁用 Expect: 100-continue，简化模拟接收端交互。
    appendHeader(headers, "Expect:");

    // 最后构造，因此退出作用域时最先释放。
    CurlHandle handle(
        curl_easy_init(),
        &curl_easy_cleanup
    );

    if (!handle) {
        throw std::bad_alloc{};
    }

    setOption(
        handle.get(),
        CURLOPT_ERRORBUFFER,
        error_buffer.data()
    );

    setOption(
        handle.get(),
        CURLOPT_URL,
        config.url.c_str()
    );

    // 兼容工程要求的 libcurl 7.62。
#if LIBCURL_VERSION_NUM >= 0x075500
    setOption(
        handle.get(),
        CURLOPT_PROTOCOLS_STR,
        "http,https"
    );
#else
    setOption(
        handle.get(),
        CURLOPT_PROTOCOLS,
        static_cast<long>(CURLPROTO_HTTP | CURLPROTO_HTTPS)
    );
#endif

    setOption(handle.get(), CURLOPT_POST, 1L);

    setOption(
        handle.get(),
        CURLOPT_POSTFIELDSIZE_LARGE,
        static_cast<curl_off_t>(json_body.size())
    );

    // 借用 json_body 的内存，直到同步请求结束。
    setOption(
        handle.get(),
        CURLOPT_POSTFIELDS,
        json_body.c_str()
    );

    setOption(
        handle.get(),
        CURLOPT_HTTPHEADER,
        headers.get()
    );

    setOption(
        handle.get(),
        CURLOPT_CONNECTTIMEOUT_MS,
        static_cast<long>(config.connect_timeout_ms)
    );

    setOption(
        handle.get(),
        CURLOPT_TIMEOUT_MS,
        static_cast<long>(config.request_timeout_ms)
    );

    setOption(handle.get(), CURLOPT_NOSIGNAL, 1L);
    setOption(handle.get(), CURLOPT_FOLLOWLOCATION, 0L);

    // 保留 HTTP 错误响应，让上层检查状态码和响应体。
    setOption(handle.get(), CURLOPT_FAILONERROR, 0L);

    // 显式转换为 curl 要求的回调函数指针类型。
    curl_write_callback callback = &receiveBody;

    setOption(
        handle.get(),
        CURLOPT_WRITEFUNCTION,
        callback
    );

    setOption(
        handle.get(),
        CURLOPT_WRITEDATA,
        static_cast<void*>(&response)
    );

    const auto result = curl_easy_perform(handle.get());

    if (response.exception) {
        std::rethrow_exception(response.exception);
    }

    if (response.too_large) {
        return HttpRequestError{
            HttpRequestErrorCode::ResponseTooLarge,
            static_cast<int>(result),
            "HTTP response body exceeded the configured limit."
        };
    }

    if (result != CURLE_OK) {
        const auto code =
            result == CURLE_OPERATION_TIMEDOUT
            ? HttpRequestErrorCode::Timeout
            : HttpRequestErrorCode::Transport;

        const std::string message =
            error_buffer[0] != '\0'
            ? std::string{error_buffer.data()}
            : std::string{curl_easy_strerror(result)};

        return HttpRequestError{
            code,
            static_cast<int>(result),
            message
        };
    }

    long status_code = 0;

    checkCurl(
        curl_easy_getinfo(
            handle.get(),
            CURLINFO_RESPONSE_CODE,
            &status_code
        ),
        "curl_easy_getinfo"
    );

    return HttpResponse{
        status_code,
        std::move(response.body)
    };
}

} // namespace edge