#include "edge/net/CurlRuntime.h"

#include <stdexcept>
#include <string>

#include <curl/curl.h>

namespace edge {

namespace {

class CurlGlobalState final {
public:
    CurlGlobalState()
    {
        const auto result = curl_global_init(CURL_GLOBAL_ALL);

        if (result != CURLE_OK) {
            // 初始化失败时，只记录返回码，
            // 不继续调用其他 libcurl 接口。
            throw std::runtime_error(
                std::string{"curl_global_init failed, code="}
                + std::to_string(static_cast<int>(result))
            );
        }
    }

    ~CurlGlobalState() noexcept
    {
        curl_global_cleanup();
    }

    CurlGlobalState(const CurlGlobalState&) = delete;
    CurlGlobalState& operator=(const CurlGlobalState&) = delete;

    CurlGlobalState(CurlGlobalState&&) = delete;
    CurlGlobalState& operator=(CurlGlobalState&&) = delete;
};

} // namespace

void ensureCurlInitialized()
{
    // 首次执行到这里时构造，成功后持续存活至正常退出。
    // 如果构造抛出异常，不会留下“已初始化”的假状态。
    static const CurlGlobalState state;

    // 明确表示该对象用于管理生命周期。
    (void)state;
}

} // namespace edge