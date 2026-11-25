#include <exception>
#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <variant>
#include <cstdlib>

#include "edge/net/HttpJsonPost.h"

namespace {

const char* errorName(edge::HttpRequestErrorCode code)
{
    switch (code) {
    case edge::HttpRequestErrorCode::Timeout:
        return "timeout";

    case edge::HttpRequestErrorCode::Transport:
        return "transport";

    case edge::HttpRequestErrorCode::ResponseTooLarge:
        return "response_too_large";
    }

    return "unknown";
}

} // namespace

int main(int argc, char* argv[])
{
    if (argc < 2
        || argc > 3
        || (argc == 3
            && std::string{argv[2]} != "--tiny-response")) {
        std::cerr
            << "Usage: edge-http-smoke REQUEST_JSON"
            << " [--tiny-response]\n";

        return 2;
    }

    try {
        std::ifstream input(argv[1], std::ios::binary);

        if (!input) {
            throw std::runtime_error(
                "Cannot open request JSON file."
            );
        }

        const std::string body(
            (std::istreambuf_iterator<char>(input)),
            std::istreambuf_iterator<char>{}
        );

        if (input.bad()) {
            throw std::runtime_error(
                "Cannot read request JSON file."
            );
        }

        edge::UploadConfig config;

        // 自动测试会在子进程环境中设置实际接收端地址。
        // 必须修改下面 postJson 使用的这个 config 对象。
        if (const char* url = std::getenv("EDGE_TEST_UPLOAD_URL")) {
            config.url = url;
        }

        const std::size_t response_limit =
            argc == 3
            ? 8
            : edge::kMaxHttpResponseBytes;

        const auto result = edge::postJson(
            config,
            body,
            response_limit
        );

        if (const auto* response =
                std::get_if<edge::HttpResponse>(&result)) {
            std::cout
                << "transfer_complete: yes\n"
                << "http_status: " << response->status_code << '\n'
                << "body: " << response->body << '\n';

            return response->status_code == 200 ? 0 : 1;
        }

        const auto& error =
            std::get<edge::HttpRequestError>(result);

        std::cerr
            << "transfer_complete: no\n"
            << "error: " << errorName(error.code) << '\n'
            << "curl_code: " << error.curl_code << '\n'
            << "message: " << error.message << '\n';

        return 1;
    }
    catch (const std::exception& error) {
        std::cerr
            << "local_error: " << error.what() << '\n';

        return 2;
    }
}