#include <exception>
#include <iostream>
#include <limits>
#include <string>
#include <variant>
#include <vector>
#include <cstdlib>

#include "edge/upload/BatchUploader.h"

namespace {

edge::SensorEvent makeEvent(
    const std::string& session_id,
    std::int64_t sequence
)
{
    edge::SensorEvent event;

    event.schema_version = 1;
    event.device_id = "device-001";
    event.session_id = session_id;
    event.sequence = sequence;

    // 验收样例使用固定时间，确保重复运行时事件内容一致。
    event.observed_at_ms = 1794441600000LL;

    event.metric = "temperature";
    event.value = 25.5;
    event.unit = "degC";

    return event;
}

const char* statusName(edge::BatchEventStatus status)
{
    switch (status) {
    case edge::BatchEventStatus::Accepted:
        return "accepted";

    case edge::BatchEventStatus::Duplicate:
        return "duplicate";

    case edge::BatchEventStatus::Conflict:
        return "conflict";
    }

    return "unknown";
}

const char* requestErrorName(edge::HttpRequestErrorCode code)
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
    if (argc < 2 || argc > 3) {
        std::cerr
            << "Usage: edge-batch-smoke SESSION_ID"
            << " [--conflict-first | --invalid-value"
            << " | --batch-limit-one]\n";

        return 2;
    }

    const std::string session_id = argv[1];

    const std::string option =
        argc == 3 ? argv[2] : "";

    if (session_id.empty()
        || (!option.empty()
            && option != "--conflict-first"
            && option != "--invalid-value"
            && option != "--batch-limit-one")) {
        std::cerr << "Invalid arguments.\n";
        return 2;
    }

    try {
        edge::UploadConfig config;

        // 仅用于验收程序指定测试接收端。
        if (const char* url = std::getenv("EDGE_TEST_UPLOAD_URL")) {
            config.url = url;
        }

        std::vector<edge::SensorEvent> events{
            makeEvent(session_id, 1),
            makeEvent(session_id, 2)
        };

        if (option == "--conflict-first") {
            events[0].value = 99.0;
        }
        else if (option == "--invalid-value") {
            events[0].value =
                std::numeric_limits<double>::quiet_NaN();
        }
        else if (option == "--batch-limit-one") {
            config.batch_size = 1;
        }

        const auto result = edge::uploadBatch(config, events);

        if (const auto* response =
                std::get_if<edge::BatchResponse>(&result)) {
            std::size_t confirmable = 0;
            std::size_t conflicts = 0;

            std::cout << "response_valid: yes\n";

            for (const auto& item : response->results) {
                std::cout
                    << item.event_id
                    << ": "
                    << statusName(item.status)
                    << '\n';

                if (item.status == edge::BatchEventStatus::Conflict) {
                    ++conflicts;
                }
                else {
                    ++confirmable;
                }
            }

            std::cout
                << "confirmable: " << confirmable << '\n'
                << "conflicts: " << conflicts << '\n';

            return conflicts == 0 ? 0 : 1;
        }

        if (const auto* error =
                std::get_if<edge::BatchRequestError>(&result)) {
            std::cerr
                << "category: encoding\n"
                << "message: " << error->message << '\n';

            if (error->event_index.has_value()) {
                std::cerr
                    << "event_index: "
                    << *error->event_index
                    << '\n';
            }

            if (error->event_error.has_value()) {
                std::cerr
                    << "event_field: "
                    << error->event_error->field
                    << '\n'
                    << "event_message: "
                    << error->event_error->message
                    << '\n';
            }

            return 1;
        }

        if (const auto* error =
                std::get_if<edge::HttpRequestError>(&result)) {
            std::cerr
                << "category: request\n"
                << "reason: " << requestErrorName(error->code) << '\n'
                << "curl_code: " << error->curl_code << '\n'
                << "message: " << error->message << '\n';

            return 1;
        }

        if (const auto* error =
                std::get_if<edge::BatchHttpError>(&result)) {
            std::cerr
                << "category: http\n"
                << "http_status: " << error->status_code << '\n'
                << "body: " << error->body << '\n';

            return 1;
        }

        const auto& error =
            std::get<edge::BatchResponseError>(result);

        std::cerr
            << "category: protocol\n"
            << "message: " << error.message << '\n';

        return 1;
    }
    catch (const std::exception& error) {
        std::cerr
            << "category: local\n"
            << "message: " << error.what() << '\n';

        return 2;
    }
}