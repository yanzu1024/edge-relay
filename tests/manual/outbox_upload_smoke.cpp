#include <cstdint>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <variant>

#include "edge/storage/OutboxSchema.h"
#include "edge/storage/OutboxStore.h"
#include "edge/storage/SqliteDatabase.h"
#include "edge/upload/OutboxUploader.h"

namespace {

edge::SensorEvent makeEvent(
    const std::string& session,
    std::int64_t sequence
)
{
    edge::SensorEvent event;

    event.schema_version = 1;
    event.device_id = "device-001";
    event.session_id = session;
    event.sequence = sequence;
    event.observed_at_ms = 1794441600000LL;
    event.metric = "temperature";
    event.value = 25.5;
    event.unit = "degC";

    return event;
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
    if (argc != 4) {
        std::cerr
            << "Usage: edge-outbox-smoke OUTBOX_DB SESSION_ID NOW_MS\n";

        return 2;
    }

    try {
        const auto database_path =
            std::filesystem::absolute(argv[1]).lexically_normal();

        const std::string session = argv[2];
        const std::string timestamp_text = argv[3];

        std::size_t consumed = 0;

        const auto now_ms = static_cast<std::int64_t>(
            std::stoll(timestamp_text, &consumed)
        );

        if (session.empty()
            || consumed != timestamp_text.size()
            || now_ms <= 0) {
            throw std::invalid_argument(
                "Session must be non-empty and timestamp positive"
            );
        }

        std::filesystem::create_directories(
            database_path.parent_path()
        );

        edge::SqliteDatabase database(database_path);
        edge::initializeOutboxSchema(database);

        edge::OutboxStore store(database);

        // 每次运行都准备相同两条事件。
        // 已存在时必须识别为 Duplicate，不能重置已有状态。
        for (std::int64_t sequence = 1; sequence <= 2; ++sequence) {
            const auto result = store.enqueue(
                makeEvent(session, sequence),
                1000
            );

            const auto* inserted =
                std::get_if<edge::OutboxInsertResult>(&result);

            if (inserted == nullptr
                || (inserted->status != edge::OutboxInsertStatus::Inserted
                    && inserted->status
                        != edge::OutboxInsertStatus::Duplicate)) {
                throw std::runtime_error(
                    "Cannot prepare Outbox test event"
                );
            }
        }

        edge::UploadConfig config;

        if (const char* url = std::getenv("EDGE_TEST_UPLOAD_URL")) {
            config.url = url;
        }

        edge::OutboxUploader uploader(
            store,
            config,
            edge::uploadBatch,
            [now_ms]() -> std::int64_t {
                return now_ms;
            }
        );

        const auto report = uploader.runOnce();
        const auto counts = store.stateCounts();

        std::cout
            << "claimed_records: " << report.claimed_records << '\n'
            << "acked_records: " << report.acked_records << '\n'
            << "retry_records: " << report.retry_records << '\n'
            << "dead_records: " << report.dead_records << '\n';

        if (const auto* error =
                std::get_if<edge::HttpRequestError>(&report.failure)) {
            std::cout
                << "failure: request\n"
                << "request_reason: "
                << requestErrorName(error->code)
                << '\n';
        }
        else if (const auto* error =
                     std::get_if<edge::BatchHttpError>(&report.failure)) {
            std::cout
                << "failure: http\n"
                << "http_status: " << error->status_code << '\n';
        }
        else if (
            std::holds_alternative<edge::BatchResponseError>(
                report.failure
            )
        ) {
            std::cout << "failure: protocol\n";
        }
        else {
            std::cout << "failure: none\n";
        }

        std::cout
            << "pending: " << counts.pending << '\n'
            << "sending: " << counts.sending << '\n'
            << "retry: " << counts.retry << '\n'
            << "acked: " << counts.acked << '\n'
            << "dead: " << counts.dead << '\n'
            << "total: " << counts.total << '\n'
            << "unacked: " << counts.unacked << '\n';

        // 失败已可靠安排 RETRY，也属于本次协调正常完成。
        return 0;
    }
    catch (const std::exception& error) {
        std::cerr
            << "local_error: " << error.what() << '\n';

        return 1;
    }
}