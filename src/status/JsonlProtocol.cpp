#include "edge/status/JsonlProtocol.h"

#include "edge/domain/EventId.h"
#include "edge/domain/EventJson.h"

#include <nlohmann/json.hpp>

#include <stdexcept>
#include <string_view>
#include <utility>

namespace edge {

namespace {

using Json = nlohmann::json;

bool validRunId(const std::string& value)
{
    if (value.size() != 32) {
        return false;
    }

    for (const char character : value) {
        const bool digit =
            character >= '0' && character <= '9';

        const bool hexadecimal =
            character >= 'a' && character <= 'f';

        if (!digit && !hexadecimal) {
            return false;
        }
    }

    return true;
}

const char* dispositionName(JsonlRecordDisposition disposition)
{
    switch (disposition) {
    case JsonlRecordDisposition::Stored:
        return "stored";

    case JsonlRecordDisposition::Duplicate:
        return "duplicate";

    case JsonlRecordDisposition::IdentityConflict:
        return "identity_conflict";

    case JsonlRecordDisposition::CapacityRejected:
        return "capacity_rejected";

    case JsonlRecordDisposition::StorageSkipped:
        return "storage_skipped";

    case JsonlRecordDisposition::StorageError:
        return "storage_error";

    case JsonlRecordDisposition::Unknown:
        break;
    }

    throw std::invalid_argument(
        "Unknown JSONL record disposition"
    );
}

template <typename PayloadBuilder>
JsonlEncodeResult encodeLine(
    const JsonlMessageMeta& meta,
    std::string_view type,
    PayloadBuilder build_payload
)
{
    if (!validRunId(meta.run_id)
        || meta.sequence == 0
        || meta.emitted_at_ms <= 0) {
        return JsonlEncodeError{
            JsonlEncodeCode::InvalidMetadata,
            "JSONL requires a valid run ID, sequence and timestamp"
        };
    }

    if ((type == "ready" && meta.sequence != 1)
        || (type != "ready" && meta.sequence < 2)) {
        return JsonlEncodeError{
            JsonlEncodeCode::InvalidMetadata,
            "Ready must use sequence 1; other messages must follow it"
        };
    }

    try {
        Json message = {
            {"protocol_version", kJsonlProtocolVersion},
            {"type", std::string(type)},
            {"run_id", meta.run_id},
            {"sequence", meta.sequence},
            {"emitted_at_ms", meta.emitted_at_ms},
            {"data", build_payload()}
        };

        std::string line = message.dump();

        // 尚未追加换行，所以 size 必须严格小于上限。
        // 使用字节数，不使用字符数量。
        if (line.size() >= kMaxJsonlLineBytes) {
            return JsonlEncodeError{
                JsonlEncodeCode::LineTooLarge,
                "JSONL line exceeds 64 KiB including newline"
            };
        }

        line.push_back('\n');
        return line;
    }
    catch (const Json::type_error&) {
        return JsonlEncodeError{
            JsonlEncodeCode::InvalidTextEncoding,
            "JSONL payload cannot be encoded as valid JSON text"
        };
    }
    catch (const Json::exception&) {
        return JsonlEncodeError{
            JsonlEncodeCode::InvalidPayload,
            "JSONL payload contains invalid JSON data"
        };
    }
    catch (const std::invalid_argument& error) {
        return JsonlEncodeError{
            JsonlEncodeCode::InvalidPayload,
            error.what()
        };
    }
}

} // namespace

JsonlEncodeResult encodeJsonlReady(
    const JsonlMessageMeta& meta,
    const JsonlReadyData& ready
)
{
    return encodeLine(meta, "ready", [&]() -> Json {
        if (ready.agent_id.empty()
            || ready.tcp_bind.empty()
            || ready.tcp_port < 1
            || ready.tcp_port > 65535) {
            throw std::invalid_argument(
                "Invalid JSONL ready data"
            );
        }

        return {
            {"agent_id", ready.agent_id},
            {"tcp_bind", ready.tcp_bind},
            {"tcp_port", ready.tcp_port},
            {"events_jsonl", ready.events_jsonl},
            {"snapshot_version", kStatusSnapshotVersion},
            {"limits", {
                {"line_bytes", kMaxJsonlLineBytes},
                {"record_rate_per_second",
                    kMaxJsonlRecordsPerSecond}
            }}
        };
    });
}

JsonlEncodeResult encodeJsonlStatus(
    const JsonlMessageMeta& meta,
    const StatusSnapshot& snapshot,
    const StatusConfig& config
)
{
    return encodeLine(meta, "status", [&]() -> Json {
        // 复用已经验收的快照序列化，避免维护两套字段映射。
        return Json::parse(
            serializeStatusSnapshot(snapshot, config)
        );
    });
}

JsonlEncodeResult encodeJsonlRecord(
    const JsonlMessageMeta& meta,
    const JsonlRecordData& record
)
{
    return encodeLine(meta, "record", [&]() -> Json {
        const char* disposition =
            dispositionName(record.disposition);

        auto serialized = serializeSensorEventJson(record.event);

        const auto* payload =
            std::get_if<std::string>(&serialized);

        if (payload == nullptr) {
            throw std::invalid_argument(
                "Cannot serialize event for JSONL record"
            );
        }

        return {
            {"event_id", makeEventId(record.event)},
            {"disposition", disposition},
            {"event", Json::parse(*payload)}
        };
    });
}

JsonlEncodeResult encodeJsonlStopped(
    const JsonlMessageMeta& meta,
    int exit_code,
    const StatusSnapshot& snapshot,
    const StatusConfig& config
)
{
    return encodeLine(meta, "stopped", [&]() -> Json {
        if (exit_code != 0 && exit_code != 1) {
            throw std::invalid_argument(
                "Unsupported Agent runtime exit code"
            );
        }

        const auto expected_state = exit_code == 0
            ? StatusProcessState::Stopped
            : StatusProcessState::Failed;

        if (snapshot.process_state != expected_state) {
            throw std::invalid_argument(
                "Exit code and final process state disagree"
            );
        }

        if (!snapshot.active_connections.has_value()
            || *snapshot.active_connections != 0
            || !snapshot.upload.has_value()
            || !snapshot.upload->finished) {
            throw std::invalid_argument(
                "Stopped message requires closed connections "
                "and a finished upload worker"
            );
        }

        if (exit_code == 0 && snapshot.upload->failed) {
            throw std::invalid_argument(
                "Successful stop cannot contain upload worker failure"
            );
        }

        return {
            {"exit_code", exit_code},
            {"snapshot", Json::parse(
                serializeStatusSnapshot(snapshot, config)
            )}
        };
    });
}

} // namespace edge