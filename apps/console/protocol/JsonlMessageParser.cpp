#include "JsonlMessageParser.h"

#include "JsonlLineFramer.h"

#include <limits>
#include <set>
#include <utility>
#include <vector>

namespace edge::console {

namespace {

using Json = nlohmann::json;
using Code = JsonlParseCode;
using Kind = JsonlMessageKind;

// 仅在本文件内部用于结束一次失败的解析。
struct Rejection {
    Code code;
    const char* message;
};

[[noreturn]]
void reject(Code code, const char* message)
{
    throw Rejection{code, message};
}

void require(
    bool condition,
    Code code,
    const char* message
)
{
    if (!condition) {
        reject(code, message);
    }
}

// 在创建 JSON 树之前检查：
// 1. 容器嵌套上限；
// 2. 同一对象中的重复键；
// 3. 超出 int64/uint64 范围的整数。
class InputCheck final : public nlohmann::json_sax<Json> {
public:
    Code error = Code::InvalidJson;

    bool null() override { return true; }

    bool boolean(bool) override { return true; }

    bool number_integer(Json::number_integer_t) override
    {
        return true;
    }

    bool number_unsigned(Json::number_unsigned_t) override
    {
        return true;
    }

    bool number_float(
        Json::number_float_t,
        const Json::string_t& token
    ) override
    {
        // 没有小数点或指数，却进入浮点回调，
        // 表示原始整数无法用库的 64 位整数类型表示。
        if (token.find_first_of(".eE") == std::string::npos) {
            error = Code::IntegerOutOfRange;
            return false;
        }

        return true;
    }

    bool string(Json::string_t&) override { return true; }

    bool binary(Json::binary_t&) override
    {
        return false;
    }

    bool start_object(std::size_t) override
    {
        return enter();
    }

    bool key(Json::string_t& name) override
    {
        if (!levels_.back().insert(name).second) {
            error = Code::DuplicateKey;
            return false;
        }

        return true;
    }

    bool end_object() override
    {
        levels_.pop_back();
        return true;
    }

    bool start_array(std::size_t) override
    {
        return enter();
    }

    bool end_array() override
    {
        levels_.pop_back();
        return true;
    }

    bool parse_error(
        std::size_t,
        const std::string&,
        const Json::exception&
    ) override
    {
        error = Code::InvalidJson;
        return false;
    }

private:
    bool enter()
    {
        if (levels_.size() >= JsonlMessageParser::kMaxNesting) {
            error = Code::NestingTooDeep;
            return false;
        }

        // 每个容器单独保存键集合。
        // 数组不会收到 key 回调，集合保持为空。
        levels_.emplace_back();
        return true;
    }

    std::vector<std::set<std::string>> levels_;
};

const Json& field(const Json& object, const char* name)
{
    require(
        object.is_object(),
        Code::InvalidField,
        "Expected a JSON object"
    );

    const auto found = object.find(name);

    require(
        found != object.end(),
        Code::InvalidField,
        "Required field is missing"
    );

    return *found;
}

std::uint64_t unsignedField(
    const Json& object,
    const char* name
)
{
    const auto& value = field(object, name);

    require(
        value.is_number_unsigned(),
        Code::InvalidField,
        "Expected an unsigned integer field"
    );

    return value.get<std::uint64_t>();
}

const std::string& textField(
    const Json& object,
    const char* name
)
{
    const auto& value = field(object, name);

    require(
        value.is_string(),
        Code::InvalidField,
        "Expected a string field"
    );

    return value.get_ref<const std::string&>();
}

bool boolField(const Json& object, const char* name)
{
    const auto& value = field(object, name);

    require(
        value.is_boolean(),
        Code::InvalidField,
        "Expected a boolean field"
    );

    return value.get<bool>();
}

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

Kind messageKind(const std::string& type)
{
    if (type == "ready") {
        return Kind::Ready;
    }

    if (type == "status") {
        return Kind::Status;
    }

    if (type == "record") {
        return Kind::Record;
    }

    if (type == "stopped") {
        return Kind::Stopped;
    }

    reject(Code::UnknownType, "Unsupported message type");
}

void checkSnapshotIdentity(
    const Json& snapshot,
    const std::string& run_id,
    const std::string& agent_id
)
{
    require(
        unsignedField(snapshot, "snapshot_version") == 1,
        Code::UnsupportedVersion,
        "Unsupported snapshot version"
    );

    require(
        textField(snapshot, "run_id") == run_id,
        Code::RunMismatch,
        "Snapshot run ID does not match its message"
    );

    require(
        textField(snapshot, "agent_id") == agent_id,
        Code::RunMismatch,
        "Snapshot Agent ID changed during the run"
    );
}

void checkPayload(
    Kind kind,
    const Json& data,
    const std::string& run_id,
    const std::string& agent_id,
    bool events_jsonl
)
{
    require(
        data.is_object(),
        Code::InvalidField,
        "Message data must be an object"
    );

    if (kind == Kind::Ready) {
        require(
            !textField(data, "agent_id").empty()
                && !textField(data, "tcp_bind").empty(),
            Code::InvalidField,
            "Ready requires Agent identity and TCP address"
        );

        const auto port = unsignedField(data, "tcp_port");

        require(
            port >= 1 && port <= 65535,
            Code::InvalidField,
            "Invalid TCP port in ready"
        );

        (void)boolField(data, "events_jsonl");

        require(
            unsignedField(data, "snapshot_version") == 1,
            Code::UnsupportedVersion,
            "Unsupported snapshot version"
        );

        const auto& limits = field(data, "limits");

        require(
            unsignedField(limits, "line_bytes")
                == JsonlLineFramer::kMaxLineBytes
                && unsignedField(limits, "record_rate_per_second")
                    == 100,
            Code::InvalidField,
            "Ready limits disagree with protocol version 1"
        );

        return;
    }

    if (kind == Kind::Status) {
        checkSnapshotIdentity(data, run_id, agent_id);
        return;
    }

    if (kind == Kind::Record) {
        require(
            events_jsonl,
            Code::UnexpectedMessage,
            "Record received without an event subscription"
        );

        require(
            !textField(data, "event_id").empty()
                && field(data, "event").is_object(),
            Code::InvalidField,
            "Record requires event identity and an event object"
        );

        const auto& disposition = textField(data, "disposition");

        require(
            disposition == "stored"
                || disposition == "duplicate"
                || disposition == "identity_conflict"
                || disposition == "capacity_rejected"
                || disposition == "storage_skipped"
                || disposition == "storage_error",
            Code::InvalidField,
            "Unknown record disposition"
        );

        return;
    }

    const auto exit_code = unsignedField(data, "exit_code");

    require(
        exit_code <= 1,
        Code::InvalidField,
        "Unsupported Agent runtime exit code"
    );

    const auto& snapshot = field(data, "snapshot");

    checkSnapshotIdentity(snapshot, run_id, agent_id);

    require(
        textField(snapshot, "process_state")
            == (exit_code == 0 ? "STOPPED" : "FAILED"),
        Code::InvalidField,
        "Exit code and final process state disagree"
    );

    require(
        unsignedField(snapshot, "active_connections") == 0,
        Code::InvalidField,
        "Stopped snapshot contains active connections"
    );

    const auto& upload = field(snapshot, "upload");
    const bool finished = boolField(upload, "finished");
    const bool failed = boolField(upload, "failed");

    require(
        finished && (exit_code != 0 || !failed),
        Code::InvalidField,
        "Stopped snapshot has an inconsistent upload result"
    );
}

} // namespace

JsonlParseResult JsonlMessageParser::parse(std::string_view line)
{
    try {
        require(
            line.size() <= JsonlLineFramer::kMaxPayloadBytes,
            Code::LineTooLarge,
            "JSONL content exceeds the line limit"
        );

        // 显式拒绝原始 NUL，避免某些库版本将它视为输入结束。
        require(
            !line.empty()
                && line.find('\0') == std::string_view::npos
                && line.find('\n') == std::string_view::npos,
            Code::InvalidJson,
            "Expected one complete JSON line without LF or raw NUL"
        );

        InputCheck input_check;

        // 先完成检查，再读取检查器写入的错误码。
        const bool input_valid = Json::sax_parse(
            line.begin(),
            line.end(),
            &input_check
        );

        require(
            input_valid,
            input_check.error,
            "JSON syntax, nesting, keys or integer range rejected"
        );

        Json document = Json::parse(line.begin(), line.end());

        const auto version =
            unsignedField(document, "protocol_version");

        require(
            version == 1,
            Code::UnsupportedVersion,
            "Unsupported JSONL protocol version"
        );

        const auto kind = messageKind(textField(document, "type"));

        const std::string run_id = textField(document, "run_id");
        const auto sequence = unsignedField(document, "sequence");
        const auto emitted = unsignedField(document, "emitted_at_ms");

        require(
            validRunId(run_id)
                && sequence != 0
                && emitted != 0
                && emitted <= static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max()
                ),
            Code::InvalidField,
            "Invalid run identity, sequence or timestamp"
        );

        require(
            !stopped_,
            Code::UnexpectedMessage,
            "Message received after stopped"
        );

        if (run_id_.empty()) {
            require(
                kind == Kind::Ready && sequence == 1,
                Code::UnexpectedMessage,
                "The first accepted message must be ready with sequence 1"
            );
        } else {
            require(
                run_id == run_id_,
                Code::RunMismatch,
                "Message belongs to another run"
            );

            require(
                kind != Kind::Ready,
                Code::UnexpectedMessage,
                "Repeated ready message"
            );

            // 只比较大小，不计算 last_sequence_ + 1，
            // 从而允许间隔，并避免最大 uint64 的加法溢出。
            require(
                sequence > last_sequence_,
                Code::SequenceOrder,
                "Sequence must be strictly increasing"
            );
        }

        const auto& data = field(document, "data");

        checkPayload(
            kind,
            data,
            run_id,
            agent_id_,
            events_jsonl_
        );

        // 先准备所有可能分配内存的数据，再提交接收状态。
        std::string next_run;
        std::string next_agent;
        bool next_events = events_jsonl_;

        if (kind == Kind::Ready) {
            next_run = run_id;
            next_agent = textField(data, "agent_id");
            next_events = boolField(data, "events_jsonl");
        }

        JsonlParseResult result{
            JsonlMessage{
                kind,
                run_id,
                sequence,
                static_cast<std::int64_t>(emitted),
                std::move(document.at("data"))
            }
        };

        if (kind == Kind::Ready) {
            run_id_.swap(next_run);
            agent_id_.swap(next_agent);
            events_jsonl_ = next_events;
        }

        last_sequence_ = sequence;
        stopped_ = kind == Kind::Stopped;

        return result;
    }
    catch (const Rejection& error) {
        return JsonlParseError{
            error.code,
            error.message
        };
    }
    catch (const Json::exception&) {
        return JsonlParseError{
            Code::InvalidJson,
            "JSON input could not be parsed"
        };
    }
}

void JsonlMessageParser::reset() noexcept
{
    run_id_.clear();
    agent_id_.clear();

    last_sequence_ = 0;
    events_jsonl_ = false;
    stopped_ = false;
}

} // namespace edge::console
