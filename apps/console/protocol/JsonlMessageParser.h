#pragma once

#include <nlohmann/json.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

namespace edge::console {

enum class JsonlMessageKind {
    Ready,
    Status,
    Record,
    Stopped
};

struct JsonlMessage {
    JsonlMessageKind kind;
    std::string run_id;
    std::uint64_t sequence;
    std::int64_t emitted_at_ms;

    nlohmann::json data;
};

enum class JsonlParseCode {
    InvalidJson,
    InvalidField,
    UnsupportedVersion,
    UnknownType,
    UnexpectedMessage,
    RunMismatch,
    SequenceOrder,
    LineTooLarge,
    IntegerOutOfRange,
    NestingTooDeep,
    DuplicateKey
};

struct JsonlParseError {
    JsonlParseCode code;
    std::string message;
};

using JsonlParseResult = std::variant<
    JsonlMessage,
    JsonlParseError
>;

class JsonlMessageParser {
public:
    static constexpr std::size_t kMaxNesting = 64;

    [[nodiscard]]
    JsonlParseResult parse(std::string_view line);

    void reset() noexcept;

private:
    std::string run_id_;
    std::string agent_id_;

    std::uint64_t last_sequence_ = 0;
    bool events_jsonl_ = false;
    bool stopped_ = false;
};

} // namespace edge::console
