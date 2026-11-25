#include "protocol/JsonlLineFramer.h"
#include "protocol/JsonlMessageParser.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

namespace {

using Json = nlohmann::json;

using edge::console::JsonlFrame;
using edge::console::JsonlFrameKind;
using edge::console::JsonlLineFramer;
using edge::console::JsonlMessage;
using edge::console::JsonlMessageKind;
using edge::console::JsonlMessageParser;
using edge::console::JsonlParseCode;
using edge::console::JsonlParseError;
using edge::console::JsonlParseResult;

const std::string kRunA(32, 'a');
const std::string kRunB(32, 'b');

Json snapshot(
    const std::string& run_id,
    const std::string& state = "RUNNING"
)
{
    const bool finished =
        state == "STOPPED" || state == "FAILED";

    return {
        {"snapshot_version", 1},
        {"agent_id", "demo-agent"},
        {"run_id", run_id},
        {"process_state", state},
        {"active_connections", 0},
        {"upload", {
            {"finished", finished},
            {"failed", false}
        }},
        {"counters", {
            {"stored_events", 0}
        }}
    };
}

// 测试主要载荷结构，不替代后续完整快照或业务事件模型。
Json message(
    const std::string& type,
    std::uint64_t sequence,
    const std::string& run_id = kRunA
)
{
    Json data = Json::object();

    if (type == "ready") {
        data = {
            {"agent_id", "demo-agent"},
            {"tcp_bind", "127.0.0.1"},
            {"tcp_port", 19090},
            {"events_jsonl", true},
            {"snapshot_version", 1},
            {"limits", {
                {"line_bytes", 65536},
                {"record_rate_per_second", 100}
            }}
        };
    } else if (type == "status") {
        data = snapshot(run_id);
    } else if (type == "record") {
        data = {
            {"event_id", "example-event"},
            {"disposition", "stored"},
            {"event", {
                {"value", 1.25}
            }}
        };
    } else if (type == "stopped") {
        data = {
            {"exit_code", 0},
            {"snapshot", snapshot(run_id, "STOPPED")}
        };
    }

    return {
        {"protocol_version", 1},
        {"type", type},
        {"run_id", run_id},
        {"sequence", sequence},
        {"emitted_at_ms", 1700000000000LL},
        {"data", data}
    };
}

bool accepted(const JsonlParseResult& result)
{
    return std::holds_alternative<JsonlMessage>(result);
}

void expectCode(
    const JsonlParseResult& result,
    JsonlParseCode expected
)
{
    const auto* error = std::get_if<JsonlParseError>(&result);

    ASSERT_NE(error, nullptr);
    EXPECT_EQ(error->code, expected);
}

class JsonlMessageParserTest : public ::testing::Test {
protected:
    JsonlParseResult start(bool events = true)
    {
        auto ready = message("ready", 1);
        ready["data"]["events_jsonl"] = events;

        return parser_.parse(ready.dump());
    }

    JsonlMessageParser parser_;
};

TEST_F(JsonlMessageParserTest, AcceptsLifecycleWithSequenceGaps)
{
    ASSERT_TRUE(accepted(start()));

    EXPECT_TRUE(accepted(
        parser_.parse(message("status", 5).dump())
    ));

    EXPECT_TRUE(accepted(
        parser_.parse(message("record", 9).dump())
    ));

    const auto stopped =
        parser_.parse(message("stopped", 20).dump());

    ASSERT_TRUE(accepted(stopped));
    EXPECT_EQ(
        std::get<JsonlMessage>(stopped).kind,
        JsonlMessageKind::Stopped
    );
}

TEST_F(JsonlMessageParserTest, RequiresReadyWithSequenceOne)
{
    expectCode(
        parser_.parse(message("status", 2).dump()),
        JsonlParseCode::UnexpectedMessage
    );

    expectCode(
        parser_.parse(message("ready", 2).dump()),
        JsonlParseCode::UnexpectedMessage
    );

    EXPECT_TRUE(accepted(start()));
}

TEST_F(JsonlMessageParserTest, RejectsRepeatedReadyAndOldSequences)
{
    ASSERT_TRUE(accepted(start()));

    expectCode(start(), JsonlParseCode::UnexpectedMessage);

    ASSERT_TRUE(accepted(
        parser_.parse(message("status", 5).dump())
    ));

    expectCode(
        parser_.parse(message("status", 5).dump()),
        JsonlParseCode::SequenceOrder
    );

    expectCode(
        parser_.parse(message("status", 4).dump()),
        JsonlParseCode::SequenceOrder
    );

    EXPECT_TRUE(accepted(
        parser_.parse(message("status", 8).dump())
    ));
}

TEST_F(JsonlMessageParserTest, RejectsWrongRunWithoutAdvancingState)
{
    ASSERT_TRUE(accepted(start()));

    expectCode(
        parser_.parse(message("status", 100, kRunB).dump()),
        JsonlParseCode::RunMismatch
    );

    auto bad_snapshot = message("status", 100);
    bad_snapshot["data"]["run_id"] = kRunB;

    expectCode(
        parser_.parse(bad_snapshot.dump()),
        JsonlParseCode::RunMismatch
    );

    EXPECT_TRUE(accepted(
        parser_.parse(message("status", 2).dump())
    ));
}

TEST_F(JsonlMessageParserTest, RejectsUnsupportedVersionsAndTypes)
{
    auto bad = message("ready", 1);
    bad["protocol_version"] = 2;

    expectCode(
        parser_.parse(bad.dump()),
        JsonlParseCode::UnsupportedVersion
    );

    bad = message("ready", 1);
    bad["type"] = "unknown";

    expectCode(
        parser_.parse(bad.dump()),
        JsonlParseCode::UnknownType
    );

    EXPECT_TRUE(accepted(start()));
}

TEST_F(JsonlMessageParserTest, RejectsInvalidIntegerFieldTypes)
{
    const std::vector<Json> invalid_values = {
        0,
        -1,
        true,
        1.0,
        "1",
        nullptr
    };

    for (const auto& value : invalid_values) {
        auto bad = message("ready", 1);
        bad["sequence"] = value;

        EXPECT_FALSE(accepted(parser_.parse(bad.dump())));
    }

    auto bad_time = message("ready", 1);
    bad_time["emitted_at_ms"] =
        std::numeric_limits<std::uint64_t>::max();

    EXPECT_FALSE(accepted(parser_.parse(bad_time.dump())));
    EXPECT_TRUE(accepted(start()));
}

TEST_F(JsonlMessageParserTest, PreservesUint64AndRealSensorValues)
{
    ASSERT_TRUE(accepted(start()));

    const auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    auto status = message("status", maximum);
    status["data"]["counters"]["stored_events"] = maximum;
    status["data"]["sensor_value"] = 1.25;

    const auto result = parser_.parse(status.dump());

    ASSERT_TRUE(accepted(result));

    const auto& parsed = std::get<JsonlMessage>(result);
    const auto& count = parsed.data["counters"]["stored_events"];

    EXPECT_EQ(parsed.sequence, maximum);
    EXPECT_TRUE(count.is_number_unsigned());
    EXPECT_EQ(count.get<std::uint64_t>(), maximum);
    EXPECT_DOUBLE_EQ(
        parsed.data["sensor_value"].get<double>(),
        1.25
    );
}

TEST_F(JsonlMessageParserTest, RejectsOverflowingIntegerLiterals)
{
    ASSERT_TRUE(accepted(start()));

    for (const std::string literal : {
            "18446744073709551616",
            "-9223372036854775809"
        }) {
        auto wire = message("status", 100).dump();

        wire.pop_back();
        wire += ",\"probe\":" + literal + "}";

        expectCode(
            parser_.parse(wire),
            JsonlParseCode::IntegerOutOfRange
        );
    }

    EXPECT_TRUE(accepted(
        parser_.parse(message("status", 2).dump())
    ));
}

TEST_F(JsonlMessageParserTest, RejectsInvalidJsonUtf8AndRawNul)
{
    EXPECT_FALSE(accepted(parser_.parse("")));
    EXPECT_FALSE(accepted(parser_.parse("{")));
    EXPECT_FALSE(accepted(parser_.parse("[]")));

    std::string invalid_utf8 = "{\"name\":\"";
    invalid_utf8.push_back(static_cast<char>(0xff));
    invalid_utf8 += "\"}";

    EXPECT_FALSE(accepted(parser_.parse(invalid_utf8)));

    const auto with_nul =
        message("ready", 1).dump()
        + std::string("\0ignored", 8);

    EXPECT_FALSE(accepted(parser_.parse(with_nul)));

    EXPECT_TRUE(accepted(start()));
}

TEST_F(JsonlMessageParserTest, RejectsDuplicateKeysAndDeepNesting)
{
    auto duplicate = message("ready", 1).dump();
    duplicate.insert(1, "\"sequence\":1,");

    expectCode(
        parser_.parse(duplicate),
        JsonlParseCode::DuplicateKey
    );

    const std::string deep =
        std::string(JsonlMessageParser::kMaxNesting + 1, '[')
        + "0"
        + std::string(JsonlMessageParser::kMaxNesting + 1, ']');

    expectCode(
        parser_.parse(deep),
        JsonlParseCode::NestingTooDeep
    );

    EXPECT_TRUE(accepted(start()));
}

TEST_F(JsonlMessageParserTest, ChecksLineSizeAtTheBoundary)
{
    expectCode(
        parser_.parse(
            std::string(JsonlLineFramer::kMaxLineBytes, ' ')
        ),
        JsonlParseCode::LineTooLarge
    );

    auto ready = message("ready", 1);
    ready["data"]["padding"] = "";

    const auto base_size = ready.dump().size();

    ready["data"]["padding"] = std::string(
        JsonlLineFramer::kMaxPayloadBytes - base_size,
        'x'
    );

    const auto wire = ready.dump();

    ASSERT_EQ(
        wire.size(),
        JsonlLineFramer::kMaxPayloadBytes
    );

    EXPECT_TRUE(accepted(parser_.parse(wire)));
}

TEST_F(JsonlMessageParserTest, ValidatesReadyBeforeAcceptingIdentity)
{
    auto bad = message("ready", 1);
    bad["data"]["limits"]["line_bytes"] = 123;

    expectCode(
        parser_.parse(bad.dump()),
        JsonlParseCode::InvalidField
    );

    auto good = message("ready", 1, kRunB);

    EXPECT_TRUE(accepted(parser_.parse(good.dump())));
}

TEST_F(JsonlMessageParserTest, EnforcesEventSubscription)
{
    ASSERT_TRUE(accepted(start(false)));

    expectCode(
        parser_.parse(message("record", 2).dump()),
        JsonlParseCode::UnexpectedMessage
    );

    EXPECT_TRUE(accepted(
        parser_.parse(message("status", 2).dump())
    ));
}

TEST_F(JsonlMessageParserTest, ValidatesStoppedBeforeClosingStream)
{
    ASSERT_TRUE(accepted(start()));

    auto bad = message("stopped", 2);
    bad["data"]["snapshot"]["active_connections"] = 1;

    expectCode(
        parser_.parse(bad.dump()),
        JsonlParseCode::InvalidField
    );

    EXPECT_TRUE(accepted(
        parser_.parse(message("stopped", 2).dump())
    ));

    expectCode(
        parser_.parse(message("status", 3).dump()),
        JsonlParseCode::UnexpectedMessage
    );

    parser_.reset();

    EXPECT_TRUE(accepted(
        parser_.parse(message("ready", 1, kRunB).dump())
    ));
}

TEST_F(JsonlMessageParserTest, IntegratesWithFragmentedByteInput)
{
    auto status = message("status", 3);
    status["data"]["note"] = u8"设备状态";

    const std::string wire =
        message("ready", 1).dump()
        + "\n{\n"
        + status.dump()
        + "\n";

    JsonlLineFramer framer;

    int valid_messages = 0;
    int rejected_messages = 0;

    const auto sink = [&](JsonlFrame frame) {
        ASSERT_EQ(frame.kind, JsonlFrameKind::Line);

        if (accepted(parser_.parse(frame.bytes))) {
            ++valid_messages;
        } else {
            ++rejected_messages;
        }
    };

    const std::string_view view(wire);

    for (std::size_t offset = 0; offset < view.size(); offset += 3) {
        framer.feed(view.substr(offset, 3), sink);
    }

    framer.finish(sink);

    EXPECT_EQ(valid_messages, 2);
    EXPECT_EQ(rejected_messages, 1);
}

} // namespace
