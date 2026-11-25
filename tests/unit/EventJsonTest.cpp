#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <variant>

#include <nlohmann/json.hpp>

#include "edge/domain/EventJson.h"
#include "edge/validation/EventValidator.h"

namespace {

using Json = nlohmann::json;
using Code = edge::EventJsonCode;

// 构造合法的 JSON 对象, 各个测试在这个基础上只修改目标字段, 这被称为“测试夹具”模式, 避免重复编写合法 JSON 的构造代码
Json makeValidJson()
{
    return Json{
        {"schema_version", 1},
        {"device_id", "sim-001"},
        {"session_id", "184ab3d6-d911-44d5-821f-0123456789ab"},
        {"sequence", 1},
        {"observed_at_ms", std::int64_t{1792713600000}},
        {"metric", "temperature"},
        {"value", 23.5},
        {"unit", "degC"}
    };
}

// 检查解析错误的类别、字段和诊断信息, 这是一个封装好的断言函数, 避免在每个测试中重复编写相同的断言代码
void expectParseError(
    std::string_view payload,
    Code expected_code,
    const char* expected_field
)
{
    const auto result = edge::parseSensorEventJson(payload);

    const auto* error = std::get_if<edge::EventJsonError>(&result);

    // 确认返回的是错误对象, 再访问其成员
    ASSERT_NE(error, nullptr);

    // 严谨地检查错误代码、字段和诊断信息, 以确保解析器正确识别错误类型
    EXPECT_EQ(error->code, expected_code);
    EXPECT_EQ(error->field, expected_field);
    EXPECT_FALSE(error->message.empty());
}

// TEST() 宏定义了一个测试用例, 第一个参数是测试套件名, 第二个参数是测试用例名, 这两个参数组合起来唯一标识一个测试用例
TEST(EventJson, ParsesValidEvent)
{
    const auto json = makeValidJson();
    const auto result = edge::parseSensorEventJson(json.dump());

    const auto* event = std::get_if<edge::SensorEvent>(&result);

    ASSERT_NE(event, nullptr);

    // 同时检查全部字段，避免出现解析成功但字段赋值遗漏的问题。
    EXPECT_EQ(event->schema_version, 1);
    EXPECT_EQ(event->device_id, "sim-001");
    EXPECT_EQ(
        event->session_id,
        "184ab3d6-d911-44d5-821f-0123456789ab"
    );
    EXPECT_EQ(event->sequence, 1);
    EXPECT_EQ(event->observed_at_ms, std::int64_t{1792713600000});
    EXPECT_EQ(event->metric, "temperature");
    EXPECT_DOUBLE_EQ(event->value, 23.5);
    EXPECT_EQ(event->unit, "degC");
}

TEST(EventJson, RejectsInvalidJson)
{
    const std::string invalid_payloads[] = {
        "",
        "   ",
        "{",
        R"({"sequence":1,})",
        "{} trailing",
        "NaN"
    };

    for (const auto& payload : invalid_payloads) {
        SCOPED_TRACE(payload);

        expectParseError(payload, Code::InvalidJson, "");
    }
}

TEST(EventJson, RejectsNonObjectPayload)
{
    // 这些输入具有合法 JSON 语法，但顶层不是事件对象。
    const std::string payloads[] = {
        "[]",
        "null",
        "123",
        "true",
        R"("hello")"
    };

    for (const auto& payload : payloads) {
        SCOPED_TRACE(payload);

        expectParseError(payload, Code::ExpectedObject, "");
    }
}

TEST(EventJson, RejectsMissingFields)
{
    const char* fields[] = {
        "schema_version",
        "device_id",
        "session_id",
        "sequence",
        "observed_at_ms",
        "metric",
        "value",
        "unit"
    };

    for (const char* field : fields) {
        SCOPED_TRACE(field);

        auto json = makeValidJson();

        // 每次只删除一个字段，检查能否准确报告该字段缺失。
        json.erase(field);

        expectParseError(
            json.dump(),
            Code::MissingField,
            field
        );
    }
}

TEST(EventJson, RejectsUnknownFields)
{
    const char* unknown_fields[] = {
        "event_id",
        "debug"
    };

    for (const char* field : unknown_fields) {
        SCOPED_TRACE(field);

        auto json = makeValidJson();
        json[field] = "extra";

        expectParseError(
            json.dump(),
            Code::UnknownField,
            field
        );
    }
}

TEST(EventJson, RejectsNonStringFields)
{
    const char* fields[] = {
        "device_id",
        "session_id",
        "metric",
        "unit"
    };

    const Json invalid_values[] = {
        nullptr,
        true,
        7,
        1.5,
        Json::array(),
        Json::object()
    };

    for (const char* field : fields) {
        SCOPED_TRACE(field);

        for (const auto& value : invalid_values) {
            SCOPED_TRACE(value.dump());

            auto json = makeValidJson();
            json[field] = value;

            expectParseError(
                json.dump(),
                Code::InvalidFieldType,
                field
            );
        }
    }
}

TEST(EventJson, RejectsNonIntegerFields)
{
    const char* fields[] = {
        "schema_version",
        "sequence",
        "observed_at_ms"
    };

    const Json invalid_values[] = {
        "1",
        true,
        nullptr,
        1.0,
        Json::array(),
        Json::object()
    };

    for (const char* field : fields) {
        SCOPED_TRACE(field);

        for (const auto& value : invalid_values) {
            SCOPED_TRACE(value.dump());

            auto json = makeValidJson();
            json[field] = value;

            expectParseError(
                json.dump(),
                Code::InvalidFieldType,
                field
            );
        }
    }
}

TEST(EventJson, RejectsOutOfRangeIntegers)
{
    // schema_version 保存为 int32_t，检查上下界之外的整数。
    const std::int64_t invalid_versions[] = {
        static_cast<std::int64_t>(
            std::numeric_limits<std::int32_t>::min()
        ) - 1,

        static_cast<std::int64_t>(
            std::numeric_limits<std::int32_t>::max()
        ) + 1
    };

    for (const auto version : invalid_versions) {
        SCOPED_TRACE(version);

        auto json = makeValidJson();
        json["schema_version"] = version;

        expectParseError(
            json.dump(),
            Code::IntegerOutOfRange,
            "schema_version"
        );
    }

    // 这个值能被 uint64_t 保存，但已经超过 int64_t 上界。
    constexpr std::uint64_t too_large = 9223372036854775808ULL;

    const char* fields[] = {
        "sequence",
        "observed_at_ms"
    };

    for (const char* field : fields) {
        SCOPED_TRACE(field);

        auto json = makeValidJson();
        json[field] = too_large;

        expectParseError(
            json.dump(),
            Code::IntegerOutOfRange,
            field
        );
    }
}

TEST(EventJson, PreservesIntegerBoundaries)
{
    struct BoundaryCase {
        std::int32_t schema_version;
        std::int64_t sequence;
        std::int64_t observed_at_ms;
    };

    const BoundaryCase cases[] = {
        {
            std::numeric_limits<std::int32_t>::min(),
            std::numeric_limits<std::int64_t>::min(),
            std::numeric_limits<std::int64_t>::min()
        },
        {
            std::numeric_limits<std::int32_t>::max(),
            std::numeric_limits<std::int64_t>::max(),
            std::numeric_limits<std::int64_t>::max()
        }
    };

    for (const auto& item : cases) {
        SCOPED_TRACE(item.sequence);

        auto json = makeValidJson();
        json["schema_version"] = item.schema_version;
        json["sequence"] = item.sequence;
        json["observed_at_ms"] = item.observed_at_ms;

        const auto result = edge::parseSensorEventJson(json.dump());
        const auto* event = std::get_if<edge::SensorEvent>(&result);

        ASSERT_NE(event, nullptr);

        // 此处检查类型表示范围和数值保真；业务合法性另行验证。
        EXPECT_EQ(event->schema_version, item.schema_version);
        EXPECT_EQ(event->sequence, item.sequence);
        EXPECT_EQ(event->observed_at_ms, item.observed_at_ms);
    }
}

TEST(EventJson, RejectsNonNumericValue)
{
    const Json invalid_values[] = {
        "23.5",
        true,
        nullptr,
        Json::array(),
        Json::object()
    };

    for (const auto& value : invalid_values) {
        SCOPED_TRACE(value.dump());

        auto json = makeValidJson();
        json["value"] = value;

        expectParseError(
            json.dump(),
            Code::InvalidFieldType,
            "value"
        );
    }
}

TEST(EventJson, AcceptsIntegerAndFloatingValues)
{
    const Json values[] = {
        0,
        -3,
        23.5
    };

    for (const auto& value : values) {
        SCOPED_TRACE(value.dump());

        auto json = makeValidJson();
        json["value"] = value;

        const auto result = edge::parseSensorEventJson(json.dump());
        const auto* event = std::get_if<edge::SensorEvent>(&result);

        ASSERT_NE(event, nullptr);

        EXPECT_DOUBLE_EQ(event->value, value.get<double>());
    }
}

TEST(EventJson, SeparatesParsingFromBusinessValidation)
{
    auto json = makeValidJson();

    // 0 是可表示的整数，因此解析成功；
    // 但协议要求序号从 1 开始，应由业务验证器拒绝。
    json["sequence"] = 0;

    const auto result = edge::parseSensorEventJson(json.dump());
    const auto* event = std::get_if<edge::SensorEvent>(&result);

    ASSERT_NE(event, nullptr);
    EXPECT_EQ(event->sequence, 0);

    constexpr std::int64_t now_ms = 1792713601000;

    const auto error = edge::validateSensorEvent(*event, now_ms);

    ASSERT_TRUE(error.has_value());

    EXPECT_EQ(
        error->code,
        edge::EventValidationCode::InvalidSequence
    );
    EXPECT_EQ(error->field, "sequence");
}


// 直接构造事件，避免序列化测试依赖解析器来生成输入。
edge::SensorEvent makeSerializableEvent()
{
    edge::SensorEvent event;

    event.schema_version = 1;
    event.device_id = "sim-001";
    event.session_id = "184ab3d6-d911-44d5-821f-0123456789ab";
    event.sequence = 1;
    event.observed_at_ms = 1792713600000;
    event.metric = "temperature";
    event.value = 23.5;
    event.unit = "degC";

    return event;
}

TEST(EventJsonSerialization, SerializesProtocolFields)
{
    const auto event = makeSerializableEvent();
    const auto result = edge::serializeSensorEventJson(event);

    const auto* payload = std::get_if<std::string>(&result);

    ASSERT_NE(payload, nullptr);

    // 使用独立构造的预期对象检查字段和值。
    const auto root = Json::parse(*payload);

    EXPECT_EQ(root, makeValidJson());
    EXPECT_EQ(root.size(), 8U);

    EXPECT_TRUE(root.at("schema_version").is_number_integer());
    EXPECT_TRUE(root.at("sequence").is_number_integer());
    EXPECT_TRUE(root.at("observed_at_ms").is_number_integer());

    // 载荷本身不附加换行。
    EXPECT_EQ(payload->find('\n'), std::string::npos);
}

TEST(EventJsonSerialization, RoundTripsEvent)
{
    auto original = makeSerializableEvent();
    original.value = -3.25;

    const auto serialized = edge::serializeSensorEventJson(original);
    const auto* payload = std::get_if<std::string>(&serialized);

    ASSERT_NE(payload, nullptr);

    // 序列化后重新解析，事件字段应保持一致。
    const auto parsed = edge::parseSensorEventJson(*payload);
    const auto* restored = std::get_if<edge::SensorEvent>(&parsed);

    ASSERT_NE(restored, nullptr);

    EXPECT_EQ(restored->schema_version, original.schema_version);
    EXPECT_EQ(restored->device_id, original.device_id);
    EXPECT_EQ(restored->session_id, original.session_id);
    EXPECT_EQ(restored->sequence, original.sequence);
    EXPECT_EQ(restored->observed_at_ms, original.observed_at_ms);
    EXPECT_EQ(restored->metric, original.metric);
    EXPECT_DOUBLE_EQ(restored->value, original.value);
    EXPECT_EQ(restored->unit, original.unit);
}

TEST(EventJsonSerialization, NormalizesSignedZero)
{
    auto event = makeSerializableEvent();

    event.value = 0.0;
    const auto positive_result = edge::serializeSensorEventJson(event);

    event.value = -0.0;
    const auto negative_result = edge::serializeSensorEventJson(event);

    const auto* positive = std::get_if<std::string>(&positive_result);
    const auto* negative = std::get_if<std::string>(&negative_result);

    ASSERT_NE(positive, nullptr);
    ASSERT_NE(negative, nullptr);

    // 正零和负零必须得到完全相同的文本。
    EXPECT_EQ(*positive, *negative);
}

TEST(EventJsonSerialization, RejectsNonFiniteValues)
{
    const double invalid_values[] = {
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity()
    };

    for (const auto value : invalid_values) {
        SCOPED_TRACE(value);

        auto event = makeSerializableEvent();
        event.value = value;

        const auto result = edge::serializeSensorEventJson(event);
        const auto* error = std::get_if<edge::EventJsonError>(&result);

        ASSERT_NE(error, nullptr);

        EXPECT_EQ(error->code, Code::NonFiniteValue);
        EXPECT_EQ(error->field, "value");
        EXPECT_FALSE(error->message.empty());
    }
}

TEST(EventJsonSerialization, RejectsInvalidUtf8)
{
    auto event = makeSerializableEvent();

    // 单独的 0xFF 字节不是合法 UTF-8。
    event.device_id = std::string(1, static_cast<char>(0xFF));

    const auto result = edge::serializeSensorEventJson(event);
    const auto* error = std::get_if<edge::EventJsonError>(&result);

    ASSERT_NE(error, nullptr);

    EXPECT_EQ(error->code, Code::InvalidTextEncoding);
    EXPECT_FALSE(error->message.empty());
}


} // namespace