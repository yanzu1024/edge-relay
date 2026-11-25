#include "edge/upload/BatchRequest.h"
#include "edge/net/HttpJsonPost.h"

#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <variant>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace {

using Json = nlohmann::json;

edge::SensorEvent makeEvent(std::int64_t sequence = 1)
{
    edge::SensorEvent event;

    event.schema_version = 1;
    event.device_id = "device-001";
    event.session_id = "session-001";
    event.sequence = sequence;
    event.observed_at_ms = 1794441600000LL;
    event.metric = "temperature";
    event.value = 25.5;
    event.unit = "degC";

    return event;
}

TEST(BatchRequestTest, EncodesEnvelopeAndStableIds)
{
    const auto result = edge::encodeBatchRequest({
        makeEvent(1),
        makeEvent(2)
    });

    const auto* request =
        std::get_if<edge::BatchRequest>(&result);

    ASSERT_NE(request, nullptr);
    ASSERT_EQ(request->event_ids.size(), 2U);

    EXPECT_EQ(
        request->event_ids[0],
        "device-001/session-001/1"
    );

    EXPECT_EQ(
        request->event_ids[1],
        "device-001/session-001/2"
    );

    const auto document = Json::parse(request->body);

    ASSERT_TRUE(document.is_object());
    EXPECT_EQ(document.size(), 2U);
    EXPECT_EQ(document.at("protocol_version"), 1);

    const auto& items = document.at("events");

    ASSERT_TRUE(items.is_array());
    ASSERT_EQ(items.size(), 2U);

    EXPECT_EQ(items[0].at("event_id"), request->event_ids[0]);
    EXPECT_EQ(items[1].at("event_id"), request->event_ids[1]);

    const auto& event = items[0].at("event");

    // 必须是嵌套对象，不能是包含 JSON 文本的字符串。
    ASSERT_TRUE(event.is_object());
    EXPECT_EQ(event.size(), 8U);

    EXPECT_EQ(event.at("schema_version"), 1);
    EXPECT_EQ(event.at("device_id"), "device-001");
    EXPECT_EQ(event.at("session_id"), "session-001");
    EXPECT_EQ(event.at("sequence"), 1);
    EXPECT_EQ(event.at("observed_at_ms"), 1794441600000LL);
    EXPECT_EQ(event.at("metric"), "temperature");
    EXPECT_DOUBLE_EQ(event.at("value").get<double>(), 25.5);
    EXPECT_EQ(event.at("unit"), "degC");
}

TEST(BatchRequestTest, PreservesIntegerValues)
{
    auto event = makeEvent();

    event.sequence = std::numeric_limits<std::int64_t>::max();
    event.observed_at_ms =
        std::numeric_limits<std::int64_t>::max() - 1;

    const auto result = edge::encodeBatchRequest({event});

    const auto* request =
        std::get_if<edge::BatchRequest>(&result);

    ASSERT_NE(request, nullptr);

    EXPECT_EQ(
        request->event_ids[0],
        "device-001/session-001/9223372036854775807"
    );

    const auto document = Json::parse(request->body);
    const auto& encoded = document.at("events")[0].at("event");

    ASSERT_TRUE(encoded.at("sequence").is_number_integer());
    ASSERT_TRUE(encoded.at("observed_at_ms").is_number_integer());

    EXPECT_EQ(
        encoded.at("sequence").get<std::int64_t>(),
        event.sequence
    );

    EXPECT_EQ(
        encoded.at("observed_at_ms").get<std::int64_t>(),
        event.observed_at_ms
    );
}

TEST(BatchRequestTest, EscapesTextAndNormalizesNegativeZero)
{
    auto event = makeEvent();

    event.metric = "温度 \"A\"\\line\n";
    event.unit = "℃";
    event.value = -0.0;

    const auto result = edge::encodeBatchRequest({event});

    const auto* request =
        std::get_if<edge::BatchRequest>(&result);

    ASSERT_NE(request, nullptr);

    const auto document = Json::parse(request->body);
    const auto& encoded = document.at("events")[0].at("event");

    EXPECT_EQ(
        encoded.at("metric").get<std::string>(),
        event.metric
    );

    EXPECT_EQ(
        encoded.at("unit").get<std::string>(),
        event.unit
    );

    const auto value = encoded.at("value").get<double>();

    EXPECT_DOUBLE_EQ(value, 0.0);
    EXPECT_FALSE(std::signbit(value));

    // 编码过程不能改变调用方的原始事件。
    EXPECT_TRUE(std::signbit(event.value));
}

TEST(BatchRequestTest, AcceptsHundredEvents)
{
    std::vector<edge::SensorEvent> events;

    for (std::int64_t sequence = 1; sequence <= 100; ++sequence) {
        events.push_back(makeEvent(sequence));
    }

    const auto result = edge::encodeBatchRequest(events);

    const auto* request =
        std::get_if<edge::BatchRequest>(&result);

    ASSERT_NE(request, nullptr);
    EXPECT_EQ(request->event_ids.size(), 100U);

    const auto document = Json::parse(request->body);

    EXPECT_EQ(document.at("events").size(), 100U);
    EXPECT_LE(request->body.size(), edge::kMaxHttpRequestBytes);
}

TEST(BatchRequestTest, RejectsInvalidBatchSize)
{
    const auto empty_result = edge::encodeBatchRequest({});

    const auto* empty_error =
        std::get_if<edge::BatchRequestError>(&empty_result);

    ASSERT_NE(empty_error, nullptr);
    EXPECT_EQ(
        empty_error->code,
        edge::BatchRequestErrorCode::InvalidBatchSize
    );
    EXPECT_FALSE(empty_error->event_index.has_value());

    const std::vector<edge::SensorEvent> events(
        101,
        makeEvent()
    );

    const auto large_result = edge::encodeBatchRequest(events);

    const auto* large_error =
        std::get_if<edge::BatchRequestError>(&large_result);

    ASSERT_NE(large_error, nullptr);
    EXPECT_EQ(
        large_error->code,
        edge::BatchRequestErrorCode::InvalidBatchSize
    );
}

TEST(BatchRequestTest, RejectsRepeatedIdentityEvenWithDifferentContent)
{
    for (const bool change_value : {false, true}) {
        const auto first = makeEvent();
        auto second = first;

        if (change_value) {
            second.value = 99.0;
        }

        const auto result = edge::encodeBatchRequest({
            first,
            second
        });

        const auto* error =
            std::get_if<edge::BatchRequestError>(&result);

        ASSERT_NE(error, nullptr);
        EXPECT_EQ(
            error->code,
            edge::BatchRequestErrorCode::DuplicateEventId
        );

        ASSERT_TRUE(error->event_index.has_value());
        EXPECT_EQ(*error->event_index, 1U);
        EXPECT_FALSE(error->event_error.has_value());
    }
}

TEST(BatchRequestTest, PreservesNonFiniteSerializationError)
{
    const std::vector<double> invalid_values = {
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity()
    };

    for (const auto value : invalid_values) {
        auto second = makeEvent(2);
        second.value = value;

        const auto result = edge::encodeBatchRequest({
            makeEvent(1),
            second
        });

        const auto* error =
            std::get_if<edge::BatchRequestError>(&result);

        ASSERT_NE(error, nullptr);
        EXPECT_EQ(
            error->code,
            edge::BatchRequestErrorCode::EventSerialization
        );

        ASSERT_TRUE(error->event_index.has_value());
        EXPECT_EQ(*error->event_index, 1U);

        ASSERT_TRUE(error->event_error.has_value());
        EXPECT_EQ(
            error->event_error->code,
            edge::EventJsonCode::NonFiniteValue
        );
        EXPECT_EQ(error->event_error->field, "value");
    }
}

TEST(BatchRequestTest, PreservesInvalidEncodingError)
{
    auto event = makeEvent();
    event.unit.assign(1, static_cast<char>(0xFF));

    const auto result = edge::encodeBatchRequest({event});

    const auto* error =
        std::get_if<edge::BatchRequestError>(&result);

    ASSERT_NE(error, nullptr);
    EXPECT_EQ(
        error->code,
        edge::BatchRequestErrorCode::EventSerialization
    );

    ASSERT_TRUE(error->event_index.has_value());
    EXPECT_EQ(*error->event_index, 0U);

    ASSERT_TRUE(error->event_error.has_value());
    EXPECT_EQ(
        error->event_error->code,
        edge::EventJsonCode::InvalidTextEncoding
    );
}

TEST(BatchRequestTest, RejectsOversizedRequest)
{
    auto event = makeEvent();

    event.metric.assign(
        edge::kMaxHttpRequestBytes,
        'x'
    );

    const auto result = edge::encodeBatchRequest({event});

    const auto* error =
        std::get_if<edge::BatchRequestError>(&result);

    ASSERT_NE(error, nullptr);
    EXPECT_EQ(
        error->code,
        edge::BatchRequestErrorCode::RequestTooLarge
    );

    ASSERT_TRUE(error->event_index.has_value());
    EXPECT_EQ(*error->event_index, 0U);
}

} // namespace