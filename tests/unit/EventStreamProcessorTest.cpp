#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <stdexcept>

#include "edge/app/EventStreamProcessor.h"
#include "edge/protocol/FrameEncoder.h"

namespace {

using Clock = edge::EventStreamProcessor::Clock;

constexpr std::int64_t kNowMs = 1792713601000;
const Clock::time_point kStart{};

edge::SensorEvent makeEvent(std::int64_t sequence = 1)
{
    edge::SensorEvent event;

    event.schema_version = 1;
    event.device_id = "sim-001";
    event.session_id = "184ab3d6-d911-44d5-821f-0123456789ab";
    event.sequence = sequence;
    event.observed_at_ms = 1792713600000;
    event.metric = "temperature";
    event.value = 23.5;
    event.unit = "degC";

    return event;
}

edge::EncodedFrame makePayloadFrame(std::string_view payload)
{
    return std::get<edge::EncodedFrame>(
        edge::encodeFrame(payload)
    );
}

edge::EncodedFrame makeEventFrame(const edge::SensorEvent& event)
{
    const auto result = edge::serializeSensorEventJson(event);

    return makePayloadFrame(
        std::get<std::string>(result)
    );
}

TEST(EventStreamProcessor, AcceptsValidEvent)
{
    const auto frame = makeEventFrame(makeEvent());

    edge::EventStreamProcessor processor;

    const auto results = processor.feed(
        frame.data(),
        frame.size(),
        kNowMs,
        kStart
    );

    ASSERT_EQ(results.size(), 1U);

    const auto* event =
        std::get_if<edge::SensorEvent>(&results[0]);

    ASSERT_NE(event, nullptr);

    EXPECT_EQ(event->device_id, "sim-001");
    EXPECT_EQ(event->sequence, 1);
    EXPECT_DOUBLE_EQ(event->value, 23.5);

    const auto stats = processor.stats();

    EXPECT_EQ(stats.framing.completed_frames, 1U);
    EXPECT_EQ(stats.valid_events, 1U);
    EXPECT_EQ(stats.json_errors, 0U);
    EXPECT_EQ(stats.validation_errors, 0U);
}

TEST(EventStreamProcessor, ReportsJsonError)
{
    // 帧结构合法，但载荷不是完整 JSON。
    const auto frame = makePayloadFrame("{");

    edge::EventStreamProcessor processor;

    const auto results = processor.feed(
        frame.data(),
        frame.size(),
        kNowMs,
        kStart
    );

    ASSERT_EQ(results.size(), 1U);

    const auto* error =
        std::get_if<edge::EventJsonError>(&results[0]);

    ASSERT_NE(error, nullptr);

    EXPECT_EQ(error->code, edge::EventJsonCode::InvalidJson);

    const auto stats = processor.stats();

    EXPECT_EQ(stats.framing.completed_frames, 1U);
    EXPECT_EQ(stats.json_errors, 1U);
    EXPECT_EQ(stats.valid_events, 0U);
}

TEST(EventStreamProcessor, ReportsValidationError)
{
    // JSON 类型正确，但序号 0 不符合业务规则。
    const auto frame = makeEventFrame(makeEvent(0));

    edge::EventStreamProcessor processor;

    const auto results = processor.feed(
        frame.data(),
        frame.size(),
        kNowMs,
        kStart
    );

    ASSERT_EQ(results.size(), 1U);

    const auto* error =
        std::get_if<edge::EventValidationError>(&results[0]);

    ASSERT_NE(error, nullptr);

    EXPECT_EQ(
        error->code,
        edge::EventValidationCode::InvalidSequence
    );
    EXPECT_EQ(error->field, "sequence");

    const auto stats = processor.stats();

    EXPECT_EQ(stats.validation_errors, 1U);
    EXPECT_EQ(stats.json_errors, 0U);
    EXPECT_EQ(stats.valid_events, 0U);
}

TEST(EventStreamProcessor, ContinuesAfterInvalidEvents)
{
    const edge::EncodedFrame frames[]{
        makeEventFrame(makeEvent(1)),
        makePayloadFrame("{"),
        makeEventFrame(makeEvent(0)),
        makeEventFrame(makeEvent(2))
    };

    edge::EncodedFrame input;

    for (const auto& frame : frames) {
        input.insert(
            input.end(),
            frame.begin(),
            frame.end()
        );
    }

    edge::EventStreamProcessor processor;

    const auto results = processor.feed(
        input.data(),
        input.size(),
        kNowMs,
        kStart
    );

    ASSERT_EQ(results.size(), 4U);

    // 返回结果保留完整帧的原始顺序。
    EXPECT_TRUE(
        std::holds_alternative<edge::SensorEvent>(results[0])
    );
    EXPECT_TRUE(
        std::holds_alternative<edge::EventJsonError>(results[1])
    );
    EXPECT_TRUE(
        std::holds_alternative<edge::EventValidationError>(results[2])
    );

    const auto* last =
        std::get_if<edge::SensorEvent>(&results[3]);

    ASSERT_NE(last, nullptr);
    EXPECT_EQ(last->sequence, 2);

    const auto stats = processor.stats();

    EXPECT_EQ(stats.framing.completed_frames, 4U);
    EXPECT_EQ(stats.valid_events, 2U);
    EXPECT_EQ(stats.json_errors, 1U);
    EXPECT_EQ(stats.validation_errors, 1U);

    // JSON 或业务错误不会被误算为帧重同步丢弃。
    EXPECT_EQ(stats.framing.resync_bytes, 0U);

    EXPECT_EQ(
        stats.framing.completed_frames,
        stats.valid_events
            + stats.json_errors
            + stats.validation_errors
    );
}

TEST(EventStreamProcessor, HandlesFragmentedEvent)
{
    const auto frame = makeEventFrame(makeEvent());

    edge::EventStreamProcessor processor;

    EXPECT_TRUE(
        processor.feed(
            frame.data(),
            5,
            kNowMs,
            kStart
        ).empty()
    );

    EXPECT_EQ(processor.stats().valid_events, 0U);

    const auto results = processor.feed(
        frame.data() + 5,
        frame.size() - 5,
        kNowMs,
        kStart + std::chrono::milliseconds{1}
    );

    ASSERT_EQ(results.size(), 1U);

    EXPECT_TRUE(
        std::holds_alternative<edge::SensorEvent>(results[0])
    );
    EXPECT_EQ(processor.bufferedBytes(), 0U);
    EXPECT_EQ(processor.stats().valid_events, 1U);
}

TEST(EventStreamProcessor, ProcessesFramesRecoveredByTimeout)
{
    // 假帧头挡住后面的合法事件帧。
    edge::EncodedFrame input{
        'E', 'T', 'L', 'M',
        0x00, 0x00, 0x20, 0x00
    };

    const auto valid = makeEventFrame(makeEvent());

    input.insert(
        input.end(),
        valid.begin(),
        valid.end()
    );

    edge::EventStreamProcessor processor;

    EXPECT_TRUE(
        processor.feed(
            input.data(),
            input.size(),
            kNowMs,
            kStart
        ).empty()
    );

    const auto results = processor.checkTimeout(
        kNowMs + 3000,
        kStart + std::chrono::milliseconds{3000}
    );

    ASSERT_EQ(results.size(), 1U);

    EXPECT_TRUE(
        std::holds_alternative<edge::SensorEvent>(results[0])
    );

    const auto stats = processor.stats();

    EXPECT_EQ(stats.valid_events, 1U);
    EXPECT_EQ(stats.framing.completed_frames, 1U);
    EXPECT_EQ(stats.framing.partial_timeouts, 1U);
    EXPECT_EQ(stats.framing.resync_bytes, 8U);
}

TEST(EventStreamProcessor, UsesTimeoutFromTcpConfig)
{
    edge::TcpConfig config;
    config.partial_timeout_ms = 250;

    edge::EventStreamProcessor processor(config);

    const auto frame = makeEventFrame(makeEvent());

    EXPECT_TRUE(
        processor.feed(
            frame.data(),
            5,
            kNowMs,
            kStart
        ).empty()
    );

    EXPECT_TRUE(
        processor.checkTimeout(
            kNowMs + 249,
            kStart + std::chrono::milliseconds{249}
        ).empty()
    );

    EXPECT_EQ(processor.stats().framing.partial_timeouts, 0U);

    EXPECT_TRUE(
        processor.checkTimeout(
            kNowMs + 250,
            kStart + std::chrono::milliseconds{250}
        ).empty()
    );

    EXPECT_EQ(processor.stats().framing.partial_timeouts, 1U);
    EXPECT_EQ(processor.bufferedBytes(), 0U);
}

TEST(EventStreamProcessor, RejectsInvalidTcpPayloadLimit)
{
    edge::TcpConfig config;
    config.max_payload_bytes = -1;

    EXPECT_THROW(
        (void)edge::EventStreamProcessor(config),
        std::invalid_argument
    );

    config.max_payload_bytes = 8193;

    EXPECT_THROW(
        (void)edge::EventStreamProcessor(config),
        std::invalid_argument
    );
}

} // namespace