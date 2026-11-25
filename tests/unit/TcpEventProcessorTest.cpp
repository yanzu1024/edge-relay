#include "edge/app/TcpEventProcessor.h"
#include "edge/protocol/FrameEncoder.h"
#include "edge/protocol/FrameFormat.h"

#include <chrono>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace {

class TcpEventProcessorTest : public ::testing::Test {
protected:
    using TimePoint = edge::TcpEventProcessor::TimePoint;

    static constexpr std::int64_t kNowMs = 1700000000000;

    static edge::TcpConfig makeConfig()
    {
        edge::TcpConfig config;
        config.max_connections = 2;
        config.partial_timeout_ms = 100;
        return config;
    }

    static edge::EncodedFrame payloadFrame(
        const std::string& payload
    )
    {
        auto result = edge::encodeFrame(payload);

        if (auto* frame =
                std::get_if<edge::EncodedFrame>(&result)) {
            return std::move(*frame);
        }

        throw std::runtime_error(
            "test payload could not be encoded"
        );
    }

    static edge::EncodedFrame eventFrame(
        std::int64_t sequence,
        const std::string& device = "sensor_a"
    )
    {
        const nlohmann::json payload{
            {"schema_version", 1},
            {"device_id", device},
            {"session_id", "12345678-1234-1234-1234-123456789abc"},
            {"sequence", sequence},
            {"observed_at_ms", kNowMs},
            {"metric", "temperature"},
            {"value", 23.5},
            {"unit", "degC"}
        };

        return payloadFrame(payload.dump());
    }

    static edge::TcpIoEvent ioEvent(
        edge::TcpIoKind kind,
        std::size_t slot,
        edge::EncodedFrame bytes = {}
    )
    {
        return {kind, slot, std::move(bytes), 0};
    }

    static edge::TcpEventBatch apply(
        edge::TcpEventProcessor& processor,
        std::vector<edge::TcpIoEvent> events,
        TimePoint time = TimePoint{}
    )
    {
        edge::TcpPollBatch io;
        io.events = std::move(events);

        return processor.consume(io, kNowMs, time);
    }
};

} // namespace

TEST_F(TcpEventProcessorTest, KeepsPartialFramesSeparate)
{
    edge::TcpEventProcessor processor{makeConfig()};

    const auto first = eventFrame(1, "sensor_a");
    const auto second = eventFrame(2, "sensor_b");

    const auto cut = first.size() / 2;

    edge::EncodedFrame prefix(
        first.begin(),
        first.begin() + cut
    );
    edge::EncodedFrame suffix(
        first.begin() + cut,
        first.end()
    );

    const auto initial = apply(processor, {
        ioEvent(edge::TcpIoKind::Accepted, 0),
        ioEvent(edge::TcpIoKind::Accepted, 1),
        ioEvent(edge::TcpIoKind::Data, 0, prefix),
        ioEvent(edge::TcpIoKind::Data, 1, second)
    });

    ASSERT_EQ(initial.size(), 1u);
    EXPECT_EQ(initial.front().slot, 1u);

    const auto* second_event =
        std::get_if<edge::SensorEvent>(
            &initial.front().result
        );

    ASSERT_NE(second_event, nullptr);
    EXPECT_EQ(second_event->device_id, "sensor_b");
    EXPECT_GT(processor.bufferedBytes(0), 0u);

    const auto completed = apply(processor, {
        ioEvent(edge::TcpIoKind::Data, 0, suffix)
    });

    ASSERT_EQ(completed.size(), 1u);
    EXPECT_EQ(completed.front().slot, 0u);

    const auto* first_event =
        std::get_if<edge::SensorEvent>(
            &completed.front().result
        );

    ASSERT_NE(first_event, nullptr);
    EXPECT_EQ(first_event->device_id, "sensor_a");

    EXPECT_NE(
        initial.front().connection_id,
        completed.front().connection_id
    );

    EXPECT_EQ(processor.stats().valid_events, 2u);
}

TEST_F(TcpEventProcessorTest, ReusedSlotStartsWithFreshState)
{
    edge::TcpEventProcessor processor{makeConfig()};

    auto partial = eventFrame(2);
    partial.resize(partial.size() / 2);

    const auto old_results = apply(processor, {
        ioEvent(edge::TcpIoKind::Accepted, 0),
        ioEvent(edge::TcpIoKind::Data, 0, eventFrame(1)),
        ioEvent(edge::TcpIoKind::Data, 0, partial)
    });

    ASSERT_EQ(old_results.size(), 1u);
    ASSERT_GT(processor.bufferedBytes(0), 0u);

    const auto new_results = apply(processor, {
        ioEvent(edge::TcpIoKind::Closed, 0),
        ioEvent(edge::TcpIoKind::Accepted, 0),
        ioEvent(edge::TcpIoKind::Data, 0, eventFrame(3))
    });

    ASSERT_EQ(new_results.size(), 1u);

    const auto* event =
        std::get_if<edge::SensorEvent>(
            &new_results.front().result
        );

    ASSERT_NE(event, nullptr);
    EXPECT_EQ(event->sequence, 3);

    EXPECT_NE(
        old_results.front().connection_id,
        new_results.front().connection_id
    );

    EXPECT_EQ(processor.bufferedBytes(0), 0u);
    EXPECT_EQ(processor.activeCount(), 1u);

    // 已关闭连接的统计没有丢失。
    EXPECT_EQ(processor.stats().valid_events, 2u);
    EXPECT_EQ(processor.stats().framing.completed_frames, 2u);
}

TEST_F(TcpEventProcessorTest, PreservesResultCategoriesAndOrder)
{
    edge::TcpEventProcessor processor{makeConfig()};

    auto bytes = payloadFrame("{");
    const auto invalid_event = eventFrame(0);
    const auto valid_event = eventFrame(9);

    bytes.insert(
        bytes.end(),
        invalid_event.begin(),
        invalid_event.end()
    );

    bytes.insert(
        bytes.end(),
        valid_event.begin(),
        valid_event.end()
    );

    const auto results = apply(processor, {
        ioEvent(edge::TcpIoKind::Accepted, 0),
        ioEvent(edge::TcpIoKind::Data, 0, bytes)
    });

    ASSERT_EQ(results.size(), 3u);

    EXPECT_TRUE(
        std::holds_alternative<edge::EventJsonError>(
            results[0].result
        )
    );

    EXPECT_TRUE(
        std::holds_alternative<edge::EventValidationError>(
            results[1].result
        )
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::SensorEvent>(
            results[2].result
        )
    );

    EXPECT_EQ(
        std::get<edge::SensorEvent>(
            results[2].result
        ).sequence,
        9
    );

    const auto stats = processor.stats();

    EXPECT_EQ(stats.json_errors, 1u);
    EXPECT_EQ(stats.validation_errors, 1u);
    EXPECT_EQ(stats.valid_events, 1u);
    EXPECT_EQ(stats.framing.completed_frames, 3u);
}

TEST_F(TcpEventProcessorTest, CollectsTimeoutRecoveryResults)
{
    // 分别验证空轮询、关闭连接时的超时恢复。
    for (const bool closing : {false, true}) {
        SCOPED_TRACE(closing);

        edge::TcpEventProcessor processor{makeConfig()};

        const auto inner = eventFrame(7);

        // 外层声称的载荷比实际数据长，因此保持未完成状态。
        auto outer = payloadFrame(
            std::string(inner.size() + 16, 'x')
        );

        outer.resize(edge::kFrameHeaderSize);
        outer.insert(outer.end(), inner.begin(), inner.end());

        const auto initial = apply(processor, {
            ioEvent(edge::TcpIoKind::Accepted, 0),
            ioEvent(edge::TcpIoKind::Data, 0, outer)
        });

        EXPECT_TRUE(initial.empty());

        const auto before_timeout = apply(
            processor,
            {},
            TimePoint{} + std::chrono::milliseconds{99}
        );

        EXPECT_TRUE(before_timeout.empty());

        std::vector<edge::TcpIoEvent> events;

        if (closing) {
            events.push_back(
                ioEvent(edge::TcpIoKind::Closed, 0)
            );
        }

        const auto recovered = apply(
            processor,
            std::move(events),
            TimePoint{} + std::chrono::milliseconds{100}
        );

        ASSERT_EQ(recovered.size(), 1u);
        ASSERT_TRUE(
            std::holds_alternative<edge::SensorEvent>(
                recovered.front().result
            )
        );

        EXPECT_EQ(
            std::get<edge::SensorEvent>(
                recovered.front().result
            ).sequence,
            7
        );

        EXPECT_EQ(
            processor.stats().framing.partial_timeouts,
            1u
        );

        EXPECT_EQ(processor.stats().valid_events, 1u);
        EXPECT_EQ(
            processor.activeCount(),
            closing ? 0u : 1u
        );
    }
}

TEST_F(TcpEventProcessorTest, PassesConfiguredPayloadLimit)
{
    auto config = makeConfig();
    config.max_payload_bytes = 64;

    edge::TcpEventProcessor processor{config};

    const auto frame = eventFrame(1);

    ASSERT_GT(
        frame.size() - edge::kFrameHeaderSize,
        64u
    );

    const auto results = apply(processor, {
        ioEvent(edge::TcpIoKind::Accepted, 0),
        ioEvent(edge::TcpIoKind::Data, 0, frame)
    });

    EXPECT_TRUE(results.empty());
    EXPECT_EQ(processor.stats().framing.invalid_lengths, 1u);
    EXPECT_EQ(processor.stats().framing.completed_frames, 0u);
}