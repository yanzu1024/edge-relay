#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include <chrono>
#include <stdexcept>

#include "edge/protocol/FrameEncoder.h"
#include "edge/protocol/StreamDecoder.h"

namespace {

// 使用已经测试过的编码器，为分片和多帧测试准备输入。
edge::EncodedFrame makeFrame(std::string_view payload)
{
    return std::get<edge::EncodedFrame>(
        edge::encodeFrame(payload)
    );
}

TEST(StreamDecoder, DecodesCompleteFrame)
{
    // 使用明确的协议字节检查解码，避免只依赖编码器生成输入。
    const edge::EncodedFrame frame{
        'E', 'T', 'L', 'M',
        0x00, 0x00, 0x00, 0x07,
        '{', '"', 'x', '"', ':', '1', '}'
    };

    edge::StreamDecoder decoder;

    const auto payloads = decoder.feed(
        frame.data(),
        frame.size()
    );

    ASSERT_EQ(payloads.size(), 1U);
    EXPECT_EQ(payloads[0], R"({"x":1})");
    EXPECT_EQ(decoder.bufferedBytes(), 0U);
}

TEST(StreamDecoder, HandlesEverySplitPoint)
{
    const std::string payload = R"({"x":1})";
    const auto frame = makeFrame(payload);

    // 在每个位置把帧拆成两段，包括头部和载荷内部。
    for (std::size_t split = 0; split <= frame.size(); ++split) {
        SCOPED_TRACE(split);

        edge::StreamDecoder decoder;

        auto decoded = decoder.feed(
            frame.data(),
            split
        );

        const auto remaining = decoder.feed(
            frame.data() + split,
            frame.size() - split
        );

        decoded.insert(
            decoded.end(),
            remaining.begin(),
            remaining.end()
        );

        ASSERT_EQ(decoded.size(), 1U);
        EXPECT_EQ(decoded[0], payload);
        EXPECT_EQ(decoder.bufferedBytes(), 0U);
    }
}

TEST(StreamDecoder, DecodesByteByByte)
{
    const std::string payload = R"({"x":1})";
    const auto frame = makeFrame(payload);

    edge::StreamDecoder decoder;
    std::vector<std::string> decoded;

    for (const auto byte : frame) {
        const auto batch = decoder.feed(&byte, 1);

        decoded.insert(
            decoded.end(),
            batch.begin(),
            batch.end()
        );
    }

    ASSERT_EQ(decoded.size(), 1U);
    EXPECT_EQ(decoded[0], payload);
    EXPECT_EQ(decoder.bufferedBytes(), 0U);
}

TEST(StreamDecoder, DecodesMultipleFramesWithIncompleteTail)
{
    const auto first = makeFrame(R"({"x":1})");
    const auto second = makeFrame(R"({"x":2})");
    const auto third = makeFrame(R"({"x":3})");

    // 前两帧完整，第三帧还差最后两个字节。
    const auto split = third.size() - 2;

    auto input = first;
    input.insert(input.end(), second.begin(), second.end());
    input.insert(input.end(), third.begin(), third.begin() + split);

    edge::StreamDecoder decoder;

    const auto first_batch = decoder.feed(
        input.data(),
        input.size()
    );

    ASSERT_EQ(first_batch.size(), 2U);
    EXPECT_EQ(first_batch[0], R"({"x":1})");
    EXPECT_EQ(first_batch[1], R"({"x":2})");
    EXPECT_EQ(decoder.bufferedBytes(), split);

    const auto second_batch = decoder.feed(
        third.data() + split,
        third.size() - split
    );

    ASSERT_EQ(second_batch.size(), 1U);
    EXPECT_EQ(second_batch[0], R"({"x":3})");
    EXPECT_EQ(decoder.bufferedBytes(), 0U);
}

TEST(StreamDecoder, ResynchronizesAcrossCalls)
{
    // "ETX" 不是 magic，末尾的 "ET" 应被保留下来。
    const std::uint8_t noise[]{
        0x00, 'E', 'T', 'X', 'E', 'T'
    };

    edge::StreamDecoder decoder;

    EXPECT_TRUE(decoder.feed(noise, sizeof(noise)).empty());
    EXPECT_EQ(decoder.bufferedBytes(), 2U);

    const auto frame = makeFrame(R"({"x":1})");

    // 前两个字节 "ET" 已经在缓冲区，继续输入 "LM..."。
    const auto payloads = decoder.feed(
        frame.data() + 2,
        frame.size() - 2
    );

    ASSERT_EQ(payloads.size(), 1U);
    EXPECT_EQ(payloads[0], R"({"x":1})");
    EXPECT_EQ(decoder.bufferedBytes(), 0U);
}

TEST(StreamDecoder, SkipsInvalidLengths)
{
    edge::EncodedFrame input{
        // 长度 0，非法。
        'E', 'T', 'L', 'M',
        0x00, 0x00, 0x00, 0x00,

        // 长度 8193，超过协议上限。
        'E', 'T', 'L', 'M',
        0x00, 0x00, 0x20, 0x01
    };

    const auto valid = makeFrame(R"({"x":1})");

    input.insert(
        input.end(),
        valid.begin(),
        valid.end()
    );

    edge::StreamDecoder decoder;

    const auto payloads = decoder.feed(
        input.data(),
        input.size()
    );

    ASSERT_EQ(payloads.size(), 1U);
    EXPECT_EQ(payloads[0], R"({"x":1})");
    EXPECT_EQ(decoder.bufferedBytes(), 0U);
}

TEST(StreamDecoder, HandlesLargeInput)
{
    const std::string payload = R"({"x":1})";
    const auto frame = makeFrame(payload);

    constexpr std::size_t frame_count = 5000;

    edge::EncodedFrame input;
    input.reserve(frame.size() * frame_count);

    for (std::size_t index = 0; index < frame_count; ++index) {
        input.insert(
            input.end(),
            frame.begin(),
            frame.end()
        );
    }

    // 一次输入超过 64 KiB，仍应逐段处理并保留全部有效帧。
    ASSERT_GT(input.size(), 64U * 1024U);

    edge::StreamDecoder decoder;

    const auto payloads = decoder.feed(
        input.data(),
        input.size()
    );

    ASSERT_EQ(payloads.size(), frame_count);

    for (const auto& decoded : payloads) {
        EXPECT_EQ(decoded, payload);
    }

    EXPECT_EQ(decoder.bufferedBytes(), 0U);
}

TEST(StreamDecoder, ResetClearsPartialFrame)
{
    const auto frame = makeFrame(R"({"x":1})");

    edge::StreamDecoder decoder;

    EXPECT_TRUE(decoder.feed(frame.data(), 5).empty());
    ASSERT_EQ(decoder.bufferedBytes(), 5U);

    decoder.reset();

    EXPECT_EQ(decoder.bufferedBytes(), 0U);

    const auto payloads = decoder.feed(
        frame.data(),
        frame.size()
    );

    ASSERT_EQ(payloads.size(), 1U);
    EXPECT_EQ(payloads[0], R"({"x":1})");
}


using Milliseconds = std::chrono::milliseconds;
using DecoderClock = edge::StreamDecoder::Clock;

TEST(StreamDecoder, RetainsPartialFrameBeforeDeadline)
{
    const auto frame = makeFrame(R"({"x":1})");
    const auto start = DecoderClock::time_point{};

    edge::StreamDecoder decoder;

    // 收到 magic 和一部分长度字段。
    EXPECT_TRUE(
        decoder.feed(frame.data(), 5, start).empty()
    );

    EXPECT_TRUE(
        decoder.checkTimeout(start + Milliseconds{2999}).empty()
    );

    EXPECT_EQ(decoder.bufferedBytes(), 5U);
    EXPECT_EQ(decoder.partialTimeoutCount(), 0U);
}

TEST(StreamDecoder, ExpiresPartialFrameAtDeadline)
{
    const auto frame = makeFrame(R"({"x":1})");
    const auto start = DecoderClock::time_point{};

    edge::StreamDecoder decoder;

    EXPECT_TRUE(
        decoder.feed(frame.data(), 5, start).empty()
    );

    EXPECT_TRUE(
        decoder.checkTimeout(start + Milliseconds{3000}).empty()
    );

    EXPECT_EQ(decoder.bufferedBytes(), 0U);
    EXPECT_EQ(decoder.partialTimeoutCount(), 1U);

    // 已经丢弃的候选不能被重复计数。
    EXPECT_TRUE(
        decoder.checkTimeout(start + Milliseconds{6000}).empty()
    );

    EXPECT_EQ(decoder.partialTimeoutCount(), 1U);
}

TEST(StreamDecoder, ProgressDoesNotRestartDeadline)
{
    const auto frame = makeFrame(R"({"x":1})");
    const auto start = DecoderClock::time_point{};

    edge::StreamDecoder decoder;

    EXPECT_TRUE(
        decoder.feed(frame.data(), 5, start).empty()
    );

    // 等待期间又收到一个字节，但仍不足以组成完整帧头。
    EXPECT_TRUE(
        decoder.feed(
            frame.data() + 5,
            1,
            start + Milliseconds{2000}
        ).empty()
    );

    // 截止时间仍是首次等待后的 3000 ms。
    EXPECT_TRUE(
        decoder.checkTimeout(start + Milliseconds{3000}).empty()
    );

    EXPECT_EQ(decoder.partialTimeoutCount(), 1U);
    EXPECT_EQ(decoder.bufferedBytes(), 0U);
}

TEST(StreamDecoder, RecoversBufferedFrameAfterTimeout)
{
    // 假候选声明载荷有 8192 字节，实际上后面直接跟着合法帧。
    edge::EncodedFrame input{
        'E', 'T', 'L', 'M',
        0x00, 0x00, 0x20, 0x00
    };

    const auto valid = makeFrame(R"({"x":1})");
    input.insert(input.end(), valid.begin(), valid.end());

    const auto start = DecoderClock::time_point{};

    edge::StreamDecoder decoder;

    EXPECT_TRUE(
        decoder.feed(input.data(), input.size(), start).empty()
    );

    // 超时后跳过假候选，找到缓冲区内已有的合法帧。
    const auto payloads = decoder.checkTimeout(
        start + Milliseconds{3000}
    );

    ASSERT_EQ(payloads.size(), 1U);
    EXPECT_EQ(payloads[0], R"({"x":1})");
    EXPECT_EQ(decoder.partialTimeoutCount(), 1U);
    EXPECT_EQ(decoder.bufferedBytes(), 0U);
}

TEST(StreamDecoder, StartsNewDeadlineForNewCandidate)
{
    edge::EncodedFrame input{
        'E', 'T', 'L', 'M',
        0x00, 0x00, 0x20, 0x00
    };

    const auto next = makeFrame(R"({"x":1})");

    // 后面再放一个新的不完整候选。
    input.insert(input.end(), next.begin(), next.begin() + 5);

    const auto start = DecoderClock::time_point{};

    edge::StreamDecoder decoder;

    EXPECT_TRUE(
        decoder.feed(input.data(), input.size(), start).empty()
    );

    EXPECT_TRUE(
        decoder.checkTimeout(start + Milliseconds{3000}).empty()
    );

    // 第一候选超时；第二候选从 3000 ms 开始计时。
    EXPECT_EQ(decoder.partialTimeoutCount(), 1U);
    EXPECT_EQ(decoder.bufferedBytes(), 5U);

    EXPECT_TRUE(
        decoder.checkTimeout(start + Milliseconds{5999}).empty()
    );

    EXPECT_EQ(decoder.partialTimeoutCount(), 1U);

    EXPECT_TRUE(
        decoder.checkTimeout(start + Milliseconds{6000}).empty()
    );

    EXPECT_EQ(decoder.partialTimeoutCount(), 2U);
    EXPECT_EQ(decoder.bufferedBytes(), 0U);
}

TEST(StreamDecoder, ResetClearsDeadline)
{
    const auto frame = makeFrame(R"({"x":1})");
    const auto start = DecoderClock::time_point{};

    edge::StreamDecoder decoder;

    EXPECT_TRUE(
        decoder.feed(frame.data(), 5, start).empty()
    );

    decoder.reset();

    EXPECT_TRUE(
        decoder.feed(
            frame.data(),
            5,
            start + Milliseconds{2500}
        ).empty()
    );

    // 新候选的截止时间为 5500 ms，不应继承旧截止时间。
    EXPECT_TRUE(
        decoder.checkTimeout(start + Milliseconds{3000}).empty()
    );

    EXPECT_EQ(decoder.partialTimeoutCount(), 0U);
    EXPECT_EQ(decoder.bufferedBytes(), 5U);

    EXPECT_TRUE(
        decoder.checkTimeout(start + Milliseconds{5500}).empty()
    );

    EXPECT_EQ(decoder.partialTimeoutCount(), 1U);
}

TEST(StreamDecoder, DiscardsLateCompletionOfExpiredFrame)
{
    const auto old_frame = makeFrame(R"({"x":1})");
    const auto new_frame = makeFrame(R"({"x":2})");

    const auto start = DecoderClock::time_point{};

    edge::StreamDecoder decoder;

    EXPECT_TRUE(
        decoder.feed(old_frame.data(), 5, start).empty()
    );

    edge::EncodedFrame late_input(
        old_frame.begin() + 5,
        old_frame.end()
    );

    late_input.insert(
        late_input.end(),
        new_frame.begin(),
        new_frame.end()
    );

    // 旧帧的剩余字节到达时已经超时，只有新帧应被输出。
    const auto payloads = decoder.feed(
        late_input.data(),
        late_input.size(),
        start + Milliseconds{3000}
    );

    ASSERT_EQ(payloads.size(), 1U);
    EXPECT_EQ(payloads[0], R"({"x":2})");
    EXPECT_EQ(decoder.partialTimeoutCount(), 1U);
}

TEST(StreamDecoder, RejectsNonPositiveTimeout)
{
    EXPECT_THROW(
        edge::StreamDecoder{Milliseconds{0}},
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::StreamDecoder{Milliseconds{-1}},
        std::invalid_argument
    );
}


TEST(StreamDecoder, CountsCompletedFrames)
{
    const auto frame = makeFrame(R"({"x":1})");
    const auto start = DecoderClock::time_point{};

    edge::StreamDecoder decoder;

    EXPECT_TRUE(
        decoder.feed(frame.data(), 5, start).empty()
    );

    // 半帧尚未完成，不能提前计入完整帧数量。
    EXPECT_EQ(decoder.stats().completed_frames, 0U);

    const auto first = decoder.feed(
        frame.data() + 5,
        frame.size() - 5,
        start
    );

    ASSERT_EQ(first.size(), 1U);

    const auto second = decoder.feed(
        frame.data(),
        frame.size(),
        start
    );

    ASSERT_EQ(second.size(), 1U);

    const auto stats = decoder.stats();

    EXPECT_EQ(stats.completed_frames, 2U);
    EXPECT_EQ(stats.invalid_lengths, 0U);
    EXPECT_EQ(stats.resync_bytes, 0U);
    EXPECT_EQ(stats.partial_timeouts, 0U);
}

TEST(StreamDecoder, CountsNoiseWhileRetainingMagicPrefix)
{
    const std::uint8_t noise[]{
        0x00, 'E', 'T', 'X', 'E', 'T'
    };

    edge::StreamDecoder decoder;

    EXPECT_TRUE(
        decoder.feed(noise, sizeof(noise)).empty()
    );

    // 共 6 字节，末尾 "ET" 保留，其余 4 字节丢弃。
    EXPECT_EQ(decoder.bufferedBytes(), 2U);
    EXPECT_EQ(decoder.stats().resync_bytes, 4U);

    const auto frame = makeFrame(R"({"x":1})");

    const auto payloads = decoder.feed(
        frame.data() + 2,
        frame.size() - 2
    );

    ASSERT_EQ(payloads.size(), 1U);

    const auto stats = decoder.stats();

    EXPECT_EQ(stats.completed_frames, 1U);

    // 保留下来的 "ET" 最终属于合法帧，不应计入丢弃。
    EXPECT_EQ(stats.resync_bytes, 4U);
}

TEST(StreamDecoder, CountsInvalidLengths)
{
    edge::EncodedFrame input{
        'E', 'T', 'L', 'M',
        0x00, 0x00, 0x00, 0x00,

        'E', 'T', 'L', 'M',
        0x00, 0x00, 0x20, 0x01
    };

    const auto valid = makeFrame(R"({"x":1})");

    input.insert(
        input.end(),
        valid.begin(),
        valid.end()
    );

    edge::StreamDecoder decoder;

    const auto payloads = decoder.feed(
        input.data(),
        input.size()
    );

    ASSERT_EQ(payloads.size(), 1U);

    const auto stats = decoder.stats();

    EXPECT_EQ(stats.completed_frames, 1U);
    EXPECT_EQ(stats.invalid_lengths, 2U);

    // 两个非法头部各 8 字节，合法帧不计入丢弃。
    EXPECT_EQ(stats.resync_bytes, 16U);
    EXPECT_EQ(stats.partial_timeouts, 0U);
}

TEST(StreamDecoder, CountsTimeoutRecovery)
{
    edge::EncodedFrame input{
        'E', 'T', 'L', 'M',
        0x00, 0x00, 0x20, 0x00
    };

    const auto valid = makeFrame(R"({"x":1})");
    input.insert(input.end(), valid.begin(), valid.end());

    const auto start = DecoderClock::time_point{};

    edge::StreamDecoder decoder;

    EXPECT_TRUE(
        decoder.feed(input.data(), input.size(), start).empty()
    );

    const auto payloads = decoder.checkTimeout(
        start + Milliseconds{3000}
    );

    ASSERT_EQ(payloads.size(), 1U);

    const auto stats = decoder.stats();

    EXPECT_EQ(stats.completed_frames, 1U);
    EXPECT_EQ(stats.invalid_lengths, 0U);
    EXPECT_EQ(stats.partial_timeouts, 1U);

    // 超时候选的头部被丢弃，后面的合法帧被正常消费。
    EXPECT_EQ(stats.resync_bytes, 8U);
}

TEST(StreamDecoder, ResetPreservesCumulativeStats)
{
    const auto frame = makeFrame(R"({"x":1})");
    const auto start = DecoderClock::time_point{};

    const std::uint8_t noise[]{0x01};

    edge::StreamDecoder decoder;

    EXPECT_TRUE(
        decoder.feed(noise, sizeof(noise), start).empty()
    );

    ASSERT_EQ(
        decoder.feed(frame.data(), frame.size(), start).size(),
        1U
    );

    EXPECT_TRUE(
        decoder.feed(frame.data(), 5, start).empty()
    );

    decoder.reset();

    EXPECT_EQ(decoder.bufferedBytes(), 0U);

    auto stats = decoder.stats();

    EXPECT_EQ(stats.completed_frames, 1U);
    EXPECT_EQ(stats.resync_bytes, 1U);
    EXPECT_EQ(stats.partial_timeouts, 0U);

    // 重置之后继续使用，统计在原值上累加。
    ASSERT_EQ(
        decoder.feed(
            frame.data(),
            frame.size(),
            start + Milliseconds{1000}
        ).size(),
        1U
    );

    stats = decoder.stats();

    EXPECT_EQ(stats.completed_frames, 2U);
    EXPECT_EQ(stats.resync_bytes, 1U);
}

TEST(StreamDecoder, HonorsConfiguredPayloadLimit)
{
    // 前一帧载荷为 8 字节，后一帧为 7 字节。
    const auto oversized = makeFrame(R"({"xx":1})");
    const auto valid = makeFrame(R"({"x":1})");

    auto input = oversized;
    input.insert(input.end(), valid.begin(), valid.end());

    edge::StreamDecoder decoder(Milliseconds{3000}, 7);

    const auto payloads = decoder.feed(
        input.data(),
        input.size()
    );

    ASSERT_EQ(payloads.size(), 1U);
    EXPECT_EQ(payloads[0], R"({"x":1})");

    const auto stats = decoder.stats();

    EXPECT_EQ(stats.invalid_lengths, 1U);
    EXPECT_EQ(stats.completed_frames, 1U);
    EXPECT_EQ(stats.resync_bytes, oversized.size());
}

TEST(StreamDecoder, AcceptsPayloadLimitBoundaries)
{
    const std::size_t limits[] = {
        1,
        edge::kMaxFramePayloadBytes
    };

    for (const auto limit : limits) {
        SCOPED_TRACE(limit);

        // 这里只检查帧载荷边界，JSON 业务内容由其他模块检查。
        const std::string payload(limit, 'x');
        const auto frame = makeFrame(payload);

        edge::StreamDecoder decoder(Milliseconds{3000}, limit);

        const auto payloads = decoder.feed(
            frame.data(),
            frame.size()
        );

        ASSERT_EQ(payloads.size(), 1U);
        EXPECT_EQ(payloads[0], payload);
        EXPECT_EQ(decoder.stats().invalid_lengths, 0U);
    }
}

TEST(StreamDecoder, RejectsInvalidPayloadLimits)
{
    EXPECT_THROW(
        (void)edge::StreamDecoder(Milliseconds{3000}, 0),
        std::invalid_argument
    );

    EXPECT_THROW(
        (void)edge::StreamDecoder(
            Milliseconds{3000},
            edge::kMaxFramePayloadBytes + 1
        ),
        std::invalid_argument
    );
}

} // namespace