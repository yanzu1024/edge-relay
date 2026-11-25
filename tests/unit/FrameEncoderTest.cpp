#include <gtest/gtest.h>

#include <cstddef>
#include <string>
#include <variant>

#include "edge/protocol/FrameEncoder.h"
#include "edge/protocol/FrameFormat.h"

namespace {

// {"x":""} 本身占 8 个字节，再加入指定数量的 ASCII 字符。
std::string makeJsonPayload(std::size_t value_length)
{
    return "{\"x\":\""
        + std::string(value_length, 'a')
        + "\"}";
}

TEST(FrameEncoder, EncodesExactFrame)
{
    const std::string payload = R"({"x":1})";

    const auto result = edge::encodeFrame(payload);
    const auto* frame = std::get_if<edge::EncodedFrame>(&result);

    ASSERT_NE(frame, nullptr);

    // 载荷为 7 字节，完整帧为 15 字节。
    const edge::EncodedFrame expected{
        'E', 'T', 'L', 'M',
        0x00, 0x00, 0x00, 0x07,
        '{', '"', 'x', '"', ':', '1', '}'
    };

    EXPECT_EQ(*frame, expected);
}

TEST(FrameEncoder, EncodesLengthInBigEndian)
{
    // 250 字节内容 + 8 字节 JSON 结构 = 258，即 0x0102。
    const auto payload = makeJsonPayload(250);

    ASSERT_EQ(payload.size(), 258U);

    const auto result = edge::encodeFrame(payload);
    const auto* frame = std::get_if<edge::EncodedFrame>(&result);

    ASSERT_NE(frame, nullptr);
    ASSERT_EQ(frame->size(), 266U);

    EXPECT_EQ((*frame)[4], 0x00);
    EXPECT_EQ((*frame)[5], 0x00);
    EXPECT_EQ((*frame)[6], 0x01);
    EXPECT_EQ((*frame)[7], 0x02);
}

TEST(FrameEncoder, PreservesUtf8PayloadBytes)
{
    // “温”的 UTF-8 编码占 3 字节，整个 JSON 共 11 字节。
    const std::string payload = u8R"({"x":"温"})";

    ASSERT_EQ(payload.size(), 11U);

    const auto result = edge::encodeFrame(payload);
    const auto* frame = std::get_if<edge::EncodedFrame>(&result);

    ASSERT_NE(frame, nullptr);
    ASSERT_EQ(frame->size(), 19U);

    EXPECT_EQ((*frame)[4], 0x00);
    EXPECT_EQ((*frame)[5], 0x00);
    EXPECT_EQ((*frame)[6], 0x00);
    EXPECT_EQ((*frame)[7], 0x0B);

    // 检查封装前后的载荷字节是否保持一致。
    const std::string restored(
        frame->begin() + edge::kFrameHeaderSize,
        frame->end()
    );

    EXPECT_EQ(restored, payload);
}

TEST(FrameEncoder, RejectsEmptyPayload)
{
    const auto result = edge::encodeFrame("");

    const auto* error =
        std::get_if<edge::FrameEncodeError>(&result);

    ASSERT_NE(error, nullptr);

    EXPECT_EQ(error->code, edge::FrameEncodeCode::EmptyPayload);
    EXPECT_FALSE(error->message.empty());
}

TEST(FrameEncoder, AcceptsMaximumPayload)
{
    const auto payload = makeJsonPayload(
        edge::kMaxFramePayloadBytes - 8
    );

    ASSERT_EQ(payload.size(), 8192U);

    const auto result = edge::encodeFrame(payload);
    const auto* frame = std::get_if<edge::EncodedFrame>(&result);

    ASSERT_NE(frame, nullptr);
    ASSERT_EQ(frame->size(), 8200U);

    // 8192 = 0x00002000。
    EXPECT_EQ((*frame)[4], 0x00);
    EXPECT_EQ((*frame)[5], 0x00);
    EXPECT_EQ((*frame)[6], 0x20);
    EXPECT_EQ((*frame)[7], 0x00);
}

TEST(FrameEncoder, RejectsOversizedPayload)
{
    const auto payload = makeJsonPayload(
        edge::kMaxFramePayloadBytes - 8 + 1
    );

    ASSERT_EQ(payload.size(), 8193U);

    const auto result = edge::encodeFrame(payload);

    const auto* error =
        std::get_if<edge::FrameEncodeError>(&result);

    ASSERT_NE(error, nullptr);

    EXPECT_EQ(error->code, edge::FrameEncodeCode::PayloadTooLarge);
    EXPECT_FALSE(error->message.empty());
}

} // namespace