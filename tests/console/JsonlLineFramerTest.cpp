#include "protocol/JsonlLineFramer.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using edge::console::JsonlFrame;
using edge::console::JsonlFrameKind;
using edge::console::JsonlLineFramer;

class JsonlLineFramerTest : public ::testing::Test {
protected:
    void feed(std::string_view bytes)
    {
        framer_.feed(bytes, [this](JsonlFrame frame) {
            frames_.push_back(std::move(frame));
        });
    }

    void finish()
    {
        framer_.finish([this](JsonlFrame frame) {
            frames_.push_back(std::move(frame));
        });
    }

    JsonlLineFramer framer_;
    std::vector<JsonlFrame> frames_;
};

TEST_F(JsonlLineFramerTest, HandlesPartialAndJoinedLines)
{
    feed("{\"x\":");

    EXPECT_TRUE(frames_.empty());
    EXPECT_EQ(framer_.bufferedBytes(), 5U);

    feed("1}\n{\"x\":2}\n{\"x\":");

    ASSERT_EQ(frames_.size(), 2U);
    EXPECT_EQ(frames_[0].kind, JsonlFrameKind::Line);
    EXPECT_EQ(frames_[0].bytes, "{\"x\":1}");
    EXPECT_EQ(frames_[1].bytes, "{\"x\":2}");

    feed("3}\n");

    ASSERT_EQ(frames_.size(), 3U);
    EXPECT_EQ(frames_[2].bytes, "{\"x\":3}");
    EXPECT_EQ(framer_.bufferedBytes(), 0U);
}

TEST_F(JsonlLineFramerTest, PreservesUtf8AtEverySplitPosition)
{
    const std::string wire = u8"{\"name\":\"设备\"}\n";
    const std::string expected = wire.substr(0, wire.size() - 1);

    for (std::size_t split = 0; split <= wire.size(); ++split) {
        SCOPED_TRACE(split);

        framer_.reset();
        frames_.clear();

        feed(std::string_view(wire).substr(0, split));
        feed(std::string_view(wire).substr(split));

        ASSERT_EQ(frames_.size(), 1U);
        EXPECT_EQ(frames_[0].kind, JsonlFrameKind::Line);
        EXPECT_EQ(frames_[0].bytes, expected);
    }
}

TEST_F(JsonlLineFramerTest, AcceptsExactLineLimitIncludingLf)
{
    const std::string payload(
        JsonlLineFramer::kMaxPayloadBytes,
        'x'
    );

    feed(payload);

    EXPECT_TRUE(frames_.empty());
    EXPECT_EQ(
        framer_.bufferedBytes(),
        JsonlLineFramer::kMaxPayloadBytes
    );

    feed("\n");

    ASSERT_EQ(frames_.size(), 1U);
    EXPECT_EQ(frames_[0].kind, JsonlFrameKind::Line);
    EXPECT_EQ(frames_[0].bytes, payload);
    EXPECT_EQ(framer_.bufferedBytes(), 0U);
}

TEST_F(JsonlLineFramerTest, DropsOversizedLineAndRecoversAtLf)
{
    feed(std::string(JsonlLineFramer::kMaxLineBytes, 'x'));

    ASSERT_EQ(frames_.size(), 1U);
    EXPECT_EQ(frames_[0].kind, JsonlFrameKind::LineTooLong);
    EXPECT_EQ(framer_.bufferedBytes(), 0U);

    // 超长行继续到达，不能增长缓冲或重复报告。
    const std::string block(4096, 'x');

    for (int index = 0; index < 32; ++index) {
        feed(block);
        EXPECT_EQ(framer_.bufferedBytes(), 0U);
    }

    EXPECT_EQ(frames_.size(), 1U);

    feed("\n{}\n");

    ASSERT_EQ(frames_.size(), 2U);
    EXPECT_EQ(frames_[1].kind, JsonlFrameKind::Line);
    EXPECT_EQ(frames_[1].bytes, "{}");
}

TEST_F(JsonlLineFramerTest, ReportsIncompleteTailOnlyOnce)
{
    feed("{\"unfinished\":");

    finish();
    finish();

    ASSERT_EQ(frames_.size(), 1U);
    EXPECT_EQ(frames_[0].kind, JsonlFrameKind::IncompleteTail);
    EXPECT_TRUE(frames_[0].bytes.empty());
    EXPECT_EQ(framer_.bufferedBytes(), 0U);
}

TEST_F(JsonlLineFramerTest, DoesNotReportOversizedTailTwice)
{
    feed(std::string(JsonlLineFramer::kMaxLineBytes, 'x'));

    finish();
    finish();

    ASSERT_EQ(frames_.size(), 1U);
    EXPECT_EQ(frames_[0].kind, JsonlFrameKind::LineTooLong);
}

TEST_F(JsonlLineFramerTest, EmptyStreamHasNoTail)
{
    feed("");
    finish();

    EXPECT_TRUE(frames_.empty());
}

TEST_F(JsonlLineFramerTest, PreservesEmptyLinesAndCarriageReturns)
{
    feed("\n{}\r\n");

    ASSERT_EQ(frames_.size(), 2U);
    EXPECT_EQ(frames_[0].kind, JsonlFrameKind::Line);
    EXPECT_TRUE(frames_[0].bytes.empty());
    EXPECT_EQ(frames_[1].bytes, "{}\r");
}

TEST_F(JsonlLineFramerTest, PreservesEmbeddedNullBytes)
{
    feed(std::string("a\0b\n", 4));

    ASSERT_EQ(frames_.size(), 1U);
    EXPECT_EQ(frames_[0].bytes, std::string("a\0b", 3));
}

TEST_F(JsonlLineFramerTest, ResetSeparatesStreams)
{
    feed("old-partial");

    framer_.reset();

    feed("new\n");
    finish();

    ASSERT_EQ(frames_.size(), 1U);
    EXPECT_EQ(frames_[0].bytes, "new");

    framer_.reset();

    feed("again\n");

    ASSERT_EQ(frames_.size(), 2U);
    EXPECT_EQ(frames_[1].bytes, "again");
}

TEST_F(JsonlLineFramerTest, RejectsFeedAfterFinish)
{
    finish();

    EXPECT_THROW(feed("{}\n"), std::logic_error);

    framer_.reset();
    feed("{}\n");

    ASSERT_EQ(frames_.size(), 1U);
    EXPECT_EQ(frames_[0].bytes, "{}");
}

TEST_F(JsonlLineFramerTest, DeliversManyLinesWithoutKeepingHistory)
{
    std::string wire;

    for (int index = 0; index < 10000; ++index) {
        wire += "{}\n";
    }

    std::size_t count = 0;

    framer_.feed(wire, [&count](JsonlFrame frame) {
        EXPECT_EQ(frame.kind, JsonlFrameKind::Line);
        EXPECT_EQ(frame.bytes, "{}");
        ++count;
    });

    EXPECT_EQ(count, 10000U);
    EXPECT_EQ(framer_.bufferedBytes(), 0U);
}

TEST_F(JsonlLineFramerTest, RejectsReentrantModification)
{
    framer_.feed("{}\n", [this](JsonlFrame frame) {
        EXPECT_EQ(frame.kind, JsonlFrameKind::Line);

        EXPECT_THROW(
            framer_.reset(),
            std::logic_error
        );

        EXPECT_THROW(
            feed("nested\n"),
            std::logic_error
        );

        EXPECT_THROW(
            finish(),
            std::logic_error
        );
    });

    // 回调结束后，可以继续正常接收。
    feed("next\n");

    ASSERT_EQ(frames_.size(), 1U);
    EXPECT_EQ(frames_[0].bytes, "next");
}

} // namespace
