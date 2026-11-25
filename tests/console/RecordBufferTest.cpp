#include "protocol/RecordBuffer.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace {

using namespace edge::console;

JsonlMessage makeRecord(std::uint64_t sequence)
{
    return JsonlMessage{
        JsonlMessageKind::Record,
        std::string(32, 'a'),
        sequence,
        1700000000000LL,
        nlohmann::json{
            {"event_id", "record-buffer-test-event"},
            {"disposition", "stored"},
            {"event", {
                {"probe",
                    std::numeric_limits<std::uint64_t>::max()}
            }}
        }
    };
}

TEST(RecordBufferTest, StartsEmpty)
{
    RecordBuffer buffer;

    EXPECT_EQ(buffer.size(), 0U);
    EXPECT_EQ(buffer.dropped(), 0U);
    EXPECT_TRUE(buffer.take(10).empty());
}

TEST(RecordBufferTest, PreservesEnvelopeAndUint64Payload)
{
    RecordBuffer buffer;

    buffer.push(makeRecord(2));

    const auto records = buffer.take(1);

    ASSERT_EQ(records.size(), 1U);

    EXPECT_EQ(records[0].kind, JsonlMessageKind::Record);
    EXPECT_EQ(records[0].run_id, std::string(32, 'a'));
    EXPECT_EQ(records[0].sequence, 2U);
    EXPECT_EQ(records[0].emitted_at_ms, 1700000000000LL);

    EXPECT_EQ(
        records[0].data.at("event").at("probe")
            .get<std::uint64_t>(),
        std::numeric_limits<std::uint64_t>::max()
    );
}

TEST(RecordBufferTest, OverflowKeepsLatestRecords)
{
    RecordBuffer buffer;

    const std::size_t count =
        RecordBuffer::kCapacity + 5;

    for (std::size_t index = 0; index < count; ++index) {
        buffer.push(
            makeRecord(
                static_cast<std::uint64_t>(index) + 2
            )
        );

        EXPECT_LE(buffer.size(), RecordBuffer::kCapacity);
    }

    EXPECT_EQ(buffer.dropped(), 5U);

    const auto records =
        buffer.take(RecordBuffer::kCapacity);

    ASSERT_EQ(records.size(), RecordBuffer::kCapacity);

    EXPECT_EQ(records.front().sequence, 7U);

    EXPECT_EQ(
        records.back().sequence,
        static_cast<std::uint64_t>(count) + 1
    );

    EXPECT_EQ(buffer.size(), 0U);

    // 取走消息不清零本次运行的累计丢弃数。
    EXPECT_EQ(buffer.dropped(), 5U);
}

TEST(RecordBufferTest, PartialTakePreservesOrder)
{
    RecordBuffer buffer;

    for (std::uint64_t sequence = 2;
         sequence < 6;
         ++sequence) {
        buffer.push(makeRecord(sequence));
    }

    const auto first = buffer.take(2);

    ASSERT_EQ(first.size(), 2U);
    EXPECT_EQ(first[0].sequence, 2U);
    EXPECT_EQ(first[1].sequence, 3U);
    EXPECT_EQ(buffer.size(), 2U);

    const auto second = buffer.take(10);

    ASSERT_EQ(second.size(), 2U);
    EXPECT_EQ(second[0].sequence, 4U);
    EXPECT_EQ(second[1].sequence, 5U);
    EXPECT_EQ(buffer.size(), 0U);
}

TEST(RecordBufferTest, TakingZeroDoesNotDrain)
{
    RecordBuffer buffer;

    buffer.push(makeRecord(2));

    EXPECT_TRUE(buffer.take(0).empty());
    EXPECT_EQ(buffer.size(), 1U);
    EXPECT_EQ(buffer.dropped(), 0U);
}

TEST(RecordBufferTest, HugeTakeRemainsBounded)
{
    RecordBuffer buffer;

    buffer.push(makeRecord(2));

    const auto records = buffer.take(
        std::numeric_limits<std::size_t>::max()
    );

    ASSERT_EQ(records.size(), 1U);
    EXPECT_EQ(records[0].sequence, 2U);
}

TEST(RecordBufferTest, ResetClearsRecordsAndDropCount)
{
    RecordBuffer buffer;

    for (std::size_t index = 0;
         index < RecordBuffer::kCapacity + 1;
         ++index) {
        buffer.push(
            makeRecord(
                static_cast<std::uint64_t>(index) + 2
            )
        );
    }

    ASSERT_EQ(buffer.dropped(), 1U);

    buffer.reset();

    EXPECT_EQ(buffer.size(), 0U);
    EXPECT_EQ(buffer.dropped(), 0U);

    auto nextRun = makeRecord(2);
    nextRun.run_id = std::string(32, 'b');

    buffer.push(std::move(nextRun));

    const auto records = buffer.take(1);

    ASSERT_EQ(records.size(), 1U);
    EXPECT_EQ(records[0].run_id, std::string(32, 'b'));
    EXPECT_EQ(records[0].sequence, 2U);
}

TEST(RecordBufferTest, RejectsOtherMessageKindsWithoutChangingBuffer)
{
    RecordBuffer buffer;

    buffer.push(makeRecord(2));

    auto invalid = makeRecord(3);
    invalid.kind = JsonlMessageKind::Status;

    EXPECT_THROW(
        buffer.push(std::move(invalid)),
        std::invalid_argument
    );

    EXPECT_EQ(buffer.size(), 1U);
    EXPECT_EQ(buffer.dropped(), 0U);

    const auto records = buffer.take(1);

    ASSERT_EQ(records.size(), 1U);
    EXPECT_EQ(records[0].sequence, 2U);
}

TEST(RecordBufferTest, DoesNotDeduplicateDisplayMessages)
{
    RecordBuffer buffer;

    auto stored = makeRecord(2);
    auto duplicate = makeRecord(3);

    duplicate.data["disposition"] = "duplicate";

    buffer.push(std::move(stored));
    buffer.push(std::move(duplicate));

    const auto records = buffer.take(2);

    ASSERT_EQ(records.size(), 2U);

    EXPECT_EQ(
        records[0].data.at("event_id"),
        records[1].data.at("event_id")
    );

    EXPECT_EQ(
        records[0].data.at("disposition"),
        "stored"
    );

    EXPECT_EQ(
        records[1].data.at("disposition"),
        "duplicate"
    );
}

} // namespace