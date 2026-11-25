#include "edge/domain/SensorEvent.h"
#include "edge/storage/OutboxSchema.h"
#include "edge/storage/OutboxStore.h"
#include "edge/storage/SqliteDatabase.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <variant>
#include <vector>

namespace {

constexpr std::int64_t kNow = 1794441600000LL;

class OutboxNextReadyTest : public testing::Test {
protected:
    void SetUp() override
    {
        edge::initializeOutboxSchema(database);
    }

    void addEvent(
        edge::OutboxStore& store,
        std::int64_t sequence
    )
    {
        edge::SensorEvent event{};

        event.schema_version = 1;
        event.device_id = "runtime_sensor";
        event.session_id =
            "12345678-1234-1234-1234-123456789abc";
        event.sequence = sequence;
        event.observed_at_ms = kNow;
        event.metric = "temperature";
        event.value = 23.5;
        event.unit = "degC";

        const auto result = store.enqueue(event, kNow);

        const auto* inserted =
            std::get_if<edge::OutboxInsertResult>(&result);

        if (inserted == nullptr
            || inserted->status
                != edge::OutboxInsertStatus::Inserted) {
            throw std::runtime_error(
                "Cannot prepare test event"
            );
        }
    }

    edge::OutboxClaimToken token(
        const edge::OutboxClaimedRecord& record
    )
    {
        return {
            record.event_id,
            record.attempt_count
        };
    }

    edge::SqliteDatabase database{":memory:"};
};

TEST_F(
    OutboxNextReadyTest,
    EmptyAndAlreadyDueRecordsHaveNoFutureTime
)
{
    edge::OutboxStore store(database);

    EXPECT_FALSE(
        store.nextReadyAfter(kNow).has_value()
    );

    addEvent(store, 1);

    // 新 PENDING 已经到期，不属于未来记录。
    EXPECT_FALSE(
        store.nextReadyAfter(kNow).has_value()
    );

    EXPECT_EQ(
        store.listReady(kNow, 10).size(),
        1U
    );
}

TEST_F(
    OutboxNextReadyTest,
    ReturnsEarliestStrictlyFutureRetry
)
{
    edge::OutboxStore store(database);

    addEvent(store, 1);
    addEvent(store, 2);

    const auto claimed = store.claimReady(kNow, 2);

    ASSERT_EQ(claimed.size(), 2U);

    const auto first = store.scheduleRetry(
        {token(claimed[0])},
        kNow,
        kNow + 300,
        "test retry"
    );

    const auto second = store.scheduleRetry(
        {token(claimed[1])},
        kNow,
        kNow + 100,
        "test retry"
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxRetrySuccess>(
            first
        )
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxRetrySuccess>(
            second
        )
    );

    EXPECT_EQ(
        store.nextReadyAfter(kNow),
        std::optional<std::int64_t>{kNow + 100}
    );

    // 恰好到期的那条不再作为未来时间返回。
    EXPECT_EQ(
        store.nextReadyAfter(kNow + 100),
        std::optional<std::int64_t>{kNow + 300}
    );

    EXPECT_FALSE(
        store.nextReadyAfter(kNow + 300).has_value()
    );

    // 查询没有领取或改变 RETRY 状态。
    EXPECT_EQ(store.stateCounts().retry, 2);
    EXPECT_EQ(store.stateCounts().sending, 0);
}

TEST_F(
    OutboxNextReadyTest,
    IgnoresSendingAckedAndDead
)
{
    edge::OutboxStore store(database);

    addEvent(store, 1);
    addEvent(store, 2);
    addEvent(store, 3);

    auto claimed = store.claimReady(kNow, 3);

    ASSERT_EQ(claimed.size(), 3U);

    std::vector<edge::OutboxClaimToken> tokens;

    for (const auto& record : claimed) {
        tokens.push_back(token(record));
    }

    const auto retried = store.scheduleRetry(
        tokens,
        kNow,
        kNow + 100,
        "test retry"
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxRetrySuccess>(
            retried
        )
    );

    claimed = store.claimReady(kNow + 100, 3);

    ASSERT_EQ(claimed.size(), 3U);

    const auto acked = store.acknowledge(
        {token(claimed[0])},
        kNow + 100
    );

    const auto dead = store.markDead(
        {token(claimed[1])},
        kNow + 100,
        "test conflict"
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxAckSuccess>(
            acked
        )
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxDeadSuccess>(
            dead
        )
    );

    // 三条记录均保留 next_attempt_at_ms = kNow + 100。
    // 但状态分别是 ACKED、DEAD、SENDING，都不能参与查询。
    EXPECT_FALSE(
        store.nextReadyAfter(kNow).has_value()
    );

    const auto counts = store.stateCounts();

    EXPECT_EQ(counts.acked, 1);
    EXPECT_EQ(counts.dead, 1);
    EXPECT_EQ(counts.sending, 1);
}

TEST_F(
    OutboxNextReadyTest,
    RejectsNonPositiveTime
)
{
    edge::OutboxStore store(database);

    EXPECT_THROW(
        store.nextReadyAfter(0),
        std::invalid_argument
    );

    EXPECT_THROW(
        store.nextReadyAfter(-1),
        std::invalid_argument
    );
}

} // namespace