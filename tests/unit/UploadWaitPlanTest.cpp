#include "edge/config/AgentConfig.h"
#include "edge/upload/UploadWaitPlan.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>

namespace {

using Clock = edge::UploadTrigger::Clock;
using namespace std::chrono_literals;

constexpr std::int64_t kNow = 1794441600000LL;

edge::UploadConfig makeConfig()
{
    edge::UploadConfig config;

    config.batch_size = 3;
    config.flush_interval_ms = 1000;

    return config;
}

class UploadWaitPlanTest : public testing::Test {
protected:
    edge::UploadWaitPlan plan(
        std::size_t ready_count,
        std::optional<std::int64_t> future = std::nullopt,
        std::int64_t wall = kNow,
        Clock::time_point steady = Clock::time_point{}
    )
    {
        return edge::makeUploadWaitPlan(
            trigger,
            ready_count,
            future,
            wall,
            steady
        );
    }

    edge::UploadTrigger trigger{makeConfig()};

    const Clock::time_point t0{};
};

TEST_F(
    UploadWaitPlanTest,
    IdleWaitsForNotification
)
{
    const auto result = plan(0);

    EXPECT_FALSE(result.upload_now);
    EXPECT_FALSE(result.deadline.has_value());
}

TEST_F(
    UploadWaitPlanTest,
    FullBatchUploadsImmediately
)
{
    const auto result = plan(3);

    EXPECT_TRUE(result.upload_now);
    EXPECT_FALSE(result.deadline.has_value());
}

TEST_F(
    UploadWaitPlanTest,
    PartialDeadlineDoesNotMove
)
{
    const auto first = plan(1);

    ASSERT_TRUE(first.deadline.has_value());

    EXPECT_EQ(
        *first.deadline,
        t0 + 1000ms
    );

    const auto second = plan(
        2,
        std::nullopt,
        kNow + 200,
        t0 + 200ms
    );

    EXPECT_FALSE(second.upload_now);
    EXPECT_EQ(second.deadline, first.deadline);

    const auto due = plan(
        2,
        std::nullopt,
        kNow + 1000,
        t0 + 1000ms
    );

    EXPECT_TRUE(due.upload_now);
}

TEST_F(
    UploadWaitPlanTest,
    FutureRetryCanFillBatchEarlier
)
{
    const auto result = plan(
        1,
        kNow + 200
    );

    EXPECT_FALSE(result.upload_now);

    ASSERT_TRUE(result.deadline.has_value());

    EXPECT_EQ(
        *result.deadline,
        t0 + 200ms
    );

    // 聚合截止仍然是原来的 1000ms。
    // 提前醒来只是为了重新检查到期记录。
    ASSERT_TRUE(trigger.nextDeadline().has_value());

    EXPECT_EQ(
        *trigger.nextDeadline(),
        t0 + 1000ms
    );
}

TEST_F(
    UploadWaitPlanTest,
    LongRetryUsesClockRecheck
)
{
    const auto result = plan(
        0,
        std::numeric_limits<std::int64_t>::max()
    );

    EXPECT_FALSE(result.upload_now);

    ASSERT_TRUE(result.deadline.has_value());

    EXPECT_EQ(
        *result.deadline,
        t0 + 1000ms
    );
}

TEST_F(
    UploadWaitPlanTest,
    IdleClearsOldAggregationDeadline
)
{
    (void)plan(1);

    const auto idle = plan(
        0,
        kNow + 500,
        kNow + 200,
        t0 + 200ms
    );

    EXPECT_FALSE(trigger.nextDeadline().has_value());

    ASSERT_TRUE(idle.deadline.has_value());

    EXPECT_EQ(
        *idle.deadline,
        t0 + 500ms
    );

    const auto later = plan(
        1,
        std::nullopt,
        kNow + 600,
        t0 + 600ms
    );

    ASSERT_TRUE(later.deadline.has_value());

    EXPECT_EQ(
        *later.deadline,
        t0 + 1600ms
    );
}

TEST_F(
    UploadWaitPlanTest,
    CycleCompletionStartsNewAggregation
)
{
    (void)plan(1);

    ASSERT_TRUE(
        plan(
            1,
            std::nullopt,
            kNow + 1000,
            t0 + 1000ms
        ).upload_now
    );

    trigger.onCycleFinished();

    const auto next = plan(
        1,
        std::nullopt,
        kNow + 1100,
        t0 + 1100ms
    );

    EXPECT_FALSE(next.upload_now);

    ASSERT_TRUE(next.deadline.has_value());

    EXPECT_EQ(
        *next.deadline,
        t0 + 2100ms
    );
}

TEST_F(
    UploadWaitPlanTest,
    RejectsInvalidWallTimes
)
{
    EXPECT_THROW(
        plan(1, std::nullopt, 0),
        std::invalid_argument
    );

    EXPECT_THROW(
        plan(1, kNow),
        std::invalid_argument
    );

    EXPECT_THROW(
        plan(1, kNow - 1),
        std::invalid_argument
    );

    // 参数错误不会先建立聚合计时。
    EXPECT_FALSE(trigger.nextDeadline().has_value());
}

TEST_F(
    UploadWaitPlanTest,
    SaturatesSteadyDeadline
)
{
    const auto near_maximum =
        Clock::time_point::max() - 1ms;

    const auto result = plan(
        0,
        kNow + 500,
        kNow,
        near_maximum
    );

    ASSERT_TRUE(result.deadline.has_value());

    EXPECT_EQ(
        *result.deadline,
        Clock::time_point::max()
    );
}

} // namespace