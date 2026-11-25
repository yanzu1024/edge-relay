#include "edge/upload/RetryBackoff.h"

#include "edge/config/AgentConfig.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace {

TEST(RetryBackoffTest, CalculatesDefaultSequence)
{
    edge::UploadConfig config;

    const std::vector<std::int32_t> expected{
        500,
        1000,
        2000,
        4000,
        8000,
        10000,
        10000
    };

    const std::int64_t now_ms = 1000000;

    for (std::size_t index = 0; index < expected.size(); ++index) {
        const auto attempt =
            static_cast<std::int64_t>(index) + 1;

        SCOPED_TRACE(attempt);

        const auto schedule = edge::calculateRetrySchedule(
            config,
            attempt,
            now_ms
        );

        EXPECT_EQ(schedule.delay_ms, expected[index]);
        EXPECT_EQ(
            schedule.next_attempt_at_ms,
            now_ms + expected[index]
        );

        EXPECT_GT(schedule.next_attempt_at_ms, now_ms);
    }
}

TEST(RetryBackoffTest, CapsNonPowerOfTwoMaximum)
{
    edge::UploadConfig config;

    config.retry_initial_ms = 700;
    config.retry_max_ms = 5000;

    const std::vector<std::int32_t> expected{
        700,
        1400,
        2800,
        5000,
        5000
    };

    for (std::size_t index = 0; index < expected.size(); ++index) {
        const auto schedule = edge::calculateRetrySchedule(
            config,
            static_cast<std::int64_t>(index) + 1,
            1000
        );

        EXPECT_EQ(schedule.delay_ms, expected[index]);
        EXPECT_EQ(
            schedule.next_attempt_at_ms,
            1000LL + expected[index]
        );
    }
}

TEST(RetryBackoffTest, SupportsEqualInitialAndMaximum)
{
    edge::UploadConfig config;

    config.retry_initial_ms = 750;
    config.retry_max_ms = 750;

    const std::vector<std::int64_t> attempts{
        1,
        2,
        100,
        std::numeric_limits<std::int64_t>::max()
    };

    for (const auto attempt : attempts) {
        const auto schedule = edge::calculateRetrySchedule(
            config,
            attempt,
            1000
        );

        EXPECT_EQ(schedule.delay_ms, 750);
        EXPECT_EQ(schedule.next_attempt_at_ms, 1750);
    }
}

TEST(RetryBackoffTest, HandlesHugeAttemptCount)
{
    edge::UploadConfig config;

    config.retry_initial_ms = 1;
    config.retry_max_ms =
        std::numeric_limits<std::int32_t>::max();

    const auto schedule = edge::calculateRetrySchedule(
        config,
        std::numeric_limits<std::int64_t>::max(),
        1000000
    );

    EXPECT_EQ(
        schedule.delay_ms,
        std::numeric_limits<std::int32_t>::max()
    );

    EXPECT_EQ(
        schedule.next_attempt_at_ms,
        1000000LL
            + std::numeric_limits<std::int32_t>::max()
    );
}

TEST(RetryBackoffTest, SafelyDoublesNearInt32Limit)
{
    edge::UploadConfig config;

    const auto maximum =
        std::numeric_limits<std::int32_t>::max();

    config.retry_initial_ms = maximum - 1;
    config.retry_max_ms = maximum;

    const auto first = edge::calculateRetrySchedule(
        config,
        1,
        1000
    );

    EXPECT_EQ(first.delay_ms, maximum - 1);

    const auto second = edge::calculateRetrySchedule(
        config,
        2,
        1000
    );

    EXPECT_EQ(second.delay_ms, maximum);

    EXPECT_EQ(
        second.next_attempt_at_ms,
        1000LL + maximum
    );
}

TEST(RetryBackoffTest, RejectsInvalidIntervalConfiguration)
{
    const std::vector<
        std::pair<std::int32_t, std::int32_t>
    > invalid_limits{
        {0, 1000},
        {-1, 1000},
        {500, 0},
        {500, -1},
        {1001, 1000}
    };

    for (const auto& limits : invalid_limits) {
        edge::UploadConfig config;

        config.retry_initial_ms = limits.first;
        config.retry_max_ms = limits.second;

        EXPECT_THROW(
            edge::calculateRetrySchedule(config, 1, 1000),
            std::invalid_argument
        );
    }
}

TEST(RetryBackoffTest, WarningThresholdDoesNotLimitRetries)
{
    edge::UploadConfig config;

    config.failure_warn_attempts = 2;

    const auto third = edge::calculateRetrySchedule(
        config,
        3,
        1000
    );

    EXPECT_EQ(third.delay_ms, 2000);
    EXPECT_EQ(third.next_attempt_at_ms, 3000);

    const auto later = edge::calculateRetrySchedule(
        config,
        100,
        1000
    );

    EXPECT_EQ(later.delay_ms, 10000);
    EXPECT_EQ(later.next_attempt_at_ms, 11000);
}

TEST(RetryBackoffTest, RejectsInvalidAttemptAndTimestamp)
{
    edge::UploadConfig config;

    EXPECT_THROW(
        edge::calculateRetrySchedule(config, 0, 1000),
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::calculateRetrySchedule(config, -1, 1000),
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::calculateRetrySchedule(config, 1, 0),
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::calculateRetrySchedule(config, 1, -1),
        std::invalid_argument
    );
}

TEST(RetryBackoffTest, AllowsLastRepresentableDeadline)
{
    edge::UploadConfig config;

    const auto maximum =
        std::numeric_limits<std::int64_t>::max();

    const auto schedule = edge::calculateRetrySchedule(
        config,
        1,
        maximum - 500
    );

    EXPECT_EQ(schedule.delay_ms, 500);
    EXPECT_EQ(schedule.next_attempt_at_ms, maximum);
}

TEST(RetryBackoffTest, RejectsUnrepresentableDeadline)
{
    edge::UploadConfig config;

    const auto maximum =
        std::numeric_limits<std::int64_t>::max();

    EXPECT_THROW(
        edge::calculateRetrySchedule(
            config,
            1,
            maximum - 499
        ),
        std::overflow_error
    );

    EXPECT_THROW(
        edge::calculateRetrySchedule(
            config,
            1,
            maximum
        ),
        std::overflow_error
    );

    EXPECT_THROW(
        edge::calculateRetrySchedule(
            config,
            6,
            maximum - 9999
        ),
        std::overflow_error
    );
}

} // namespace