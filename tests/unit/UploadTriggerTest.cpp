#include "edge/upload/UploadTrigger.h"

#include "edge/config/AgentConfig.h"

#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

namespace {

using Clock = edge::UploadTrigger::Clock;

Clock::time_point at(std::int64_t milliseconds)
{
    return Clock::time_point{}
        + std::chrono::duration_cast<Clock::duration>(
            std::chrono::milliseconds(milliseconds)
        );
}

TEST(UploadTriggerTest, RejectsInvalidConfiguration)
{
    const std::vector<
        std::pair<std::int32_t, std::int32_t>
    > invalid{
        {0, 1000},
        {-1, 1000},
        {101, 1000},
        {20, 0},
        {20, -1}
    };

    for (const auto& values : invalid) {
        edge::UploadConfig config;

        config.batch_size = values.first;
        config.flush_interval_ms = values.second;

        EXPECT_THROW(
            (edge::UploadTrigger{config}),
            std::invalid_argument
        );
    }
}

TEST(UploadTriggerTest, EmptyQueueDoesNotTrigger)
{
    edge::UploadConfig config;
    edge::UploadTrigger trigger(config);

    EXPECT_FALSE(trigger.shouldUpload(0, at(0)));
    EXPECT_FALSE(trigger.shouldUpload(0, at(1000)));
    EXPECT_FALSE(trigger.shouldUpload(0, at(100000)));
}

TEST(UploadTriggerTest, FullBatchTriggersImmediately)
{
    edge::UploadConfig config;

    config.batch_size = 20;
    config.flush_interval_ms = 1000;

    edge::UploadTrigger trigger(config);

    EXPECT_TRUE(trigger.shouldUpload(20, at(0)));
    EXPECT_TRUE(trigger.shouldUpload(21, at(0)));
}

TEST(UploadTriggerTest, PartialBatchWaitsUntilExactDeadline)
{
    edge::UploadConfig config;

    config.batch_size = 20;
    config.flush_interval_ms = 1000;

    edge::UploadTrigger trigger(config);

    EXPECT_FALSE(trigger.shouldUpload(1, at(0)));
    EXPECT_FALSE(trigger.shouldUpload(1, at(999)));
    EXPECT_TRUE(trigger.shouldUpload(1, at(1000)));
}

TEST(UploadTriggerTest, MoreArrivalsDoNotExtendDeadline)
{
    edge::UploadConfig config;

    config.batch_size = 20;
    config.flush_interval_ms = 1000;

    edge::UploadTrigger trigger(config);

    EXPECT_FALSE(trigger.shouldUpload(1, at(0)));
    EXPECT_FALSE(trigger.shouldUpload(2, at(700)));

    // 后到事件不能把截止时间推迟到 1700。
    EXPECT_TRUE(trigger.shouldUpload(3, at(1000)));
}

TEST(UploadTriggerTest, EmptyQueueRestartsWaitingWindow)
{
    edge::UploadConfig config;

    config.batch_size = 20;
    config.flush_interval_ms = 1000;

    edge::UploadTrigger trigger(config);

    EXPECT_FALSE(trigger.shouldUpload(1, at(0)));

    // 队列变空，旧计时失效。
    EXPECT_FALSE(trigger.shouldUpload(0, at(500)));

    EXPECT_FALSE(trigger.shouldUpload(1, at(1000)));
    EXPECT_FALSE(trigger.shouldUpload(1, at(1999)));
    EXPECT_TRUE(trigger.shouldUpload(1, at(2000)));
}

TEST(UploadTriggerTest, CompletedCycleRestartsWaitingWindow)
{
    edge::UploadConfig config;

    config.batch_size = 20;
    config.flush_interval_ms = 1000;

    edge::UploadTrigger trigger(config);

    EXPECT_FALSE(trigger.shouldUpload(1, at(0)));

    // 等待期间凑满一批，立即发送。
    EXPECT_TRUE(trigger.shouldUpload(20, at(300)));

    trigger.onCycleFinished();

    // 模拟上传完成后发现下一批不足一批。
    EXPECT_FALSE(trigger.shouldUpload(1, at(600)));
    EXPECT_FALSE(trigger.shouldUpload(1, at(1599)));
    EXPECT_TRUE(trigger.shouldUpload(1, at(1600)));
}

TEST(UploadTriggerTest, BatchSizeOneNeedsNoAggregationWait)
{
    edge::UploadConfig config;

    config.batch_size = 1;
    config.flush_interval_ms = 1000;

    edge::UploadTrigger trigger(config);

    EXPECT_TRUE(trigger.shouldUpload(1, at(0)));

    trigger.onCycleFinished();

    EXPECT_TRUE(trigger.shouldUpload(1, at(100)));
}

TEST(UploadTriggerTest, DueDecisionRemainsUntilCycleFinishes)
{
    edge::UploadConfig config;

    config.batch_size = 20;
    config.flush_interval_ms = 1000;

    edge::UploadTrigger trigger(config);

    EXPECT_FALSE(trigger.shouldUpload(1, at(0)));
    EXPECT_TRUE(trigger.shouldUpload(1, at(1000)));

    // 单纯查询不能隐式清除到期状态。
    EXPECT_TRUE(trigger.shouldUpload(1, at(1100)));

    trigger.onCycleFinished();

    EXPECT_FALSE(trigger.shouldUpload(1, at(1200)));
    EXPECT_TRUE(trigger.shouldUpload(1, at(2200)));
}

TEST(UploadTriggerTest, DeadlineDoesNotWrapNearClockMaximum)
{
    edge::UploadConfig config;

    config.batch_size = 20;
    config.flush_interval_ms = 1000;

    edge::UploadTrigger trigger(config);

    const auto maximum = Clock::time_point::max();
    const auto start =
        maximum - std::chrono::milliseconds(500);

    EXPECT_FALSE(trigger.shouldUpload(1, start));

    EXPECT_FALSE(
        trigger.shouldUpload(
            1,
            maximum - std::chrono::milliseconds(1)
        )
    );

    EXPECT_TRUE(trigger.shouldUpload(1, maximum));
}

} // namespace