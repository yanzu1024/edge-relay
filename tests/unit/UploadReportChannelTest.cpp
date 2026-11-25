#include "edge/upload/UploadReportChannel.h"
#include "edge/upload/UploadWorker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <future>
#include <optional>
#include <variant>

namespace {

using Channel = edge::UploadReportChannel;
using Worker = edge::UploadWorker;

using namespace std::chrono_literals;

edge::UploadCycleResult makeReport(
    std::int64_t records
)
{
    edge::UploadCycleResult report;

    report.claimed_records = records;
    report.acked_records = records;

    return report;
}

// 必须声明在 Worker 对象之后。
// 这样异常或 ASSERT 提前返回时，先关闭通道，
// 再由 Worker 析构请求停止并 join。
struct CloseChannelOnExit {
    Channel& channel;

    ~CloseChannelOnExit()
    {
        channel.close();
    }
};

// 有界观察操作结果。
// 若等待失败，先关闭通道，避免生产者仍阻塞在 publish。
std::optional<bool> finishChannelOperation(
    Channel& channel,
    Worker& worker,
    std::future<bool>& result
)
{
    const bool finished =
        result.wait_for(1s) == std::future_status::ready;

    if (!finished) {
        channel.close();
    }

    worker.requestStop();
    worker.join();

    EXPECT_FALSE(
        static_cast<bool>(worker.status().failure)
    );

    if (!finished) {
        ADD_FAILURE()
            << "Channel operation did not finish";

        return std::nullopt;
    }

    return result.get();
}

TEST(
    UploadReportChannelTest,
    EmptyTryTakeReturnsEmpty
)
{
    Channel channel;

    EXPECT_FALSE(
        channel.tryTake().has_value()
    );
}

TEST(
    UploadReportChannelTest,
    CopiesPublishedReport
)
{
    Channel channel;

    auto original = makeReport(2);

    ASSERT_TRUE(channel.publish(original));

    // 修改调用者对象，不能改变已经交付的报告。
    original.claimed_records = 99;
    original.acked_records = 99;

    const auto taken = channel.tryTake();

    ASSERT_TRUE(taken.has_value());

    EXPECT_EQ(taken->claimed_records, 2);
    EXPECT_EQ(taken->acked_records, 2);

    EXPECT_TRUE(
        std::holds_alternative<std::monostate>(
            taken->failure
        )
    );

    EXPECT_FALSE(
        channel.tryTake().has_value()
    );
}

TEST(
    UploadReportChannelTest,
    CopiesFailureDetails
)
{
    Channel channel;

    auto original = makeReport(2);

    original.acked_records = 0;
    original.retry_records = 2;

    edge::BatchHttpError error{};
    error.status_code = 503;

    original.failure = error;

    ASSERT_TRUE(channel.publish(original));

    std::get<edge::BatchHttpError>(
        original.failure
    ).status_code = 500;

    const auto taken = channel.tryTake();

    ASSERT_TRUE(taken.has_value());

    EXPECT_EQ(taken->claimed_records, 2);
    EXPECT_EQ(taken->retry_records, 2);

    const auto* copied_error =
        std::get_if<edge::BatchHttpError>(
            &taken->failure
        );

    ASSERT_NE(copied_error, nullptr);

    EXPECT_EQ(copied_error->status_code, 503);
}

TEST(
    UploadReportChannelTest,
    SecondPublisherWaitsForFreeSlot
)
{
    Channel channel;

    ASSERT_TRUE(channel.publish(makeReport(1)));

    std::promise<bool> published;
    auto result = published.get_future();

    Worker worker([&](Worker::Context& context) {
        context.signalReady();

        const bool accepted = channel.publish(
            makeReport(2)
        );

        published.set_value(accepted);

        context.waitForStop();
    });

    CloseChannelOnExit cleanup{channel};

    worker.start();

    // 第一份报告尚未取走，第二次发布不能完成。
    EXPECT_EQ(
        result.wait_for(30ms),
        std::future_status::timeout
    );

    const auto first = channel.tryTake();

    const auto accepted = finishChannelOperation(
        channel,
        worker,
        result
    );

    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->claimed_records, 1);

    ASSERT_TRUE(accepted.has_value());
    EXPECT_TRUE(*accepted);

    const auto second = channel.tryTake();

    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(second->claimed_records, 2);

    EXPECT_FALSE(
        channel.tryTake().has_value()
    );
}

TEST(
    UploadReportChannelTest,
    CloseUnblocksPublisherAndKeepsQueuedReport
)
{
    Channel channel;

    ASSERT_TRUE(channel.publish(makeReport(1)));

    std::promise<bool> published;
    auto result = published.get_future();

    Worker worker([&](Worker::Context& context) {
        context.signalReady();

        const bool accepted = channel.publish(
            makeReport(2)
        );

        published.set_value(accepted);

        context.waitForStop();
    });

    CloseChannelOnExit cleanup{channel};

    worker.start();

    EXPECT_EQ(
        result.wait_for(30ms),
        std::future_status::timeout
    );

    channel.close();

    const auto accepted = finishChannelOperation(
        channel,
        worker,
        result
    );

    ASSERT_TRUE(accepted.has_value());

    // 第二份报告没有被通道接收。
    EXPECT_FALSE(*accepted);

    // 第一份已经接收的报告仍然可以取走。
    const auto first = channel.tryTake();

    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->claimed_records, 1);

    EXPECT_FALSE(
        channel.tryTake().has_value()
    );
}

TEST(
    UploadReportChannelTest,
    CloseUnblocksWaitingConsumer
)
{
    Channel channel;

    std::promise<bool> received;
    auto result = received.get_future();

    Worker worker([&](Worker::Context& context) {
        context.signalReady();

        const auto report = channel.waitTakeUntil(
            Channel::Clock::now()
                + std::chrono::hours(1)
        );

        received.set_value(report.has_value());

        context.waitForStop();
    });

    CloseChannelOnExit cleanup{channel};

    worker.start();

    EXPECT_EQ(
        result.wait_for(30ms),
        std::future_status::timeout
    );

    channel.close();

    const auto had_report = finishChannelOperation(
        channel,
        worker,
        result
    );

    ASSERT_TRUE(had_report.has_value());

    EXPECT_FALSE(*had_report);
}

TEST(
    UploadReportChannelTest,
    DeadlineExpiresWithoutReport
)
{
    Channel channel;

    const auto deadline =
        Channel::Clock::now() + 30ms;

    const auto report = channel.waitTakeUntil(
        deadline
    );

    EXPECT_FALSE(report.has_value());

    EXPECT_GE(
        Channel::Clock::now(),
        deadline
    );

    // 超时不代表关闭，之后仍能发布和读取。
    ASSERT_TRUE(channel.publish(makeReport(3)));

    const auto later = channel.tryTake();

    ASSERT_TRUE(later.has_value());
    EXPECT_EQ(later->claimed_records, 3);
}

TEST(
    UploadReportChannelTest,
    RejectsPublishAfterClose
)
{
    Channel channel;

    channel.close();
    channel.close();

    EXPECT_FALSE(
        channel.publish(makeReport(1))
    );

    EXPECT_FALSE(
        channel.tryTake().has_value()
    );

    // 已关闭且没有报告时，不应等待到远处截止时间。
    EXPECT_FALSE(
        channel.waitTakeUntil(
            Channel::Clock::now()
                + std::chrono::hours(1)
        ).has_value()
    );
}

} // namespace