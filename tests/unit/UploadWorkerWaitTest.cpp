#include "edge/upload/UploadWorker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <future>
#include <optional>
#include <stdexcept>
#include <utility>

namespace {

using Worker = edge::UploadWorker;
using Context = Worker::Context;
using Result = Worker::WaitResult;
using Clock = Worker::Clock;

using namespace std::chrono_literals;

struct Observation {
    Result reason;
    Clock::time_point returned_at;
};

using WaitFunction = std::function<Result(Context&)>;

// 统一测试任务结构：
// 就绪 -> 执行待测试的等待 -> 返回观察结果 -> 等待停止。
Worker::Task waitingTask(
    std::promise<Observation>& output,
    WaitFunction wait_function
)
{
    return [
        &output,
        wait_function = std::move(wait_function)
    ](Context& context) {
        context.signalReady();

        const auto reason = wait_function(context);

        output.set_value(Observation{
            reason,
            Clock::now()
        });

        context.waitForStop();
    };
}

// 先观察等待是否按预期结束，再停止并回收工作线程。
// 即使被测等待没有正确唤醒，也会请求停止来完成收尾。
std::optional<Observation> finishObservation(
    Worker& worker,
    std::future<Observation>& result
)
{
    const bool completed =
        result.wait_for(1s) == std::future_status::ready;

    worker.requestStop();
    worker.join();

    EXPECT_FALSE(
        static_cast<bool>(worker.status().failure)
    );

    if (!completed) {
        ADD_FAILURE()
            << "Worker did not produce the expected wait result";

        return std::nullopt;
    }

    return result.get();
}

void waitForGate(std::future<void>& gate)
{
    if (gate.wait_for(2s)
        != std::future_status::ready) {
        throw std::runtime_error(
            "Test gate timed out"
        );
    }
}

TEST(
    UploadWorkerWaitTest,
    NotificationBeforeWaitIsNotLost
)
{
    std::promise<void> release;
    auto gate = release.get_future();

    std::promise<Observation> output;
    auto result = output.get_future();

    Worker worker(waitingTask(
        output,
        [&](Context& context) {
            waitForGate(gate);

            return context.waitForWork();
        }
    ));

    worker.start();

    // 工作线程尚未进入 waitForWork。
    worker.notifyWork();

    release.set_value();

    const auto observation = finishObservation(
        worker,
        result
    );

    ASSERT_TRUE(observation.has_value());

    EXPECT_EQ(
        observation->reason,
        Result::Notified
    );
}

TEST(
    UploadWorkerWaitTest,
    MultipleNotificationsAreCoalesced
)
{
    std::promise<void> release;
    auto gate = release.get_future();

    std::promise<Observation> output;
    auto result = output.get_future();

    Result first = Result::StopRequested;
    Clock::time_point second_deadline{};

    Worker worker(waitingTask(
        output,
        [&](Context& context) {
            waitForGate(gate);

            first = context.waitForWork();

            second_deadline = Clock::now() + 40ms;

            // 三次通知已被第一次等待合并消费。
            // 第二次等待应当等到截止时间。
            return context.waitUntil(second_deadline);
        }
    ));

    worker.start();

    worker.notifyWork();
    worker.notifyWork();
    worker.notifyWork();

    release.set_value();

    const auto observation = finishObservation(
        worker,
        result
    );

    ASSERT_TRUE(observation.has_value());

    EXPECT_EQ(first, Result::Notified);

    EXPECT_EQ(
        observation->reason,
        Result::Deadline
    );

    EXPECT_GE(
        observation->returned_at,
        second_deadline
    );
}

TEST(
    UploadWorkerWaitTest,
    ReturnsDeadlineWhenTimeExpires
)
{
    const auto deadline = Clock::now() + 60ms;

    std::promise<Observation> output;
    auto result = output.get_future();

    Worker worker(waitingTask(
        output,
        [deadline](Context& context) {
            return context.waitUntil(deadline);
        }
    ));

    worker.start();

    const auto observation = finishObservation(
        worker,
        result
    );

    ASSERT_TRUE(observation.has_value());

    EXPECT_EQ(
        observation->reason,
        Result::Deadline
    );

    EXPECT_GE(
        observation->returned_at,
        deadline
    );
}

TEST(
    UploadWorkerWaitTest,
    NotificationWakesLongDeadlineWait
)
{
    std::promise<Observation> output;
    auto result = output.get_future();

    Worker worker(waitingTask(
        output,
        [](Context& context) {
            return context.waitUntil(
                Clock::now() + std::chrono::hours(1)
            );
        }
    ));

    worker.start();

    // 尚无工作、尚未到期，不应自行返回。
    EXPECT_EQ(
        result.wait_for(20ms),
        std::future_status::timeout
    );

    worker.notifyWork();

    const auto observation = finishObservation(
        worker,
        result
    );

    ASSERT_TRUE(observation.has_value());

    EXPECT_EQ(
        observation->reason,
        Result::Notified
    );
}

TEST(
    UploadWorkerWaitTest,
    StopWakesIdleWait
)
{
    std::promise<Observation> output;
    auto result = output.get_future();

    Worker worker(waitingTask(
        output,
        [](Context& context) {
            return context.waitForWork();
        }
    ));

    worker.start();

    EXPECT_EQ(
        result.wait_for(20ms),
        std::future_status::timeout
    );

    worker.requestStop();

    const auto observation = finishObservation(
        worker,
        result
    );

    ASSERT_TRUE(observation.has_value());

    EXPECT_EQ(
        observation->reason,
        Result::StopRequested
    );
}

TEST(
    UploadWorkerWaitTest,
    StopWakesLongDeadlineWait
)
{
    std::promise<Observation> output;
    auto result = output.get_future();

    Worker worker(waitingTask(
        output,
        [](Context& context) {
            return context.waitUntil(
                Clock::now() + std::chrono::hours(1)
            );
        }
    ));

    worker.start();

    EXPECT_EQ(
        result.wait_for(20ms),
        std::future_status::timeout
    );

    worker.requestStop();

    const auto observation = finishObservation(
        worker,
        result
    );

    ASSERT_TRUE(observation.has_value());

    EXPECT_EQ(
        observation->reason,
        Result::StopRequested
    );
}

TEST(
    UploadWorkerWaitTest,
    StopHasPriorityOverNotificationAndDeadline
)
{
    std::promise<void> release;
    auto gate = release.get_future();

    std::promise<Observation> output;
    auto result = output.get_future();

    Worker worker(waitingTask(
        output,
        [&](Context& context) {
            waitForGate(gate);

            return context.waitUntil(
                Clock::now() - 1ms
            );
        }
    ));

    worker.start();

    // 工作通知、停止和过期截止时间同时存在。
    worker.notifyWork();
    worker.requestStop();

    release.set_value();

    const auto observation = finishObservation(
        worker,
        result
    );

    ASSERT_TRUE(observation.has_value());

    EXPECT_EQ(
        observation->reason,
        Result::StopRequested
    );
}

TEST(
    UploadWorkerWaitTest,
    NotificationHasPriorityOverExpiredDeadline
)
{
    std::promise<void> release;
    auto gate = release.get_future();

    std::promise<Observation> output;
    auto result = output.get_future();

    Worker worker(waitingTask(
        output,
        [&](Context& context) {
            waitForGate(gate);

            return context.waitUntil(
                Clock::now() - 1ms
            );
        }
    ));

    worker.start();

    worker.notifyWork();

    release.set_value();

    const auto observation = finishObservation(
        worker,
        result
    );

    ASSERT_TRUE(observation.has_value());

    EXPECT_EQ(
        observation->reason,
        Result::Notified
    );
}

} // namespace