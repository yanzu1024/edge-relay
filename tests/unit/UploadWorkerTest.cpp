#include "edge/upload/UploadWorker.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <stdexcept>
#include <thread>

namespace {

using Worker = edge::UploadWorker;
using namespace std::chrono_literals;

TEST(UploadWorkerTest, RejectsEmptyTask)
{
    EXPECT_THROW(
        (Worker{Worker::Task{}}),
        std::invalid_argument
    );
}

TEST(UploadWorkerTest, StartsOnAnotherThreadAndStops)
{
    std::thread::id task_thread;
    bool stopped = false;

    Worker worker([&](Worker::Context& context) {
        task_thread = std::this_thread::get_id();

        context.signalReady();
        context.waitForStop();

        stopped = context.stopRequested();
    });

    worker.start();

    const auto running = worker.status();

    EXPECT_TRUE(running.ready);
    EXPECT_FALSE(running.finished);
    EXPECT_FALSE(static_cast<bool>(running.failure));

    worker.requestStop();
    worker.join();

    EXPECT_NE(
        task_thread,
        std::this_thread::get_id()
    );

    EXPECT_TRUE(stopped);
    EXPECT_TRUE(worker.status().finished);

    EXPECT_FALSE(
        static_cast<bool>(worker.status().failure)
    );
}

TEST(UploadWorkerTest, PropagatesInitializationFailure)
{
    Worker worker([](Worker::Context&) {
        throw std::runtime_error(
            "initialization failed"
        );
    });

    try {
        worker.start();

        FAIL() << "start should have thrown";
    }
    catch (const std::runtime_error& error) {
        EXPECT_STREQ(
            error.what(),
            "initialization failed"
        );
    }

    EXPECT_FALSE(worker.status().ready);
    EXPECT_TRUE(worker.status().finished);

    EXPECT_TRUE(
        static_cast<bool>(worker.status().failure)
    );

    worker.join();
}

TEST(UploadWorkerTest, RejectsReturnBeforeReady)
{
    Worker worker([](Worker::Context&) {
    });

    EXPECT_THROW(
        worker.start(),
        std::runtime_error
    );

    EXPECT_TRUE(worker.status().finished);
}

TEST(UploadWorkerTest, CapturesRuntimeFailureAfterReady)
{
    std::promise<void> release;
    auto gate = release.get_future();

    Worker worker([&](Worker::Context& context) {
        context.signalReady();

        if (gate.wait_for(2s)
            != std::future_status::ready) {
            throw std::runtime_error(
                "test gate timed out"
            );
        }

        throw std::runtime_error(
            "runtime failed"
        );
    });

    worker.start();

    release.set_value();

    worker.join();

    const auto result = worker.status();

    EXPECT_TRUE(result.ready);
    EXPECT_TRUE(result.finished);

    ASSERT_TRUE(
        static_cast<bool>(result.failure)
    );

    try {
        std::rethrow_exception(result.failure);

        FAIL() << "failure should have been rethrown";
    }
    catch (const std::runtime_error& error) {
        EXPECT_STREQ(
            error.what(),
            "runtime failed"
        );
    }
}

TEST(UploadWorkerTest, CapturesNonStandardException)
{
    Worker worker([](Worker::Context&) {
        throw 42;
    });

    EXPECT_THROW(
        worker.start(),
        int
    );

    EXPECT_TRUE(worker.status().finished);
}

TEST(UploadWorkerTest, StopBeforeWaitIsNotLost)
{
    std::promise<void> release;
    auto gate = release.get_future();

    Worker worker([&](Worker::Context& context) {
        context.signalReady();

        if (gate.wait_for(2s)
            != std::future_status::ready) {
            throw std::runtime_error(
                "test gate timed out"
            );
        }

        // 主线程已经请求停止，此时才进入条件变量等待。
        context.waitForStop();
    });

    worker.start();

    worker.requestStop();

    release.set_value();

    worker.join();

    EXPECT_FALSE(
        static_cast<bool>(worker.status().failure)
    );
}

TEST(
    UploadWorkerTest,
    DestructorStopsAndDestroysResourcesOnTaskThread
)
{
    struct Resource {
        std::thread::id& destroyed_on;

        ~Resource()
        {
            destroyed_on = std::this_thread::get_id();
        }
    };

    std::thread::id created_on;
    std::thread::id destroyed_on;

    {
        Worker worker([&](Worker::Context& context) {
            created_on = std::this_thread::get_id();

            Resource resource{destroyed_on};

            context.signalReady();
            context.waitForStop();
        });

        worker.start();

        // 不显式调用 stop/join，验证 owner 析构路径。
    }

    EXPECT_NE(
        created_on,
        std::this_thread::get_id()
    );

    EXPECT_EQ(
        created_on,
        destroyed_on
    );
}

TEST(UploadWorkerTest, RejectsUnexpectedExitAfterReady)
{
    Worker worker([](Worker::Context& context) {
        context.signalReady();

        // 未收到停止请求就返回，应被识别为异常退出。
    });

    worker.start();
    worker.join();

    EXPECT_TRUE(worker.status().finished);

    EXPECT_TRUE(
        static_cast<bool>(worker.status().failure)
    );
}

TEST(UploadWorkerTest, StartsOnlyOnceAndJoinIsRepeatable)
{
    Worker worker([](Worker::Context& context) {
        context.signalReady();
        context.waitForStop();
    });

    worker.start();

    EXPECT_THROW(
        worker.start(),
        std::logic_error
    );

    worker.requestStop();
    worker.requestStop();

    worker.join();
    worker.join();

    EXPECT_THROW(
        worker.start(),
        std::logic_error
    );

    EXPECT_FALSE(
        static_cast<bool>(worker.status().failure)
    );
}

} // namespace