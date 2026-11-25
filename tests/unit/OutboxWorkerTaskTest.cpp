#include "edge/upload/OutboxWorkerTask.h"

#include "edge/domain/SensorEvent.h"
#include "edge/storage/OutboxDatabase.h"
#include "edge/storage/OutboxStore.h"
#include "edge/storage/SqliteStatement.h"
#include "edge/storage/SqliteTransaction.h"
#include "edge/upload/OutboxUploader.h"
#include "edge/domain/EventId.h"
#include "edge/upload/OutboxUploadLoop.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <iterator>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <variant>
#include <vector>
#include <atomic>
#include <sqlite3.h>
#include <exception>
#include <functional>

namespace {

using namespace std::chrono_literals;

using Context = edge::UploadWorker::Context;

constexpr std::int64_t kNow = 1794441600000LL;

class TemporaryDirectory {
public:
    TemporaryDirectory()
    {
        const auto pattern = (
            std::filesystem::temp_directory_path()
            / "edge-outbox-worker-XXXXXX"
        ).string();

        std::vector<char> writable(
            pattern.begin(),
            pattern.end()
        );

        writable.push_back('\0');

        const auto* created = ::mkdtemp(writable.data());

        if (created == nullptr) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "mkdtemp"
            );
        }

        path_ = created;
    }

    ~TemporaryDirectory()
    {
        std::error_code ignored;

        std::filesystem::remove_all(
            path_,
            ignored
        );
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;

    TemporaryDirectory& operator=(
        const TemporaryDirectory&
    ) = delete;

    edge::StorageConfig storage() const
    {
        edge::StorageConfig config;

        config.path = path_ / "outbox.db";

        return config;
    }

private:
    std::filesystem::path path_;
};

edge::SensorEvent makeEvent()
{
    edge::SensorEvent event{};

    event.schema_version = 1;
    event.device_id = "runtime_sensor";
    event.session_id =
        "12345678-1234-1234-1234-123456789abc";
    event.sequence = 1;
    event.observed_at_ms = kNow;
    event.metric = "temperature";
    event.value = 23.5;
    event.unit = "degC";

    return event;
}

void executeSql(
    edge::SqliteDatabase& database,
    const std::string& sql
)
{
    edge::SqliteStatement statement(database, sql);

    if (statement.step() != edge::SqliteStepResult::Done) {
        throw std::runtime_error(
            "Test SQL unexpectedly returned rows"
        );
    }
}

bool hasFailure(const edge::UploadWorker& worker)
{
    return static_cast<bool>(
        worker.status().failure
    );
}

TEST(OutboxWorkerTaskTest, RejectsEmptyBody)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    EXPECT_THROW(
        edge::makeOutboxWorkerTask(
            storage,
            edge::UploadConfig{},
            edge::OutboxWorkerBody{}
        ),
        std::invalid_argument
    );

    EXPECT_FALSE(
        std::filesystem::exists(storage.path)
    );
}

TEST(
    OutboxWorkerTaskTest,
    OpensExistingOutboxAndBuildsUploader
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    auto database = edge::openOutboxDatabase(
        storage.path
    );

    edge::OutboxStore main_store(database, storage);

    std::thread::id body_thread;

    std::int64_t total = -1;
    std::int64_t claimed = -1;

    edge::UploadWorker worker(
        edge::makeOutboxWorkerTask(
            storage,
            edge::UploadConfig{},
            [&](Context& context,
                edge::OutboxStore& store,
                edge::OutboxUploader& uploader) {
                body_thread = std::this_thread::get_id();

                total = store.stateCounts().total;

                // 空 Outbox 不调用 HTTP 发送函数。
                claimed = uploader.runOnce().claimed_records;

                context.waitForStop();
            }
        )
    );

    worker.start();

    EXPECT_TRUE(worker.status().ready);

    worker.requestStop();
    worker.join();

    EXPECT_FALSE(hasFailure(worker));

    EXPECT_NE(
        body_thread,
        std::this_thread::get_id()
    );

    EXPECT_EQ(total, 0);
    EXPECT_EQ(claimed, 0);

    EXPECT_EQ(
        main_store.stateCounts().total,
        0
    );
}

TEST(
    OutboxWorkerTaskTest,
    UsesIndependentConnection
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    auto database = edge::openOutboxDatabase(
        storage.path
    );

    edge::OutboxStore main_store(database, storage);

    const auto inserted = main_store.enqueue(
        makeEvent(),
        kNow
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxInsertResult>(
            inserted
        )
    );

    ASSERT_EQ(
        main_store.stateCounts().total,
        1
    );

    std::promise<void> release;
    auto gate = release.get_future();

    std::promise<std::int64_t> observed;
    auto result = observed.get_future();

    edge::UploadWorker worker(
        edge::makeOutboxWorkerTask(
            storage,
            edge::UploadConfig{},
            [&](Context& context,
                edge::OutboxStore& store,
                edge::OutboxUploader&) {
                if (gate.wait_for(2s)
                    != std::future_status::ready) {
                    throw std::runtime_error(
                        "Test gate timed out"
                    );
                }

                observed.set_value(
                    store.stateCounts().total
                );

                context.waitForStop();
            }
        )
    );

    // 先完成第二套连接初始化。
    // 此时主连接没有事务。
    worker.start();

    edge::SqliteTransaction transaction(database);

    executeSql(
        database,
        "DELETE FROM outbox;"
    );

    EXPECT_EQ(
        main_store.stateCounts().total,
        0
    );

    // 删除尚未提交。
    // 独立连接应仍看到之前提交的那条记录。
    release.set_value();

    const auto state = result.wait_for(2s);

    // 无论观察是否成功，先解除主连接事务，
    // 再收尾工作线程。
    transaction.rollback();

    worker.requestStop();
    worker.join();

    ASSERT_EQ(
        state,
        std::future_status::ready
    );

    EXPECT_EQ(result.get(), 1);

    EXPECT_FALSE(hasFailure(worker));

    EXPECT_EQ(
        main_store.stateCounts().total,
        1
    );
}

TEST(
    OutboxWorkerTaskTest,
    DoesNotRecoverSending
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    auto database = edge::openOutboxDatabase(
        storage.path
    );

    edge::OutboxStore main_store(database, storage);

    (void)main_store.enqueue(
        makeEvent(),
        kNow
    );

    const auto claimed = main_store.claimReady(
        kNow,
        1
    );

    ASSERT_EQ(claimed.size(), 1U);

    std::int64_t sending = -1;

    edge::UploadWorker worker(
        edge::makeOutboxWorkerTask(
            storage,
            edge::UploadConfig{},
            [&](Context& context,
                edge::OutboxStore& store,
                edge::OutboxUploader&) {
                sending = store.stateCounts().sending;

                context.waitForStop();
            }
        )
    );

    worker.start();

    worker.requestStop();
    worker.join();

    EXPECT_FALSE(hasFailure(worker));

    EXPECT_EQ(sending, 1);

    EXPECT_EQ(
        main_store.stateCounts().sending,
        1
    );

    EXPECT_EQ(
        main_store.stateCounts().retry,
        0
    );
}

TEST(
    OutboxWorkerTaskTest,
    PropagatesCorruptDatabaseFailure
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    const std::string original =
        "not a SQLite database";

    {
        std::ofstream output(
            storage.path,
            std::ios::binary
        );

        output << original;
        output.close();

        ASSERT_TRUE(output);
    }

    bool entered_body = false;

    edge::UploadWorker worker(
        edge::makeOutboxWorkerTask(
            storage,
            edge::UploadConfig{},
            [&](Context& context,
                edge::OutboxStore&,
                edge::OutboxUploader&) {
                entered_body = true;

                context.waitForStop();
            }
        )
    );

    EXPECT_THROW(
        worker.start(),
        edge::SqliteError
    );

    worker.requestStop();
    worker.join();

    EXPECT_FALSE(worker.status().ready);
    EXPECT_TRUE(worker.status().finished);
    EXPECT_TRUE(hasFailure(worker));
    EXPECT_FALSE(entered_body);

    std::ifstream input(
        storage.path,
        std::ios::binary
    );

    const std::string after(
        std::istreambuf_iterator<char>{input},
        std::istreambuf_iterator<char>{}
    );

    EXPECT_EQ(after, original);
}

TEST(
    OutboxWorkerTaskTest,
    PropagatesInvalidStorageFailure
)
{
    TemporaryDirectory temporary;

    auto storage = temporary.storage();

    auto database = edge::openOutboxDatabase(
        storage.path
    );

    storage.max_pending_records = 0;

    bool entered_body = false;

    edge::UploadWorker worker(
        edge::makeOutboxWorkerTask(
            storage,
            edge::UploadConfig{},
            [&](Context& context,
                edge::OutboxStore&,
                edge::OutboxUploader&) {
                entered_body = true;

                context.waitForStop();
            }
        )
    );

    EXPECT_THROW(
        worker.start(),
        std::invalid_argument
    );

    worker.requestStop();
    worker.join();

    EXPECT_FALSE(worker.status().ready);
    EXPECT_TRUE(hasFailure(worker));
    EXPECT_FALSE(entered_body);
}

TEST(
    OutboxWorkerTaskTest,
    PropagatesInvalidUploadFailure
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    auto database = edge::openOutboxDatabase(
        storage.path
    );

    edge::UploadConfig upload;

    upload.batch_size = 0;

    bool entered_body = false;

    edge::UploadWorker worker(
        edge::makeOutboxWorkerTask(
            storage,
            upload,
            [&](Context& context,
                edge::OutboxStore&,
                edge::OutboxUploader&) {
                entered_body = true;

                context.waitForStop();
            }
        )
    );

    EXPECT_THROW(
        worker.start(),
        std::invalid_argument
    );

    worker.requestStop();
    worker.join();

    EXPECT_FALSE(worker.status().ready);
    EXPECT_TRUE(hasFailure(worker));
    EXPECT_FALSE(entered_body);
}

TEST(
    OutboxWorkerTaskTest,
    CapturesBodyFailureAndAllowsReopen
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    {
        auto database = edge::openOutboxDatabase(
            storage.path
        );
    }

    edge::UploadWorker worker(
        edge::makeOutboxWorkerTask(
            storage,
            edge::UploadConfig{},
            [](Context&,
               edge::OutboxStore&,
               edge::OutboxUploader&) {
                throw std::runtime_error(
                    "Worker body failed"
                );
            }
        )
    );

    worker.start();
    worker.join();

    const auto status = worker.status();

    EXPECT_TRUE(status.ready);
    EXPECT_TRUE(status.finished);

    ASSERT_TRUE(
        static_cast<bool>(status.failure)
    );

    try {
        std::rethrow_exception(status.failure);

        FAIL() << "Expected the original body exception";
    }
    catch (const std::runtime_error& error) {
        EXPECT_STREQ(
            error.what(),
            "Worker body failed"
        );
    }

    // 工作任务异常展开后，
    // 数据库仍可重新打开并使用。
    auto reopened = edge::openOutboxDatabase(
        storage.path
    );

    edge::OutboxStore store(reopened, storage);

    EXPECT_EQ(store.stateCounts().total, 0);

    const auto inserted = store.enqueue(
        makeEvent(),
        kNow
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxInsertResult>(
            inserted
        )
    );

    EXPECT_EQ(store.stateCounts().total, 1);
}

edge::UploadConfig loopTestConfig()
{
    edge::UploadConfig config;

    config.batch_size = 2;
    config.flush_interval_ms = 100;

    config.retry_initial_ms = 5000;
    config.retry_max_ms = 10000;

    return config;
}

void seedLoopEvents(
    edge::OutboxStore& store,
    std::int64_t count
)
{
    for (std::int64_t sequence = 1;
         sequence <= count;
         ++sequence) {
        auto event = makeEvent();
        event.sequence = sequence;

        const auto result = store.enqueue(
            event,
            kNow
        );

        const auto* inserted =
            std::get_if<edge::OutboxInsertResult>(&result);

        if (inserted == nullptr
            || inserted->status
                != edge::OutboxInsertStatus::Inserted) {
            throw std::runtime_error(
                "Cannot prepare loop test event"
            );
        }
    }
}

edge::BatchUploadResult acceptLoopEvents(
    const edge::UploadConfig&,
    const std::vector<edge::SensorEvent>& events
)
{
    edge::BatchResponse response;

    for (const auto& event : events) {
        response.results.emplace_back();

        auto& result = response.results.back();

        result.event_id = edge::makeEventId(event);
        result.status = edge::BatchEventStatus::Accepted;
    }

    return response;
}

// 测试中使用同一工作连接和 store，
// 将真实发送组件替换为可控的发送函数。
// 工厂创建的默认 uploader 不执行发送。
edge::UploadWorker::Task loopTestTask(
    const edge::StorageConfig& storage,
    const edge::UploadConfig& upload,
    edge::OutboxBatchSender sender,
    edge::OutboxCycleHandler on_cycle,
    edge::OutboxUploadClock clock
)
{
    return edge::makeOutboxWorkerTask(
        storage,
        upload,
        [
            upload,
            sender = std::move(sender),
            on_cycle = std::move(on_cycle),
            clock = std::move(clock)
        ](
            Context& context,
            edge::OutboxStore& store,
            edge::OutboxUploader&
        ) {
            edge::OutboxUploader injected(
                store,
                upload,
                sender,
                clock
            );

            edge::runOutboxUploadLoop(
                context,
                store,
                injected,
                upload,
                on_cycle,
                clock
            );
        }
    );
}

TEST(
    OutboxUploadLoopTest,
    DrainsFullBatchesWithoutExtraNotification
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();
    const auto upload = loopTestConfig();

    auto database = edge::openOutboxDatabase(storage.path);
    edge::OutboxStore store(database, storage);

    seedLoopEvents(store, 4);

    std::promise<edge::UploadCycleResult> completed;
    auto result = completed.get_future();

    int cycles = 0;

    edge::UploadWorker worker(loopTestTask(
        storage,
        upload,
        acceptLoopEvents,
        [&](const edge::UploadCycleResult& report) {
            ++cycles;

            if (cycles == 2) {
                completed.set_value(report);
            }
        },
        [] { return kNow; }
    ));

    worker.start();

    const bool delivered =
        result.wait_for(2s) == std::future_status::ready;

    worker.requestStop();
    worker.join();

    ASSERT_TRUE(delivered);
    EXPECT_FALSE(hasFailure(worker));
    EXPECT_EQ(cycles, 2);

    const auto report = result.get();

    EXPECT_EQ(report.claimed_records, 2);
    EXPECT_EQ(report.acked_records, 2);

    const auto counts = store.stateCounts();

    EXPECT_EQ(counts.acked, 4);
    EXPECT_EQ(counts.unacked, 0);
}

TEST(
    OutboxUploadLoopTest,
    PartialBatchWaitsForAggregationDeadline
)
{
    using Clock = edge::UploadWorker::Clock;

    TemporaryDirectory temporary;

    const auto storage = temporary.storage();
    const auto upload = loopTestConfig();

    auto database = edge::openOutboxDatabase(storage.path);
    edge::OutboxStore store(database, storage);

    seedLoopEvents(store, 1);

    std::promise<edge::UploadCycleResult> completed;
    auto result = completed.get_future();

    bool first_clock = true;
    Clock::time_point first_check{};
    Clock::time_point reported_at{};

    edge::UploadWorker worker(loopTestTask(
        storage,
        upload,
        acceptLoopEvents,
        [&](const edge::UploadCycleResult& report) {
            reported_at = Clock::now();
            completed.set_value(report);
        },
        [&] {
            if (first_clock) {
                first_clock = false;
                first_check = Clock::now();
            }

            return kNow;
        }
    ));

    worker.start();

    const bool delivered =
        result.wait_for(2s) == std::future_status::ready;

    worker.requestStop();
    worker.join();

    ASSERT_TRUE(delivered);
    EXPECT_FALSE(hasFailure(worker));

    EXPECT_GE(
        reported_at - first_check,
        std::chrono::milliseconds(upload.flush_interval_ms)
    );

    const auto report = result.get();

    EXPECT_EQ(report.claimed_records, 1);
    EXPECT_EQ(report.acked_records, 1);
    EXPECT_EQ(store.stateCounts().acked, 1);
}

TEST(
    OutboxUploadLoopTest,
    RetriesWhenDatabaseTimeBecomesDue
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();
    const auto upload = loopTestConfig();

    auto database = edge::openOutboxDatabase(storage.path);
    edge::OutboxStore store(database, storage);

    seedLoopEvents(store, 2);

    std::atomic<std::int64_t> wall{kNow};

    std::promise<edge::UploadCycleResult> first;
    std::promise<edge::UploadCycleResult> second;

    auto first_result = first.get_future();
    auto second_result = second.get_future();

    int sends = 0;
    int cycles = 0;

    edge::UploadWorker worker(loopTestTask(
        storage,
        upload,
        [&](const edge::UploadConfig& config,
            const std::vector<edge::SensorEvent>& events)
            -> edge::BatchUploadResult {
            ++sends;

            if (sends == 1) {
                edge::BatchHttpError error{};
                error.status_code = 503;

                return error;
            }

            return acceptLoopEvents(config, events);
        },
        [&](const edge::UploadCycleResult& report) {
            ++cycles;

            if (cycles == 1) {
                first.set_value(report);
            }
            else if (cycles == 2) {
                second.set_value(report);
            }
        },
        [&] { return wall.load(); }
    ));

    worker.start();

    const bool got_first =
        first_result.wait_for(2s)
            == std::future_status::ready;

    bool stayed_waiting = false;
    bool got_second = false;

    if (got_first) {
        // 数据库时间尚未到期，不应再次发送。
        stayed_waiting =
            second_result.wait_for(30ms)
                == std::future_status::timeout;

        // 推进测试时间，并通知工作线程重新查询。
        wall.store(kNow + upload.retry_initial_ms);

        worker.notifyWork();

        got_second =
            second_result.wait_for(2s)
                == std::future_status::ready;
    }

    worker.requestStop();
    worker.join();

    ASSERT_TRUE(got_first);
    EXPECT_TRUE(stayed_waiting);
    ASSERT_TRUE(got_second);

    EXPECT_FALSE(hasFailure(worker));

    const auto failed = first_result.get();
    const auto succeeded = second_result.get();

    EXPECT_EQ(failed.retry_records, 2);

    EXPECT_TRUE(
        std::holds_alternative<edge::BatchHttpError>(
            failed.failure
        )
    );

    EXPECT_EQ(succeeded.acked_records, 2);
    EXPECT_EQ(sends, 2);
    EXPECT_EQ(cycles, 2);

    EXPECT_EQ(store.stateCounts().acked, 2);
    EXPECT_EQ(store.stateCounts().retry, 0);
}

TEST(
    OutboxUploadLoopTest,
    StopDoesNotFlushPartialBatch
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    auto upload = loopTestConfig();
    upload.flush_interval_ms = 60000;

    auto database = edge::openOutboxDatabase(storage.path);
    edge::OutboxStore store(database, storage);

    seedLoopEvents(store, 1);

    int sends = 0;
    int cycles = 0;

    edge::UploadWorker worker(loopTestTask(
        storage,
        upload,
        [&](const edge::UploadConfig& config,
            const std::vector<edge::SensorEvent>& events) {
            ++sends;

            return acceptLoopEvents(config, events);
        },
        [&](const edge::UploadCycleResult&) {
            ++cycles;
        },
        [] { return kNow; }
    ));

    worker.start();

    worker.requestStop();
    worker.join();

    EXPECT_FALSE(hasFailure(worker));
    EXPECT_EQ(sends, 0);
    EXPECT_EQ(cycles, 0);

    EXPECT_EQ(store.stateCounts().pending, 1);

    const auto ready = store.listReady(kNow, 10);

    ASSERT_EQ(ready.size(), 1U);
    EXPECT_EQ(ready[0].attempt_count, 0);
}

TEST(
    OutboxUploadLoopTest,
    StopDuringSendKeepsCurrentReportAndLeavesBacklog
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();
    const auto upload = loopTestConfig();

    auto database = edge::openOutboxDatabase(storage.path);
    edge::OutboxStore store(database, storage);

    seedLoopEvents(store, 4);

    std::promise<void> entered;
    auto inflight = entered.get_future();

    std::promise<void> release;
    auto gate = release.get_future();

    int sends = 0;
    int cycles = 0;
    std::int64_t acked = 0;

    edge::UploadWorker worker(loopTestTask(
        storage,
        upload,
        [&](const edge::UploadConfig& config,
            const std::vector<edge::SensorEvent>& events) {
            ++sends;

            entered.set_value();

            if (gate.wait_for(2s)
                != std::future_status::ready) {
                throw std::runtime_error(
                    "Send gate timed out"
                );
            }

            return acceptLoopEvents(config, events);
        },
        [&](const edge::UploadCycleResult& report) {
            ++cycles;
            acked += report.acked_records;
        },
        [] { return kNow; }
    ));

    worker.start();

    const bool was_sending =
        inflight.wait_for(2s) == std::future_status::ready;

    worker.requestStop();

    release.set_value();

    worker.join();

    ASSERT_TRUE(was_sending);

    EXPECT_FALSE(hasFailure(worker));
    EXPECT_EQ(sends, 1);
    EXPECT_EQ(cycles, 1);
    EXPECT_EQ(acked, 2);

    const auto counts = store.stateCounts();

    EXPECT_EQ(counts.acked, 2);
    EXPECT_EQ(counts.pending, 2);
    EXPECT_EQ(counts.sending, 0);
}

TEST(
    OutboxUploadLoopTest,
    LocalSenderFailureReachesOwner
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();
    const auto upload = loopTestConfig();

    auto database = edge::openOutboxDatabase(storage.path);
    edge::OutboxStore store(database, storage);

    seedLoopEvents(store, 2);

    int cycles = 0;

    edge::UploadWorker worker(loopTestTask(
        storage,
        upload,
        [](const edge::UploadConfig&,
           const std::vector<edge::SensorEvent>&)
            -> edge::BatchUploadResult {
            throw std::runtime_error(
                "Simulated local sender failure"
            );
        },
        [&](const edge::UploadCycleResult&) {
            ++cycles;
        },
        [] { return kNow; }
    ));

    worker.start();
    worker.join();

    const auto status = worker.status();

    EXPECT_TRUE(status.ready);
    EXPECT_TRUE(status.finished);

    ASSERT_TRUE(
        static_cast<bool>(status.failure)
    );

    try {
        std::rethrow_exception(status.failure);

        FAIL() << "Expected the sender exception";
    }
    catch (const std::runtime_error& error) {
        EXPECT_STREQ(
            error.what(),
            "Simulated local sender failure"
        );
    }

    EXPECT_EQ(cycles, 0);

    // 领取已经提交，但发送函数抛出本地异常。
    // 不在这里擅自转成 RETRY，留待既有启动恢复处理。
    EXPECT_EQ(store.stateCounts().sending, 2);
}

struct ContentionAttempt {
    bool succeeded = false;
    int sqlite_code = SQLITE_OK;
    bool autocommit = false;
    std::string message;
};

using ContentionOperation = std::function<void(
    Context&,
    edge::OutboxStore&
)>;

// 在工作线程内创建独立连接。
// 等待通知后执行一次操作，并记录操作结束后的事务状态。
edge::UploadWorker::Task contentionTestTask(
    const edge::StorageConfig& storage,
    ContentionOperation operation,
    std::promise<ContentionAttempt>& completed
)
{
    return [
        storage,
        operation = std::move(operation),
        &completed
    ](Context& context) {
        auto database = edge::openOutboxDatabase(
            storage.path
        );

        edge::OutboxStore store(database, storage);

        context.signalReady();

        if (context.waitForWork()
            == edge::UploadWorker::WaitResult::StopRequested) {
            return;
        }

        ContentionAttempt result;
        std::exception_ptr failure;

        try {
            operation(context, store);

            result.succeeded = true;
        }
        catch (const edge::SqliteError& error) {
            result.sqlite_code = error.code();
            result.message = error.what();

            failure = std::current_exception();
        }
        catch (...) {
            result.sqlite_code = -1;

            failure = std::current_exception();
        }

        // operation 已经返回或完成异常展开。
        // 此处检查事务析构之后，连接是否恢复自动提交。
        result.autocommit =
            sqlite3_get_autocommit(database.get()) != 0;

        completed.set_value(result);

        if (failure) {
            std::rethrow_exception(failure);
        }

        context.waitForStop();
    };
}

bool isBusyResult(int code)
{
    // 同时识别 SQLITE_BUSY 的扩展结果码。
    return (code & 0xff) == SQLITE_BUSY;
}

TEST(
    SqliteContentionTest,
    ShortWriteLockCanClear
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    auto database = edge::openOutboxDatabase(
        storage.path
    );

    edge::OutboxStore store(database, storage);

    std::promise<ContentionAttempt> completed;
    auto result = completed.get_future();

    edge::UploadWorker worker(contentionTestTask(
        storage,
        [](Context&, edge::OutboxStore& worker_store) {
            (void)worker_store.enqueue(
                makeEvent(),
                kNow
            );
        },
        completed
    ));

    // 先完成工作连接初始化，主连接此时没有事务。
    worker.start();

    edge::SqliteTransaction held(database);

    worker.notifyWork();

    // 主连接仍持有写锁，不应已经完成入库。
    const auto while_locked = result.wait_for(30ms);

    // 在忙等待预算内释放写锁。
    held.rollback();

    const bool finished =
        result.wait_for(2s) == std::future_status::ready;

    worker.requestStop();
    worker.join();

    EXPECT_EQ(
        while_locked,
        std::future_status::timeout
    );

    ASSERT_TRUE(finished);

    const auto attempt = result.get();

    EXPECT_TRUE(attempt.succeeded);
    EXPECT_TRUE(attempt.autocommit);
    EXPECT_EQ(attempt.sqlite_code, SQLITE_OK);

    EXPECT_FALSE(hasFailure(worker));

    EXPECT_EQ(store.stateCounts().pending, 1);
}

TEST(
    SqliteContentionTest,
    HeldWriteLockFailsWithoutInsert
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    auto database = edge::openOutboxDatabase(
        storage.path
    );

    edge::OutboxStore store(database, storage);

    std::promise<ContentionAttempt> completed;
    auto result = completed.get_future();

    edge::UploadWorker worker(contentionTestTask(
        storage,
        [](Context&, edge::OutboxStore& worker_store) {
            (void)worker_store.enqueue(
                makeEvent(),
                kNow
            );
        },
        completed
    ));

    worker.start();

    edge::SqliteTransaction held(database);

    worker.notifyWork();

    // 保持写锁，直到工作操作自行报告结果。
    const bool finished_while_locked =
        result.wait_for(2s) == std::future_status::ready;

    held.rollback();

    worker.requestStop();
    worker.join();

    ASSERT_TRUE(finished_while_locked);

    const auto attempt = result.get();

    EXPECT_FALSE(attempt.succeeded);
    EXPECT_TRUE(isBusyResult(attempt.sqlite_code));
    EXPECT_TRUE(attempt.autocommit);

    EXPECT_NE(
        attempt.message.find("BEGIN IMMEDIATE;"),
        std::string::npos
    );

    EXPECT_TRUE(hasFailure(worker));

    // BEGIN 失败，没有插入任何记录。
    EXPECT_EQ(store.stateCounts().total, 0);
}

TEST(
    SqliteContentionTest,
    ReaderBlockedCommitRollsBackInsert
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    auto database = edge::openOutboxDatabase(
        storage.path
    );

    edge::OutboxStore store(database, storage);

    seedLoopEvents(store, 1);

    std::promise<ContentionAttempt> completed;
    auto result = completed.get_future();

    edge::UploadWorker worker(contentionTestTask(
        storage,
        [](Context&, edge::OutboxStore& worker_store) {
            auto event = makeEvent();
            event.sequence = 2;

            (void)worker_store.enqueue(
                event,
                kNow
            );
        },
        completed
    ));

    worker.start();

    // 保持未结束的 SELECT，持有读锁。
    edge::SqliteStatement reader(
        database,
        "SELECT event_id FROM outbox;"
    );

    ASSERT_EQ(
        reader.step(),
        edge::SqliteStepResult::Row
    );

    worker.notifyWork();

    const bool finished_while_reading =
        result.wait_for(2s) == std::future_status::ready;

    reader.close();

    worker.requestStop();
    worker.join();

    ASSERT_TRUE(finished_while_reading);

    const auto attempt = result.get();

    EXPECT_FALSE(attempt.succeeded);
    EXPECT_TRUE(isBusyResult(attempt.sqlite_code));
    EXPECT_TRUE(attempt.autocommit);

    // 验证这次失败发生在 COMMIT，而不是 BEGIN。
    EXPECT_NE(
        attempt.message.find("COMMIT;"),
        std::string::npos
    );

    EXPECT_TRUE(hasFailure(worker));

    // 新插入的第二条记录必须随失败事务回滚。
    const auto ready = store.listReady(kNow, 10);

    ASSERT_EQ(ready.size(), 1U);

    EXPECT_EQ(
        ready[0].event_id,
        edge::makeEventId(makeEvent())
    );

    EXPECT_EQ(store.stateCounts().total, 1);
}

TEST(
    SqliteContentionTest,
    ReaderBlockedClaimDoesNotSend
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    auto upload = loopTestConfig();
    upload.batch_size = 1;

    auto database = edge::openOutboxDatabase(
        storage.path
    );

    edge::OutboxStore store(database, storage);

    seedLoopEvents(store, 1);

    std::promise<ContentionAttempt> completed;
    auto result = completed.get_future();

    int sends = 0;

    edge::UploadWorker worker(contentionTestTask(
        storage,
        [&](Context&, edge::OutboxStore& worker_store) {
            edge::OutboxUploader uploader(
                worker_store,
                upload,
                [&](const edge::UploadConfig& config,
                    const std::vector<edge::SensorEvent>& events) {
                    ++sends;

                    return acceptLoopEvents(config, events);
                },
                [] { return kNow; }
            );

            (void)uploader.runOnce();
        },
        completed
    ));

    worker.start();

    edge::SqliteStatement reader(
        database,
        "SELECT event_id FROM outbox;"
    );

    ASSERT_EQ(
        reader.step(),
        edge::SqliteStepResult::Row
    );

    worker.notifyWork();

    const bool finished_while_reading =
        result.wait_for(2s) == std::future_status::ready;

    reader.close();

    worker.requestStop();
    worker.join();

    ASSERT_TRUE(finished_while_reading);

    const auto attempt = result.get();

    EXPECT_FALSE(attempt.succeeded);
    EXPECT_TRUE(isBusyResult(attempt.sqlite_code));
    EXPECT_TRUE(attempt.autocommit);

    EXPECT_NE(
        attempt.message.find("COMMIT;"),
        std::string::npos
    );

    EXPECT_TRUE(hasFailure(worker));

    // 领取没有提交成功，不能进入发送函数。
    EXPECT_EQ(sends, 0);

    EXPECT_EQ(store.stateCounts().pending, 1);
    EXPECT_EQ(store.stateCounts().sending, 0);

    const auto ready = store.listReady(kNow, 10);

    ASSERT_EQ(ready.size(), 1U);
    EXPECT_EQ(ready[0].attempt_count, 0);
}

TEST(
    SqliteContentionTest,
    AckCommitFailureDoesNotReplayHttp
)
{
    TemporaryDirectory temporary;

    const auto storage = temporary.storage();

    auto upload = loopTestConfig();
    upload.batch_size = 1;

    auto database = edge::openOutboxDatabase(
        storage.path
    );

    edge::OutboxStore store(database, storage);

    seedLoopEvents(store, 1);

    std::promise<void> entered;
    auto inflight = entered.get_future();

    std::promise<void> release;
    auto gate = release.get_future();

    std::promise<ContentionAttempt> completed;
    auto result = completed.get_future();

    int sends = 0;
    int cycles = 0;

    edge::UploadWorker worker(contentionTestTask(
        storage,
        [&](Context& context,
            edge::OutboxStore& worker_store) {
            edge::OutboxUploader uploader(
                worker_store,
                upload,
                [&](const edge::UploadConfig& config,
                    const std::vector<edge::SensorEvent>& events) {
                    ++sends;

                    entered.set_value();

                    if (gate.wait_for(2s)
                        != std::future_status::ready) {
                        throw std::runtime_error(
                            "Send gate timed out"
                        );
                    }

                    return acceptLoopEvents(config, events);
                },
                [] { return kNow; }
            );

            edge::runOutboxUploadLoop(
                context,
                worker_store,
                uploader,
                upload,
                [&](const edge::UploadCycleResult&) {
                    ++cycles;
                },
                [] { return kNow; }
            );
        },
        completed
    ));

    worker.start();
    worker.notifyWork();

    const bool was_sending =
        inflight.wait_for(2s) == std::future_status::ready;

    // 领取已提交，发送函数尚未返回。
    // 此时再建立读锁，专门阻塞后续 ACK 提交。
    edge::SqliteStatement reader(
        database,
        "SELECT event_id FROM outbox;"
    );

    EXPECT_EQ(
        reader.step(),
        edge::SqliteStepResult::Row
    );

    release.set_value();

    const bool finished_while_reading =
        result.wait_for(2s) == std::future_status::ready;

    reader.close();

    worker.requestStop();
    worker.join();

    ASSERT_TRUE(was_sending);
    ASSERT_TRUE(finished_while_reading);

    const auto attempt = result.get();

    EXPECT_FALSE(attempt.succeeded);
    EXPECT_TRUE(isBusyResult(attempt.sqlite_code));
    EXPECT_TRUE(attempt.autocommit);

    EXPECT_NE(
        attempt.message.find("COMMIT;"),
        std::string::npos
    );

    EXPECT_TRUE(hasFailure(worker));

    // 已经发送一次，但 ACK 提交失败。
    // 循环必须报告错误，不能直接再发一次 HTTP。
    EXPECT_EQ(sends, 1);

    // runOnce 没有正常返回，不生成完整周期报告。
    EXPECT_EQ(cycles, 0);

    EXPECT_EQ(store.stateCounts().sending, 1);
    EXPECT_EQ(store.stateCounts().acked, 0);
    EXPECT_EQ(store.stateCounts().retry, 0);

    edge::SqliteStatement query(
        database,
        "SELECT attempt_count FROM outbox;"
    );

    ASSERT_EQ(
        query.step(),
        edge::SqliteStepResult::Row
    );

    EXPECT_EQ(
        sqlite3_column_int64(query.get(), 0),
        1
    );

    EXPECT_EQ(
        query.step(),
        edge::SqliteStepResult::Done
    );
}

} // namespace