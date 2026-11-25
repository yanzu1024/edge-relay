#include "edge/storage/OutboxAccessGate.h"
#include "edge/storage/OutboxSchema.h"
#include "edge/storage/OutboxStore.h"
#include "edge/storage/SqliteDatabase.h"
#include "edge/config/AgentConfig.h"
#include "edge/domain/SensorEvent.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <chrono>
#include <cstring>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <variant>

namespace {

constexpr std::int64_t kNow = 1700000000100LL;

edge::SensorEvent makeEvent()
{
    edge::SensorEvent event;

    event.schema_version = 1;
    event.device_id = "gate-sensor";
    event.session_id =
        "550e8400-e29b-41d4-a716-446655440000";
    event.sequence = 1;
    event.observed_at_ms = 1700000000000LL;
    event.metric = "temperature";
    event.value = 21.5;
    event.unit = "degC";

    return event;
}

// 数据库在调用此函数的线程内创建和使用。
edge::OutboxInsertStatus insertInCurrentThread(
    const std::shared_ptr<edge::OutboxAccessGate>& gate,
    std::promise<void>* about_to_enqueue = nullptr
)
{
    edge::SqliteDatabase database(":memory:");
    edge::initializeOutboxSchema(database);

    edge::OutboxStore store(
        database,
        edge::StorageConfig{},
        gate
    );

    if (about_to_enqueue != nullptr) {
        about_to_enqueue->set_value();
    }

    const auto result = store.enqueue(makeEvent(), kNow);

    const auto* inserted =
        std::get_if<edge::OutboxInsertResult>(&result);

    if (inserted == nullptr) {
        throw std::runtime_error(
            "Unexpected test enqueue result"
        );
    }

    return inserted->status;
}

TEST(OutboxAccessGateTest, StoreUsesSharedGateAcrossThreads)
{
    auto gate = std::make_shared<edge::OutboxAccessGate>();

    std::promise<void> about_to_enqueue;
    auto about_future = about_to_enqueue.get_future();

    std::promise<edge::OutboxInsertStatus> result;
    auto result_future = result.get_future();

    std::unique_lock<edge::OutboxAccessGate> owner(*gate);

    std::thread worker([&] {
        try {
            result.set_value(
                insertInCurrentThread(
                    gate,
                    &about_to_enqueue
                )
            );
        }
        catch (...) {
            result.set_exception(std::current_exception());
        }
    });

    const auto prepared = about_future.wait_for(
        std::chrono::seconds(2)
    );

    const auto while_owned = result_future.wait_for(
        std::chrono::milliseconds(50)
    );

    // 无论断言结果如何，都先释放并回收线程。
    owner.unlock();
    worker.join();

    EXPECT_EQ(prepared, std::future_status::ready);
    EXPECT_EQ(while_owned, std::future_status::timeout);

    EXPECT_EQ(
        result_future.get(),
        edge::OutboxInsertStatus::Inserted
    );
}

TEST(OutboxAccessGateTest, SupportsNestedStoreOperations)
{
    auto gate = std::make_shared<edge::OutboxAccessGate>();

    edge::SqliteDatabase database(":memory:");
    edge::initializeOutboxSchema(database);

    edge::OutboxStore store(
        database,
        edge::StorageConfig{},
        gate
    );

    // 外层门闩不会阻止同一线程执行内部组合查询。
    std::lock_guard<edge::OutboxAccessGate> owner(*gate);

    const auto inserted = store.enqueue(makeEvent(), kNow);

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxInsertResult>(
            inserted
        )
    );

    const auto claimed = store.claimReady(kNow, 1);
    ASSERT_EQ(claimed.size(), 1);

    EXPECT_EQ(store.stateCounts().sending, 1);

    // 独立内存数据库中模拟遗留 SENDING。
    // recoverSending 内部会再次调用 stateCounts。
    EXPECT_EQ(store.recoverSending(kNow + 1), 1);

    const auto counts = store.stateCounts();

    EXPECT_EQ(counts.sending, 0);
    EXPECT_EQ(counts.retry, 1);
    EXPECT_EQ(counts.unacked, 1);

    EXPECT_EQ(store.listReady(kNow + 1, 1).size(), 1);
}

TEST(OutboxAccessGateTest, SqlFailureReleasesGateForAnotherThread)
{
    auto gate = std::make_shared<edge::OutboxAccessGate>();

    edge::SqliteDatabase database(":memory:");
    edge::initializeOutboxSchema(database);

    edge::OutboxStore store(
        database,
        edge::StorageConfig{},
        gate
    );

    const auto deny_insert = [](
        void*,
        int action,
        const char* first,
        const char*,
        const char*,
        const char*
    ) -> int {
        if (action == SQLITE_INSERT
            && first != nullptr
            && std::strcmp(first, "outbox") == 0) {
            return SQLITE_DENY;
        }

        return SQLITE_OK;
    };

    ASSERT_EQ(
        sqlite3_set_authorizer(
            database.get(),
            deny_insert,
            nullptr
        ),
        SQLITE_OK
    );

    EXPECT_THROW(
        store.enqueue(makeEvent(), kNow),
        edge::SqliteError
    );

    ASSERT_EQ(
        sqlite3_set_authorizer(
            database.get(),
            nullptr,
            nullptr
        ),
        SQLITE_OK
    );

    EXPECT_NE(sqlite3_get_autocommit(database.get()), 0);

    std::promise<edge::OutboxInsertStatus> result;
    auto result_future = result.get_future();

    std::thread worker([gate, &result] {
        try {
            result.set_value(
                insertInCurrentThread(gate)
            );
        }
        catch (...) {
            result.set_exception(std::current_exception());
        }
    });

    worker.join();

    EXPECT_EQ(
        result_future.get(),
        edge::OutboxInsertStatus::Inserted
    );
}

} // namespace