#include "edge/config/AgentConfig.h"
#include "edge/domain/EventId.h"
#include "edge/domain/SensorEvent.h"

#include "edge/storage/OutboxSchema.h"
#include "edge/storage/OutboxStore.h"
#include "edge/storage/SqliteDatabase.h"
#include "edge/storage/SqliteStatement.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

namespace {

using edge::OutboxCapacityCode;
using edge::OutboxCapacityError;
using edge::OutboxEnqueueResult;
using edge::OutboxInsertResult;
using edge::OutboxInsertStatus;
using edge::OutboxStore;
using edge::SensorEvent;
using edge::SqliteDatabase;
using edge::SqliteStatement;
using edge::SqliteStepResult;
using edge::StorageConfig;

constexpr std::int64_t kNow = 1700000000100LL;

SensorEvent makeEvent(std::int64_t sequence)
{
    SensorEvent event;

    event.device_id = "sensor-01";
    event.session_id = "550e8400-e29b-41d4-a716-446655440000";
    event.sequence = sequence;
    event.observed_at_ms = 1700000000000LL;
    event.metric = "temperature";
    event.value = 21.5;
    event.unit = "degC";

    return event;
}

StorageConfig makeStorage(
    std::int64_t pending,
    std::int64_t total
)
{
    StorageConfig storage;

    storage.max_pending_records = pending;
    storage.max_total_records = total;

    // 测试使用小容量，也让保留数量符合配置关系。
    // 本节尚不执行 ACKED 清理。
    storage.acked_keep_max_records = 1;

    return storage;
}

OutboxInsertResult requireInsertResult(OutboxEnqueueResult result)
{
    if (!std::holds_alternative<OutboxInsertResult>(result)) {
        throw std::runtime_error(
            "Expected an Outbox insert result"
        );
    }

    return std::get<OutboxInsertResult>(std::move(result));
}

OutboxCapacityError requireCapacityError(OutboxEnqueueResult result)
{
    if (!std::holds_alternative<OutboxCapacityError>(result)) {
        throw std::runtime_error(
            "Expected an Outbox capacity error"
        );
    }

    return std::get<OutboxCapacityError>(std::move(result));
}

std::int64_t scalarInt64(
    SqliteDatabase& db,
    const std::string& sql
)
{
    SqliteStatement query(db, sql);

    if (query.step() != SqliteStepResult::Row) {
        throw std::logic_error("Test query returned no row");
    }

    const auto value = static_cast<std::int64_t>(
        sqlite3_column_int64(query.get(), 0)
    );

    if (query.step() != SqliteStepResult::Done) {
        throw std::logic_error(
            "Test query returned multiple rows"
        );
    }

    return value;
}

void setUnackedState(
    SqliteDatabase& db,
    const std::string& event_id,
    const std::string& state
)
{
    SqliteStatement update(
        db,
        "UPDATE outbox SET state = ?1 WHERE event_id = ?2;"
    );

    update.bindText(1, state);
    update.bindText(2, event_id);

    if (update.step() != SqliteStepResult::Done) {
        throw std::logic_error("State update did not finish");
    }
}

void markAcked(
    SqliteDatabase& db,
    const std::string& event_id
)
{
    // 测试辅助操作，不是正式上传状态转换接口。
    SqliteStatement update(
        db,
        "UPDATE outbox SET "
        "state = 'ACKED', acked_at_ms = ?1, updated_at_ms = ?1 "
        "WHERE event_id = ?2;"
    );

    update.bindInt64(1, kNow + 100);
    update.bindText(2, event_id);

    if (update.step() != SqliteStepResult::Done) {
        throw std::logic_error("ACKED update did not finish");
    }
}

class OutboxCapacityTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        edge::initializeOutboxSchema(db_);
    }

    SqliteDatabase db_{":memory:"};
};

TEST_F(OutboxCapacityTest, AllowsLastSlotAndRejectsNext)
{
    const auto storage = makeStorage(2, 4);
    OutboxStore store(db_, storage);

    for (std::int64_t sequence : {1, 2}) {
        ASSERT_EQ(
            requireInsertResult(
                store.enqueue(makeEvent(sequence), kNow)
            ).status,
            OutboxInsertStatus::Inserted
        );
    }

    const auto rejected = makeEvent(3);

    const auto error = requireCapacityError(
        store.enqueue(rejected, kNow)
    );

    EXPECT_EQ(
        error.code,
        OutboxCapacityCode::PendingLimitReached
    );

    EXPECT_EQ(error.event_id, edge::makeEventId(rejected));
    EXPECT_EQ(error.pending_records, 2);
    EXPECT_EQ(error.total_records, 2);
    EXPECT_EQ(error.limit, 2);

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        2
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);
}

TEST_F(OutboxCapacityTest, AllUnackedStatesCountTowardPending)
{
    const auto storage = makeStorage(1, 5);
    OutboxStore store(db_, storage);

    const auto first = makeEvent(1);

    ASSERT_EQ(
        requireInsertResult(store.enqueue(first, kNow)).status,
        OutboxInsertStatus::Inserted
    );

    for (const char* state : {
            "PENDING",
            "SENDING",
            "RETRY",
            "DEAD"
        }) {
        setUnackedState(
            db_,
            edge::makeEventId(first),
            state
        );

        const auto error = requireCapacityError(
            store.enqueue(makeEvent(2), kNow)
        );

        EXPECT_EQ(
            error.code,
            OutboxCapacityCode::PendingLimitReached
        );

        EXPECT_EQ(error.pending_records, 1);
        EXPECT_EQ(error.total_records, 1);
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        1
    );
}

TEST_F(OutboxCapacityTest, AckedRecordsDoNotUsePendingQuota)
{
    const auto storage = makeStorage(1, 3);
    OutboxStore store(db_, storage);

    const auto first = makeEvent(1);

    ASSERT_EQ(
        requireInsertResult(store.enqueue(first, kNow)).status,
        OutboxInsertStatus::Inserted
    );

    markAcked(db_, edge::makeEventId(first));

    EXPECT_EQ(
        requireInsertResult(
            store.enqueue(makeEvent(2), kNow)
        ).status,
        OutboxInsertStatus::Inserted
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        2
    );

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM outbox WHERE state = 'ACKED';"
        ),
        1
    );
}

TEST_F(OutboxCapacityTest, AckedRecordsStillUseTotalQuota)
{
    const auto storage = makeStorage(2, 2);
    OutboxStore store(db_, storage);

    for (std::int64_t sequence : {1, 2}) {
        const auto event = makeEvent(sequence);

        ASSERT_EQ(
            requireInsertResult(store.enqueue(event, kNow)).status,
            OutboxInsertStatus::Inserted
        );

        markAcked(db_, edge::makeEventId(event));
    }

    const auto error = requireCapacityError(
        store.enqueue(makeEvent(3), kNow)
    );

    EXPECT_EQ(
        error.code,
        OutboxCapacityCode::TotalLimitReached
    );

    EXPECT_EQ(error.pending_records, 0);
    EXPECT_EQ(error.total_records, 2);
    EXPECT_EQ(error.limit, 2);

    // 容量检查本身不清理 ACKED。
    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM outbox WHERE state = 'ACKED';"
        ),
        2
    );
}

TEST_F(OutboxCapacityTest, DuplicateAcceptedWhenFull)
{
    const auto storage = makeStorage(1, 1);
    OutboxStore store(db_, storage);

    const auto event = makeEvent(1);

    ASSERT_EQ(
        requireInsertResult(store.enqueue(event, kNow)).status,
        OutboxInsertStatus::Inserted
    );

    // 两个容量同时达到上限，重复事件仍应正常识别。
    EXPECT_EQ(
        requireInsertResult(store.enqueue(event, kNow + 1)).status,
        OutboxInsertStatus::Duplicate
    );

    markAcked(db_, edge::makeEventId(event));

    // ACKED 仍占总容量，但也不影响重复识别。
    EXPECT_EQ(
        requireInsertResult(store.enqueue(event, kNow + 2)).status,
        OutboxInsertStatus::Duplicate
    );

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM outbox WHERE state = 'ACKED';"
        ),
        1
    );
}

TEST_F(OutboxCapacityTest, ConflictReportedWhenFull)
{
    const auto storage = makeStorage(1, 1);
    OutboxStore store(db_, storage);

    auto event = makeEvent(1);

    ASSERT_EQ(
        requireInsertResult(store.enqueue(event, kNow)).status,
        OutboxInsertStatus::Inserted
    );

    event.value = 99.0;

    EXPECT_EQ(
        requireInsertResult(store.enqueue(event, kNow + 1)).status,
        OutboxInsertStatus::Conflict
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        1
    );
}

TEST_F(OutboxCapacityTest, AckFreesPendingButNotTotalCapacity)
{
    const auto storage = makeStorage(1, 2);
    OutboxStore store(db_, storage);

    const auto first = makeEvent(1);
    const auto second = makeEvent(2);

    ASSERT_EQ(
        requireInsertResult(store.enqueue(first, kNow)).status,
        OutboxInsertStatus::Inserted
    );

    EXPECT_EQ(
        requireCapacityError(store.enqueue(second, kNow)).code,
        OutboxCapacityCode::PendingLimitReached
    );

    markAcked(db_, edge::makeEventId(first));

    ASSERT_EQ(
        requireInsertResult(store.enqueue(second, kNow)).status,
        OutboxInsertStatus::Inserted
    );

    markAcked(db_, edge::makeEventId(second));

    const auto error = requireCapacityError(
        store.enqueue(makeEvent(3), kNow)
    );

    EXPECT_EQ(
        error.code,
        OutboxCapacityCode::TotalLimitReached
    );

    EXPECT_EQ(error.pending_records, 0);
    EXPECT_EQ(error.total_records, 2);
}

TEST_F(OutboxCapacityTest, RejectsInvalidLimits)
{
    const std::pair<std::int64_t, std::int64_t> invalid[] = {
        {0, 2},
        {-1, 2},
        {1, 0},
        {1, -1},
        {3, 2}
    };

    for (const auto& limits : invalid) {
        const auto storage = makeStorage(
            limits.first,
            limits.second
        );

        EXPECT_THROW(
            {
                OutboxStore store(db_, storage);
            },
            std::invalid_argument
        );
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        0
    );
}

TEST_F(OutboxCapacityTest, ReducedLimitsDoNotDeleteExistingRecords)
{
    const auto initial_storage = makeStorage(3, 3);
    OutboxStore initial(db_, initial_storage);

    for (std::int64_t sequence : {1, 2, 3}) {
        ASSERT_EQ(
            requireInsertResult(
                initial.enqueue(makeEvent(sequence), kNow)
            ).status,
            OutboxInsertStatus::Inserted
        );
    }

    // 模拟使用更低的容量重新创建存储对象。
    const auto reduced_storage = makeStorage(1, 2);
    OutboxStore reduced(db_, reduced_storage);

    const auto error = requireCapacityError(
        reduced.enqueue(makeEvent(4), kNow)
    );

    EXPECT_EQ(
        error.code,
        OutboxCapacityCode::PendingLimitReached
    );

    EXPECT_EQ(error.pending_records, 3);
    EXPECT_EQ(error.total_records, 3);
    EXPECT_EQ(error.limit, 1);

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        3
    );
}

TEST_F(OutboxCapacityTest, CopiesCapacityValuesAtConstruction)
{
    auto storage = makeStorage(1, 2);
    OutboxStore store(db_, storage);

    // 修改原配置对象，不改变已创建实例的容量。
    storage.max_pending_records = 10;
    storage.max_total_records = 10;

    ASSERT_EQ(
        requireInsertResult(
            store.enqueue(makeEvent(1), kNow)
        ).status,
        OutboxInsertStatus::Inserted
    );

    const auto error = requireCapacityError(
        store.enqueue(makeEvent(2), kNow)
    );

    EXPECT_EQ(
        error.code,
        OutboxCapacityCode::PendingLimitReached
    );

    EXPECT_EQ(error.limit, 1);
}

} // namespace