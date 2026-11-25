#include "edge/config/AgentConfig.h"
#include "edge/domain/SensorEvent.h"

#include "edge/storage/OutboxSchema.h"
#include "edge/storage/OutboxStore.h"
#include "edge/storage/SqliteDatabase.h"
#include "edge/storage/SqliteStatement.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <cstdint>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <variant>

namespace {

using edge::OutboxCapacityCode;
using edge::OutboxCapacityError;
using edge::OutboxInsertResult;
using edge::OutboxInsertStatus;
using edge::OutboxStore;
using edge::SensorEvent;
using edge::SqliteDatabase;
using edge::SqliteError;
using edge::SqliteStatement;
using edge::SqliteStepResult;
using edge::StorageConfig;

constexpr std::int64_t kDayMs = 86400000LL;
constexpr std::int64_t kNow = 1700000000000LL;

StorageConfig makeStorage(
    std::int32_t days = 7,
    std::int64_t keep = 10
)
{
    StorageConfig storage;

    storage.max_pending_records = 100;
    storage.max_total_records = 100;
    storage.acked_keep_days = days;
    storage.acked_keep_max_records = keep;

    return storage;
}

// 清理测试只关注状态和确认时间。
// "{}" 是结构测试数据，不代表通过业务事件验证。
void addRecord(
    SqliteDatabase& db,
    const std::string& event_id,
    const std::string& state,
    std::int64_t acked_at_ms = 0
)
{
    SqliteStatement insert(
        db,
        "INSERT INTO outbox("
        "event_id, payload_json, state, "
        "created_at_ms, updated_at_ms, acked_at_ms"
        ") VALUES (?1, '{}', ?2, 1, 1, ?3);"
    );

    insert.bindText(1, event_id);
    insert.bindText(2, state);

    if (state == "ACKED") {
        insert.bindInt64(3, acked_at_ms);
    } else {
        insert.bindNull(3);
    }

    if (insert.step() != SqliteStepResult::Done) {
        throw std::logic_error(
            "Test record insertion did not finish"
        );
    }
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

class OutboxCleanupTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        edge::initializeOutboxSchema(db_);
    }

    SqliteDatabase db_{":memory:"};
};

TEST_F(OutboxCleanupTest, EmptyCleanupReturnsZero)
{
    const auto storage = makeStorage();
    OutboxStore store(db_, storage);

    const auto result = store.cleanupAcked(kNow);

    EXPECT_EQ(result.expired_deleted, 0);
    EXPECT_EQ(result.excess_deleted, 0);
    EXPECT_EQ(result.remaining_acked, 0);
}

TEST_F(OutboxCleanupTest, ExpiryBoundaryAndUnconfirmedRecords)
{
    const auto storage = makeStorage(7, 10);
    OutboxStore store(db_, storage);

    const auto cutoff = kNow - 7 * kDayMs;

    addRecord(db_, "expired", "ACKED", cutoff - 1);
    addRecord(db_, "boundary", "ACKED", cutoff);
    addRecord(db_, "recent", "ACKED", cutoff + 1);

    addRecord(db_, "pending", "PENDING");
    addRecord(db_, "sending", "SENDING");
    addRecord(db_, "retry", "RETRY");
    addRecord(db_, "dead", "DEAD");

    const auto result = store.cleanupAcked(kNow);

    EXPECT_EQ(result.expired_deleted, 1);
    EXPECT_EQ(result.excess_deleted, 0);
    EXPECT_EQ(result.remaining_acked, 2);

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM outbox "
            "WHERE event_id = 'boundary';"
        ),
        1
    );

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM outbox WHERE state <> 'ACKED';"
        ),
        4
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        6
    );
}

TEST_F(OutboxCleanupTest, CountLimitKeepsNewestRecords)
{
    const auto storage = makeStorage(7, 2);
    OutboxStore store(db_, storage);

    addRecord(db_, "old", "ACKED", kNow - 300);
    addRecord(db_, "new", "ACKED", kNow - 100);
    addRecord(db_, "middle", "ACKED", kNow - 200);

    const auto result = store.cleanupAcked(kNow);

    EXPECT_EQ(result.expired_deleted, 0);
    EXPECT_EQ(result.excess_deleted, 1);
    EXPECT_EQ(result.remaining_acked, 2);

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM outbox WHERE event_id = 'old';"
        ),
        0
    );

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM outbox "
            "WHERE event_id IN ('middle', 'new');"
        ),
        2
    );
}

TEST_F(OutboxCleanupTest, TiesAreDeterministicAndCleanupIsIdempotent)
{
    const auto storage = makeStorage(7, 2);
    OutboxStore store(db_, storage);

    for (const char* id : {"a", "c", "b"}) {
        addRecord(db_, id, "ACKED", kNow - 100);
    }

    const auto first = store.cleanupAcked(kNow);

    EXPECT_EQ(first.excess_deleted, 1);

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM outbox "
            "WHERE event_id IN ('b', 'c');"
        ),
        2
    );

    const auto second = store.cleanupAcked(kNow);

    EXPECT_EQ(second.expired_deleted, 0);
    EXPECT_EQ(second.excess_deleted, 0);
    EXPECT_EQ(second.remaining_acked, 2);
}

TEST_F(OutboxCleanupTest, AppliesAgeThenCount)
{
    const auto storage = makeStorage(7, 2);
    OutboxStore store(db_, storage);

    addRecord(db_, "expired", "ACKED", kNow - 8 * kDayMs);
    addRecord(db_, "recent-old", "ACKED", kNow - 3 * kDayMs);
    addRecord(db_, "middle", "ACKED", kNow - 2 * kDayMs);
    addRecord(db_, "latest", "ACKED", kNow - kDayMs);

    const auto result = store.cleanupAcked(kNow);

    EXPECT_EQ(result.expired_deleted, 1);
    EXPECT_EQ(result.excess_deleted, 1);
    EXPECT_EQ(result.remaining_acked, 2);

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM outbox "
            "WHERE event_id IN ('middle', 'latest');"
        ),
        2
    );
}

TEST_F(OutboxCleanupTest, LargeRetentionAndEarlyTimestampAreSafe)
{
    const auto storage = makeStorage(
        std::numeric_limits<std::int32_t>::max(),
        10
    );

    OutboxStore store(db_, storage);

    addRecord(db_, "past", "ACKED", 500);
    addRecord(db_, "future", "ACKED", 2000);

    const auto result = store.cleanupAcked(1000);

    EXPECT_EQ(result.expired_deleted, 0);
    EXPECT_EQ(result.excess_deleted, 0);
    EXPECT_EQ(result.remaining_acked, 2);
}

TEST_F(OutboxCleanupTest, CleanupReleasesTotalCapacity)
{
    auto storage = makeStorage(7, 1);

    storage.max_pending_records = 2;
    storage.max_total_records = 2;

    OutboxStore store(db_, storage);

    addRecord(db_, "old", "ACKED", kNow - 200);
    addRecord(db_, "recent", "ACKED", kNow - 100);

    SensorEvent event;

    event.device_id = "sensor-01";
    event.session_id = "550e8400-e29b-41d4-a716-446655440000";
    event.sequence = 3;
    event.observed_at_ms = kNow - 1000;
    event.metric = "temperature";
    event.value = 21.5;
    event.unit = "degC";

    const auto blocked = store.enqueue(event, kNow);

    ASSERT_TRUE(
        std::holds_alternative<OutboxCapacityError>(blocked)
    );

    EXPECT_EQ(
        std::get<OutboxCapacityError>(blocked).code,
        OutboxCapacityCode::TotalLimitReached
    );

    const auto cleanup = store.cleanupAcked(kNow);

    EXPECT_EQ(cleanup.excess_deleted, 1);
    EXPECT_EQ(cleanup.remaining_acked, 1);

    const auto accepted = store.enqueue(event, kNow);

    ASSERT_TRUE(
        std::holds_alternative<OutboxInsertResult>(accepted)
    );

    EXPECT_EQ(
        std::get<OutboxInsertResult>(accepted).status,
        OutboxInsertStatus::Inserted
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        2
    );
}

TEST_F(OutboxCleanupTest, SecondDeleteFailureRollsBackAgeDeletion)
{
    const auto storage = makeStorage(7, 1);
    OutboxStore store(db_, storage);

    addRecord(db_, "expired", "ACKED", kNow - 8 * kDayMs);
    addRecord(db_, "middle", "ACKED", kNow - 200);
    addRecord(db_, "latest", "ACKED", kNow - 100);

    int delete_calls = 0;

    const auto deny_second_delete = [](
        void* context,
        int action,
        const char* first,
        const char*,
        const char*,
        const char*
    ) -> int {
        if (action == SQLITE_DELETE &&
            first != nullptr &&
            std::strcmp(first, "outbox") == 0) {
            auto* calls = static_cast<int*>(context);
            ++(*calls);

            if (*calls == 2) {
                return SQLITE_DENY;
            }
        }

        return SQLITE_OK;
    };

    ASSERT_EQ(
        sqlite3_set_authorizer(
            db_.get(),
            deny_second_delete,
            &delete_calls
        ),
        SQLITE_OK
    );

    EXPECT_THROW(
        store.cleanupAcked(kNow),
        SqliteError
    );

    EXPECT_EQ(
        sqlite3_set_authorizer(db_.get(), nullptr, nullptr),
        SQLITE_OK
    );

    EXPECT_EQ(delete_calls, 2);

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        3
    );

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM outbox WHERE event_id = 'expired';"
        ),
        1
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);

    const auto retry = store.cleanupAcked(kNow);

    EXPECT_EQ(retry.expired_deleted, 1);
    EXPECT_EQ(retry.excess_deleted, 1);
    EXPECT_EQ(retry.remaining_acked, 1);
}

TEST_F(OutboxCleanupTest, CommitFailureRollsBackDeletes)
{
    const auto storage = makeStorage(7, 1);
    OutboxStore store(db_, storage);

    addRecord(db_, "expired", "ACKED", kNow - 8 * kDayMs);
    addRecord(db_, "middle", "ACKED", kNow - 200);
    addRecord(db_, "latest", "ACKED", kNow - 100);

    const auto deny_commit = [](
        void*,
        int action,
        const char* first,
        const char*,
        const char*,
        const char*
    ) -> int {
        if (action == SQLITE_TRANSACTION &&
            first != nullptr &&
            std::strcmp(first, "COMMIT") == 0) {
            return SQLITE_DENY;
        }

        return SQLITE_OK;
    };

    ASSERT_EQ(
        sqlite3_set_authorizer(db_.get(), deny_commit, nullptr),
        SQLITE_OK
    );

    EXPECT_THROW(
        store.cleanupAcked(kNow),
        SqliteError
    );

    EXPECT_EQ(
        sqlite3_set_authorizer(db_.get(), nullptr, nullptr),
        SQLITE_OK
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        3
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);
}

TEST_F(OutboxCleanupTest, RejectsInvalidArguments)
{
    const auto storage = makeStorage();
    OutboxStore store(db_, storage);

    addRecord(db_, "expired", "ACKED", kNow - 8 * kDayMs);

    EXPECT_THROW(
        store.cleanupAcked(0),
        std::invalid_argument
    );

    EXPECT_THROW(
        store.cleanupAcked(-1),
        std::invalid_argument
    );

    for (int scenario = 0; scenario < 5; ++scenario) {
        auto invalid = makeStorage();

        if (scenario == 0) {
            invalid.acked_keep_days = 0;
        } else if (scenario == 1) {
            invalid.acked_keep_days = -1;
        } else if (scenario == 2) {
            invalid.acked_keep_max_records = 0;
        } else if (scenario == 3) {
            invalid.acked_keep_max_records = -1;
        } else {
            invalid.acked_keep_max_records = 101;
        }

        EXPECT_THROW(
            {
                OutboxStore rejected(db_, invalid);
            },
            std::invalid_argument
        );
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        1
    );
}

} // namespace