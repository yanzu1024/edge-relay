#include "edge/domain/EventId.h"
#include "edge/domain/EventJson.h"
#include "edge/domain/SensorEvent.h"

#include "edge/storage/OutboxSchema.h"
#include "edge/storage/OutboxStore.h"
#include "edge/storage/SqliteDatabase.h"
#include "edge/storage/SqliteStatement.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

namespace {

using edge::OutboxStore;
using edge::SensorEvent;
using edge::SqliteDatabase;
using edge::SqliteError;
using edge::SqliteStatement;
using edge::SqliteStepResult;

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

std::string eventId(std::int64_t sequence)
{
    return edge::makeEventId(makeEvent(sequence));
}

std::string payload(std::int64_t sequence)
{
    auto result = edge::serializeSensorEventJson(
        makeEvent(sequence)
    );

    if (!std::holds_alternative<std::string>(result)) {
        throw std::runtime_error("Cannot serialize test event");
    }

    return std::get<std::string>(std::move(result));
}

void addRecord(
    SqliteDatabase& db,
    std::int64_t sequence,
    const std::string& state,
    std::int64_t created_at_ms,
    std::int64_t next_attempt_at_ms = 0,
    std::int64_t attempt_count = 0
)
{
    SqliteStatement insert(
        db,
        "INSERT INTO outbox("
        "event_id, payload_json, state, attempt_count, "
        "next_attempt_at_ms, created_at_ms, updated_at_ms, "
        "acked_at_ms, last_error"
        ") VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, ?7, 'previous error');"
    );

    insert.bindText(1, eventId(sequence));
    insert.bindText(2, payload(sequence));
    insert.bindText(3, state);
    insert.bindInt64(4, attempt_count);
    insert.bindInt64(5, next_attempt_at_ms);
    insert.bindInt64(6, created_at_ms);

    if (state == "ACKED") {
        insert.bindInt64(7, created_at_ms + 1);
    } else {
        insert.bindNull(7);
    }

    if (insert.step() != SqliteStepResult::Done) {
        throw std::logic_error("Test insertion did not finish");
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

class OutboxRecoveryTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        edge::initializeOutboxSchema(db_);
    }

    SqliteDatabase db_{":memory:"};
};

TEST_F(OutboxRecoveryTest, EmptyStoreHasZeroCounts)
{
    OutboxStore store(db_);
    const auto counts = store.stateCounts();

    EXPECT_EQ(counts.pending, 0);
    EXPECT_EQ(counts.sending, 0);
    EXPECT_EQ(counts.retry, 0);
    EXPECT_EQ(counts.acked, 0);
    EXPECT_EQ(counts.dead, 0);
    EXPECT_EQ(counts.total, 0);
    EXPECT_EQ(counts.unacked, 0);

    EXPECT_TRUE(store.listReady(kNow, 10).empty());
    EXPECT_EQ(store.recoverSending(kNow), 0);
}

TEST_F(OutboxRecoveryTest, CountsEveryState)
{
    OutboxStore store(db_);

    std::int64_t sequence = 0;

    for (const char* state : {
            "PENDING", "SENDING", "RETRY", "ACKED", "DEAD"
        }) {
        addRecord(db_, ++sequence, state, kNow - 1000);
    }

    const auto counts = store.stateCounts();

    EXPECT_EQ(counts.pending, 1);
    EXPECT_EQ(counts.sending, 1);
    EXPECT_EQ(counts.retry, 1);
    EXPECT_EQ(counts.acked, 1);
    EXPECT_EQ(counts.dead, 1);
    EXPECT_EQ(counts.total, 5);
    EXPECT_EQ(counts.unacked, 4);
}

TEST_F(OutboxRecoveryTest, RecoveryPreservesIdentityPayloadAndAttempts)
{
    OutboxStore store(db_);

    addRecord(db_, 1, "SENDING", kNow - 500, kNow + 9999, 3);
    addRecord(db_, 2, "PENDING", kNow - 400);
    addRecord(db_, 3, "RETRY", kNow - 300, kNow + 2000, 2);
    addRecord(db_, 4, "ACKED", kNow - 200, 0, 4);
    addRecord(db_, 5, "DEAD", kNow - 100, 0, 8);

    EXPECT_EQ(store.recoverSending(kNow), 1);

    const auto counts = store.stateCounts();

    EXPECT_EQ(counts.pending, 1);
    EXPECT_EQ(counts.sending, 0);
    EXPECT_EQ(counts.retry, 2);
    EXPECT_EQ(counts.acked, 1);
    EXPECT_EQ(counts.dead, 1);
    EXPECT_EQ(counts.total, 5);
    EXPECT_EQ(counts.unacked, 4);

    SqliteStatement query(
        db_,
        "SELECT event_id, payload_json, state, attempt_count, "
        "created_at_ms, updated_at_ms, next_attempt_at_ms "
        "FROM outbox WHERE event_id = ?1;"
    );

    query.bindText(1, eventId(1));

    ASSERT_EQ(query.step(), SqliteStepResult::Row);

    EXPECT_STREQ(
        reinterpret_cast<const char*>(
            sqlite3_column_text(query.get(), 0)
        ),
        eventId(1).c_str()
    );

    EXPECT_STREQ(
        reinterpret_cast<const char*>(
            sqlite3_column_text(query.get(), 1)
        ),
        payload(1).c_str()
    );

    EXPECT_STREQ(
        reinterpret_cast<const char*>(
            sqlite3_column_text(query.get(), 2)
        ),
        "RETRY"
    );

    EXPECT_EQ(sqlite3_column_int64(query.get(), 3), 3);
    EXPECT_EQ(sqlite3_column_int64(query.get(), 4), kNow - 500);
    EXPECT_EQ(sqlite3_column_int64(query.get(), 5), kNow);
    EXPECT_EQ(sqlite3_column_int64(query.get(), 6), kNow);

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT next_attempt_at_ms FROM outbox "
            "WHERE attempt_count = 2;"
        ),
        kNow + 2000
    );
}

TEST_F(OutboxRecoveryTest, RecoveryIsIdempotent)
{
    OutboxStore store(db_);

    addRecord(db_, 1, "SENDING", kNow - 100, kNow + 9999, 2);

    EXPECT_EQ(store.recoverSending(kNow), 1);
    EXPECT_EQ(store.recoverSending(kNow + 100), 0);

    EXPECT_EQ(
        scalarInt64(db_, "SELECT updated_at_ms FROM outbox;"),
        kNow
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT attempt_count FROM outbox;"),
        2
    );
}

TEST_F(OutboxRecoveryTest, ReadyQueryFiltersOrdersAndLimits)
{
    OutboxStore store(db_);

    addRecord(db_, 3, "PENDING", kNow - 500);
    addRecord(db_, 10, "RETRY", kNow - 400, kNow, 4);
    addRecord(db_, 2, "PENDING", kNow - 400);

    addRecord(db_, 4, "RETRY", kNow - 900, kNow + 1);
    addRecord(db_, 5, "SENDING", kNow - 1000);
    addRecord(db_, 6, "ACKED", kNow - 1100);
    addRecord(db_, 7, "DEAD", kNow - 1200);

    const auto limited = store.listReady(kNow, 2);

    ASSERT_EQ(limited.size(), 2u);

    EXPECT_EQ(limited[0].event_id, eventId(3));

    // 创建时间相同，按 event_id 的字典序排列。
    EXPECT_EQ(limited[1].event_id, eventId(10));
    EXPECT_EQ(limited[1].attempt_count, 4);

    const auto all = store.listReady(kNow, 10);

    ASSERT_EQ(all.size(), 3u);
    EXPECT_EQ(all[2].event_id, eventId(2));

    // 时间推进到重试截止点后，原先被过滤的旧记录具备资格。
    const auto later = store.listReady(kNow + 1, 1);

    ASSERT_EQ(later.size(), 1u);
    EXPECT_EQ(later[0].event_id, eventId(4));
}

TEST_F(OutboxRecoveryTest, ReadyQueryOwnsDataAndDoesNotClaimRecords)
{
    OutboxStore store(db_);

    addRecord(db_, 1, "RETRY", kNow - 100, kNow, 5);

    const auto first = store.listReady(kNow, 10);

    // 查询已经结束，语句已经释放。
    EXPECT_EQ(sqlite3_next_stmt(db_.get(), nullptr), nullptr);

    const auto second = store.listReady(kNow, 10);

    ASSERT_EQ(first.size(), 1u);
    ASSERT_EQ(second.size(), 1u);

    EXPECT_EQ(first[0].event_id, eventId(1));
    EXPECT_EQ(first[0].payload_json, payload(1));
    EXPECT_EQ(first[0].attempt_count, 5);
    EXPECT_EQ(second[0].event_id, first[0].event_id);

    const auto counts = store.stateCounts();

    EXPECT_EQ(counts.retry, 1);
    EXPECT_EQ(counts.sending, 0);
}

TEST_F(OutboxRecoveryTest, RejectsInvalidArguments)
{
    OutboxStore store(db_);

    addRecord(db_, 1, "SENDING", kNow - 100);

    EXPECT_THROW(store.recoverSending(0), std::invalid_argument);
    EXPECT_THROW(store.recoverSending(-1), std::invalid_argument);

    EXPECT_THROW(store.listReady(0, 1), std::invalid_argument);
    EXPECT_THROW(store.listReady(-1, 1), std::invalid_argument);
    EXPECT_THROW(store.listReady(kNow, 0), std::invalid_argument);
    EXPECT_THROW(store.listReady(kNow, -1), std::invalid_argument);

    EXPECT_EQ(store.stateCounts().sending, 1);
}

TEST_F(OutboxRecoveryTest, CommitFailureRollsBackRecovery)
{
    OutboxStore store(db_);

    addRecord(db_, 1, "SENDING", kNow - 100, kNow + 9999, 3);

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

    EXPECT_THROW(store.recoverSending(kNow), SqliteError);

    EXPECT_EQ(
        sqlite3_set_authorizer(db_.get(), nullptr, nullptr),
        SQLITE_OK
    );

    EXPECT_EQ(store.stateCounts().sending, 1);

    EXPECT_EQ(
        scalarInt64(db_, "SELECT updated_at_ms FROM outbox;"),
        kNow - 100
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);

    EXPECT_EQ(store.recoverSending(kNow), 1);
}

TEST_F(OutboxRecoveryTest, ReadyQueryReportsCorruptedPayload)
{
    OutboxStore store(db_);

    addRecord(db_, 1, "PENDING", kNow - 100);

    {
        SqliteStatement damage(
            db_,
            "UPDATE outbox SET payload_json = 'broken';"
        );

        ASSERT_EQ(damage.step(), SqliteStepResult::Done);
    }

    try {
        store.listReady(kNow, 10);
        FAIL() << "Corrupted payload must be reported";
    } catch (const std::runtime_error& error) {
        EXPECT_NE(
            std::string(error.what()).find(
                "Stored Outbox payload is invalid"
            ),
            std::string::npos
        );
    }

    EXPECT_EQ(store.stateCounts().pending, 1);
    EXPECT_EQ(store.stateCounts().sending, 0);
}

} // namespace