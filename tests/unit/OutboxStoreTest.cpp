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
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

namespace {

using edge::EventJsonCode;
using edge::EventJsonError;
using edge::OutboxEnqueueResult;
using edge::OutboxInsertResult;
using edge::OutboxInsertStatus;
using edge::OutboxStore;
using edge::SensorEvent;
using edge::SqliteDatabase;
using edge::SqliteError;
using edge::SqliteStatement;
using edge::SqliteStepResult;

constexpr std::int64_t kNow = 1700000000100LL;

SensorEvent makeEvent()
{
    SensorEvent event;

    event.device_id = "sensor-01";
    event.session_id = "550e8400-e29b-41d4-a716-446655440000";
    event.sequence = 1;
    event.observed_at_ms = 1700000000000LL;
    event.metric = "temperature";
    event.value = 21.5;
    event.unit = "degC";

    return event;
}

OutboxInsertResult requireInsertResult(OutboxEnqueueResult result)
{
    if (!std::holds_alternative<OutboxInsertResult>(result)) {
        throw std::runtime_error(
            "Unexpected event serialization error in test"
        );
    }

    return std::get<OutboxInsertResult>(std::move(result));
}

std::string serializedEvent(const SensorEvent& event)
{
    auto result = edge::serializeSensorEventJson(event);

    if (!std::holds_alternative<std::string>(result)) {
        throw std::runtime_error(
            "Cannot serialize test event"
        );
    }

    return std::get<std::string>(std::move(result));
}

void executeSql(SqliteDatabase& db, const std::string& sql)
{
    SqliteStatement statement(db, sql);

    if (statement.step() != SqliteStepResult::Done) {
        throw std::logic_error(
            "Test SQL unexpectedly returned rows"
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

std::string scalarText(
    SqliteDatabase& db,
    const std::string& sql
)
{
    SqliteStatement query(db, sql);

    if (query.step() != SqliteStepResult::Row) {
        throw std::logic_error("Test query returned no row");
    }

    const unsigned char* text =
        sqlite3_column_text(query.get(), 0);

    if (text == nullptr) {
        throw std::logic_error("Test query returned null text");
    }

    const int length = sqlite3_column_bytes(query.get(), 0);

    const std::string value(
        reinterpret_cast<const char*>(text),
        static_cast<std::size_t>(length)
    );

    if (query.step() != SqliteStepResult::Done) {
        throw std::logic_error(
            "Test query returned multiple rows"
        );
    }

    return value;
}

void replaceStoredPayload(
    SqliteDatabase& db,
    const std::string& event_id,
    const std::string& payload
)
{
    SqliteStatement update(
        db,
        "UPDATE outbox SET payload_json = ?1 WHERE event_id = ?2;"
    );

    update.bindText(1, payload);
    update.bindText(2, event_id);

    if (update.step() != SqliteStepResult::Done) {
        throw std::logic_error("Test payload update did not finish");
    }
}

class OutboxStoreTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        edge::initializeOutboxSchema(db_);
    }

    SqliteDatabase db_{":memory:"};
};

TEST_F(OutboxStoreTest, InsertsEventAndMetadata)
{
    OutboxStore store(db_);
    const auto event = makeEvent();

    const auto result = requireInsertResult(
        store.enqueue(event, kNow)
    );

    EXPECT_EQ(result.status, OutboxInsertStatus::Inserted);
    EXPECT_EQ(result.event_id, edge::makeEventId(event));

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        1
    );

    EXPECT_EQ(
        scalarText(db_, "SELECT payload_json FROM outbox;"),
        serializedEvent(event)
    );

    EXPECT_EQ(
        scalarText(db_, "SELECT state FROM outbox;"),
        "PENDING"
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT created_at_ms FROM outbox;"),
        kNow
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT updated_at_ms FROM outbox;"),
        kNow
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT attempt_count FROM outbox;"),
        0
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);
}

TEST_F(OutboxStoreTest, DuplicatePreservesAckedRecord)
{
    OutboxStore store(db_);
    const auto event = makeEvent();

    ASSERT_EQ(
        requireInsertResult(store.enqueue(event, kNow)).status,
        OutboxInsertStatus::Inserted
    );

    executeSql(
        db_,
        "UPDATE outbox SET "
        "state = 'ACKED', "
        "attempt_count = 3, "
        "next_attempt_at_ms = created_at_ms + 500, "
        "updated_at_ms = created_at_ms + 100, "
        "acked_at_ms = created_at_ms + 100, "
        "last_error = 'old note';"
    );

    const auto result = requireInsertResult(
        store.enqueue(event, kNow + 9999)
    );

    EXPECT_EQ(result.status, OutboxInsertStatus::Duplicate);

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        1
    );

    EXPECT_EQ(
        scalarText(db_, "SELECT state FROM outbox;"),
        "ACKED"
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT attempt_count FROM outbox;"),
        3
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT created_at_ms FROM outbox;"),
        kNow
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT updated_at_ms FROM outbox;"),
        kNow + 100
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT acked_at_ms FROM outbox;"),
        kNow + 100
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT next_attempt_at_ms FROM outbox;"),
        kNow + 500
    );

    EXPECT_EQ(
        scalarText(db_, "SELECT last_error FROM outbox;"),
        "old note"
    );

    EXPECT_EQ(
        scalarText(db_, "SELECT payload_json FROM outbox;"),
        serializedEvent(event)
    );
}

TEST_F(OutboxStoreTest, ConflictPreservesOriginalRecord)
{
    OutboxStore store(db_);
    const auto original = makeEvent();

    ASSERT_EQ(
        requireInsertResult(store.enqueue(original, kNow)).status,
        OutboxInsertStatus::Inserted
    );

    for (int scenario = 0; scenario < 3; ++scenario) {
        auto changed = original;

        if (scenario == 0) {
            changed.value = 99.0;
        } else if (scenario == 1) {
            ++changed.observed_at_ms;
        } else {
            changed.metric = "vibration";
            changed.unit = "mm_s";
        }

        const auto result = requireInsertResult(
            store.enqueue(changed, kNow + 100)
        );

        EXPECT_EQ(result.status, OutboxInsertStatus::Conflict);
        EXPECT_EQ(result.event_id, edge::makeEventId(original));
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        1
    );

    EXPECT_EQ(
        scalarText(db_, "SELECT payload_json FROM outbox;"),
        serializedEvent(original)
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT updated_at_ms FROM outbox;"),
        kNow
    );
}

TEST_F(OutboxStoreTest, DifferentIdentityCreatesNewRecord)
{
    OutboxStore store(db_);

    auto event = makeEvent();

    const auto first = requireInsertResult(
        store.enqueue(event, kNow)
    );

    ++event.sequence;

    const auto second = requireInsertResult(
        store.enqueue(event, kNow + 1)
    );

    EXPECT_EQ(first.status, OutboxInsertStatus::Inserted);
    EXPECT_EQ(second.status, OutboxInsertStatus::Inserted);
    EXPECT_NE(first.event_id, second.event_id);

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        2
    );
}

TEST_F(OutboxStoreTest, SignedZerosAreDuplicate)
{
    OutboxStore store(db_);
    auto event = makeEvent();

    event.value = -0.0;

    ASSERT_EQ(
        requireInsertResult(store.enqueue(event, kNow)).status,
        OutboxInsertStatus::Inserted
    );

    event.value = 0.0;

    EXPECT_EQ(
        requireInsertResult(store.enqueue(event, kNow + 1)).status,
        OutboxInsertStatus::Duplicate
    );
}

TEST_F(OutboxStoreTest, EquivalentJsonFormattingIsDuplicate)
{
    OutboxStore store(db_);
    const auto event = makeEvent();

    ASSERT_EQ(
        requireInsertResult(store.enqueue(event, kNow)).status,
        OutboxInsertStatus::Inserted
    );

    // 内容相同，但字段顺序和空白不同。
    const std::string reformatted = R"json({
    "unit": "degC",
    "value": 21.5,
    "metric": "temperature",
    "observed_at_ms": 1700000000000,
    "sequence": 1,
    "session_id": "550e8400-e29b-41d4-a716-446655440000",
    "device_id": "sensor-01",
    "schema_version": 1
})json";

    ASSERT_NE(reformatted, serializedEvent(event));

    replaceStoredPayload(
        db_,
        edge::makeEventId(event),
        reformatted
    );

    EXPECT_EQ(
        requireInsertResult(store.enqueue(event, kNow + 1)).status,
        OutboxInsertStatus::Duplicate
    );

    // 重复检查不重写已有文本。
    EXPECT_EQ(
        scalarText(db_, "SELECT payload_json FROM outbox;"),
        reformatted
    );
}

TEST_F(OutboxStoreTest, SerializationErrorsDoNotWrite)
{
    OutboxStore store(db_);
    auto event = makeEvent();

    event.value = std::numeric_limits<double>::infinity();

    auto result = store.enqueue(event, kNow);

    ASSERT_TRUE(std::holds_alternative<EventJsonError>(result));

    EXPECT_EQ(
        std::get<EventJsonError>(result).code,
        EventJsonCode::NonFiniteValue
    );

    EXPECT_EQ(
        std::get<EventJsonError>(result).field,
        "value"
    );

    event = makeEvent();
    event.device_id = std::string(1, static_cast<char>(0xff));

    result = store.enqueue(event, kNow);

    ASSERT_TRUE(std::holds_alternative<EventJsonError>(result));

    EXPECT_EQ(
        std::get<EventJsonError>(result).code,
        EventJsonCode::InvalidTextEncoding
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        0
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);
}

TEST_F(OutboxStoreTest, RejectsInvalidLocalTimestamp)
{
    OutboxStore store(db_);
    const auto event = makeEvent();

    EXPECT_THROW(
        store.enqueue(event, 0),
        std::invalid_argument
    );

    EXPECT_THROW(
        store.enqueue(event, -1),
        std::invalid_argument
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        0
    );
}

TEST_F(OutboxStoreTest, SqliteInsertErrorRollsBack)
{
    OutboxStore store(db_);
    const auto event = makeEvent();

    const auto deny_insert = [](
        void*,
        int action,
        const char* first,
        const char*,
        const char*,
        const char*
    ) -> int {
        if (action == SQLITE_INSERT &&
            first != nullptr &&
            std::strcmp(first, "outbox") == 0) {
            return SQLITE_DENY;
        }

        return SQLITE_OK;
    };

    ASSERT_EQ(
        sqlite3_set_authorizer(db_.get(), deny_insert, nullptr),
        SQLITE_OK
    );

    EXPECT_THROW(
        store.enqueue(event, kNow),
        SqliteError
    );

    EXPECT_EQ(
        sqlite3_set_authorizer(db_.get(), nullptr, nullptr),
        SQLITE_OK
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        0
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);

    EXPECT_EQ(
        requireInsertResult(store.enqueue(event, kNow)).status,
        OutboxInsertStatus::Inserted
    );
}

TEST_F(OutboxStoreTest, CommitFailureRollsBack)
{
    OutboxStore store(db_);
    const auto event = makeEvent();

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

        // 允许 ROLLBACK，让异常退出能够完成清理。
        return SQLITE_OK;
    };

    ASSERT_EQ(
        sqlite3_set_authorizer(db_.get(), deny_commit, nullptr),
        SQLITE_OK
    );

    EXPECT_THROW(
        store.enqueue(event, kNow),
        SqliteError
    );

    EXPECT_EQ(
        sqlite3_set_authorizer(db_.get(), nullptr, nullptr),
        SQLITE_OK
    );

    // INSERT 曾经执行，但 COMMIT 被拒绝，记录必须被回滚。
    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        0
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);

    EXPECT_EQ(
        requireInsertResult(store.enqueue(event, kNow)).status,
        OutboxInsertStatus::Inserted
    );
}

TEST_F(OutboxStoreTest, CorruptedStoredPayloadIsReported)
{
    OutboxStore store(db_);
    const auto event = makeEvent();

    ASSERT_EQ(
        requireInsertResult(store.enqueue(event, kNow)).status,
        OutboxInsertStatus::Inserted
    );

    replaceStoredPayload(
        db_,
        edge::makeEventId(event),
        "not-json"
    );

    try {
        store.enqueue(event, kNow + 1);
        FAIL() << "Corrupted stored payload must be reported";
    } catch (const std::runtime_error& error) {
        EXPECT_NE(
            std::string(error.what()).find(
                "Stored Outbox payload is invalid"
            ),
            std::string::npos
        );
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        1
    );

    EXPECT_EQ(
        scalarText(db_, "SELECT payload_json FROM outbox;"),
        "not-json"
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);
}

} // namespace