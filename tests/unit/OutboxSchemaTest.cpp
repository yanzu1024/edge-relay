#include "edge/storage/OutboxSchema.h"
#include "edge/storage/SqliteDatabase.h"
#include "edge/storage/SqliteStatement.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

namespace {

using edge::initializeOutboxSchema;
using edge::kOutboxApplicationId;
using edge::kOutboxSchemaVersion;
using edge::SqliteDatabase;
using edge::SqliteError;
using edge::SqliteStatement;
using edge::SqliteStepResult;

void executeSql(
    SqliteDatabase& db,
    const std::string& sql
)
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
    SqliteStatement statement(db, sql);

    if (statement.step() != SqliteStepResult::Row) {
        throw std::logic_error("Test query returned no row");
    }

    const auto value = static_cast<std::int64_t>(
        sqlite3_column_int64(statement.get(), 0)
    );

    if (statement.step() != SqliteStepResult::Done) {
        throw std::logic_error(
            "Test query returned multiple rows"
        );
    }

    return value;
}

// 本函数只创建结构测试用记录。
// "{}" 不代表通过了项目的事件解析或业务验证。
void insertProbeRecord(
    SqliteDatabase& db,
    const std::string& event_id
)
{
    SqliteStatement insert(
        db,
        "INSERT INTO outbox("
        "event_id, payload_json, created_at_ms, updated_at_ms"
        ") VALUES (?1, ?2, ?3, ?4);"
    );

    insert.bindText(1, event_id);
    insert.bindText(2, "{}");
    insert.bindInt64(3, 1000);
    insert.bindInt64(4, 1000);

    if (insert.step() != SqliteStepResult::Done) {
        throw std::logic_error("Probe insert did not finish");
    }
}

class OutboxSchemaTest : public ::testing::Test {
protected:
    SqliteDatabase db_{":memory:"};
};

TEST_F(OutboxSchemaTest, CreatesSchemaAndDefaults)
{
    initializeOutboxSchema(db_);

    EXPECT_EQ(
        scalarInt64(db_, "PRAGMA main.application_id;"),
        kOutboxApplicationId
    );

    EXPECT_EQ(
        scalarInt64(db_, "PRAGMA main.user_version;"),
        kOutboxSchemaVersion
    );

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM main.sqlite_master "
            "WHERE name NOT GLOB 'sqlite_*';"
        ),
        3
    );

    insertProbeRecord(db_, "event-1");

    SqliteStatement query(
        db_,
        "SELECT state, attempt_count, next_attempt_at_ms, "
        "acked_at_ms, last_error FROM outbox;"
    );

    ASSERT_EQ(query.step(), SqliteStepResult::Row);

    EXPECT_STREQ(
        reinterpret_cast<const char*>(
            sqlite3_column_text(query.get(), 0)
        ),
        "PENDING"
    );

    EXPECT_EQ(sqlite3_column_int64(query.get(), 1), 0);
    EXPECT_EQ(sqlite3_column_int64(query.get(), 2), 0);

    EXPECT_EQ(
        sqlite3_column_type(query.get(), 3),
        SQLITE_NULL
    );

    EXPECT_STREQ(
        reinterpret_cast<const char*>(
            sqlite3_column_text(query.get(), 4)
        ),
        ""
    );
}

TEST_F(OutboxSchemaTest, ReinitializationKeepsRecords)
{
    initializeOutboxSchema(db_);

    insertProbeRecord(db_, "event-1");

    initializeOutboxSchema(db_);
    initializeOutboxSchema(db_);

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        1
    );

    EXPECT_EQ(
        scalarInt64(db_, "PRAGMA main.user_version;"),
        1
    );
}

TEST_F(OutboxSchemaTest, RejectsUnknownVersion)
{
    executeSql(db_, "PRAGMA main.user_version = 99;");

    EXPECT_THROW(
        initializeOutboxSchema(db_),
        std::runtime_error
    );

    EXPECT_EQ(
        scalarInt64(db_, "PRAGMA main.user_version;"),
        99
    );

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM main.sqlite_master "
            "WHERE name = 'outbox';"
        ),
        0
    );
}

TEST_F(OutboxSchemaTest, RejectsForeignApplicationId)
{
    executeSql(db_, "PRAGMA main.application_id = 42;");

    EXPECT_THROW(
        initializeOutboxSchema(db_),
        std::runtime_error
    );

    EXPECT_EQ(
        scalarInt64(db_, "PRAGMA main.application_id;"),
        42
    );

    EXPECT_EQ(
        scalarInt64(db_, "PRAGMA main.user_version;"),
        0
    );
}

TEST_F(OutboxSchemaTest, RejectsUnversionedExistingTable)
{
    executeSql(db_, "CREATE TABLE other (value INTEGER);");
    executeSql(db_, "INSERT INTO other(value) VALUES (7);");

    EXPECT_THROW(
        initializeOutboxSchema(db_),
        std::runtime_error
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT value FROM other;"),
        7
    );

    EXPECT_EQ(
        scalarInt64(db_, "PRAGMA main.user_version;"),
        0
    );
}

TEST_F(OutboxSchemaTest, RejectsAlteredIndexDefinition)
{
    initializeOutboxSchema(db_);

    insertProbeRecord(db_, "event-1");

    executeSql(db_, "DROP INDEX idx_outbox_ready;");

    // 名字相同，但索引列与当前版本定义不同。
    executeSql(
        db_,
        "CREATE INDEX idx_outbox_ready ON outbox(state);"
    );

    EXPECT_THROW(
        initializeOutboxSchema(db_),
        std::runtime_error
    );

    // 不能通过重建表或清空数据来“修复”初始化。
    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        1
    );
}

TEST_F(OutboxSchemaTest, RejectsInvalidStateAndDuplicateIdentity)
{
    initializeOutboxSchema(db_);

    insertProbeRecord(db_, "event-1");

    EXPECT_THROW(
        insertProbeRecord(db_, "event-1"),
        SqliteError
    );

    for (const char* sql : {
            "UPDATE outbox SET state = 'UNKNOWN';",
            "UPDATE outbox SET attempt_count = -1;",
            "UPDATE outbox SET attempt_count = 1.5;"
        }) {
        EXPECT_THROW(
            executeSql(db_, sql),
            SqliteError
        );
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM outbox;"),
        1
    );
}

TEST_F(OutboxSchemaTest, AckedStateRequiresTimestamp)
{
    initializeOutboxSchema(db_);

    insertProbeRecord(db_, "event-1");

    EXPECT_THROW(
        executeSql(db_, "UPDATE outbox SET state = 'ACKED';"),
        SqliteError
    );

    executeSql(
        db_,
        "UPDATE outbox SET "
        "state = 'ACKED', acked_at_ms = 2000;"
    );

    // 离开 ACKED 状态时，也必须同时清除确认时间。
    EXPECT_THROW(
        executeSql(db_, "UPDATE outbox SET state = 'RETRY';"),
        SqliteError
    );

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT acked_at_ms FROM outbox WHERE state = 'ACKED';"
        ),
        2000
    );
}

TEST_F(OutboxSchemaTest, InitializationFailureRollsBackEverything)
{
    // 拒绝写入 user_version。
    // 此时表、索引和 application_id 已经写入事务，
    // 可以验证后续失败是否把它们一起撤销。
    const auto deny_version_write = [](
        void*,
        int action,
        const char* first,
        const char* second,
        const char*,
        const char*
    ) -> int {
        if (action == SQLITE_PRAGMA &&
            first != nullptr &&
            second != nullptr &&
            std::strcmp(first, "user_version") == 0) {
            return SQLITE_DENY;
        }

        return SQLITE_OK;
    };

    ASSERT_EQ(
        sqlite3_set_authorizer(
            db_.get(),
            deny_version_write,
            nullptr
        ),
        SQLITE_OK
    );

    EXPECT_THROW(
        initializeOutboxSchema(db_),
        SqliteError
    );

    // 移除测试钩子，再检查回滚后的数据库。
    EXPECT_EQ(
        sqlite3_set_authorizer(db_.get(), nullptr, nullptr),
        SQLITE_OK
    );

    EXPECT_EQ(
        scalarInt64(db_, "PRAGMA main.application_id;"),
        0
    );

    EXPECT_EQ(
        scalarInt64(db_, "PRAGMA main.user_version;"),
        0
    );

    EXPECT_EQ(
        scalarInt64(
            db_,
            "SELECT COUNT(*) FROM main.sqlite_master "
            "WHERE name NOT GLOB 'sqlite_*';"
        ),
        0
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);

    EXPECT_NO_THROW(initializeOutboxSchema(db_));
}

TEST_F(OutboxSchemaTest, RejectsClosedDatabase)
{
    db_.close();

    EXPECT_THROW(
        initializeOutboxSchema(db_),
        std::logic_error
    );
}

} // namespace