#include "edge/storage/OutboxSchema.h"

#include "edge/storage/SqliteDatabase.h"
#include "edge/storage/SqliteStatement.h"
#include "edge/storage/SqliteTransaction.h"

#include <sqlite3.h>

#include <cstdint>
#include <stdexcept>
#include <string>

namespace edge {

namespace {

// 不添加结尾分号，便于与 sqlite_master 中保存的定义比较。
// 第一版结构固定后，不随意修改这些 SQL 文本。
constexpr const char* kCreateTable = R"sql(CREATE TABLE outbox (
    event_id TEXT NOT NULL PRIMARY KEY CHECK(length(event_id) > 0),
    payload_json TEXT NOT NULL CHECK(length(payload_json) > 0),
    state TEXT NOT NULL DEFAULT 'PENDING'
        CHECK(state IN ('PENDING', 'SENDING', 'RETRY', 'ACKED', 'DEAD')),
    attempt_count INTEGER NOT NULL DEFAULT 0
        CHECK(typeof(attempt_count) = 'integer' AND attempt_count >= 0),
    next_attempt_at_ms INTEGER NOT NULL DEFAULT 0
        CHECK(typeof(next_attempt_at_ms) = 'integer' AND next_attempt_at_ms >= 0),
    created_at_ms INTEGER NOT NULL
        CHECK(typeof(created_at_ms) = 'integer' AND created_at_ms > 0),
    updated_at_ms INTEGER NOT NULL
        CHECK(typeof(updated_at_ms) = 'integer' AND updated_at_ms > 0),
    acked_at_ms INTEGER
        CHECK(acked_at_ms IS NULL OR
              (typeof(acked_at_ms) = 'integer' AND acked_at_ms > 0)),
    last_error TEXT NOT NULL DEFAULT '',
    CHECK(
        (state = 'ACKED' AND acked_at_ms IS NOT NULL) OR
        (state <> 'ACKED' AND acked_at_ms IS NULL)
    )
))sql";

constexpr const char* kCreateReadyIndex =
    "CREATE INDEX idx_outbox_ready ON outbox"
    "(state, next_attempt_at_ms, created_at_ms, event_id)";

constexpr const char* kCreateAckedIndex =
    "CREATE INDEX idx_outbox_acked ON outbox"
    "(state, acked_at_ms, event_id)";

void executeSql(
    SqliteDatabase& database,
    const std::string& sql
)
{
    SqliteStatement statement(database, sql);

    if (statement.step() != SqliteStepResult::Done) {
        throw std::runtime_error(
            "Outbox initialization SQL unexpectedly returned rows"
        );
    }
}

std::int64_t readInteger(
    SqliteDatabase& database,
    const std::string& sql
)
{
    SqliteStatement statement(database, sql);

    if (statement.step() != SqliteStepResult::Row) {
        throw std::runtime_error(
            "Outbox metadata query returned no row"
        );
    }

    if (sqlite3_column_type(statement.get(), 0) != SQLITE_INTEGER) {
        throw std::runtime_error(
            "Outbox metadata is not an integer"
        );
    }

    const auto value = static_cast<std::int64_t>(
        sqlite3_column_int64(statement.get(), 0)
    );

    if (statement.step() != SqliteStepResult::Done) {
        throw std::runtime_error(
            "Outbox metadata query returned multiple rows"
        );
    }

    return value;
}

std::int64_t userObjectCount(SqliteDatabase& database)
{
    // 排除 SQLite 自动生成的内部对象，例如主键自动索引。
    // GLOB 中的下划线是普通字符，不会像 LIKE 那样表示通配符。
    return readInteger(
        database,
        "SELECT COUNT(*) FROM main.sqlite_master "
        "WHERE name NOT GLOB 'sqlite_*';"
    );
}

void requireDefinition(
    SqliteDatabase& database,
    const char* type,
    const char* name,
    const char* expected_sql
)
{
    SqliteStatement query(
        database,
        "SELECT sql FROM main.sqlite_master "
        "WHERE type = ?1 AND name = ?2;"
    );

    query.bindText(1, type);
    query.bindText(2, name);

    if (query.step() != SqliteStepResult::Row) {
        throw std::runtime_error(
            std::string("Missing Outbox schema object: ") + name
        );
    }

    if (sqlite3_column_type(query.get(), 0) != SQLITE_TEXT) {
        throw std::runtime_error(
            std::string("Invalid Outbox schema definition: ") + name
        );
    }

    const unsigned char* text =
        sqlite3_column_text(query.get(), 0);

    if (text == nullptr) {
        throw SqliteError(
            SQLITE_NOMEM,
            "Cannot read Outbox schema definition"
        );
    }

    const int length = sqlite3_column_bytes(query.get(), 0);

    const std::string actual_sql(
        reinterpret_cast<const char*>(text),
        static_cast<std::size_t>(length)
    );

    if (actual_sql != expected_sql) {
        throw std::runtime_error(
            std::string("Outbox schema definition mismatch: ") + name
        );
    }

    if (query.step() != SqliteStepResult::Done) {
        throw std::runtime_error(
            std::string("Duplicate Outbox schema object: ") + name
        );
    }
}

void verifySchema(SqliteDatabase& database)
{
    if (readInteger(database, "PRAGMA main.application_id;")
            != kOutboxApplicationId ||
        readInteger(database, "PRAGMA main.user_version;")
            != kOutboxSchemaVersion) {
        throw std::runtime_error(
            "Outbox database identity or version mismatch"
        );
    }

    // 本版本只允许一个业务表和两个显式索引。
    if (userObjectCount(database) != 3) {
        throw std::runtime_error(
            "Unexpected objects in Outbox database"
        );
    }

    requireDefinition(
        database,
        "table",
        "outbox",
        kCreateTable
    );

    requireDefinition(
        database,
        "index",
        "idx_outbox_ready",
        kCreateReadyIndex
    );

    requireDefinition(
        database,
        "index",
        "idx_outbox_acked",
        kCreateAckedIndex
    );
}

} // namespace

void initializeOutboxSchema(SqliteDatabase& database)
{
    // 先开启事务，再检查版本和对象，避免检查与建表分离。
    SqliteTransaction transaction(database);

    const auto application_id = readInteger(
        database,
        "PRAGMA main.application_id;"
    );

    const auto version = readInteger(
        database,
        "PRAGMA main.user_version;"
    );

    if (application_id == 0 && version == 0) {
        if (userObjectCount(database) != 0) {
            throw std::runtime_error(
                "Refusing to initialize a nonempty unmarked database"
            );
        }

        executeSql(database, kCreateTable);
        executeSql(database, kCreateReadyIndex);
        executeSql(database, kCreateAckedIndex);

        // 这里只拼接程序内部的整数常量，不拼接事件内容。
        executeSql(
            database,
            "PRAGMA main.application_id = " +
                std::to_string(kOutboxApplicationId) +
                ";"
        );

        executeSql(
            database,
            "PRAGMA main.user_version = " +
                std::to_string(kOutboxSchemaVersion) +
                ";"
        );
    } else if (
        application_id != kOutboxApplicationId ||
        version != kOutboxSchemaVersion
    ) {
        throw std::runtime_error(
            "Unsupported Outbox database identity or version"
        );
    }

    verifySchema(database);

    transaction.commit();
}

} // namespace edge