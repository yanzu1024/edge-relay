#include "edge/storage/OutboxDatabase.h"

#include "edge/storage/OutboxSchema.h"
#include "edge/storage/SqliteStatement.h"

#include <sqlite3.h>

#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>

namespace edge {

namespace {

constexpr int kBusyTimeoutMs = 250;

void validateDatabasePath(const std::filesystem::path& path)
{
    if (path.empty() ||
        !path.is_absolute() ||
        !path.has_filename() ||
        path.filename() == "." ||
        path.filename() == "..") {
        throw std::invalid_argument(
            "Outbox database path must be an absolute file path"
        );
    }

    // 必须在调用文件系统 API 前检查，避免 C 字符串路径被截断。
    if (path.u8string().find('\0') != std::string::npos) {
        throw std::invalid_argument(
            "Outbox database path must not contain NUL"
        );
    }

    if (std::filesystem::exists(path) &&
        !std::filesystem::is_regular_file(path)) {
        throw std::invalid_argument(
            "Outbox database path must name a regular file"
        );
    }
}

void executeSql(
    SqliteDatabase& database,
    const std::string& sql
)
{
    SqliteStatement statement(database, sql);

    if (statement.step() != SqliteStepResult::Done) {
        throw std::runtime_error(
            "Outbox setup SQL unexpectedly returned rows"
        );
    }
}

std::int64_t readInteger(
    SqliteDatabase& database,
    const std::string& sql
)
{
    SqliteStatement query(database, sql);

    if (query.step() != SqliteStepResult::Row ||
        sqlite3_column_type(query.get(), 0) != SQLITE_INTEGER) {
        throw std::runtime_error(
            "Outbox setup query did not return an integer"
        );
    }

    const auto value = static_cast<std::int64_t>(
        sqlite3_column_int64(query.get(), 0)
    );

    if (query.step() != SqliteStepResult::Done) {
        throw std::runtime_error(
            "Outbox setup query returned multiple rows"
        );
    }

    return value;
}

std::string readText(
    SqliteDatabase& database,
    const std::string& sql
)
{
    SqliteStatement query(database, sql);

    if (query.step() != SqliteStepResult::Row ||
        sqlite3_column_type(query.get(), 0) != SQLITE_TEXT) {
        throw std::runtime_error(
            "Outbox setup query did not return text"
        );
    }

    const unsigned char* text =
        sqlite3_column_text(query.get(), 0);

    if (text == nullptr) {
        throw SqliteError(
            SQLITE_NOMEM,
            "Cannot read Outbox database setting"
        );
    }

    const int length = sqlite3_column_bytes(query.get(), 0);

    const std::string value(
        reinterpret_cast<const char*>(text),
        static_cast<std::size_t>(length)
    );

    if (query.step() != SqliteStepResult::Done) {
        throw std::runtime_error(
            "Outbox setup query returned multiple rows"
        );
    }

    return value;
}

} // namespace

SqliteDatabase openOutboxDatabase(
    const std::filesystem::path& absolute_path
)
{
    validateDatabasePath(absolute_path);

    // 不重新解释配置路径，也不改变进程工作目录。
    std::filesystem::create_directories(
        absolute_path.parent_path()
    );

    SqliteDatabase database(absolute_path);

    const int timeout_result = sqlite3_busy_timeout(
        database.get(),
        kBusyTimeoutMs
    );

    if (timeout_result != SQLITE_OK) {
        throw SqliteError(
            timeout_result,
            "Cannot configure Outbox busy timeout"
        );
    }

    // 这是连接设置，需要在每次打开连接时配置。
    executeSql(database, "PRAGMA main.synchronous = EXTRA;");

    if (readInteger(database, "PRAGMA main.synchronous;") != 3) {
        throw std::runtime_error(
            "Outbox synchronous setting did not become EXTRA"
        );
    }

    // 先确认数据库身份和结构，再设置项目采用的日志模式。
    initializeOutboxSchema(database);

    const auto mode = readText(
        database,
        "PRAGMA main.journal_mode = DELETE;"
    );

    if (mode != "delete") {
        throw std::runtime_error(
            "Outbox journal mode did not become DELETE: " + mode
        );
    }

    // 返回连接所有权。此时还没有创建借用连接的 OutboxStore。
    return database;
}

} // namespace edge