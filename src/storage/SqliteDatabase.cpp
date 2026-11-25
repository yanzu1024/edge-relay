#include "edge/storage/SqliteDatabase.h"

#include <sqlite3.h>

#include <memory>
#include <utility>

namespace edge {

namespace {

// 用于构造期间的临时保护。
// 即使复制错误信息或构造异常对象时再次抛异常，也会清理句柄。
struct SqliteCloser {
    void operator()(sqlite3* db) const noexcept
    {
        if (db != nullptr) {
            (void)sqlite3_close_v2(db);
        }
    }
};

using ConnectionGuard = std::unique_ptr<sqlite3, SqliteCloser>;

} // namespace

SqliteError::SqliteError(int code, const std::string& message)
    : std::runtime_error(message),
      code_(code)
{
}

int SqliteError::code() const noexcept
{
    return code_;
}

SqliteDatabase::SqliteDatabase(const std::filesystem::path& path)
{
    // SQLite 将空文件名解释为临时数据库。
    // 本项目拒绝空路径，避免配置错误被悄悄解释成临时存储。
    if (path.empty()) {
        throw std::invalid_argument(
            "SQLite database path must not be empty"
        );
    }

    // sqlite3_open_v2 要求 UTF-8 文件名。
    // 在本项目的 C++17 下，u8string() 返回 std::string。
    const std::string filename = path.u8string();

    // SQLite 接收 C 字符串；内嵌 NUL 会截断实际使用的路径。
    if (filename.find('\0') != std::string::npos) {
        throw std::invalid_argument(
            "SQLite database path must not contain NUL"
        );
    }

    sqlite3* raw = nullptr;

    const int flags =
        SQLITE_OPEN_READWRITE |
        SQLITE_OPEN_CREATE |
        SQLITE_OPEN_NOMUTEX |
        SQLITE_OPEN_PRIVATECACHE;

    const int result = sqlite3_open_v2(
        filename.c_str(),
        &raw,
        flags,
        nullptr
    );

    // 打开失败时 raw 也可能非空，因此立即交给临时保护对象。
    ConnectionGuard guard(raw);

    if (result != SQLITE_OK) {
        const char* detail = raw != nullptr
            ? sqlite3_errmsg(raw)
            : sqlite3_errstr(result);

        // 必须在 guard 释放连接前，把错误文本复制出来。
        const std::string message = detail != nullptr
            ? detail
            : "unknown SQLite error";

        throw SqliteError(
            result,
            "sqlite3_open_v2: " + message
        );
    }

    // 从临时保护对象转交给当前对象。
    // release() 只转移所有权，不关闭连接。
    db_ = guard.release();
}

SqliteDatabase::~SqliteDatabase() noexcept
{
    close();
}

SqliteDatabase::SqliteDatabase(SqliteDatabase&& other) noexcept
    : db_(std::exchange(other.db_, nullptr))
{
}

SqliteDatabase& SqliteDatabase::operator=(
    SqliteDatabase&& other
) noexcept
{
    if (this != &other) {
        // 先释放当前对象原来拥有的连接，再接管新连接。
        close();
        db_ = std::exchange(other.db_, nullptr);
    }

    return *this;
}

sqlite3* SqliteDatabase::get() const noexcept
{
    return db_;
}

bool SqliteDatabase::isOpen() const noexcept
{
    return db_ != nullptr;
}

void SqliteDatabase::close() noexcept
{
    // 先让成员进入空状态，保证重复 close() 不会重复关闭。
    sqlite3* old = std::exchange(db_, nullptr);

    if (old != nullptr) {
        // 对有效且未关闭的句柄，close_v2 可处理尚存语句的情况。
        // 借用者不得自行关闭句柄，否则会破坏本类的所有权约定。
        (void)sqlite3_close_v2(old);
    }
}

} // namespace edge