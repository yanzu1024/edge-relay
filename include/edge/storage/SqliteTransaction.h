#pragma once

struct sqlite3;

namespace edge {

class SqliteDatabase;

class SqliteTransaction {
public:
    // 开启写事务，不拥有数据库连接。
    // database 在本对象存活期间必须保持打开，且不能移动。
    explicit SqliteTransaction(SqliteDatabase& database);

    // 未提交的活动事务会尝试回滚；析构不抛异常。
    ~SqliteTransaction() noexcept;

    SqliteTransaction(const SqliteTransaction&) = delete;
    SqliteTransaction& operator=(const SqliteTransaction&) = delete;

    SqliteTransaction(SqliteTransaction&&) = delete;
    SqliteTransaction& operator=(SqliteTransaction&&) = delete;

    bool isActive() const noexcept;

    // 成功后事务结束；失败时报告 SQLite 错误。
    void commit();

    // 显式回滚，允许调用者获知回滚错误。
    void rollback();

private:
    void requireActive();

    // 仅执行本类内部固定的事务控制 SQL。
    void execute(const char* sql);

    sqlite3* db_ = nullptr;
    bool active_ = false;
};

} // namespace edge