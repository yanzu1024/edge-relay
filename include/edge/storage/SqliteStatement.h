#pragma once

#include <cstdint>
#include <string>
#include <string_view>

struct sqlite3_stmt;

namespace edge {

class SqliteDatabase;

enum class SqliteStepResult {
    Row,
    Done
};

class SqliteStatement {
public:
    // 编译单条 SQL，不执行。
    // 使用期间，database 必须保持打开。
    SqliteStatement(
        SqliteDatabase& database,
        const std::string& sql
    );

    ~SqliteStatement() noexcept;

    SqliteStatement(const SqliteStatement&) = delete;
    SqliteStatement& operator=(const SqliteStatement&) = delete;

    SqliteStatement(SqliteStatement&& other) noexcept;
    SqliteStatement& operator=(SqliteStatement&& other) noexcept;

    // 借用句柄，供读取结果列、观察资源等使用。
    // 不得自行 finalize，也不要绕过本类执行、重置或绑定。
    sqlite3_stmt* get() const noexcept;

    bool isOpen() const noexcept;

    // 参数下标从 1 开始，只能在 Ready 状态绑定。
    void bindInt64(int index, std::int64_t value);

    void bindDouble(int index, double value);

    // value 必须是有效 UTF-8；绑定返回前复制内容。
    void bindText(int index, std::string_view value);

    void bindNull(int index);

    // 返回 Row 时可以读取当前行。
    // 返回 Done 或抛出执行错误后，必须 reset 才能再次执行。
    SqliteStepResult step();

    // 重置执行位置，保留参数。
    // SQLite 可能返回上一轮执行错误，本方法会将其报告出来；
    // 即使报告该错误，语句也已经回到 Ready 状态。
    void reset();

    // 将全部参数清为 NULL；要求当前处于 Ready 状态。
    void clearBindings();

    // 释放资源，不提交事务，不抛异常。
    void close() noexcept;

private:
    enum class State {
        Ready,
        Row,
        Done,
        Failed
    };

    void requireOpen() const;
    void requireBindable() const;

    void checkResult(
        int result,
        const char* operation
    ) const;

    sqlite3_stmt* statement_ = nullptr;
    State state_ = State::Ready;
};

} // namespace edge