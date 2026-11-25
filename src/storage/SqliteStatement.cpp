#include "edge/storage/SqliteStatement.h"

#include "edge/storage/SqliteDatabase.h"

#include <sqlite3.h>

#include <cctype>
#include <memory>
#include <stdexcept>
#include <utility>
#include <limits>

namespace edge {

namespace {

struct StatementFinalizer {
    void operator()(sqlite3_stmt* statement) const noexcept
    {
        if (statement != nullptr) {
            (void)sqlite3_finalize(statement);
        }
    }
};

using StatementGuard =
    std::unique_ptr<sqlite3_stmt, StatementFinalizer>;

bool containsOnlyWhitespace(const char* text) noexcept
{
    if (text == nullptr) {
        return true;
    }

    for (const char* current = text; *current != '\0'; ++current) {
        // isspace 的参数必须是 EOF 或 unsigned char 可表示的值。
        // 直接传入可能为负数的 char 会产生未定义行为。
        if (std::isspace(
                static_cast<unsigned char>(*current)
            ) == 0) {
            return false;
        }
    }

    return true;
}

} // namespace

SqliteStatement::SqliteStatement(
    SqliteDatabase& database,
    const std::string& sql
)
{
    if (!database.isOpen()) {
        throw std::logic_error(
            "Cannot prepare SQL on a closed database"
        );
    }

    if (sql.find('\0') != std::string::npos) {
        throw std::invalid_argument(
            "SQL must not contain NUL"
        );
    }

    sqlite3_stmt* raw = nullptr;
    const char* tail = nullptr;

    const int result = sqlite3_prepare_v2(
        database.get(),
        sql.c_str(),
        -1,
        &raw,
        &tail
    );

    // 后续错误文本复制、输入检查等步骤都可能抛异常。
    // 在完成全部检查之前，由临时对象负责释放。
    StatementGuard guard(raw);

    if (result != SQLITE_OK) {
        const char* detail = sqlite3_errmsg(database.get());

        const std::string message = detail != nullptr
            ? detail
            : "unknown SQLite error";

        throw SqliteError(
            result,
            "sqlite3_prepare_v2: " + message
        );
    }

    // 空文本、纯空白、仅注释等输入可能返回 SQLITE_OK，
    // 但没有生成任何语句，不能视为可用对象。
    if (raw == nullptr) {
        throw std::invalid_argument(
            "SQL must contain an executable statement"
        );
    }

    // SQLite 只编译第一条语句。
    // 明确拒绝剩余内容，避免调用者误以为全部 SQL 都会执行。
    if (!containsOnlyWhitespace(tail)) {
        throw std::invalid_argument(
            "Only one SQL statement is allowed; "
            "trailing content must be whitespace"
        );
    }

    statement_ = guard.release();
}

SqliteStatement::~SqliteStatement() noexcept
{
    close();
}

SqliteStatement::SqliteStatement(
    SqliteStatement&& other
) noexcept
    : statement_(std::exchange(other.statement_, nullptr)),
    state_(std::exchange(other.state_, State::Ready))
{
}

SqliteStatement& SqliteStatement::operator=(
    SqliteStatement&& other
) noexcept
{
    if (this != &other) {
        close();

        // 接管句柄，源对象的句柄置空。
        statement_ = std::exchange(
            other.statement_,
            nullptr
        );

        // 接管执行状态，源对象的状态恢复为 Ready。
        state_ = std::exchange(
            other.state_,
            State::Ready
        );
    }

    return *this;
}

sqlite3_stmt* SqliteStatement::get() const noexcept
{
    return statement_;
}

bool SqliteStatement::isOpen() const noexcept
{
    return statement_ != nullptr;
}

void SqliteStatement::close() noexcept
{
    sqlite3_stmt* old = std::exchange(statement_, nullptr);

    state_ = State::Ready;

    if (old != nullptr) {
        (void)sqlite3_finalize(old);
    }
}

void SqliteStatement::requireOpen() const
{
    if (statement_ == nullptr) {
        throw std::logic_error(
            "SQLite statement is closed"
        );
    }
}

void SqliteStatement::requireBindable() const
{
    requireOpen();

    if (state_ != State::Ready) {
        throw std::logic_error(
            "Reset the statement before binding parameters"
        );
    }
}

void SqliteStatement::checkResult(
    int result,
    const char* operation
) const
{
    if (result == SQLITE_OK) {
        return;
    }

    // 正常使用期间，语句所属的数据库连接必须保持打开。
    sqlite3* db = sqlite3_db_handle(statement_);

    const char* detail = sqlite3_errmsg(db);

    const std::string message = detail != nullptr
        ? detail
        : "unknown SQLite error";

    throw SqliteError(
        result,
        std::string(operation) + ": " + message
    );
}

void SqliteStatement::bindInt64(
    int index,
    std::int64_t value
)
{
    requireBindable();

    const int result = sqlite3_bind_int64(
        statement_,
        index,
        static_cast<sqlite3_int64>(value)
    );

    checkResult(result, "sqlite3_bind_int64");
}

void SqliteStatement::bindDouble(
    int index,
    double value
)
{
    requireBindable();

    const int result = sqlite3_bind_double(
        statement_,
        index,
        value
    );

    checkResult(result, "sqlite3_bind_double");
}

void SqliteStatement::bindText(
    int index,
    std::string_view value
)
{
    requireBindable();

    // sqlite3_bind_text 的长度参数是 int。
    // 必须先检查，再转换，避免大长度被截断。
    const auto maximum_length = static_cast<std::size_t>(
        std::numeric_limits<int>::max()
    );

    if (value.size() > maximum_length) {
        throw std::length_error(
            "SQLite text parameter exceeds int length range"
        );
    }

    // 默认构造的 string_view，其 data() 可以为 nullptr。
    // SQLite 会把空指针解释成 NULL。
    // 空文本必须传入非空指针，才能和 SQL NULL 区分。
    const char* data = value.empty()
        ? ""
        : value.data();

    const int result = sqlite3_bind_text(
        statement_,
        index,
        data,
        static_cast<int>(value.size()),
        SQLITE_TRANSIENT
    );

    checkResult(result, "sqlite3_bind_text");
}

void SqliteStatement::bindNull(int index)
{
    requireBindable();

    const int result = sqlite3_bind_null(
        statement_,
        index
    );

    checkResult(result, "sqlite3_bind_null");
}

SqliteStepResult SqliteStatement::step()
{
    requireOpen();

    if (state_ == State::Done || state_ == State::Failed) {
        throw std::logic_error(
            "Reset the statement before executing it again"
        );
    }

    const int result = sqlite3_step(statement_);

    if (result == SQLITE_ROW) {
        state_ = State::Row;
        return SqliteStepResult::Row;
    }

    if (result == SQLITE_DONE) {
        state_ = State::Done;
        return SqliteStepResult::Done;
    }

    // 必须先记录失败，再复制错误信息并抛异常。
    // 即使错误信息构造失败，也不能允许直接再次执行。
    state_ = State::Failed;

    checkResult(result, "sqlite3_step");

    // 上面的 result 既不是 ROW，也不是 DONE，
    // 正常情况下 checkResult 一定会抛出。
    throw std::logic_error(
        "Unexpected SQLite execution result"
    );
}

void SqliteStatement::reset()
{
    requireOpen();

    const int result = sqlite3_reset(statement_);

    // SQLite 已经重置执行位置。
    // 非 OK 返回值可能是在报告上一轮执行错误，
    // 不能因此让封装仍停留在 Failed 状态。
    state_ = State::Ready;

    // 这里不调用其他 SQLite API，直接读取并复制错误信息。
    checkResult(result, "sqlite3_reset");
}

void SqliteStatement::clearBindings()
{
    requireBindable();

    const int result = sqlite3_clear_bindings(statement_);

    checkResult(result, "sqlite3_clear_bindings");
}

} // namespace edge