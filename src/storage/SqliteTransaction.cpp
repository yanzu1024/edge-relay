#include "edge/storage/SqliteTransaction.h"

#include "edge/storage/SqliteDatabase.h"

#include <sqlite3.h>

#include <stdexcept>
#include <string>

namespace edge {

SqliteTransaction::SqliteTransaction(
    SqliteDatabase& database
)
    : db_(database.get())
{
    if (db_ == nullptr) {
        throw std::logic_error(
            "Cannot begin a transaction on a closed database"
        );
    }

    // BEGIN 会关闭自动提交模式。
    // 已经处于显式事务中时，拒绝再创建一个事务所有者。
    if (sqlite3_get_autocommit(db_) == 0) {
        throw std::logic_error(
            "Nested SQLite transactions are not supported"
        );
    }

    execute("BEGIN IMMEDIATE;");

    // 只有 BEGIN 成功后，当前对象才负责结束这笔事务。
    active_ = true;
}

SqliteTransaction::~SqliteTransaction() noexcept
{
    if (isActive()) {
        // 析构只做兜底清理，不构造异常或覆盖正在传播的异常。
        // 需要获知回滚是否成功时，应显式调用 rollback()。
        (void)sqlite3_exec(
            db_,
            "ROLLBACK;",
            nullptr,
            nullptr,
            nullptr
        );
    }
}

bool SqliteTransaction::isActive() const noexcept
{
    // 某些 SQLite 错误可能自动回滚整笔事务。
    // 因此不能仅凭本类的 active_ 标记判断实际状态。
    return active_ && sqlite3_get_autocommit(db_) == 0;
}

void SqliteTransaction::requireActive()
{
    if (!isActive()) {
        active_ = false;

        throw std::logic_error(
            "SQLite transaction is no longer active"
        );
    }
}

void SqliteTransaction::execute(const char* sql)
{
    // 这里只执行内部固定 SQL，不包含事件内容。
    // 传入空的错误文本输出参数，错误信息从连接复制。
    const int result = sqlite3_exec(
        db_,
        sql,
        nullptr,
        nullptr,
        nullptr
    );

    if (result == SQLITE_OK) {
        return;
    }

    const char* detail = sqlite3_errmsg(db_);

    const std::string message = detail != nullptr
        ? detail
        : "unknown SQLite error";

    throw SqliteError(
        result,
        std::string("SQLite transaction [") +
            sql +
            "]: " +
            message
    );
}

void SqliteTransaction::commit()
{
    requireActive();

    execute("COMMIT;");

    // 不能在 COMMIT 执行前清除此标记。
    // 提交失败时，SQLite 可能仍然保留着活动事务。
    active_ = false;
}

void SqliteTransaction::rollback()
{
    requireActive();

    execute("ROLLBACK;");

    active_ = false;
}

} // namespace edge