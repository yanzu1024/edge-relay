#include "edge/storage/SqliteDatabase.h"
#include "edge/storage/SqliteStatement.h"
#include "edge/storage/SqliteTransaction.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <cstdint>
#include <stdexcept>
#include <string>
#include <type_traits>

namespace {

using edge::SqliteDatabase;
using edge::SqliteError;
using edge::SqliteStatement;
using edge::SqliteStepResult;
using edge::SqliteTransaction;

static_assert(
    !std::is_copy_constructible_v<SqliteTransaction>,
    "Transaction must not be copy constructible"
);

static_assert(
    !std::is_copy_assignable_v<SqliteTransaction>,
    "Transaction must not be copy assignable"
);

static_assert(
    !std::is_move_constructible_v<SqliteTransaction>,
    "Transaction must remain in its original scope"
);

static_assert(
    !std::is_move_assignable_v<SqliteTransaction>,
    "Transaction must not be move assignable"
);

// 测试辅助函数：执行没有结果行的单条 SQL。
void executeSql(
    SqliteDatabase& db,
    const std::string& sql
)
{
    SqliteStatement statement(db, sql);

    if (statement.step() != SqliteStepResult::Done) {
        throw std::logic_error(
            "Test SQL unexpectedly returned a row"
        );
    }
}

// 测试辅助函数：读取仅有一行一列的整数结果。
std::int64_t scalarInt64(
    SqliteDatabase& db,
    const std::string& sql
)
{
    SqliteStatement statement(db, sql);

    if (statement.step() != SqliteStepResult::Row) {
        throw std::logic_error(
            "Test SQL did not return a row"
        );
    }

    const auto value = static_cast<std::int64_t>(
        sqlite3_column_int64(statement.get(), 0)
    );

    if (statement.step() != SqliteStepResult::Done) {
        throw std::logic_error(
            "Test SQL returned more than one row"
        );
    }

    return value;
}

class SqliteTransactionTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        executeSql(
            db_,
            "CREATE TABLE probe (value INTEGER PRIMARY KEY);"
        );
    }

    SqliteDatabase db_{":memory:"};
};

TEST_F(SqliteTransactionTest, CommitKeepsAllRows)
{
    {
        SqliteTransaction transaction(db_);

        ASSERT_TRUE(transaction.isActive());

        executeSql(db_, "INSERT INTO probe(value) VALUES (1);");
        executeSql(db_, "INSERT INTO probe(value) VALUES (2);");

        transaction.commit();

        EXPECT_FALSE(transaction.isActive());

        EXPECT_THROW(
            transaction.commit(),
            std::logic_error
        );
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM probe;"),
        2
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);
}

TEST_F(SqliteTransactionTest, ScopeExitRollsBack)
{
    {
        SqliteTransaction transaction(db_);

        executeSql(db_, "INSERT INTO probe(value) VALUES (1);");
        executeSql(db_, "INSERT INTO probe(value) VALUES (2);");

        // 不提交，由析构处理。
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM probe;"),
        0
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);
}

TEST_F(SqliteTransactionTest, ExplicitRollback)
{
    {
        SqliteTransaction transaction(db_);

        executeSql(db_, "INSERT INTO probe(value) VALUES (1);");

        transaction.rollback();

        EXPECT_FALSE(transaction.isActive());

        EXPECT_THROW(
            transaction.rollback(),
            std::logic_error
        );
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM probe;"),
        0
    );
}

TEST_F(SqliteTransactionTest, ExceptionUnwindingRollsBack)
{
    try {
        SqliteTransaction transaction(db_);

        executeSql(db_, "INSERT INTO probe(value) VALUES (1);");

        throw std::runtime_error("simulated application failure");
    } catch (const std::runtime_error& error) {
        // 确保捕获的是主动模拟的异常，而不是前面 SQL 意外失败。
        EXPECT_STREQ(
            error.what(),
            "simulated application failure"
        );
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM probe;"),
        0
    );

    EXPECT_NE(sqlite3_get_autocommit(db_.get()), 0);
}

TEST_F(SqliteTransactionTest, RejectsNestedTransaction)
{
    SqliteTransaction outer(db_);

    executeSql(db_, "INSERT INTO probe(value) VALUES (1);");

    EXPECT_THROW(
        {
            SqliteTransaction inner(db_);
        },
        std::logic_error
    );

    // 内层构造失败，不能影响外层事务。
    ASSERT_TRUE(outer.isActive());

    outer.commit();

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM probe;"),
        1
    );
}

TEST_F(SqliteTransactionTest, RejectsClosedConnection)
{
    SqliteDatabase closed(":memory:");
    closed.close();

    EXPECT_THROW(
        {
            SqliteTransaction transaction(closed);
        },
        std::logic_error
    );
}

TEST_F(SqliteTransactionTest, CommitFailureCanBeRolledBack)
{
    // 延迟外键约束用于制造“INSERT 成功，但 COMMIT 失败”的情况。
    // 这里只用于测试事务封装，不代表正式 Outbox 会使用外键。
    executeSql(db_, "PRAGMA foreign_keys = ON;");

    ASSERT_EQ(
        scalarInt64(db_, "PRAGMA foreign_keys;"),
        1
    );

    executeSql(
        db_,
        "CREATE TABLE child ("
        "value INTEGER REFERENCES probe(value) "
        "DEFERRABLE INITIALLY DEFERRED"
        ");"
    );

    {
        SqliteTransaction transaction(db_);

        executeSql(
            db_,
            "INSERT INTO probe(value) VALUES (1);"
        );

        // 父表中没有 999，但延迟约束允许本条 INSERT 先完成。
        executeSql(
            db_,
            "INSERT INTO child(value) VALUES (999);"
        );

        try {
            transaction.commit();
            FAIL() << "Commit with an invalid deferred foreign key must fail";
        } catch (const SqliteError& error) {
            EXPECT_EQ(
                error.code() & 0xff,
                SQLITE_CONSTRAINT
            );
        }

        // 提交失败后，不能错误地把事务标记为已完成。
        ASSERT_TRUE(transaction.isActive());

        transaction.rollback();

        EXPECT_FALSE(transaction.isActive());
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM probe;"),
        0
    );

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM child;"),
        0
    );
}

TEST_F(SqliteTransactionTest, DetectsAutomaticRollback)
{
    {
        SqliteTransaction transaction(db_);

        executeSql(db_, "INSERT INTO probe(value) VALUES (1);");

        // OR ROLLBACK 使这次主键冲突回滚整笔事务。
        EXPECT_THROW(
            executeSql(
                db_,
                "INSERT OR ROLLBACK INTO probe(value) VALUES (1);"
            ),
            SqliteError
        );

        EXPECT_FALSE(transaction.isActive());

        EXPECT_THROW(
            transaction.commit(),
            std::logic_error
        );

        // 当前事务作用域结束后，再开启下一笔事务。
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM probe;"),
        0
    );

    {
        SqliteTransaction next(db_);

        executeSql(db_, "INSERT INTO probe(value) VALUES (2);");

        next.commit();
    }

    EXPECT_EQ(
        scalarInt64(db_, "SELECT COUNT(*) FROM probe;"),
        1
    );
}

} // namespace