#include "edge/storage/SqliteDatabase.h"
#include "edge/storage/SqliteStatement.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <cstdint>
#include <limits>
#include <string_view>

namespace {

using edge::SqliteDatabase;
using edge::SqliteError;
using edge::SqliteStatement;
using edge::SqliteStepResult;

static_assert(
    !std::is_copy_constructible_v<SqliteStatement>,
    "Statement must not be copy constructible"
);

static_assert(
    !std::is_copy_assignable_v<SqliteStatement>,
    "Statement must not be copy assignable"
);

static_assert(
    std::is_nothrow_move_constructible_v<SqliteStatement>,
    "Statement move construction must be noexcept"
);

static_assert(
    std::is_nothrow_move_assignable_v<SqliteStatement>,
    "Statement move assignment must be noexcept"
);

TEST(SqliteStatementTest, PreparesAndReadsRow)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT 42; \n\t");

    ASSERT_TRUE(statement.isOpen());
    ASSERT_NE(statement.get(), nullptr);

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int(statement.get(), 0),
        42
    );

    EXPECT_EQ(statement.step(), SqliteStepResult::Done);
}

TEST(SqliteStatementTest, PreparationDoesNotExecuteSql)
{
    SqliteDatabase db(":memory:");

    SqliteStatement create(
        db,
        "CREATE TABLE probe (value INTEGER);"
    );

    {
        SqliteStatement inspect(
            db,
            "SELECT COUNT(*) FROM sqlite_master "
            "WHERE type = 'table' AND name = 'probe';"
        );

        ASSERT_EQ(inspect.step(), SqliteStepResult::Row);

        // CREATE 只是编译完成, 表还没有创建
        EXPECT_EQ(
            sqlite3_column_int(inspect.get(), 0),
            0
        );

        ASSERT_EQ(inspect.step(), SqliteStepResult::Done);
    }

    ASSERT_EQ(create.step(), SqliteStepResult::Done);

    // 真正执行 CREATE 之后, 才可以编译对该表的查询
    SqliteStatement query(db, "SELECT value FROM probe;");

    EXPECT_EQ(query.step(), SqliteStepResult::Done);
}

TEST(SqliteStatementTest, DestructorFinalizesStatement)
{
    SqliteDatabase db(":memory:");

    ASSERT_EQ(sqlite3_next_stmt(db.get(), nullptr), nullptr);

    {
        SqliteStatement statement(db, "SELECT 1;");

        EXPECT_EQ(
            sqlite3_next_stmt(db.get(), nullptr),
            statement.get()
        );
    }

    // 通过 SQLite 自身的语句枚举接口确认资源已经释放
    EXPECT_EQ(sqlite3_next_stmt(db.get(), nullptr), nullptr);
}

TEST(SqliteStatementTest, InvalidSqlReportsError)
{
    SqliteDatabase db(":memory:");

    try {
        SqliteStatement statement(db, "SELEC 1;");
        FAIL() << "Invalid SQL must fail";
    } catch (const SqliteError& error) {
        EXPECT_EQ(error.code(), SQLITE_ERROR);

        const std::string message = error.what();

        EXPECT_NE(
            message.find("sqlite3_prepare_v2"),
            std::string::npos
        );
    }

    EXPECT_EQ(sqlite3_next_stmt(db.get(), nullptr), nullptr);

    // 一次编译失败不应该使连接失效
    SqliteStatement valid(db, "SELECT 1;");

    EXPECT_EQ(valid.step(), SqliteStepResult::Row);
}

TEST(SqliteStatementTest, RejectsSqlWithoutStatement)
{
    SqliteDatabase db(":memory:");

    for (const char* sql : {
            "",
            " \n\t",
            "-- comment only"
        }) {
        EXPECT_THROW(
            {
                SqliteStatement statement(db, sql);
            },
            std::invalid_argument
        );
    }

    EXPECT_EQ(sqlite3_next_stmt(db.get(), nullptr), nullptr);
}

TEST(SqliteStatementTest, RejectsEmbeddedNul)
{
    SqliteDatabase db(":memory:");

    std::string sql = "SELECT 1;";
    sql.push_back('\0');
    sql += "SELECT 2;";

    EXPECT_THROW(
        {
            SqliteStatement statement(db, sql);
        },
        std::invalid_argument
    );

    EXPECT_EQ(sqlite3_next_stmt(db.get(), nullptr), nullptr);
}

TEST(SqliteStatementTest, RejectsTrailingContentAndCleansStatement)
{
    SqliteDatabase db(":memory:");

    EXPECT_THROW(
        {
            SqliteStatement statement(
                db,
                "SELECT 1; SELECT 2;"
            );
        },
        std::invalid_argument
    );

    // 第一条语句已经编译成功, 随后才发现多余内容
    // 此处确认构造失败路径也释放了这条语句
    EXPECT_EQ(sqlite3_next_stmt(db.get(), nullptr), nullptr);
}

TEST(SqliteStatementTest, RejectsClosedDatabase)
{
    SqliteDatabase db(":memory:");
    db.close();

    EXPECT_THROW(
        {
            SqliteStatement statement(db, "SELECT 1;");
        },
        std::logic_error
    );
}

TEST(SqliteStatementTest, MoveConstructorTransfersOwnership)
{
    SqliteDatabase db(":memory:");
    SqliteStatement source(db, "SELECT 7;");

    sqlite3_stmt* original = source.get();

    SqliteStatement target(std::move(source));

    EXPECT_FALSE(source.isOpen());
    EXPECT_EQ(source.get(), nullptr);
    EXPECT_EQ(target.get(), original);

    ASSERT_EQ(target.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int(target.get(), 0),
        7
    );

    source.close();
}

TEST(SqliteStatementTest, MoveAssignmentFinalizesPreviousStatement)
{
    SqliteDatabase db(":memory:");

    SqliteStatement target(db, "SELECT 1;");
    SqliteStatement source(db, "SELECT 2;");

    sqlite3_stmt* incoming = source.get();

    target = std::move(source);

    EXPECT_FALSE(source.isOpen());
    EXPECT_EQ(target.get(), incoming);

    // 移动赋值应释放 target 原来的语句
    // 数据库中应该只剩接管的这一条
    EXPECT_EQ(
        sqlite3_next_stmt(db.get(), nullptr),
        incoming
    );

    EXPECT_EQ(
        sqlite3_next_stmt(db.get(), incoming),
        nullptr
    );

    ASSERT_EQ(target.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int(target.get(), 0),
        2
    );
}

TEST(SqliteStatementTest, CloseIsIdempotent)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT 1;");

    statement.close();

    EXPECT_FALSE(statement.isOpen());
    EXPECT_EQ(statement.get(), nullptr);
    EXPECT_EQ(sqlite3_next_stmt(db.get(), nullptr), nullptr);

    statement.close();

    EXPECT_FALSE(statement.isOpen());
    EXPECT_EQ(statement.get(), nullptr);
}

TEST(SqliteStatementTest, BindsInt64Limits)
{
    SqliteDatabase db(":memory:");

    SqliteStatement statement(
        db,
        "SELECT ?1, ?2, ?3;"
    );

    const auto minimum =
        std::numeric_limits<std::int64_t>::min();

    const auto maximum =
        std::numeric_limits<std::int64_t>::max();

    const std::int64_t large_value = 5000000000LL;

    statement.bindInt64(1, minimum);
    statement.bindInt64(2, maximum);
    statement.bindInt64(3, large_value);

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int64(statement.get(), 0),
        minimum
    );

    EXPECT_EQ(
        sqlite3_column_int64(statement.get(), 1),
        maximum
    );

    EXPECT_EQ(
        sqlite3_column_int64(statement.get(), 2),
        large_value
    );
}

TEST(SqliteStatementTest, BindsDouble)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT ?1;");

    statement.bindDouble(1, -12.75);

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_type(statement.get(), 0),
        SQLITE_FLOAT
    );

    EXPECT_DOUBLE_EQ(
        sqlite3_column_double(statement.get(), 0),
        -12.75
    );
}

TEST(SqliteStatementTest, TextBindingCopiesSource)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT ?1;");

    // 引号和 SQL 片段都应当只是参数内容
    const std::string expected =
        "O'Reilly'); DROP TABLE events; --";

    {
        std::string source = expected;

        statement.bindText(1, source);

        // 绑定后修改原字符串, 随后销毁
        // 正确实现应使用 SQLite 已经保存的副本
        source.assign(source.size(), 'x');
    }

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    ASSERT_EQ(
        sqlite3_column_type(statement.get(), 0),
        SQLITE_TEXT
    );

    const unsigned char* text =
        sqlite3_column_text(statement.get(), 0);

    ASSERT_NE(text, nullptr);

    EXPECT_EQ(
        std::string(reinterpret_cast<const char*>(text)),
        expected
    );
}

TEST(SqliteStatementTest, TextBindingRespectsViewLength)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT ?1;");

    const std::string source = "abcDEFxyz";

    // 此子串后面还有 xyz, 不是以 NUL 结尾的独立字符串
    const std::string_view part(source.data() + 3, 3);

    statement.bindText(1, part);

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    const unsigned char* text =
        sqlite3_column_text(statement.get(), 0);

    ASSERT_NE(text, nullptr);

    EXPECT_STREQ(
        reinterpret_cast<const char*>(text),
        "DEF"
    );

    EXPECT_EQ(
        sqlite3_column_bytes(statement.get(), 0),
        3
    );
}

TEST(SqliteStatementTest, EmptyTextIsNotNull)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT ?1, ?2;");

    const std::string empty_string;

    statement.bindText(1, empty_string);

    // 默认构造的 string_view 也必须绑定成空文本
    statement.bindText(2, std::string_view{});

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    for (int column = 0; column < 2; ++column) {
        EXPECT_EQ(
            sqlite3_column_type(statement.get(), column),
            SQLITE_TEXT
        );

        EXPECT_EQ(
            sqlite3_column_bytes(statement.get(), column),
            0
        );
    }
}

TEST(SqliteStatementTest, BindNullReplacesPreviousValue)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT ?1;");

    statement.bindText(1, "previous value");
    statement.bindNull(1);

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_type(statement.get(), 0),
        SQLITE_NULL
    );
}

TEST(SqliteStatementTest, InvalidParameterIndexReportsRange)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT ?1;");

    for (int index : {0, 2}) {
        try {
            statement.bindInt64(index, 100);
            FAIL() << "Invalid parameter index must fail";
        } catch (const SqliteError& error) {
            EXPECT_EQ(error.code(), SQLITE_RANGE);

            EXPECT_NE(
                std::string(error.what()).find(
                    "sqlite3_bind_int64"
                ),
                std::string::npos
            );
        }
    }

    // 参数下标错误不应导致语句失效
    statement.bindInt64(1, 123);

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int64(statement.get(), 0),
        123
    );
}

TEST(SqliteStatementTest, ClosedStatementRejectsBinding)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT ?1;");

    statement.close();

    EXPECT_THROW(
        statement.bindInt64(1, 1),
        std::logic_error
    );

    EXPECT_THROW(
        statement.bindDouble(1, 1.0),
        std::logic_error
    );

    EXPECT_THROW(
        statement.bindText(1, "value"),
        std::logic_error
    );

    EXPECT_THROW(
        statement.bindNull(1),
        std::logic_error
    );
}

TEST(SqliteStatementTest, MovedFromStatementRejectsBinding)
{
    SqliteDatabase db(":memory:");
    SqliteStatement source(db, "SELECT ?1;");

    SqliteStatement target(std::move(source));

    EXPECT_THROW(
        source.bindInt64(1, 1),
        std::logic_error
    );

    target.bindInt64(1, 88);

    ASSERT_EQ(target.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int64(target.get(), 0),
        88
    );
}

TEST(SqliteStatementTest, TextLengthLimitReportsTooBig)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT ?1;");

    // 降低此测试连接的文本长度限制, 
    // 用很小的数据覆盖 SQLITE_TOOBIG 错误路径
    const int previous_limit = sqlite3_limit(
        db.get(),
        SQLITE_LIMIT_LENGTH,
        16
    );

    try {
        statement.bindText(1, std::string(32, 'x'));
        FAIL() << "Text exceeding SQLite limit must fail";
    } catch (const SqliteError& error) {
        EXPECT_EQ(error.code(), SQLITE_TOOBIG);
    }

    sqlite3_limit(
        db.get(),
        SQLITE_LIMIT_LENGTH,
        previous_limit
    );

    // 恢复限制并重新绑定后, 语句仍然应该可用
    statement.bindText(1, "ok");

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    const unsigned char* text =
        sqlite3_column_text(statement.get(), 0);

    ASSERT_NE(text, nullptr);

    EXPECT_STREQ(
        reinterpret_cast<const char*>(text),
        "ok"
    );
}

TEST(SqliteStatementTest, StepReadsAllRows)
{
    SqliteDatabase db(":memory:");

    SqliteStatement statement(
        db,
        "SELECT 1 UNION ALL SELECT 2;"
    );

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int(statement.get(), 0),
        1
    );

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int(statement.get(), 0),
        2
    );

    EXPECT_EQ(statement.step(), SqliteStepResult::Done);

    EXPECT_THROW(
        statement.step(),
        std::logic_error
    );
}

TEST(SqliteStatementTest, DonePreventsDuplicateInsert)
{
    SqliteDatabase db(":memory:");

    SqliteStatement create(
        db,
        "CREATE TABLE probe (value INTEGER);"
    );

    ASSERT_EQ(create.step(), SqliteStepResult::Done);

    SqliteStatement insert(
        db,
        "INSERT INTO probe(value) VALUES (7);"
    );

    ASSERT_EQ(insert.step(), SqliteStepResult::Done);

    // 不重置直接再执行, 必须在进入 SQLite 前拒绝
    EXPECT_THROW(
        insert.step(),
        std::logic_error
    );

    SqliteStatement count(
        db,
        "SELECT COUNT(*) FROM probe;"
    );

    ASSERT_EQ(count.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int(count.get(), 0),
        1
    );

    ASSERT_EQ(count.step(), SqliteStepResult::Done);

    // 显式重置后, 才允许有意地重新执行
    insert.reset();

    ASSERT_EQ(insert.step(), SqliteStepResult::Done);

    count.reset();

    ASSERT_EQ(count.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int(count.get(), 0),
        2
    );
}

TEST(SqliteStatementTest, ResetPreservesBindings)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT ?1;");

    statement.bindInt64(1, 123);

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int64(statement.get(), 0),
        123
    );

    statement.reset();

    // 没有重新绑定, 原值仍然存在
    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int64(statement.get(), 0),
        123
    );
}

TEST(SqliteStatementTest, ClearBindingsRemovesValues)
{
    SqliteDatabase db(":memory:");

    SqliteStatement statement(
        db,
        "SELECT ?1, ?2;"
    );

    statement.bindInt64(1, 100);
    statement.bindText(2, "temperature");

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    statement.reset();
    statement.clearBindings();

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_type(statement.get(), 0),
        SQLITE_NULL
    );

    EXPECT_EQ(
        sqlite3_column_type(statement.get(), 1),
        SQLITE_NULL
    );
}

TEST(SqliteStatementTest, BindingRequiresReset)
{
    SqliteDatabase db(":memory:");
    SqliteStatement statement(db, "SELECT ?1;");

    statement.bindInt64(1, 10);

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_THROW(
        statement.bindInt64(1, 20),
        std::logic_error
    );

    EXPECT_THROW(
        statement.bindDouble(1, 20.0),
        std::logic_error
    );

    EXPECT_THROW(
        statement.bindText(1, "value"),
        std::logic_error
    );

    EXPECT_THROW(
        statement.bindNull(1),
        std::logic_error
    );

    EXPECT_THROW(
        statement.clearBindings(),
        std::logic_error
    );

    ASSERT_EQ(statement.step(), SqliteStepResult::Done);

    EXPECT_THROW(
        statement.bindInt64(1, 30),
        std::logic_error
    );

    statement.reset();
    statement.bindInt64(1, 30);

    ASSERT_EQ(statement.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int64(statement.get(), 0),
        30
    );
}

TEST(SqliteStatementTest, ConstraintErrorCanBeResetAndReused)
{
    SqliteDatabase db(":memory:");

    SqliteStatement create(
        db,
        "CREATE TABLE probe (value INTEGER PRIMARY KEY);"
    );

    ASSERT_EQ(create.step(), SqliteStepResult::Done);

    SqliteStatement insert(
        db,
        "INSERT INTO probe(value) VALUES (?1);"
    );

    insert.bindInt64(1, 1);

    ASSERT_EQ(insert.step(), SqliteStepResult::Done);

    insert.reset();

    // reset 保留参数, 再次插入 1 应触发唯一性约束错误
    try {
        insert.step();
        FAIL() << "Duplicate primary key must fail";
    } catch (const SqliteError& error) {
        // 低 8 位是主错误码, 兼容未来启用扩展错误码
        EXPECT_EQ(
            error.code() & 0xff,
            SQLITE_CONSTRAINT
        );
    }

    EXPECT_THROW(
        insert.step(),
        std::logic_error
    );

    EXPECT_THROW(
        insert.bindInt64(1, 2),
        std::logic_error
    );

    // SQLite reset 会再次报告刚才的执行错误, 
    // 但执行位置已经重置
    try {
        insert.reset();
        FAIL() << "Reset must report the previous constraint error";
    } catch (const SqliteError& error) {
        EXPECT_EQ(
            error.code() & 0xff,
            SQLITE_CONSTRAINT
        );

        EXPECT_NE(
            std::string(error.what()).find("sqlite3_reset"),
            std::string::npos
        );
    }

    insert.clearBindings();
    insert.bindInt64(1, 2);

    ASSERT_EQ(insert.step(), SqliteStepResult::Done);

    SqliteStatement count(
        db,
        "SELECT COUNT(*) FROM probe;"
    );

    ASSERT_EQ(count.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int(count.get(), 0),
        2
    );
}

TEST(SqliteStatementTest, ClosedOrMovedFromRejectsExecution)
{
    SqliteDatabase db(":memory:");
    SqliteStatement source(db, "SELECT 1;");

    SqliteStatement target(std::move(source));

    EXPECT_THROW(source.step(), std::logic_error);
    EXPECT_THROW(source.reset(), std::logic_error);
    EXPECT_THROW(source.clearBindings(), std::logic_error);

    target.close();

    EXPECT_THROW(target.step(), std::logic_error);
    EXPECT_THROW(target.reset(), std::logic_error);
    EXPECT_THROW(target.clearBindings(), std::logic_error);
}

TEST(SqliteStatementTest, MoveConstructorPreservesDoneState)
{
    SqliteDatabase db(":memory:");
    SqliteStatement source(db, "SELECT 1;");

    ASSERT_EQ(source.step(), SqliteStepResult::Row);
    ASSERT_EQ(source.step(), SqliteStepResult::Done);

    SqliteStatement target(std::move(source));

    // 移动不能使已经完成的语句变回 Ready
    EXPECT_THROW(
        target.step(),
        std::logic_error
    );

    target.reset();

    EXPECT_EQ(target.step(), SqliteStepResult::Row);
}

TEST(SqliteStatementTest, MoveAssignmentPreservesRowState)
{
    SqliteDatabase db(":memory:");

    SqliteStatement source(
        db,
        "SELECT ?1 UNION ALL SELECT 2;"
    );

    source.bindInt64(1, 1);

    ASSERT_EQ(source.step(), SqliteStepResult::Row);

    SqliteStatement target(db, "SELECT 99;");

    target = std::move(source);

    // 接管后仍在读取结果, 不能重新绑定
    EXPECT_THROW(
        target.bindInt64(1, 10),
        std::logic_error
    );

    // 继续读取第二行, 而不是从第一行重新开始
    ASSERT_EQ(target.step(), SqliteStepResult::Row);

    EXPECT_EQ(
        sqlite3_column_int(target.get(), 0),
        2
    );

    EXPECT_EQ(target.step(), SqliteStepResult::Done);
}

} // namespace