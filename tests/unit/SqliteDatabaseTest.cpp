#include "edge/storage/SqliteDatabase.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using edge::SqliteDatabase;
using edge::SqliteError;

static_assert(
    !std::is_copy_constructible_v<SqliteDatabase>,
    "Database connection must not be copy constructible"
);

static_assert(
    !std::is_copy_assignable_v<SqliteDatabase>,
    "Database connection must not be copy assignable"
);

static_assert(
    std::is_nothrow_move_constructible_v<SqliteDatabase>,
    "Database move construction must be noexcept"
);

static_assert(
    std::is_nothrow_move_assignable_v<SqliteDatabase>,
    "Database move assignment must be noexcept"
);

class SqliteDatabaseTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        // mkdtemp 原子创建独立目录，避免并行测试互相干扰。
        const std::string pattern =
            (std::filesystem::temp_directory_path() /
             "edge-relay-sqlite-XXXXXX").string();

        std::vector<char> buffer(pattern.begin(), pattern.end());
        buffer.push_back('\0');

        char* created = ::mkdtemp(buffer.data());

        if (created == nullptr) {
            throw std::runtime_error(
                "Failed to create SQLite test directory"
            );
        }

        directory_ = created;
    }

    void TearDown() override
    {
        // 清理失败通过测试结果报告，不从清理函数抛异常。
        std::error_code error;
        std::filesystem::remove_all(directory_, error);

        EXPECT_FALSE(error)
            << "Temporary directory cleanup failed: "
            << error.message();
    }

    std::filesystem::path directory_;
};

TEST_F(SqliteDatabaseTest, OpensMemoryDatabase)
{
    SqliteDatabase db(":memory:");

    ASSERT_TRUE(db.isOpen());
    ASSERT_NE(db.get(), nullptr);

    EXPECT_EQ(
        sqlite3_exec(db.get(), "SELECT 1;", nullptr, nullptr, nullptr),
        SQLITE_OK
    );
}

TEST_F(SqliteDatabaseTest, CreatesFileAndCanReopen)
{
    const auto path = directory_ / "outbox.db";

    EXPECT_FALSE(std::filesystem::exists(path));

    {
        SqliteDatabase db(path);

        ASSERT_TRUE(db.isOpen());

        // user_version 是数据库文件中的持久化元数据。
        ASSERT_EQ(
            sqlite3_exec(
                db.get(),
                "PRAGMA user_version = 17;",
                nullptr,
                nullptr,
                nullptr
            ),
            SQLITE_OK
        );
    }

    ASSERT_TRUE(std::filesystem::exists(path));

    SqliteDatabase reopened(path);

    int version = -1;

    // sqlite3_exec 在当前调用期间同步执行回调。
    const auto read_version = [](
        void* context,
        int column_count,
        char** values,
        char**
    ) -> int {
        if (column_count != 1 || values[0] == nullptr) {
            return 1;
        }

        *static_cast<int*>(context) = std::atoi(values[0]);
        return 0;
    };

    ASSERT_EQ(
        sqlite3_exec(
            reopened.get(),
            "PRAGMA user_version;",
            read_version,
            &version,
            nullptr
        ),
        SQLITE_OK
    );

    EXPECT_EQ(version, 17);
}

TEST_F(SqliteDatabaseTest, RejectsEmptyPath)
{
    EXPECT_THROW(
        {
            SqliteDatabase db(std::filesystem::path{});
        },
        std::invalid_argument
    );
}

TEST_F(SqliteDatabaseTest, RejectsEmbeddedNul)
{
    std::string filename = "outbox";
    filename.push_back('\0');
    filename += ".db";

    const std::filesystem::path path(filename);

    EXPECT_THROW(
        {
            SqliteDatabase db(path);
        },
        std::invalid_argument
    );
}

TEST_F(SqliteDatabaseTest, MissingParentReportsOpenError)
{
    const auto parent = directory_ / "missing";
    const auto path = parent / "outbox.db";

    try {
        SqliteDatabase db(path);
        FAIL() << "Opening a database under a missing parent must fail";
    } catch (const SqliteError& error) {
        EXPECT_EQ(error.code(), SQLITE_CANTOPEN);

        // 到这里，构造函数内的临时连接已经被清理。
        // what() 仍然可读，因为错误文本已经由异常对象保存。
        const std::string message = error.what();

        EXPECT_NE(
            message.find("sqlite3_open_v2"),
            std::string::npos
        );
    }

    EXPECT_FALSE(std::filesystem::exists(parent));
}

TEST_F(SqliteDatabaseTest, MoveConstructorTransfersOwnership)
{
    SqliteDatabase source(":memory:");
    sqlite3* original = source.get();

    SqliteDatabase target(std::move(source));

    EXPECT_FALSE(source.isOpen());
    EXPECT_EQ(source.get(), nullptr);
    EXPECT_EQ(target.get(), original);

    EXPECT_EQ(
        sqlite3_exec(
            target.get(),
            "SELECT 1;",
            nullptr,
            nullptr,
            nullptr
        ),
        SQLITE_OK
    );

    // 移动后的源对象允许重复关闭。
    source.close();
}

TEST_F(SqliteDatabaseTest, MoveAssignmentReleasesPreviousConnection)
{
    const auto old_path = directory_ / "old.db";

    SqliteDatabase target(old_path);

    ASSERT_EQ(
        sqlite3_exec(
            target.get(),
            "BEGIN IMMEDIATE;",
            nullptr,
            nullptr,
            nullptr
        ),
        SQLITE_OK
    );

    SqliteDatabase source(":memory:");
    sqlite3* incoming = source.get();

    target = std::move(source);

    EXPECT_EQ(target.get(), incoming);
    EXPECT_FALSE(source.isOpen());

    // 如果赋值前的旧连接没有关闭，它持有的写事务会阻止这里取锁。
    SqliteDatabase probe(old_path);

    ASSERT_EQ(
        sqlite3_exec(
            probe.get(),
            "BEGIN IMMEDIATE;",
            nullptr,
            nullptr,
            nullptr
        ),
        SQLITE_OK
    );

    EXPECT_EQ(
        sqlite3_exec(
            probe.get(),
            "ROLLBACK;",
            nullptr,
            nullptr,
            nullptr
        ),
        SQLITE_OK
    );
}

TEST_F(SqliteDatabaseTest, CloseIsIdempotent)
{
    SqliteDatabase db(":memory:");

    db.close();

    EXPECT_FALSE(db.isOpen());
    EXPECT_EQ(db.get(), nullptr);

    db.close();

    EXPECT_FALSE(db.isOpen());
    EXPECT_EQ(db.get(), nullptr);
}

TEST_F(SqliteDatabaseTest, DestructorReleasesWriteTransaction)
{
    const auto path = directory_ / "transaction.db";

    {
        SqliteDatabase db(path);

        ASSERT_EQ(
            sqlite3_exec(
                db.get(),
                "BEGIN IMMEDIATE;",
                nullptr,
                nullptr,
                nullptr
            ),
            SQLITE_OK
        );

        // 故意不手动回滚，用析构后的取锁行为检查连接释放。
    }

    SqliteDatabase reopened(path);

    ASSERT_EQ(
        sqlite3_exec(
            reopened.get(),
            "BEGIN IMMEDIATE;",
            nullptr,
            nullptr,
            nullptr
        ),
        SQLITE_OK
    );

    EXPECT_EQ(
        sqlite3_exec(
            reopened.get(),
            "ROLLBACK;",
            nullptr,
            nullptr,
            nullptr
        ),
        SQLITE_OK
    );
}

TEST_F(SqliteDatabaseTest, OutstandingStatementCanBeFinalizedAfterOwnerDestruction)
{
    // 测试局部保护，防止断言提前返回时遗漏 finalize。
    // 正式的语句 RAII 封装放到下一小节。
    using StatementGuard =
        std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;

    StatementGuard statement(nullptr, &sqlite3_finalize);

    {
        SqliteDatabase db(":memory:");

        sqlite3_stmt* raw = nullptr;

        const int result = sqlite3_prepare_v2(
            db.get(),
            "SELECT 1;",
            -1,
            &raw,
            nullptr
        );

        statement.reset(raw);

        ASSERT_EQ(result, SQLITE_OK);
        ASSERT_NE(statement.get(), nullptr);

        // 故意让语句存活到数据库所有者析构之后，
        // 验证 close_v2 的延后回收路径。
    }

    // 此时不能再操作已关闭的连接，只释放尚存的语句。
    EXPECT_EQ(
        sqlite3_finalize(statement.release()),
        SQLITE_OK
    );
}

} // namespace