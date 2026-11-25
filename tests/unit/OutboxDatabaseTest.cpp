#include "edge/domain/EventId.h"
#include "edge/domain/SensorEvent.h"

#include "edge/storage/OutboxDatabase.h"
#include "edge/storage/OutboxSchema.h"
#include "edge/storage/OutboxStore.h"
#include "edge/storage/SqliteDatabase.h"
#include "edge/storage/SqliteStatement.h"
#include "edge/storage/SqliteTransaction.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

namespace {

using edge::openOutboxDatabase;
using edge::OutboxInsertResult;
using edge::OutboxInsertStatus;
using edge::OutboxStore;
using edge::SensorEvent;
using edge::SqliteDatabase;
using edge::SqliteError;
using edge::SqliteStatement;
using edge::SqliteStepResult;
using edge::SqliteTransaction;

constexpr std::int64_t kNow = 1700000000100LL;

SensorEvent makeEvent()
{
    SensorEvent event;

    event.device_id = "sensor-01";
    event.session_id = "550e8400-e29b-41d4-a716-446655440000";
    event.sequence = 1;
    event.observed_at_ms = 1700000000000LL;
    event.metric = "temperature";
    event.value = 21.5;
    event.unit = "degC";

    return event;
}

void executeSql(SqliteDatabase& db, const std::string& sql)
{
    SqliteStatement statement(db, sql);

    if (statement.step() != SqliteStepResult::Done) {
        throw std::logic_error(
            "Test SQL unexpectedly returned rows"
        );
    }
}

std::int64_t scalarInt64(
    SqliteDatabase& db,
    const std::string& sql
)
{
    SqliteStatement query(db, sql);

    if (query.step() != SqliteStepResult::Row) {
        throw std::logic_error("Test query returned no row");
    }

    const auto value = static_cast<std::int64_t>(
        sqlite3_column_int64(query.get(), 0)
    );

    if (query.step() != SqliteStepResult::Done) {
        throw std::logic_error(
            "Test query returned multiple rows"
        );
    }

    return value;
}

std::string scalarText(
    SqliteDatabase& db,
    const std::string& sql
)
{
    SqliteStatement query(db, sql);

    if (query.step() != SqliteStepResult::Row) {
        throw std::logic_error("Test query returned no row");
    }

    const unsigned char* text =
        sqlite3_column_text(query.get(), 0);

    if (text == nullptr) {
        throw std::logic_error("Test query returned null text");
    }

    const int length = sqlite3_column_bytes(query.get(), 0);

    const std::string value(
        reinterpret_cast<const char*>(text),
        static_cast<std::size_t>(length)
    );

    if (query.step() != SqliteStepResult::Done) {
        throw std::logic_error(
            "Test query returned multiple rows"
        );
    }

    return value;
}

OutboxInsertResult requireResult(edge::OutboxEnqueueResult result)
{
    if (!std::holds_alternative<OutboxInsertResult>(result)) {
        throw std::runtime_error(
            "Expected an Outbox insert result"
        );
    }

    return std::get<OutboxInsertResult>(std::move(result));
}

// Linux 测试辅助函数。
// 调用时父进程不持有此文件的数据库连接，子进程独立打开。
// committed=false 时保留未提交事务，然后直接退出。
int runAbruptWriter(
    const std::filesystem::path& path,
    bool committed
)
{
    const pid_t child = ::fork();

    if (child == -1) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "fork"
        );
    }

    if (child == 0) {
        try {
            auto db = openOutboxDatabase(path);

            if (committed) {
                OutboxStore store(db);

                const auto result = requireResult(
                    store.enqueue(makeEvent(), kNow)
                );

                if (result.status != OutboxInsertStatus::Inserted) {
                    ::_exit(2);
                }

                // db 仍然存活，直接退出，跳过连接析构。
                ::_exit(0);
            }

            SqliteTransaction transaction(db);

            SqliteStatement insert(
                db,
                "INSERT INTO outbox("
                "event_id, payload_json, created_at_ms, updated_at_ms"
                ") VALUES ('uncommitted', '{}', 1, 1);"
            );

            if (insert.step() != SqliteStepResult::Done) {
                ::_exit(3);
            }

            // 跳过语句、事务和连接的析构，不主动回滚。
            ::_exit(0);
        } catch (const std::exception& error) {
            std::fprintf(
                stderr,
                "Child database test failed: %s\n",
                error.what()
            );

            ::_exit(90);
        } catch (...) {
            ::_exit(91);
        }
    }

    int status = 0;
    pid_t waited;

    do {
        waited = ::waitpid(child, &status, 0);
    } while (waited == -1 && errno == EINTR);

    if (waited == -1) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "waitpid"
        );
    }

    if (!WIFEXITED(status)) {
        throw std::runtime_error(
            "Child database test did not exit normally"
        );
    }

    return WEXITSTATUS(status);
}

class OutboxDatabaseTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        const auto temp_root = std::filesystem::absolute(
            std::filesystem::temp_directory_path()
        );

        const std::string pattern =
            (temp_root / "edge-relay-outbox-XXXXXX").string();

        std::vector<char> buffer(pattern.begin(), pattern.end());
        buffer.push_back('\0');

        char* created = ::mkdtemp(buffer.data());

        if (created == nullptr) {
            throw std::runtime_error(
                "Cannot create Outbox test directory"
            );
        }

        directory_ = created;
        database_path_ = directory_ / "agent" / "outbox.db";
    }

    void TearDown() override
    {
        std::error_code error;
        std::filesystem::remove_all(directory_, error);

        EXPECT_FALSE(error)
            << "Temporary directory cleanup failed: "
            << error.message();
    }

    std::filesystem::path directory_;
    std::filesystem::path database_path_;
};

TEST_F(OutboxDatabaseTest, CreatesParentsAndConfiguresConnection)
{
    ASSERT_FALSE(
        std::filesystem::exists(database_path_.parent_path())
    );

    auto db = openOutboxDatabase(database_path_);

    EXPECT_TRUE(std::filesystem::is_regular_file(database_path_));

    EXPECT_EQ(
        scalarText(db, "PRAGMA main.journal_mode;"),
        "delete"
    );

    EXPECT_EQ(
        scalarInt64(db, "PRAGMA main.synchronous;"),
        3
    );

    EXPECT_EQ(
        scalarInt64(db, "PRAGMA busy_timeout;"),
        250
    );

    EXPECT_EQ(
        scalarInt64(db, "PRAGMA main.application_id;"),
        edge::kOutboxApplicationId
    );

    EXPECT_EQ(
        scalarInt64(db, "PRAGMA main.user_version;"),
        edge::kOutboxSchemaVersion
    );
}

TEST_F(OutboxDatabaseTest, ReopenKeepsEventMetadataAndDuplicateIdentity)
{
    const auto event = makeEvent();

    {
        auto db = openOutboxDatabase(database_path_);
        OutboxStore store(db);

        ASSERT_EQ(
            requireResult(store.enqueue(event, kNow)).status,
            OutboxInsertStatus::Inserted
        );

        executeSql(
            db,
            "UPDATE outbox SET "
            "state = 'SENDING', "
            "attempt_count = 2, "
            "next_attempt_at_ms = 1700000000500, "
            "updated_at_ms = 1700000000200, "
            "last_error = 'previous attempt';"
        );
    }

    auto reopened = openOutboxDatabase(database_path_);
    OutboxStore store(reopened);

    EXPECT_EQ(
        scalarInt64(reopened, "SELECT COUNT(*) FROM outbox;"),
        1
    );

    EXPECT_EQ(
        scalarText(reopened, "SELECT event_id FROM outbox;"),
        edge::makeEventId(event)
    );

    EXPECT_EQ(
        scalarText(reopened, "SELECT state FROM outbox;"),
        "SENDING"
    );

    EXPECT_EQ(
        scalarInt64(reopened, "SELECT attempt_count FROM outbox;"),
        2
    );

    EXPECT_EQ(
        scalarInt64(
            reopened,
            "SELECT next_attempt_at_ms FROM outbox;"
        ),
        1700000000500LL
    );

    EXPECT_EQ(
        scalarText(reopened, "SELECT last_error FROM outbox;"),
        "previous attempt"
    );

    EXPECT_EQ(
        requireResult(store.enqueue(event, kNow + 1000)).status,
        OutboxInsertStatus::Duplicate
    );

    EXPECT_EQ(
        scalarText(reopened, "SELECT state FROM outbox;"),
        "SENDING"
    );

    EXPECT_EQ(
        scalarInt64(reopened, "PRAGMA main.synchronous;"),
        3
    );
}

TEST_F(OutboxDatabaseTest, RejectsInvalidPathsBeforeCreatingParents)
{
    EXPECT_THROW(
        openOutboxDatabase(std::filesystem::path{}),
        std::invalid_argument
    );

    // 相对路径仍指向本测试目录，避免错误实现写到其他位置。
    const auto relative = std::filesystem::relative(
        database_path_,
        std::filesystem::current_path()
    );

    ASSERT_TRUE(relative.is_relative());

    EXPECT_THROW(
        openOutboxDatabase(relative),
        std::invalid_argument
    );

    std::string invalid = database_path_.string();
    invalid.push_back('\0');
    invalid += ".suffix";

    EXPECT_THROW(
        openOutboxDatabase(std::filesystem::path(invalid)),
        std::invalid_argument
    );

    EXPECT_FALSE(
        std::filesystem::exists(database_path_.parent_path())
    );
}

TEST_F(OutboxDatabaseTest, RejectsDirectoryAndParentFile)
{
    EXPECT_THROW(
        openOutboxDatabase(directory_),
        std::invalid_argument
    );

    const auto blocker = directory_ / "blocker";

    {
        std::ofstream output(blocker);
        ASSERT_TRUE(output.is_open());
        output << "existing file";
    }

    EXPECT_THROW(
        openOutboxDatabase(blocker / "outbox.db"),
        std::filesystem::filesystem_error
    );

    EXPECT_TRUE(std::filesystem::is_regular_file(blocker));
}

TEST_F(OutboxDatabaseTest, ExistingForeignDatabaseIsPreserved)
{
    std::filesystem::create_directories(
        database_path_.parent_path()
    );

    {
        SqliteDatabase foreign(database_path_);

        executeSql(
            foreign,
            "CREATE TABLE other (value INTEGER);"
        );

        executeSql(
            foreign,
            "INSERT INTO other(value) VALUES (77);"
        );
    }

    try {
        auto unexpected = openOutboxDatabase(database_path_);
        FAIL() << "Foreign database must be rejected";
    } catch (const std::runtime_error& error) {
        EXPECT_NE(
            std::string(error.what()).find("nonempty unmarked"),
            std::string::npos
        );
    }

    SqliteDatabase check(database_path_);

    EXPECT_EQ(
        scalarInt64(check, "SELECT value FROM other;"),
        77
    );

    EXPECT_EQ(
        scalarInt64(check, "PRAGMA main.application_id;"),
        0
    );

    EXPECT_EQ(
        scalarInt64(check, "PRAGMA main.user_version;"),
        0
    );
}

TEST_F(OutboxDatabaseTest, AbruptExitKeepsCommittedEvent)
{
    ASSERT_EQ(
        runAbruptWriter(database_path_, true),
        0
    );

    auto db = openOutboxDatabase(database_path_);
    OutboxStore store(db);

    EXPECT_EQ(
        scalarInt64(db, "SELECT COUNT(*) FROM outbox;"),
        1
    );

    EXPECT_EQ(
        requireResult(store.enqueue(makeEvent(), kNow + 1)).status,
        OutboxInsertStatus::Duplicate
    );
}

TEST_F(OutboxDatabaseTest, AbruptExitDoesNotExposeUncommittedEvent)
{
    ASSERT_EQ(
        runAbruptWriter(database_path_, false),
        0
    );

    auto db = openOutboxDatabase(database_path_);

    EXPECT_EQ(
        scalarInt64(db, "SELECT COUNT(*) FROM outbox;"),
        0
    );

    EXPECT_NE(sqlite3_get_autocommit(db.get()), 0);

    OutboxStore store(db);

    EXPECT_EQ(
        requireResult(store.enqueue(makeEvent(), kNow)).status,
        OutboxInsertStatus::Inserted
    );
}

TEST_F(OutboxDatabaseTest, LockConflictIsReportedAndOpenCanBeRetried)
{
    auto owner = openOutboxDatabase(database_path_);
    SqliteTransaction transaction(owner);

    try {
        auto unexpected = openOutboxDatabase(database_path_);
        FAIL() << "A second write transaction must not start";
    } catch (const SqliteError& error) {
        EXPECT_EQ(error.code() & 0xff, SQLITE_BUSY);
    }

    EXPECT_TRUE(transaction.isActive());

    transaction.rollback();

    EXPECT_NO_THROW(
        {
            auto reopened = openOutboxDatabase(database_path_);
        }
    );
}

} // namespace