#include "edge/storage/OutboxStore.h"

#include "edge/domain/EventId.h"
#include "edge/storage/OutboxSchema.h"
#include "edge/storage/SqliteDatabase.h"
#include "edge/storage/SqliteStatement.h"
#include "edge/storage/SqliteTransaction.h"
#include "edge/config/AgentConfig.h"

#include <sqlite3.h>

#include <cstdlib>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include <gtest/gtest.h>

namespace {

edge::SensorEvent makeEvent(std::int64_t sequence)
{
    edge::SensorEvent event;

    event.schema_version = 1;
    event.device_id = "device-001";
    event.session_id = "session-001";
    event.sequence = sequence;
    event.observed_at_ms = 1794441600000LL;
    event.metric = "temperature";
    event.value = 25.5;
    event.unit = "degC";

    return event;
}

std::string eventId(std::int64_t sequence)
{
    return edge::makeEventId(makeEvent(sequence));
}

void executeSql(
    edge::SqliteDatabase& database,
    const std::string& sql
)
{
    edge::SqliteStatement statement(database, sql);

    if (statement.step() != edge::SqliteStepResult::Done) {
        throw std::runtime_error(
            "Test SQL unexpectedly returned rows"
        );
    }
}

void insertEvent(
    edge::OutboxStore& store,
    std::int64_t sequence,
    std::int64_t created_at_ms
)
{
    const auto result = store.enqueue(
        makeEvent(sequence),
        created_at_ms
    );

    const auto* inserted =
        std::get_if<edge::OutboxInsertResult>(&result);

    if (inserted == nullptr
        || inserted->status != edge::OutboxInsertStatus::Inserted) {
        throw std::runtime_error(
            "Cannot prepare test event"
        );
    }
}

struct RecordSnapshot {
    std::string state;
    std::int64_t attempt_count;
    std::int64_t next_attempt_at_ms;
    std::int64_t created_at_ms;
    std::int64_t updated_at_ms;
    std::string last_error;
    std::string payload_json;
    bool acked_is_null;
};

class TemporaryDirectory {
public:
    TemporaryDirectory()
    {
        const auto pattern = (
            std::filesystem::temp_directory_path()
            / "edge-outbox-claim-XXXXXX"
        ).string();

        std::vector<char> buffer(pattern.begin(), pattern.end());
        buffer.push_back('\0');

        const char* directory = ::mkdtemp(buffer.data());

        if (directory == nullptr) {
            throw std::runtime_error(
                "Cannot create temporary test directory"
            );
        }

        path_ = directory;
    }

    ~TemporaryDirectory() noexcept
    {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    const std::filesystem::path& path() const noexcept
    {
        return path_;
    }

private:
    std::filesystem::path path_;
};

class OutboxClaimTest : public ::testing::Test {
protected:
    edge::SqliteDatabase database_{":memory:"};
    edge::OutboxStore store_{database_};

    void SetUp() override
    {
        edge::initializeOutboxSchema(database_);
    }

    void insert(
        std::int64_t sequence,
        std::int64_t created_at_ms = 1000
    )
    {
        insertEvent(store_, sequence, created_at_ms);
    }

    void setState(
        std::int64_t sequence,
        const std::string& state,
        std::int64_t attempt_count,
        std::int64_t next_attempt_at_ms
    )
    {
        edge::SqliteStatement update(
            database_,
            "UPDATE outbox SET "
            "state = ?1, "
            "attempt_count = ?2, "
            "next_attempt_at_ms = ?3, "
            "updated_at_ms = 1100, "
            "acked_at_ms = CASE "
                "WHEN ?1 = 'ACKED' THEN 1100 ELSE NULL END, "
            "last_error = 'previous failure' "
            "WHERE event_id = ?4;"
        );

        update.bindText(1, state);
        update.bindInt64(2, attempt_count);
        update.bindInt64(3, next_attempt_at_ms);
        update.bindText(4, eventId(sequence));

        if (update.step() != edge::SqliteStepResult::Done) {
            throw std::runtime_error("Cannot prepare test state");
        }
    }

    void setPayload(
        std::int64_t sequence,
        const std::string& payload
    )
    {
        edge::SqliteStatement update(
            database_,
            "UPDATE outbox SET payload_json = ?1 "
            "WHERE event_id = ?2;"
        );

        update.bindText(1, payload);
        update.bindText(2, eventId(sequence));

        if (update.step() != edge::SqliteStepResult::Done) {
            throw std::runtime_error("Cannot prepare test payload");
        }
    }

    RecordSnapshot snapshot(std::int64_t sequence)
    {
        edge::SqliteStatement query(
            database_,
            "SELECT state, attempt_count, next_attempt_at_ms, "
            "created_at_ms, updated_at_ms, last_error, "
            "payload_json, acked_at_ms "
            "FROM outbox WHERE event_id = ?1;"
        );

        query.bindText(1, eventId(sequence));

        if (query.step() != edge::SqliteStepResult::Row) {
            throw std::runtime_error("Test record not found");
        }

        const auto text = [&query](int column) {
            const auto* value =
                sqlite3_column_text(query.get(), column);

            if (value == nullptr) {
                throw std::runtime_error("Cannot read test text");
            }

            return std::string(
                reinterpret_cast<const char*>(value),
                static_cast<std::size_t>(
                    sqlite3_column_bytes(query.get(), column)
                )
            );
        };

        const RecordSnapshot result{
            text(0),
            static_cast<std::int64_t>(
                sqlite3_column_int64(query.get(), 1)
            ),
            static_cast<std::int64_t>(
                sqlite3_column_int64(query.get(), 2)
            ),
            static_cast<std::int64_t>(
                sqlite3_column_int64(query.get(), 3)
            ),
            static_cast<std::int64_t>(
                sqlite3_column_int64(query.get(), 4)
            ),
            text(5),
            text(6),
            sqlite3_column_type(query.get(), 7) == SQLITE_NULL
        };

        if (query.step() != edge::SqliteStepResult::Done) {
            throw std::runtime_error("Unexpected duplicate test record");
        }

        return result;
    }
};

TEST_F(OutboxClaimTest, ClaimsRetryAndPreservesMetadata)
{
    insert(1);
    setState(1, "RETRY", 3, 1500);

    const auto before = snapshot(1);

    const auto claimed = store_.claimReady(2000, 20);

    ASSERT_EQ(claimed.size(), 1U);
    EXPECT_EQ(claimed[0].event_id, eventId(1));
    EXPECT_EQ(claimed[0].payload_json, before.payload_json);
    EXPECT_EQ(claimed[0].attempt_count, 4);

    const auto after = snapshot(1);

    EXPECT_EQ(after.state, "SENDING");
    EXPECT_EQ(after.attempt_count, 4);
    EXPECT_EQ(after.updated_at_ms, 2000);

    EXPECT_EQ(after.created_at_ms, before.created_at_ms);
    EXPECT_EQ(after.next_attempt_at_ms, before.next_attempt_at_ms);
    EXPECT_EQ(after.last_error, before.last_error);
    EXPECT_EQ(after.payload_json, before.payload_json);
    EXPECT_TRUE(after.acked_is_null);

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, SelectsOnlyDuePendingAndRetry)
{
    for (std::int64_t sequence = 1; sequence <= 6; ++sequence) {
        insert(sequence);
    }

    setState(1, "PENDING", 0, 2000);
    setState(2, "RETRY", 3, 2000);
    setState(3, "RETRY", 1, 2001);
    setState(4, "SENDING", 2, 0);
    setState(5, "ACKED", 4, 0);
    setState(6, "DEAD", 5, 0);

    const auto claimed = store_.claimReady(2000, 100);

    ASSERT_EQ(claimed.size(), 2U);

    EXPECT_EQ(claimed[0].event_id, eventId(1));
    EXPECT_EQ(claimed[0].attempt_count, 1);

    EXPECT_EQ(claimed[1].event_id, eventId(2));
    EXPECT_EQ(claimed[1].attempt_count, 4);

    EXPECT_EQ(snapshot(3).state, "RETRY");
    EXPECT_EQ(snapshot(3).attempt_count, 1);

    EXPECT_EQ(snapshot(4).attempt_count, 2);
    EXPECT_EQ(snapshot(5).state, "ACKED");
    EXPECT_EQ(snapshot(6).state, "DEAD");

    const auto counts = store_.stateCounts();

    EXPECT_EQ(counts.sending, 3);
    EXPECT_EQ(counts.total, 6);
    EXPECT_EQ(counts.unacked, 5);
}

TEST_F(OutboxClaimTest, OrdersAndLimitsWithoutReclaiming)
{
    insert(2, 1000);
    insert(1, 1000);
    insert(3, 900);

    const auto first = store_.claimReady(2000, 2);

    ASSERT_EQ(first.size(), 2U);
    EXPECT_EQ(first[0].event_id, eventId(3));
    EXPECT_EQ(first[1].event_id, eventId(1));

    const auto second = store_.claimReady(2000, 2);

    ASSERT_EQ(second.size(), 1U);
    EXPECT_EQ(second[0].event_id, eventId(2));

    EXPECT_TRUE(store_.claimReady(2000, 2).empty());

    EXPECT_EQ(snapshot(1).attempt_count, 1);
    EXPECT_EQ(snapshot(2).attempt_count, 1);
    EXPECT_EQ(snapshot(3).attempt_count, 1);
}

TEST_F(OutboxClaimTest, HandlesEmptyStoreAndReleasesTransaction)
{
    EXPECT_TRUE(store_.claimReady(2000, 20).empty());
    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);

    insert(1);

    const auto claimed = store_.claimReady(2000, 20);

    ASSERT_EQ(claimed.size(), 1U);
    EXPECT_TRUE(store_.listReady(2000, 20).empty());
    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, RecoveryPreservesCountAndReclaimIncrements)
{
    insert(1);

    const auto first = store_.claimReady(2000, 20);

    ASSERT_EQ(first.size(), 1U);
    EXPECT_EQ(first[0].attempt_count, 1);

    EXPECT_EQ(store_.recoverSending(3000), 1);
    EXPECT_EQ(snapshot(1).state, "RETRY");
    EXPECT_EQ(snapshot(1).attempt_count, 1);

    const auto second = store_.claimReady(3000, 20);

    ASSERT_EQ(second.size(), 1U);
    EXPECT_EQ(second[0].event_id, first[0].event_id);
    EXPECT_EQ(second[0].payload_json, first[0].payload_json);
    EXPECT_EQ(second[0].attempt_count, 2);
    EXPECT_EQ(snapshot(1).attempt_count, 2);
}

TEST_F(OutboxClaimTest, RejectsInvalidArguments)
{
    insert(1);

    EXPECT_THROW(
        store_.claimReady(0, 20),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.claimReady(-1, 20),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.claimReady(2000, 0),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.claimReady(2000, -1),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.claimReady(2000, 101),
        std::invalid_argument
    );

    EXPECT_EQ(snapshot(1).state, "PENDING");
    EXPECT_EQ(snapshot(1).attempt_count, 0);
    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, RejectsCorruptOrMismatchedPayloads)
{
    insert(1);
    insert(2);

    const std::vector<std::string> invalid_payloads{
        "{",
        snapshot(1).payload_json
    };

    for (const auto& payload : invalid_payloads) {
        // 第二种情况：第 2 条记录保存了第 1 条事件的载荷。
        setPayload(2, payload);

        EXPECT_THROW(
            store_.claimReady(2000, 20),
            std::runtime_error
        );

        EXPECT_EQ(snapshot(1).state, "PENDING");
        EXPECT_EQ(snapshot(1).attempt_count, 0);
        EXPECT_EQ(snapshot(2).state, "PENDING");
        EXPECT_EQ(snapshot(2).attempt_count, 0);

        EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
    }
}

TEST_F(OutboxClaimTest, PreventsAttemptCountOverflow)
{
    insert(1);
    insert(2);

    const auto maximum =
        std::numeric_limits<std::int64_t>::max();

    setState(1, "PENDING", maximum - 1, 0);
    setState(2, "PENDING", maximum, 0);

    // 第二条不能增加次数，因此第一条也不能提前提交。
    EXPECT_THROW(
        store_.claimReady(2000, 20),
        std::overflow_error
    );

    EXPECT_EQ(snapshot(1).state, "PENDING");
    EXPECT_EQ(snapshot(1).attempt_count, maximum - 1);
    EXPECT_EQ(snapshot(2).state, "PENDING");
    EXPECT_EQ(snapshot(2).attempt_count, maximum);

    // 单独领取第一条，最后一次合法加一应成功。
    const auto claimed = store_.claimReady(2000, 1);

    ASSERT_EQ(claimed.size(), 1U);
    EXPECT_EQ(claimed[0].attempt_count, maximum);
    EXPECT_EQ(snapshot(1).attempt_count, maximum);
}

TEST_F(OutboxClaimTest, RollsBackEarlierUpdatesOnWriteFailure)
{
    insert(1);
    insert(2);

    // TEMP 触发器只用于注入测试故障，不修改 main 的 v1 DDL。
    executeSql(
        database_,
        "CREATE TEMP TRIGGER reject_second_claim "
        "BEFORE UPDATE OF state ON main.outbox "
        "WHEN NEW.state = 'SENDING' "
        "AND NEW.event_id = 'device-001/session-001/2' "
        "BEGIN "
            "SELECT RAISE(ABORT, 'simulated claim failure'); "
        "END;"
    );

    EXPECT_THROW(
        store_.claimReady(2000, 20),
        edge::SqliteError
    );

    // 第一条 UPDATE 虽然先执行成功，也必须随事务回滚。
    EXPECT_EQ(snapshot(1).state, "PENDING");
    EXPECT_EQ(snapshot(1).attempt_count, 0);
    EXPECT_EQ(snapshot(1).updated_at_ms, 1000);

    EXPECT_EQ(snapshot(2).state, "PENDING");
    EXPECT_EQ(snapshot(2).attempt_count, 0);
    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);

    executeSql(
        database_,
        "DROP TRIGGER temp.reject_second_claim;"
    );

    EXPECT_EQ(store_.claimReady(2000, 20).size(), 2U);
}

TEST_F(OutboxClaimTest, RejectsUnexpectedZeroRowUpdate)
{
    insert(1);
    insert(2);

    executeSql(
        database_,
        "CREATE TEMP TRIGGER ignore_second_claim "
        "BEFORE UPDATE OF state ON main.outbox "
        "WHEN NEW.state = 'SENDING' "
        "AND NEW.event_id = 'device-001/session-001/2' "
        "BEGIN "
            "SELECT RAISE(IGNORE); "
        "END;"
    );

    // SQL 可以返回 Done，但实际更新行数为 0。
    EXPECT_THROW(
        store_.claimReady(2000, 20),
        std::runtime_error
    );

    EXPECT_EQ(snapshot(1).state, "PENDING");
    EXPECT_EQ(snapshot(1).attempt_count, 0);
    EXPECT_EQ(snapshot(2).state, "PENDING");
    EXPECT_EQ(snapshot(2).attempt_count, 0);
    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, SeparateConnectionsRespectWriteLockAndClaims)
{
    TemporaryDirectory directory;

    const auto path = directory.path() / "outbox.db";

    edge::SqliteDatabase first_database(path);
    edge::initializeOutboxSchema(first_database);

    edge::SqliteDatabase second_database(path);
    edge::initializeOutboxSchema(second_database);

    // 直接配置连接：遇到锁竞争时立即返回
    ASSERT_EQ(
        sqlite3_busy_timeout(second_database.get(), 0),
        SQLITE_OK
    );

    edge::OutboxStore first_store(first_database);
    edge::OutboxStore second_store(second_database);

    insertEvent(first_store, 1, 1000);
    insertEvent(first_store, 2, 1000);

    {
        edge::SqliteTransaction writer(first_database);

        EXPECT_THROW(
            second_store.claimReady(2000, 20),
            edge::SqliteError
        );

        EXPECT_EQ(
            sqlite3_get_autocommit(second_database.get()),
            1
        );

        writer.commit();
    }

    const auto claimed = second_store.claimReady(2000, 20);

    ASSERT_EQ(claimed.size(), 2U);

    // 第二个连接已提交领取，第一个连接不能重复领取。
    EXPECT_TRUE(first_store.claimReady(2000, 20).empty());
    EXPECT_EQ(first_store.stateCounts().sending, 2);
    EXPECT_EQ(second_store.stateCounts().sending, 2);
}

std::vector<edge::OutboxClaimToken> makeClaimTokens(
    const std::vector<edge::OutboxClaimedRecord>& records
)
{
    std::vector<edge::OutboxClaimToken> tokens;
    tokens.reserve(records.size());

    for (const auto& record : records) {
        tokens.push_back(edge::OutboxClaimToken{
            record.event_id,
            record.attempt_count
        });
    }

    return tokens;
}

std::int64_t readAckedAt(
    edge::SqliteDatabase& database,
    const std::string& id
)
{
    edge::SqliteStatement query(
        database,
        "SELECT acked_at_ms FROM outbox WHERE event_id = ?1;"
    );

    query.bindText(1, id);

    if (query.step() != edge::SqliteStepResult::Row
        || sqlite3_column_type(query.get(), 0) != SQLITE_INTEGER) {
        throw std::runtime_error(
            "Expected an acknowledged test record"
        );
    }

    const auto value = static_cast<std::int64_t>(
        sqlite3_column_int64(query.get(), 0)
    );

    if (query.step() != edge::SqliteStepResult::Done) {
        throw std::runtime_error(
            "Unexpected duplicate acknowledged test record"
        );
    }

    return value;
}

TEST_F(OutboxClaimTest, AckWritesConfirmedRecordsAndPreservesContent)
{
    insert(1);
    insert(2);

    setState(1, "RETRY", 3, 1500);

    const auto claimed = store_.claimReady(2000, 20);
    ASSERT_EQ(claimed.size(), 2U);

    const std::vector<RecordSnapshot> before{
        snapshot(1),
        snapshot(2)
    };

    const auto result = store_.acknowledge(
        makeClaimTokens(claimed),
        3000
    );

    const auto* success =
        std::get_if<edge::OutboxAckSuccess>(&result);

    ASSERT_NE(success, nullptr);
    EXPECT_EQ(success->acked_records, 2);

    for (std::int64_t sequence = 1; sequence <= 2; ++sequence) {
        const auto after = snapshot(sequence);

        const auto& previous =
            before[static_cast<std::size_t>(sequence - 1)];

        EXPECT_EQ(after.state, "ACKED");
        EXPECT_EQ(after.updated_at_ms, 3000);
        EXPECT_FALSE(after.acked_is_null);
        EXPECT_TRUE(after.last_error.empty());

        EXPECT_EQ(after.attempt_count, previous.attempt_count);
        EXPECT_EQ(after.created_at_ms, previous.created_at_ms);
        EXPECT_EQ(after.next_attempt_at_ms, previous.next_attempt_at_ms);
        EXPECT_EQ(after.payload_json, previous.payload_json);

        EXPECT_EQ(
            readAckedAt(database_, eventId(sequence)),
            3000
        );
    }

    const auto counts = store_.stateCounts();

    EXPECT_EQ(counts.acked, 2);
    EXPECT_EQ(counts.sending, 0);
    EXPECT_EQ(counts.unacked, 0);
    EXPECT_EQ(counts.total, 2);

    EXPECT_TRUE(store_.listReady(5000, 20).empty());
    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, AckAllowsConfirmedSubset)
{
    insert(1);
    insert(2);
    insert(3);

    const auto claimed = store_.claimReady(2000, 20);
    ASSERT_EQ(claimed.size(), 3U);

    const auto tokens = makeClaimTokens(claimed);

    // 模拟第 1、3 条得到 accepted / duplicate，
    // 第 2 条尚不能确认。
    const auto result = store_.acknowledge(
        {tokens[0], tokens[2]},
        3000
    );

    const auto* success =
        std::get_if<edge::OutboxAckSuccess>(&result);

    ASSERT_NE(success, nullptr);
    EXPECT_EQ(success->acked_records, 2);

    EXPECT_EQ(snapshot(1).state, "ACKED");
    EXPECT_EQ(snapshot(3).state, "ACKED");

    EXPECT_EQ(snapshot(2).state, "SENDING");
    EXPECT_EQ(snapshot(2).attempt_count, 1);
    EXPECT_EQ(snapshot(2).updated_at_ms, 2000);
    EXPECT_TRUE(snapshot(2).acked_is_null);
}

TEST_F(OutboxClaimTest, AckRejectsOldAttemptAndAcceptsCurrentAttempt)
{
    insert(1);

    const auto first = store_.claimReady(2000, 20);
    ASSERT_EQ(first.size(), 1U);

    const auto old_tokens = makeClaimTokens(first);

    EXPECT_EQ(store_.recoverSending(2500), 1);

    const auto second = store_.claimReady(2600, 20);
    ASSERT_EQ(second.size(), 1U);
    EXPECT_EQ(second[0].attempt_count, 2);

    const auto stale_result = store_.acknowledge(
        old_tokens,
        3000
    );

    const auto* mismatch =
        std::get_if<edge::OutboxClaimMismatch>(&stale_result);

    ASSERT_NE(mismatch, nullptr);
    EXPECT_EQ(mismatch->event_id, eventId(1));
    EXPECT_EQ(mismatch->attempt_count, 1);

    EXPECT_EQ(snapshot(1).state, "SENDING");
    EXPECT_EQ(snapshot(1).attempt_count, 2);
    EXPECT_EQ(snapshot(1).updated_at_ms, 2600);
    EXPECT_TRUE(snapshot(1).acked_is_null);

    const auto current_result = store_.acknowledge(
        makeClaimTokens(second),
        3100
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxAckSuccess>(
            current_result
        )
    );

    EXPECT_EQ(snapshot(1).state, "ACKED");
    EXPECT_EQ(snapshot(1).attempt_count, 2);
    EXPECT_EQ(readAckedAt(database_, eventId(1)), 3100);
}

TEST_F(OutboxClaimTest, AckRejectsWholeListIfOneTokenMismatch)
{
    insert(1);
    insert(2);

    const auto claimed = store_.claimReady(2000, 20);
    ASSERT_EQ(claimed.size(), 2U);

    auto tokens = makeClaimTokens(claimed);
    ++tokens[1].attempt_count;

    const auto result = store_.acknowledge(tokens, 3000);

    const auto* mismatch =
        std::get_if<edge::OutboxClaimMismatch>(&result);

    ASSERT_NE(mismatch, nullptr);
    EXPECT_EQ(mismatch->event_id, eventId(2));
    EXPECT_EQ(mismatch->attempt_count, 2);

    for (std::int64_t sequence = 1; sequence <= 2; ++sequence) {
        const auto record = snapshot(sequence);

        EXPECT_EQ(record.state, "SENDING");
        EXPECT_EQ(record.attempt_count, 1);
        EXPECT_EQ(record.updated_at_ms, 2000);
        EXPECT_TRUE(record.acked_is_null);
    }

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, AckRejectsMissingAndNonSendingRecords)
{
    insert(1);
    insert(2);

    const auto claimed = store_.claimReady(2000, 20);
    ASSERT_EQ(claimed.size(), 2U);

    const auto tokens = makeClaimTokens(claimed);

    for (const std::string state : {
             "PENDING", "RETRY", "DEAD", "ACKED"
         }) {
        setState(2, state, 1, 0);

        const auto before = snapshot(2);
        const auto result = store_.acknowledge(tokens, 3000);

        const auto* mismatch =
            std::get_if<edge::OutboxClaimMismatch>(&result);

        ASSERT_NE(mismatch, nullptr);
        EXPECT_EQ(mismatch->event_id, eventId(2));

        EXPECT_EQ(snapshot(1).state, "SENDING");
        EXPECT_EQ(snapshot(1).updated_at_ms, 2000);

        const auto after = snapshot(2);

        EXPECT_EQ(after.state, before.state);
        EXPECT_EQ(after.attempt_count, before.attempt_count);
        EXPECT_EQ(after.updated_at_ms, before.updated_at_ms);
        EXPECT_EQ(after.acked_is_null, before.acked_is_null);
    }

    auto missing_tokens = tokens;
    missing_tokens[1].event_id = eventId(999);

    const auto missing_result = store_.acknowledge(
        missing_tokens,
        3000
    );

    const auto* missing =
        std::get_if<edge::OutboxClaimMismatch>(&missing_result);

    ASSERT_NE(missing, nullptr);
    EXPECT_EQ(missing->event_id, eventId(999));
    EXPECT_EQ(snapshot(1).state, "SENDING");
}

TEST_F(OutboxClaimTest, AckRepeatedCallDoesNotRefreshTime)
{
    insert(1);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);

    const auto first = store_.acknowledge(tokens, 3000);

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxAckSuccess>(first)
    );

    const auto repeated = store_.acknowledge(tokens, 4000);

    EXPECT_TRUE(
        std::holds_alternative<edge::OutboxClaimMismatch>(
            repeated
        )
    );

    EXPECT_EQ(snapshot(1).state, "ACKED");
    EXPECT_EQ(snapshot(1).attempt_count, 1);
    EXPECT_EQ(snapshot(1).updated_at_ms, 3000);
    EXPECT_EQ(readAckedAt(database_, eventId(1)), 3000);
}

TEST_F(OutboxClaimTest, AckValidatesArgumentsAndAllowsEmptyList)
{
    insert(1);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);
    ASSERT_EQ(tokens.size(), 1U);

    const auto empty = store_.acknowledge({}, 3000);

    const auto* success =
        std::get_if<edge::OutboxAckSuccess>(&empty);

    ASSERT_NE(success, nullptr);
    EXPECT_EQ(success->acked_records, 0);

    EXPECT_THROW(
        store_.acknowledge(tokens, 0),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.acknowledge(tokens, -1),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.acknowledge({}, 0),
        std::invalid_argument
    );

    auto invalid = tokens;
    invalid[0].event_id.clear();

    EXPECT_THROW(
        store_.acknowledge(invalid, 3000),
        std::invalid_argument
    );

    invalid = tokens;
    invalid[0].attempt_count = 0;

    EXPECT_THROW(
        store_.acknowledge(invalid, 3000),
        std::invalid_argument
    );

    invalid[0].attempt_count = -1;

    EXPECT_THROW(
        store_.acknowledge(invalid, 3000),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.acknowledge({tokens[0], tokens[0]}, 3000),
        std::invalid_argument
    );

    auto different_attempt = tokens[0];
    ++different_attempt.attempt_count;

    EXPECT_THROW(
        store_.acknowledge(
            {tokens[0], different_attempt},
            3000
        ),
        std::invalid_argument
    );

    const std::vector<edge::OutboxClaimToken> too_many(
        101,
        tokens[0]
    );

    EXPECT_THROW(
        store_.acknowledge(too_many, 3000),
        std::invalid_argument
    );

    EXPECT_EQ(snapshot(1).state, "SENDING");
    EXPECT_EQ(snapshot(1).attempt_count, 1);
    EXPECT_EQ(snapshot(1).updated_at_ms, 2000);
    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, AckRollsBackOnSecondWriteFailure)
{
    insert(1);
    insert(2);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);

    executeSql(
        database_,
        "CREATE TEMP TRIGGER reject_second_ack "
        "BEFORE UPDATE OF state ON main.outbox "
        "WHEN NEW.state = 'ACKED' "
        "AND NEW.event_id = 'device-001/session-001/2' "
        "BEGIN "
            "SELECT RAISE(ABORT, 'simulated ack failure'); "
        "END;"
    );

    EXPECT_THROW(
        store_.acknowledge(tokens, 3000),
        edge::SqliteError
    );

    for (std::int64_t sequence = 1; sequence <= 2; ++sequence) {
        const auto record = snapshot(sequence);

        EXPECT_EQ(record.state, "SENDING");
        EXPECT_EQ(record.attempt_count, 1);
        EXPECT_EQ(record.updated_at_ms, 2000);
        EXPECT_TRUE(record.acked_is_null);
    }

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);

    executeSql(
        database_,
        "DROP TRIGGER temp.reject_second_ack;"
    );

    const auto recovered = store_.acknowledge(tokens, 3100);

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxAckSuccess>(
            recovered
        )
    );

    EXPECT_EQ(store_.stateCounts().acked, 2);
}

TEST_F(OutboxClaimTest, AckRejectsZeroRowUpdateAfterMatchingClaims)
{
    insert(1);
    insert(2);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);

    executeSql(
        database_,
        "CREATE TEMP TRIGGER ignore_second_ack "
        "BEFORE UPDATE OF state ON main.outbox "
        "WHEN NEW.state = 'ACKED' "
        "AND NEW.event_id = 'device-001/session-001/2' "
        "BEGIN "
            "SELECT RAISE(IGNORE); "
        "END;"
    );

    // 令牌预检查已经匹配，实际写入却被忽略，必须报告异常。
    EXPECT_THROW(
        store_.acknowledge(tokens, 3000),
        std::runtime_error
    );

    EXPECT_EQ(snapshot(1).state, "SENDING");
    EXPECT_EQ(snapshot(1).updated_at_ms, 2000);
    EXPECT_TRUE(snapshot(1).acked_is_null);

    EXPECT_EQ(snapshot(2).state, "SENDING");
    EXPECT_TRUE(snapshot(2).acked_is_null);

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, AckAcceptsMaximumAttemptCount)
{
    insert(1);

    const auto maximum =
        std::numeric_limits<std::int64_t>::max();

    setState(1, "PENDING", maximum - 1, 0);

    const auto claimed = store_.claimReady(2000, 20);

    ASSERT_EQ(claimed.size(), 1U);
    ASSERT_EQ(claimed[0].attempt_count, maximum);

    const auto result = store_.acknowledge(
        makeClaimTokens(claimed),
        3000
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxAckSuccess>(result)
    );

    EXPECT_EQ(snapshot(1).state, "ACKED");
    EXPECT_EQ(snapshot(1).attempt_count, maximum);
}

TEST_F(OutboxClaimTest, RetryWritesScheduleAndPreservesMetadata)
{
    insert(1);
    insert(2);

    setState(1, "RETRY", 3, 1500);

    const auto claimed = store_.claimReady(2000, 20);
    ASSERT_EQ(claimed.size(), 2U);

    const std::vector<RecordSnapshot> before{
        snapshot(1),
        snapshot(2)
    };

    const auto result = store_.scheduleRetry(
        makeClaimTokens(claimed),
        2500,
        4000,
        "HTTP request timed out"
    );

    const auto* success =
        std::get_if<edge::OutboxRetrySuccess>(&result);

    ASSERT_NE(success, nullptr);
    EXPECT_EQ(success->retry_records, 2);

    for (std::int64_t sequence = 1; sequence <= 2; ++sequence) {
        const auto after = snapshot(sequence);

        const auto& previous =
            before[static_cast<std::size_t>(sequence - 1)];

        EXPECT_EQ(after.state, "RETRY");
        EXPECT_EQ(after.next_attempt_at_ms, 4000);
        EXPECT_EQ(after.updated_at_ms, 2500);
        EXPECT_EQ(after.last_error, "HTTP request timed out");
        EXPECT_TRUE(after.acked_is_null);

        EXPECT_EQ(after.attempt_count, previous.attempt_count);
        EXPECT_EQ(after.created_at_ms, previous.created_at_ms);
        EXPECT_EQ(after.payload_json, previous.payload_json);
    }

    const auto counts = store_.stateCounts();

    EXPECT_EQ(counts.retry, 2);
    EXPECT_EQ(counts.sending, 0);
    EXPECT_EQ(counts.unacked, 2);
    EXPECT_EQ(counts.total, 2);

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, RetryBecomesEligibleAtExactDueTime)
{
    insert(1);

    const auto first = store_.claimReady(2000, 20);
    ASSERT_EQ(first.size(), 1U);

    const auto result = store_.scheduleRetry(
        makeClaimTokens(first),
        2100,
        3000,
        "Temporary network failure"
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxRetrySuccess>(result)
    );

    EXPECT_EQ(snapshot(1).attempt_count, 1);

    EXPECT_TRUE(store_.listReady(2999, 20).empty());
    EXPECT_TRUE(store_.claimReady(2999, 20).empty());
    EXPECT_EQ(snapshot(1).attempt_count, 1);

    const auto ready = store_.listReady(3000, 20);

    ASSERT_EQ(ready.size(), 1U);
    EXPECT_EQ(ready[0].attempt_count, 1);

    const auto second = store_.claimReady(3000, 20);

    ASSERT_EQ(second.size(), 1U);
    EXPECT_EQ(second[0].event_id, first[0].event_id);
    EXPECT_EQ(second[0].payload_json, first[0].payload_json);
    EXPECT_EQ(second[0].attempt_count, 2);
}

TEST_F(OutboxClaimTest, RetryRejectsOldFailureForNewClaim)
{
    insert(1);

    const auto first = store_.claimReady(2000, 20);
    ASSERT_EQ(first.size(), 1U);

    const auto old_tokens = makeClaimTokens(first);

    EXPECT_EQ(store_.recoverSending(2500), 1);

    const auto second = store_.claimReady(2600, 20);
    ASSERT_EQ(second.size(), 1U);
    ASSERT_EQ(second[0].attempt_count, 2);

    const auto before = snapshot(1);

    const auto stale_result = store_.scheduleRetry(
        old_tokens,
        3000,
        9000,
        "Stale failure"
    );

    const auto* mismatch =
        std::get_if<edge::OutboxClaimMismatch>(&stale_result);

    ASSERT_NE(mismatch, nullptr);
    EXPECT_EQ(mismatch->event_id, eventId(1));
    EXPECT_EQ(mismatch->attempt_count, 1);

    const auto after = snapshot(1);

    EXPECT_EQ(after.state, before.state);
    EXPECT_EQ(after.attempt_count, before.attempt_count);
    EXPECT_EQ(after.updated_at_ms, before.updated_at_ms);
    EXPECT_EQ(after.next_attempt_at_ms, before.next_attempt_at_ms);
    EXPECT_EQ(after.last_error, before.last_error);

    const auto current_result = store_.scheduleRetry(
        makeClaimTokens(second),
        3100,
        4000,
        "Current failure"
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxRetrySuccess>(
            current_result
        )
    );

    EXPECT_EQ(snapshot(1).state, "RETRY");
    EXPECT_EQ(snapshot(1).attempt_count, 2);
    EXPECT_EQ(snapshot(1).next_attempt_at_ms, 4000);
    EXPECT_EQ(snapshot(1).last_error, "Current failure");
}

TEST_F(OutboxClaimTest, RetryCannotMoveAckedRecordBackToRetry)
{
    insert(1);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);
    ASSERT_EQ(tokens.size(), 1U);

    const auto ack = store_.acknowledge(tokens, 3000);

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxAckSuccess>(ack)
    );

    const auto result = store_.scheduleRetry(
        tokens,
        4000,
        5000,
        "Delayed failure"
    );

    EXPECT_TRUE(
        std::holds_alternative<edge::OutboxClaimMismatch>(
            result
        )
    );

    const auto record = snapshot(1);

    EXPECT_EQ(record.state, "ACKED");
    EXPECT_EQ(record.updated_at_ms, 3000);
    EXPECT_EQ(record.attempt_count, 1);
    EXPECT_TRUE(record.last_error.empty());

    EXPECT_EQ(readAckedAt(database_, eventId(1)), 3000);
}

TEST_F(OutboxClaimTest, RetryRejectsWholeListOnMismatchOrMissingRecord)
{
    insert(1);
    insert(2);

    const auto claimed = store_.claimReady(2000, 20);
    ASSERT_EQ(claimed.size(), 2U);

    auto tokens = makeClaimTokens(claimed);
    ++tokens[1].attempt_count;

    const auto mismatch_result = store_.scheduleRetry(
        tokens,
        3000,
        4000,
        "Temporary failure"
    );

    const auto* mismatch =
        std::get_if<edge::OutboxClaimMismatch>(&mismatch_result);

    ASSERT_NE(mismatch, nullptr);
    EXPECT_EQ(mismatch->event_id, eventId(2));

    tokens = makeClaimTokens(claimed);
    tokens[1].event_id = eventId(999);

    const auto missing_result = store_.scheduleRetry(
        tokens,
        3000,
        4000,
        "Temporary failure"
    );

    const auto* missing =
        std::get_if<edge::OutboxClaimMismatch>(&missing_result);

    ASSERT_NE(missing, nullptr);
    EXPECT_EQ(missing->event_id, eventId(999));

    for (std::int64_t sequence = 1; sequence <= 2; ++sequence) {
        const auto record = snapshot(sequence);

        EXPECT_EQ(record.state, "SENDING");
        EXPECT_EQ(record.attempt_count, 1);
        EXPECT_EQ(record.updated_at_ms, 2000);
        EXPECT_EQ(record.next_attempt_at_ms, 0);
        EXPECT_TRUE(record.last_error.empty());
    }

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, RetryAllowsSubsetAndEmptyList)
{
    insert(1);
    insert(2);
    insert(3);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);
    ASSERT_EQ(tokens.size(), 3U);

    const auto subset = store_.scheduleRetry(
        {tokens[0], tokens[2]},
        3000,
        4000,
        "Temporary failure"
    );

    const auto* success =
        std::get_if<edge::OutboxRetrySuccess>(&subset);

    ASSERT_NE(success, nullptr);
    EXPECT_EQ(success->retry_records, 2);

    EXPECT_EQ(snapshot(1).state, "RETRY");
    EXPECT_EQ(snapshot(3).state, "RETRY");

    EXPECT_EQ(snapshot(2).state, "SENDING");
    EXPECT_EQ(snapshot(2).updated_at_ms, 2000);
    EXPECT_EQ(snapshot(2).attempt_count, 1);

    const auto empty = store_.scheduleRetry(
        {},
        3100,
        4100,
        "Unused failure"
    );

    const auto* empty_success =
        std::get_if<edge::OutboxRetrySuccess>(&empty);

    ASSERT_NE(empty_success, nullptr);
    EXPECT_EQ(empty_success->retry_records, 0);

    // 空操作不能改变前面已经安排好的重试时间。
    EXPECT_EQ(snapshot(1).next_attempt_at_ms, 4000);
    EXPECT_EQ(snapshot(3).next_attempt_at_ms, 4000);
    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, RetryValidatesArgumentsAndAllowsImmediateRetry)
{
    insert(1);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);
    ASSERT_EQ(tokens.size(), 1U);

    EXPECT_THROW(
        store_.scheduleRetry(tokens, 0, 4000, "Failure"),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.scheduleRetry(tokens, -1, 4000, "Failure"),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.scheduleRetry(tokens, 3000, 2999, "Failure"),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.scheduleRetry(tokens, 3000, 0, "Failure"),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.scheduleRetry(tokens, 3000, 4000, ""),
        std::invalid_argument
    );

    const std::string null_text("bad\0error", 9);

    EXPECT_THROW(
        store_.scheduleRetry(tokens, 3000, 4000, null_text),
        std::invalid_argument
    );

    const std::string invalid_utf8(
        1,
        static_cast<char>(0xFF)
    );

    EXPECT_THROW(
        store_.scheduleRetry(tokens, 3000, 4000, invalid_utf8),
        std::invalid_argument
    );

    auto invalid = tokens;
    invalid[0].event_id.clear();

    EXPECT_THROW(
        store_.scheduleRetry(invalid, 3000, 4000, "Failure"),
        std::invalid_argument
    );

    invalid = tokens;
    invalid[0].attempt_count = 0;

    EXPECT_THROW(
        store_.scheduleRetry(invalid, 3000, 4000, "Failure"),
        std::invalid_argument
    );

    invalid[0].attempt_count = -1;

    EXPECT_THROW(
        store_.scheduleRetry(invalid, 3000, 4000, "Failure"),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.scheduleRetry(
            {tokens[0], tokens[0]},
            3000,
            4000,
            "Failure"
        ),
        std::invalid_argument
    );

    const std::vector<edge::OutboxClaimToken> too_many(
        101,
        tokens[0]
    );

    EXPECT_THROW(
        store_.scheduleRetry(too_many, 3000, 4000, "Failure"),
        std::invalid_argument
    );

    EXPECT_EQ(snapshot(1).state, "SENDING");
    EXPECT_EQ(snapshot(1).attempt_count, 1);
    EXPECT_EQ(snapshot(1).updated_at_ms, 2000);

    // 时间相等是合法的立即重试。
    const auto immediate = store_.scheduleRetry(
        tokens,
        3000,
        3000,
        "Immediate retry"
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxRetrySuccess>(
            immediate
        )
    );

    EXPECT_EQ(store_.listReady(3000, 20).size(), 1U);
}

TEST_F(OutboxClaimTest, RetryRollsBackOnSecondWriteFailure)
{
    insert(1);
    insert(2);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);

    executeSql(
        database_,
        "CREATE TEMP TRIGGER reject_second_retry "
        "BEFORE UPDATE OF state ON main.outbox "
        "WHEN NEW.state = 'RETRY' "
        "AND NEW.event_id = 'device-001/session-001/2' "
        "BEGIN "
            "SELECT RAISE(ABORT, 'simulated retry failure'); "
        "END;"
    );

    EXPECT_THROW(
        store_.scheduleRetry(
            tokens,
            3000,
            4000,
            "Temporary failure"
        ),
        edge::SqliteError
    );

    for (std::int64_t sequence = 1; sequence <= 2; ++sequence) {
        const auto record = snapshot(sequence);

        EXPECT_EQ(record.state, "SENDING");
        EXPECT_EQ(record.attempt_count, 1);
        EXPECT_EQ(record.updated_at_ms, 2000);
        EXPECT_EQ(record.next_attempt_at_ms, 0);
        EXPECT_TRUE(record.last_error.empty());
        EXPECT_TRUE(record.acked_is_null);
    }

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);

    executeSql(
        database_,
        "DROP TRIGGER temp.reject_second_retry;"
    );

    const auto recovered = store_.scheduleRetry(
        tokens,
        3100,
        4100,
        "Temporary failure"
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxRetrySuccess>(
            recovered
        )
    );

    EXPECT_EQ(store_.stateCounts().retry, 2);
}

TEST_F(OutboxClaimTest, RetryRejectsZeroRowUpdateAfterMatchingClaims)
{
    insert(1);
    insert(2);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);

    executeSql(
        database_,
        "CREATE TEMP TRIGGER ignore_second_retry "
        "BEFORE UPDATE OF state ON main.outbox "
        "WHEN NEW.state = 'RETRY' "
        "AND NEW.event_id = 'device-001/session-001/2' "
        "BEGIN "
            "SELECT RAISE(IGNORE); "
        "END;"
    );

    EXPECT_THROW(
        store_.scheduleRetry(
            tokens,
            3000,
            4000,
            "Temporary failure"
        ),
        std::runtime_error
    );

    for (std::int64_t sequence = 1; sequence <= 2; ++sequence) {
        const auto record = snapshot(sequence);

        EXPECT_EQ(record.state, "SENDING");
        EXPECT_EQ(record.attempt_count, 1);
        EXPECT_EQ(record.updated_at_ms, 2000);
        EXPECT_EQ(record.next_attempt_at_ms, 0);
        EXPECT_TRUE(record.last_error.empty());
    }

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, RetryPreservesUnicodeAndMaximumTimestamp)
{
    insert(1);

    const auto claimed = store_.claimReady(2000, 20);
    ASSERT_EQ(claimed.size(), 1U);

    const auto maximum =
        std::numeric_limits<std::int64_t>::max();

    const std::string message = "连接超时，等待重试。";

    const auto result = store_.scheduleRetry(
        makeClaimTokens(claimed),
        maximum - 1,
        maximum,
        message
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxRetrySuccess>(result)
    );

    const auto record = snapshot(1);

    EXPECT_EQ(record.state, "RETRY");
    EXPECT_EQ(record.updated_at_ms, maximum - 1);
    EXPECT_EQ(record.next_attempt_at_ms, maximum);
    EXPECT_EQ(record.attempt_count, 1);
    EXPECT_EQ(record.last_error, message);

    EXPECT_TRUE(store_.listReady(maximum - 1, 20).empty());
    EXPECT_EQ(store_.listReady(maximum, 20).size(), 1U);

    const auto second = store_.claimReady(maximum, 20);

    ASSERT_EQ(second.size(), 1U);
    EXPECT_EQ(second[0].attempt_count, 2);
}

TEST_F(OutboxClaimTest, DeadPreservesEventAndClaimMetadata)
{
    insert(1);
    setState(1, "RETRY", 3, 1500);

    const auto claimed = store_.claimReady(2000, 20);
    ASSERT_EQ(claimed.size(), 1U);

    const auto before = snapshot(1);

    const std::string reason = "服务端确认事件身份冲突。";

    const auto result = store_.markDead(
        makeClaimTokens(claimed),
        3000,
        reason
    );

    const auto* success =
        std::get_if<edge::OutboxDeadSuccess>(&result);

    ASSERT_NE(success, nullptr);
    EXPECT_EQ(success->dead_records, 1);

    const auto after = snapshot(1);

    EXPECT_EQ(after.state, "DEAD");
    EXPECT_EQ(after.updated_at_ms, 3000);
    EXPECT_EQ(after.last_error, reason);
    EXPECT_TRUE(after.acked_is_null);

    EXPECT_EQ(after.attempt_count, before.attempt_count);
    EXPECT_EQ(after.created_at_ms, before.created_at_ms);
    EXPECT_EQ(after.next_attempt_at_ms, before.next_attempt_at_ms);
    EXPECT_EQ(after.payload_json, before.payload_json);

    const auto counts = store_.stateCounts();

    EXPECT_EQ(counts.dead, 1);
    EXPECT_EQ(counts.sending, 0);
    EXPECT_EQ(counts.unacked, 1);
    EXPECT_EQ(counts.total, 1);

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, DeadSubsetIsNotReclaimedOrRecovered)
{
    insert(1);
    insert(2);
    insert(3);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);
    ASSERT_EQ(tokens.size(), 3U);

    const auto result = store_.markDead(
        {tokens[1]},
        3000,
        "Server reported event identity conflict"
    );

    const auto* success =
        std::get_if<edge::OutboxDeadSuccess>(&result);

    ASSERT_NE(success, nullptr);
    EXPECT_EQ(success->dead_records, 1);

    EXPECT_EQ(snapshot(1).state, "SENDING");
    EXPECT_EQ(snapshot(2).state, "DEAD");
    EXPECT_EQ(snapshot(3).state, "SENDING");

    const auto empty = store_.markDead(
        {},
        3500,
        "Unused terminal reason"
    );

    const auto* empty_success =
        std::get_if<edge::OutboxDeadSuccess>(&empty);

    ASSERT_NE(empty_success, nullptr);
    EXPECT_EQ(empty_success->dead_records, 0);

    EXPECT_EQ(snapshot(2).updated_at_ms, 3000);

    // 启动恢复只处理剩余的两条 SENDING。
    EXPECT_EQ(store_.recoverSending(4000), 2);

    EXPECT_EQ(snapshot(2).state, "DEAD");
    EXPECT_EQ(snapshot(2).attempt_count, 1);
    EXPECT_EQ(snapshot(2).updated_at_ms, 3000);

    const auto ready = store_.listReady(4000, 20);

    ASSERT_EQ(ready.size(), 2U);
    EXPECT_EQ(ready[0].event_id, eventId(1));
    EXPECT_EQ(ready[1].event_id, eventId(3));

    const auto next = store_.claimReady(4000, 20);

    ASSERT_EQ(next.size(), 2U);
    EXPECT_EQ(next[0].event_id, eventId(1));
    EXPECT_EQ(next[1].event_id, eventId(3));
}

TEST_F(OutboxClaimTest, DeadRejectsOldClaimAndRepeatedResult)
{
    insert(1);

    const auto first = store_.claimReady(2000, 20);
    const auto old_tokens = makeClaimTokens(first);
    ASSERT_EQ(old_tokens.size(), 1U);

    EXPECT_EQ(store_.recoverSending(2500), 1);

    const auto second = store_.claimReady(2600, 20);
    const auto current_tokens = makeClaimTokens(second);

    ASSERT_EQ(current_tokens.size(), 1U);
    ASSERT_EQ(current_tokens[0].attempt_count, 2);

    const auto stale = store_.markDead(
        old_tokens,
        3000,
        "Stale conflict result"
    );

    const auto* mismatch =
        std::get_if<edge::OutboxClaimMismatch>(&stale);

    ASSERT_NE(mismatch, nullptr);
    EXPECT_EQ(mismatch->event_id, eventId(1));
    EXPECT_EQ(mismatch->attempt_count, 1);

    EXPECT_EQ(snapshot(1).state, "SENDING");
    EXPECT_EQ(snapshot(1).attempt_count, 2);
    EXPECT_EQ(snapshot(1).updated_at_ms, 2600);

    const auto current = store_.markDead(
        current_tokens,
        3100,
        "Current conflict result"
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxDeadSuccess>(
            current
        )
    );

    const auto repeated = store_.markDead(
        current_tokens,
        4000,
        "Repeated conflict result"
    );

    EXPECT_TRUE(
        std::holds_alternative<edge::OutboxClaimMismatch>(
            repeated
        )
    );

    EXPECT_EQ(snapshot(1).state, "DEAD");
    EXPECT_EQ(snapshot(1).attempt_count, 2);
    EXPECT_EQ(snapshot(1).updated_at_ms, 3100);
    EXPECT_EQ(snapshot(1).last_error, "Current conflict result");
}

TEST_F(OutboxClaimTest, DeadRejectsWholeListOnMismatchOrWrongState)
{
    insert(1);
    insert(2);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);
    ASSERT_EQ(tokens.size(), 2U);

    auto wrong_attempt = tokens;
    ++wrong_attempt[1].attempt_count;

    const auto wrong_result = store_.markDead(
        wrong_attempt,
        3000,
        "Conflict"
    );

    EXPECT_TRUE(
        std::holds_alternative<edge::OutboxClaimMismatch>(
            wrong_result
        )
    );

    auto missing = tokens;
    missing[1].event_id = eventId(999);

    const auto missing_result = store_.markDead(
        missing,
        3000,
        "Conflict"
    );

    const auto* missing_claim =
        std::get_if<edge::OutboxClaimMismatch>(&missing_result);

    ASSERT_NE(missing_claim, nullptr);
    EXPECT_EQ(missing_claim->event_id, eventId(999));

    for (const std::string state : {
             "PENDING", "RETRY", "ACKED", "DEAD"
         }) {
        setState(2, state, 1, 0);
        const auto before = snapshot(2);

        const auto result = store_.markDead(
            tokens,
            3000,
            "Conflict"
        );

        const auto* mismatch =
            std::get_if<edge::OutboxClaimMismatch>(&result);

        ASSERT_NE(mismatch, nullptr);
        EXPECT_EQ(mismatch->event_id, eventId(2));

        EXPECT_EQ(snapshot(1).state, "SENDING");
        EXPECT_EQ(snapshot(1).updated_at_ms, 2000);
        EXPECT_TRUE(snapshot(1).last_error.empty());

        const auto after = snapshot(2);

        EXPECT_EQ(after.state, before.state);
        EXPECT_EQ(after.attempt_count, before.attempt_count);
        EXPECT_EQ(after.updated_at_ms, before.updated_at_ms);
        EXPECT_EQ(after.last_error, before.last_error);
        EXPECT_EQ(after.acked_is_null, before.acked_is_null);
    }

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, DeadValidatesArgumentsBeforeChangingRecords)
{
    insert(1);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);
    ASSERT_EQ(tokens.size(), 1U);

    EXPECT_THROW(
        store_.markDead(tokens, 0, "Conflict"),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.markDead(tokens, -1, "Conflict"),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.markDead(tokens, 3000, ""),
        std::invalid_argument
    );

    const std::string null_reason("bad\0reason", 10);

    EXPECT_THROW(
        store_.markDead(tokens, 3000, null_reason),
        std::invalid_argument
    );

    const std::string invalid_utf8(
        1,
        static_cast<char>(0xFF)
    );

    EXPECT_THROW(
        store_.markDead(tokens, 3000, invalid_utf8),
        std::invalid_argument
    );

    auto invalid = tokens;
    invalid[0].event_id.clear();

    EXPECT_THROW(
        store_.markDead(invalid, 3000, "Conflict"),
        std::invalid_argument
    );

    invalid = tokens;
    invalid[0].attempt_count = 0;

    EXPECT_THROW(
        store_.markDead(invalid, 3000, "Conflict"),
        std::invalid_argument
    );

    invalid[0].attempt_count = -1;

    EXPECT_THROW(
        store_.markDead(invalid, 3000, "Conflict"),
        std::invalid_argument
    );

    EXPECT_THROW(
        store_.markDead(
            {tokens[0], tokens[0]},
            3000,
            "Conflict"
        ),
        std::invalid_argument
    );

    const std::vector<edge::OutboxClaimToken> too_many(
        101,
        tokens[0]
    );

    EXPECT_THROW(
        store_.markDead(too_many, 3000, "Conflict"),
        std::invalid_argument
    );

    EXPECT_EQ(snapshot(1).state, "SENDING");
    EXPECT_EQ(snapshot(1).attempt_count, 1);
    EXPECT_EQ(snapshot(1).updated_at_ms, 2000);
    EXPECT_TRUE(snapshot(1).last_error.empty());

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, DeadRollsBackOnSecondWriteFailure)
{
    insert(1);
    insert(2);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);

    executeSql(
        database_,
        "CREATE TEMP TRIGGER reject_second_dead "
        "BEFORE UPDATE OF state ON main.outbox "
        "WHEN NEW.state = 'DEAD' "
        "AND NEW.event_id = 'device-001/session-001/2' "
        "BEGIN "
            "SELECT RAISE(ABORT, 'simulated terminal failure'); "
        "END;"
    );

    EXPECT_THROW(
        store_.markDead(tokens, 3000, "Conflict"),
        edge::SqliteError
    );

    for (std::int64_t sequence = 1; sequence <= 2; ++sequence) {
        const auto record = snapshot(sequence);

        EXPECT_EQ(record.state, "SENDING");
        EXPECT_EQ(record.attempt_count, 1);
        EXPECT_EQ(record.updated_at_ms, 2000);
        EXPECT_TRUE(record.last_error.empty());
        EXPECT_TRUE(record.acked_is_null);
    }

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);

    executeSql(
        database_,
        "DROP TRIGGER temp.reject_second_dead;"
    );

    const auto recovered = store_.markDead(
        tokens,
        3100,
        "Conflict"
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxDeadSuccess>(
            recovered
        )
    );

    EXPECT_EQ(store_.stateCounts().dead, 2);
}

TEST_F(OutboxClaimTest, DeadRejectsZeroRowUpdateAfterMatchingClaims)
{
    insert(1);
    insert(2);

    const auto claimed = store_.claimReady(2000, 20);
    const auto tokens = makeClaimTokens(claimed);

    executeSql(
        database_,
        "CREATE TEMP TRIGGER ignore_second_dead "
        "BEFORE UPDATE OF state ON main.outbox "
        "WHEN NEW.state = 'DEAD' "
        "AND NEW.event_id = 'device-001/session-001/2' "
        "BEGIN "
            "SELECT RAISE(IGNORE); "
        "END;"
    );

    EXPECT_THROW(
        store_.markDead(tokens, 3000, "Conflict"),
        std::runtime_error
    );

    for (std::int64_t sequence = 1; sequence <= 2; ++sequence) {
        const auto record = snapshot(sequence);

        EXPECT_EQ(record.state, "SENDING");
        EXPECT_EQ(record.attempt_count, 1);
        EXPECT_EQ(record.updated_at_ms, 2000);
        EXPECT_TRUE(record.last_error.empty());
        EXPECT_TRUE(record.acked_is_null);
    }

    EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
}

TEST_F(OutboxClaimTest, DeadStillConsumesCapacityAndSurvivesCleanup)
{
    edge::StorageConfig config;

    config.max_pending_records = 1;
    config.max_total_records = 2;
    config.acked_keep_max_records = 1;

    edge::OutboxStore limited(database_, config);

    insertEvent(limited, 1, 1000);

    const auto claimed = limited.claimReady(2000, 20);
    ASSERT_EQ(claimed.size(), 1U);

    const auto terminal = limited.markDead(
        makeClaimTokens(claimed),
        3000,
        "Conflict"
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxDeadSuccess>(
            terminal
        )
    );

    const auto rejected = limited.enqueue(
        makeEvent(2),
        4000
    );

    const auto* capacity =
        std::get_if<edge::OutboxCapacityError>(&rejected);

    ASSERT_NE(capacity, nullptr);
    EXPECT_EQ(
        capacity->code,
        edge::OutboxCapacityCode::PendingLimitReached
    );

    EXPECT_EQ(capacity->pending_records, 1);
    EXPECT_EQ(capacity->total_records, 1);
    EXPECT_EQ(capacity->limit, 1);

    const auto cleanup = limited.cleanupAcked(1000000000LL);

    EXPECT_EQ(cleanup.expired_deleted, 0);
    EXPECT_EQ(cleanup.excess_deleted, 0);

    EXPECT_EQ(limited.recoverSending(5000), 0);
    EXPECT_TRUE(limited.listReady(5000, 20).empty());

    const auto counts = limited.stateCounts();

    EXPECT_EQ(counts.dead, 1);
    EXPECT_EQ(counts.unacked, 1);
    EXPECT_EQ(counts.total, 1);
    EXPECT_EQ(snapshot(1).state, "DEAD");
    EXPECT_EQ(snapshot(1).last_error, "Conflict");
}

TEST_F(OutboxClaimTest, DeadAcceptsMaximumCountAndTimestamp)
{
    insert(1);

    const auto maximum =
        std::numeric_limits<std::int64_t>::max();

    setState(1, "PENDING", maximum - 1, 0);

    const auto claimed = store_.claimReady(2000, 20);

    ASSERT_EQ(claimed.size(), 1U);
    ASSERT_EQ(claimed[0].attempt_count, maximum);

    const auto result = store_.markDead(
        makeClaimTokens(claimed),
        maximum,
        "Conflict"
    );

    ASSERT_TRUE(
        std::holds_alternative<edge::OutboxDeadSuccess>(result)
    );

    EXPECT_EQ(snapshot(1).state, "DEAD");
    EXPECT_EQ(snapshot(1).attempt_count, maximum);
    EXPECT_EQ(snapshot(1).updated_at_ms, maximum);
}

} // namespace