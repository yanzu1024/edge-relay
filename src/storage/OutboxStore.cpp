#include "edge/storage/OutboxStore.h"

#include "edge/domain/EventId.h"
#include "edge/storage/SqliteDatabase.h"
#include "edge/storage/SqliteStatement.h"
#include "edge/storage/SqliteTransaction.h"

#include "edge/config/AgentConfig.h"

#include <sqlite3.h>
#include <nlohmann/json.hpp>

#include <cmath>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <limits>
#include <unordered_set>

namespace edge {

namespace {

bool sameEventContent(
    const SensorEvent& left,
    const SensorEvent& right
) noexcept
{
    // 比较领域字段，不比较 JSON 的排版或字段顺序。
    // 浮点数使用精确比较：这是重复事件识别，不是近似测量比较。
    // +0.0 与 -0.0 在这里相等。
    return left.schema_version == right.schema_version &&
           left.device_id == right.device_id &&
           left.session_id == right.session_id &&
           left.sequence == right.sequence &&
           left.observed_at_ms == right.observed_at_ms &&
           left.metric == right.metric &&
           left.value == right.value &&
           left.unit == right.unit;
}

struct OutboxCounts {
    std::int64_t pending_records;
    std::int64_t total_records;
};

OutboxCounts readOutboxCounts(SqliteDatabase& database){
    SqliteStatement query(
        database,
        "SELECT "
        "COUNT(CASE WHEN state <> 'ACKED' THEN 1 END), "
        "COUNT(*) "
        "FROM outbox;"
    );

    if (query.step() != SqliteStepResult::Row) {
        throw std::runtime_error(
            "Outbox count query returned no row"
        );
    }

    if (sqlite3_column_type(query.get(), 0) != SQLITE_INTEGER ||
        sqlite3_column_type(query.get(), 1) != SQLITE_INTEGER) {
        throw std::runtime_error(
            "Outbox counts are not integers"
        );
    }

    const OutboxCounts counts{
        static_cast<std::int64_t>(
            sqlite3_column_int64(query.get(), 0)
        ),
        static_cast<std::int64_t>(
            sqlite3_column_int64(query.get(), 1)
        )
    };

    if (query.step() != SqliteStepResult::Done) {
        throw std::runtime_error(
            "Outbox count query returned multiple rows"
        );
    }

    if (counts.pending_records < 0 ||
        counts.total_records < counts.pending_records) {
        throw std::runtime_error(
            "Outbox counts are inconsistent"
        );
    }

    return counts;
}

std::int64_t readAckedCount(SqliteDatabase& database){
    SqliteStatement query(
        database,
        "SELECT COUNT(*) FROM outbox WHERE state = 'ACKED';"
    );

    if (query.step() != SqliteStepResult::Row) {
        throw std::runtime_error(
            "ACKED count query returned no row"
        );
    }

    if (sqlite3_column_type(query.get(), 0) != SQLITE_INTEGER) {
        throw std::runtime_error(
            "ACKED count is not an integer"
        );
    }

    const auto count = static_cast<std::int64_t>(
        sqlite3_column_int64(query.get(), 0)
    );

    if (query.step() != SqliteStepResult::Done) {
        throw std::runtime_error(
            "ACKED count query returned multiple rows"
        );
    }

    if (count < 0) {
        throw std::runtime_error(
            "ACKED count must not be negative"
        );
    }

    return count;
}

std::string readTextColumn(sqlite3_stmt* statement, int column)
{
    if (sqlite3_column_type(statement, column) != SQLITE_TEXT) {
        throw std::runtime_error(
            "Stored Outbox column is not text"
        );
    }

    const unsigned char* text =
        sqlite3_column_text(statement, column);

    if (text == nullptr) {
        throw SqliteError(
            SQLITE_NOMEM,
            "Cannot read stored Outbox text"
        );
    }

    const int length = sqlite3_column_bytes(statement, column);

    return std::string(
        reinterpret_cast<const char*>(text),
        static_cast<std::size_t>(length)
    );
}

std::int64_t readIntegerColumn(
    sqlite3_stmt* statement,
    int column
)
{
    if (sqlite3_column_type(statement, column) != SQLITE_INTEGER) {
        throw std::runtime_error(
            "Stored Outbox column is not an integer"
        );
    }

    return static_cast<std::int64_t>(
        sqlite3_column_int64(statement, column)
    );
}

SensorEvent decodeStoredEvent(
    const std::string& payload,
    const std::string& event_id
)
{
    auto parsed = parseSensorEventJson(payload);

    if (!std::holds_alternative<SensorEvent>(parsed)) {
        throw std::runtime_error(
            "Stored Outbox payload is invalid"
        );
    }

    SensorEvent stored =
        std::get<SensorEvent>(std::move(parsed));

    if (!std::isfinite(stored.value)) {
        throw std::runtime_error(
            "Stored Outbox value is not finite"
        );
    }

    if (makeEventId(stored) != event_id) {
        throw std::runtime_error(
            "Stored Outbox event identity mismatch"
        );
    }

    return stored;
}

std::optional<SensorEvent> findStoredEvent(
    SqliteDatabase& database,
    const std::string& event_id
)
{
    SqliteStatement query(
        database,
        "SELECT payload_json FROM outbox WHERE event_id = ?1;"
    );

    query.bindText(1, event_id);

    if (query.step() == SqliteStepResult::Done) {
        return std::nullopt;
    }

    const std::string payload = readTextColumn(query.get(), 0);

    if (query.step() != SqliteStepResult::Done) {
        throw std::runtime_error(
            "Multiple Outbox records have the same event identity"
        );
    }

    return std::optional<SensorEvent>{
        decodeStoredEvent(payload, event_id)
    };
}

} // namespace

OutboxStore::OutboxStore(SqliteDatabase& database)
    : OutboxStore(database, StorageConfig{})
{
}

OutboxStore::OutboxStore(
    SqliteDatabase& database,
    const StorageConfig& storage,
    std::shared_ptr<OutboxAccessGate> access_gate
)
    : database_(database),
      access_gate_(std::move(access_gate)),
      max_pending_records_(storage.max_pending_records),
      max_total_records_(storage.max_total_records),
      acked_keep_days_(storage.acked_keep_days),
      acked_keep_max_records_(storage.acked_keep_max_records)
{
    if (!database_.isOpen()) {
        throw std::logic_error(
            "Cannot create OutboxStore with a closed database"
        );
    }

    if (max_pending_records_ <= 0
        || max_total_records_ <= 0
        || max_pending_records_ > max_total_records_) {
        throw std::invalid_argument(
            "Invalid Outbox capacity limits"
        );
    }

    if (acked_keep_days_ <= 0
        || acked_keep_max_records_ <= 0
        || acked_keep_max_records_ > max_total_records_) {
        throw std::invalid_argument(
            "Invalid Outbox ACKED retention limits"
        );
    }
}

std::unique_lock<OutboxAccessGate> OutboxStore::lockAccess()
{
    if (access_gate_) {
        return std::unique_lock<OutboxAccessGate>(
            *access_gate_
        );
    }

    return std::unique_lock<OutboxAccessGate>{};
}

OutboxEnqueueResult OutboxStore::enqueue(
    const SensorEvent& event,
    std::int64_t now_ms
)
{
    if (now_ms <= 0) {
        throw std::invalid_argument(
            "Outbox local timestamp must be positive"
        );
    }

    // 先序列化，再开启事务，缩短持有写事务的时间。
    auto serialized = serializeSensorEventJson(event);

    if (auto* error = std::get_if<EventJsonError>(&serialized)) {
        return std::move(*error);
    }

    const std::string& payload =
        std::get<std::string>(serialized);

    std::string event_id = makeEventId(event);

    // BEGIN IMMEDIATE 将身份检查和后续插入放在同一写事务内。
    // 必须先取得门闩，再开启 SQLite 事务。
    // 声明顺序保证异常时先回滚事务，再释放门闩。
    auto access = lockAccess();
    SqliteTransaction transaction(database_);

    const auto stored = findStoredEvent(database_, event_id);

    OutboxInsertStatus status;

    if (stored.has_value()) {
        status = sameEventContent(*stored, event)
            ? OutboxInsertStatus::Duplicate
            : OutboxInsertStatus::Conflict;

        // 这两个分支都不修改原记录。
        // 特别是不能把 ACKED、RETRY 等状态重新改成 PENDING。
    } else {
        // 查询语句已在 findStoredEvent 内释放。
        // 插入语句也在此局部作用域内完成并释放。
        {
            const auto counts = readOutboxCounts(database_);

            const bool pending_full =
                counts.pending_records >= max_pending_records_;

            const bool total_full =
                counts.total_records >= max_total_records_;

            if (pending_full || total_full) {
                // 如果配置被调低，现有记录可能已经超过新上限，
                // 因此使用 >=，不能只判断相等。
                const auto code = pending_full
                    ? OutboxCapacityCode::PendingLimitReached
                    : OutboxCapacityCode::TotalLimitReached;

                const auto limit = pending_full
                    ? max_pending_records_
                    : max_total_records_;

                OutboxEnqueueResult result = OutboxCapacityError{
                    code,
                    std::move(event_id),
                    counts.pending_records,
                    counts.total_records,
                    limit
                };

                // 此分支没有修改记录，正常结束当前事务。
                transaction.commit();

                return result;
            }

            SqliteStatement insert(
                database_,
                "INSERT INTO outbox("
                "event_id, payload_json, created_at_ms, updated_at_ms"
                ") VALUES (?1, ?2, ?3, ?4);"
            );

            insert.bindText(1, event_id);
            insert.bindText(2, payload);
            insert.bindInt64(3, now_ms);
            insert.bindInt64(4, now_ms);

            if (insert.step() != SqliteStepResult::Done) {
                throw std::runtime_error(
                    "Outbox insert unexpectedly returned rows"
                );
            }
        }

        // 其他字段使用上一小节确定的数据库默认值。
        status = OutboxInsertStatus::Inserted;
    }

    // 提前构造返回对象，避免在提交后才复制 event_id。
    OutboxEnqueueResult result = OutboxInsertResult{
        status,
        std::move(event_id)
    };

    // 提交失败会抛异常，不能返回 Inserted。
    transaction.commit();

    return result;
}

OutboxCleanupResult OutboxStore::cleanupAcked(
    std::int64_t now_ms
){
    if (now_ms <= 0) {
        throw std::invalid_argument(
            "Outbox cleanup timestamp must be positive"
        );
    }

    // 必须先转为 int64_t，再乘以每天的毫秒数。
    // acked_keep_days_ 是 int32_t，其最大正值乘此常量
    // 仍然处于 int64_t 的表示范围内。
    const std::int64_t retention_ms =
        static_cast<std::int64_t>(acked_keep_days_) *
        86400000LL;

    // 很早的时间点或很长的保留期下，将截止点限制为 0。
    // 数据库约束要求有效确认时间为正，因此此时不会按时间删除。
    const std::int64_t cutoff_ms = now_ms > retention_ms
        ? now_ms - retention_ms
        : 0;

    // 必须先取得门闩，再开启 SQLite 事务。
    // 声明顺序保证异常时先回滚事务，再释放门闩。
    auto access = lockAccess();
    SqliteTransaction transaction(database_);

    const auto before = readAckedCount(database_);

    {
        SqliteStatement remove_expired(
            database_,
            "DELETE FROM outbox "
            "WHERE state = 'ACKED' AND acked_at_ms < ?1;"
        );

        remove_expired.bindInt64(1, cutoff_ms);

        if (remove_expired.step() != SqliteStepResult::Done) {
            throw std::runtime_error(
                "Expired ACKED cleanup unexpectedly returned rows"
            );
        }
    }

    const auto after_expiration = readAckedCount(database_);

    if (after_expiration > acked_keep_max_records_) {
        SqliteStatement remove_excess(
            database_,
            "DELETE FROM outbox "
            "WHERE state = 'ACKED' AND event_id IN ("
                "SELECT event_id FROM outbox "
                "WHERE state = 'ACKED' "
                "ORDER BY acked_at_ms DESC, event_id DESC "
                "LIMIT -1 OFFSET ?1"
            ");"
        );

        // 跳过应该保留的最新 N 条，删除其余 ACKED。
        remove_excess.bindInt64(1, acked_keep_max_records_);

        if (remove_excess.step() != SqliteStepResult::Done) {
            throw std::runtime_error(
                "Excess ACKED cleanup unexpectedly returned rows"
            );
        }
    }

    const auto after_cleanup = readAckedCount(database_);

    if (after_expiration > before ||
        after_cleanup > after_expiration ||
        after_cleanup > acked_keep_max_records_) {
        throw std::runtime_error(
            "ACKED cleanup counts are inconsistent"
        );
    }

    const OutboxCleanupResult result{
        before - after_expiration,
        after_expiration - after_cleanup,
        after_cleanup
    };

    transaction.commit();

    return result;
}

OutboxStateCounts OutboxStore::stateCounts()
{
    auto access = lockAccess();

    SqliteStatement query(
        database_,
        "SELECT "
        "COUNT(CASE WHEN state = 'PENDING' THEN 1 END), "
        "COUNT(CASE WHEN state = 'SENDING' THEN 1 END), "
        "COUNT(CASE WHEN state = 'RETRY' THEN 1 END), "
        "COUNT(CASE WHEN state = 'ACKED' THEN 1 END), "
        "COUNT(CASE WHEN state = 'DEAD' THEN 1 END), "
        "COUNT(*) "
        "FROM outbox;"
    );

    if (query.step() != SqliteStepResult::Row) {
        throw std::runtime_error(
            "Outbox state count query returned no row"
        );
    }

    std::int64_t values[6]{};

    for (int column = 0; column < 6; ++column) {
        values[column] = readIntegerColumn(query.get(), column);
    }

    if (query.step() != SqliteStepResult::Done) {
        throw std::runtime_error(
            "Outbox state count query returned multiple rows"
        );
    }

    if (values[5] < 0) {
        throw std::runtime_error(
            "Outbox total count is negative"
        );
    }

    // 用逐项减法核对总数，避免直接累加时的溢出问题。
    std::int64_t remaining = values[5];

    for (int index = 0; index < 5; ++index) {
        if (values[index] < 0 || values[index] > remaining) {
            throw std::runtime_error(
                "Outbox state counts are inconsistent"
            );
        }

        remaining -= values[index];
    }

    if (remaining != 0) {
        throw std::runtime_error(
            "Outbox contains an unknown state"
        );
    }

    return OutboxStateCounts{
        values[0],
        values[1],
        values[2],
        values[3],
        values[4],
        values[5],
        values[5] - values[3]
    };
}

std::int64_t OutboxStore::recoverSending(
    std::int64_t now_ms
)
{
    if (now_ms <= 0) {
        throw std::invalid_argument(
            "Outbox recovery timestamp must be positive"
        );
    }

    // 必须先取得门闩，再开启 SQLite 事务。
    // 声明顺序保证异常时先回滚事务，再释放门闩。
    auto access = lockAccess();
    SqliteTransaction transaction(database_);

    const auto before = stateCounts().sending;

    if (before == 0) {
        transaction.commit();
        return 0;
    }

    {
        SqliteStatement recover(
            database_,
            "UPDATE outbox SET "
            "state = 'RETRY', "
            "next_attempt_at_ms = ?1, "
            "updated_at_ms = ?1, "
            "last_error = ?2 "
            "WHERE state = 'SENDING';"
        );

        recover.bindInt64(1, now_ms);

        recover.bindText(
            2,
            "Recovered unfinished send after restart"
        );

        if (recover.step() != SqliteStepResult::Done) {
            throw std::runtime_error(
                "Outbox recovery unexpectedly returned rows"
            );
        }
    }

    if (stateCounts().sending != 0) {
        throw std::runtime_error(
            "Outbox recovery left unfinished SENDING records"
        );
    }

    transaction.commit();

    return before;
}

std::vector<OutboxReadyRecord> OutboxStore::listReady(
    std::int64_t now_ms,
    std::int32_t limit
)
{
    if (now_ms <= 0 || limit <= 0) {
        throw std::invalid_argument(
            "Outbox ready query requires a positive time and limit"
        );
    }
    
    auto access = lockAccess();

    SqliteStatement query(
        database_,
        "SELECT event_id, payload_json, attempt_count "
        "FROM outbox "
        "WHERE state IN ('PENDING', 'RETRY') "
        "AND next_attempt_at_ms <= ?1 "
        "ORDER BY created_at_ms ASC, event_id ASC "
        "LIMIT ?2;"
    );

    query.bindInt64(1, now_ms);
    query.bindInt64(2, limit);

    std::vector<OutboxReadyRecord> records;

    while (query.step() == SqliteStepResult::Row) {
        std::string event_id = readTextColumn(query.get(), 0);
        std::string payload = readTextColumn(query.get(), 1);

        const auto attempt_count =
            readIntegerColumn(query.get(), 2);

        if (attempt_count < 0) {
            throw std::runtime_error(
                "Stored Outbox attempt count is negative"
            );
        }

        // 校验载荷结构、有限值以及身份一致性。
        // 任意记录损坏时抛出错误，不返回一个不完整批次。
        (void)decodeStoredEvent(payload, event_id);

        records.push_back(OutboxReadyRecord{
            std::move(event_id),
            std::move(payload),
            attempt_count
        });
    }

    return records;
}

std::vector<OutboxClaimedRecord> OutboxStore::claimReady(
    std::int64_t now_ms,
    std::int32_t limit
)
{
    if (now_ms <= 0 || limit <= 0 || limit > 100) {
        throw std::invalid_argument(
            "Outbox claim requires a positive time and limit 1..100"
        );
    }

    // 复用 M3 的 BEGIN IMMEDIATE 事务封装。
    // 候选查询和状态更新必须在同一个事务内。
    // 必须先取得门闩，再开启 SQLite 事务。
    // 声明顺序保证异常时先回滚事务，再释放门闩。
    auto access = lockAccess();
    SqliteTransaction transaction(database_);

    // listReady 不另开事务，并且返回前已经释放查询语句。
    // 它会检查字段类型、载荷、有限数值以及事件身份。
    auto ready = listReady(now_ms, limit);

    std::vector<OutboxClaimedRecord> records;
    records.reserve(ready.size());

    // 在任何 UPDATE 之前检查计数，并构造完整返回批次。
    for (auto& record : ready) {
        if (record.attempt_count
            == std::numeric_limits<std::int64_t>::max()) {
            throw std::overflow_error(
                "Outbox attempt count cannot be incremented"
            );
        }

        records.push_back(OutboxClaimedRecord{
            std::move(record.event_id),
            std::move(record.payload_json),
            record.attempt_count + 1
        });
    }

    if (!records.empty()) {
        // 局部作用域确保更新语句在 commit 前释放。
        SqliteStatement update(
            database_,
            "UPDATE outbox SET "
            "state = 'SENDING', "
            "attempt_count = ?1, "
            "updated_at_ms = ?2 "
            "WHERE event_id = ?3 "
            "AND state IN ('PENDING', 'RETRY') "
            "AND next_attempt_at_ms <= ?2 "
            "AND attempt_count = ?4;"
        );

        for (std::size_t index = 0;
             index < records.size();
             ++index) {
            if (index != 0) {
                update.reset();
            }

            const auto& record = records[index];

            update.bindInt64(1, record.attempt_count);
            update.bindInt64(2, now_ms);
            update.bindText(3, record.event_id);
            update.bindInt64(4, record.attempt_count - 1);

            if (update.step() != SqliteStepResult::Done) {
                throw std::runtime_error(
                    "Outbox claim update unexpectedly returned rows"
                );
            }

            // 每个主键对应的条件更新必须恰好修改一条记录。
            if (sqlite3_changes(database_.get()) != 1) {
                throw std::runtime_error(
                    "Outbox claim did not update exactly one record"
                );
            }
        }
    }

    // 提交失败时抛异常，不能把批次交给上传模块。
    transaction.commit();

    return records;
}

OutboxAckResult OutboxStore::acknowledge(
    const std::vector<OutboxClaimToken>& claims,
    std::int64_t now_ms
)
{
    if (now_ms <= 0 || claims.size() > 100) {
        throw std::invalid_argument(
            "Outbox acknowledgement requires a positive time "
            "and at most 100 claims"
        );
    }

    // 参数先整体检查，避免错误参数进入数据库事务。
    std::unordered_set<std::string> identities;
    identities.reserve(claims.size());

    for (const auto& claim : claims) {
        if (claim.event_id.empty()
            || claim.attempt_count <= 0
            || !identities.insert(claim.event_id).second) {
            throw std::invalid_argument(
                "Outbox acknowledgement requires non-empty unique IDs "
                "and positive attempt counts"
            );
        }
    }

    if (claims.empty()) {
        return OutboxAckSuccess{0};
    }

    // 必须先取得门闩，再开启 SQLite 事务。
    // 声明顺序保证异常时先回滚事务，再释放门闩。
    auto access = lockAccess();
    SqliteTransaction transaction(database_);

    // 先在同一个写事务中核对全部令牌。
    // 旧响应、错误状态或记录缺失都在写入前识别。
    {
        SqliteStatement query(
            database_,
            "SELECT 1 FROM outbox "
            "WHERE event_id = ?1 "
            "AND state = 'SENDING' "
            "AND attempt_count = ?2;"
        );

        for (std::size_t index = 0;
             index < claims.size();
             ++index) {
            if (index != 0) {
                query.reset();
            }

            const auto& claim = claims[index];

            query.bindText(1, claim.event_id);
            query.bindInt64(2, claim.attempt_count);

            if (query.step() == SqliteStepResult::Done) {
                // 在结束事务前构造返回对象。
                OutboxAckResult result = OutboxClaimMismatch{
                    claim.event_id,
                    claim.attempt_count
                };

                // 回滚失败仍应报告异常，不能伪装成普通不匹配。
                transaction.rollback();

                return result;
            }

            if (query.step() != SqliteStepResult::Done) {
                throw std::runtime_error(
                    "Outbox acknowledgement matched multiple records"
                );
            }
        }
    }

    {
        SqliteStatement update(
            database_,
            "UPDATE outbox SET "
            "state = 'ACKED', "
            "acked_at_ms = ?1, "
            "updated_at_ms = ?1, "
            "last_error = '' "
            "WHERE event_id = ?2 "
            "AND state = 'SENDING' "
            "AND attempt_count = ?3;"
        );

        for (std::size_t index = 0;
             index < claims.size();
             ++index) {
            if (index != 0) {
                update.reset();
            }

            const auto& claim = claims[index];

            update.bindInt64(1, now_ms);
            update.bindText(2, claim.event_id);
            update.bindInt64(3, claim.attempt_count);

            if (update.step() != SqliteStepResult::Done) {
                throw std::runtime_error(
                    "Outbox acknowledgement unexpectedly returned rows"
                );
            }

            // 已在同一写事务中确认匹配，更新仍未命中属于异常。
            if (sqlite3_changes(database_.get()) != 1) {
                throw std::runtime_error(
                    "Outbox acknowledgement did not update exactly one record"
                );
            }
        }
    }

    OutboxAckResult result = OutboxAckSuccess{
        static_cast<std::int64_t>(claims.size())
    };

    transaction.commit();

    return result;
}

OutboxRetryResult OutboxStore::scheduleRetry(
    const std::vector<OutboxClaimToken>& claims,
    std::int64_t now_ms,
    std::int64_t next_attempt_at_ms,
    const std::string& last_error
)
{
    if (now_ms <= 0
        || next_attempt_at_ms < now_ms
        || claims.size() > 100) {
        throw std::invalid_argument(
            "Outbox retry requires a positive time, "
            "a non-past retry time and at most 100 claims"
        );
    }

    if (last_error.empty()
        || last_error.find('\0') != std::string::npos) {
        throw std::invalid_argument(
            "Outbox retry error must be non-empty "
            "and contain no null bytes"
        );
    }

    // 复用工程已有 JSON 库检查 UTF-8，
    // 不另外实现一套文本编码检查算法。
    try {
        (void)nlohmann::json(last_error).dump();
    }
    catch (const nlohmann::json::type_error&) {
        throw std::invalid_argument(
            "Outbox retry error must contain valid UTF-8"
        );
    }

    std::unordered_set<std::string> identities;
    identities.reserve(claims.size());

    for (const auto& claim : claims) {
        if (claim.event_id.empty()
            || claim.attempt_count <= 0
            || !identities.insert(claim.event_id).second) {
            throw std::invalid_argument(
                "Outbox retry requires non-empty unique IDs "
                "and positive attempt counts"
            );
        }
    }

    if (claims.empty()) {
        return OutboxRetrySuccess{0};
    }

    // 必须先取得门闩，再开启 SQLite 事务。
    // 声明顺序保证异常时先回滚事务，再释放门闩。
    auto access = lockAccess();
    SqliteTransaction transaction(database_);

    // 核对整批领取身份，防止旧失败结果覆盖后一次领取。
    {
        SqliteStatement query(
            database_,
            "SELECT 1 FROM outbox "
            "WHERE event_id = ?1 "
            "AND state = 'SENDING' "
            "AND attempt_count = ?2;"
        );

        for (std::size_t index = 0;
             index < claims.size();
             ++index) {
            if (index != 0) {
                query.reset();
            }

            const auto& claim = claims[index];

            query.bindText(1, claim.event_id);
            query.bindInt64(2, claim.attempt_count);

            if (query.step() == SqliteStepResult::Done) {
                OutboxRetryResult result = OutboxClaimMismatch{
                    claim.event_id,
                    claim.attempt_count
                };

                transaction.rollback();

                return result;
            }

            if (query.step() != SqliteStepResult::Done) {
                throw std::runtime_error(
                    "Outbox retry matched multiple records"
                );
            }
        }
    }

    {
        SqliteStatement update(
            database_,
            "UPDATE outbox SET "
            "state = 'RETRY', "
            "next_attempt_at_ms = ?1, "
            "updated_at_ms = ?2, "
            "last_error = ?3, "
            "acked_at_ms = NULL "
            "WHERE event_id = ?4 "
            "AND state = 'SENDING' "
            "AND attempt_count = ?5;"
        );

        for (std::size_t index = 0;
             index < claims.size();
             ++index) {
            if (index != 0) {
                update.reset();
            }

            const auto& claim = claims[index];

            update.bindInt64(1, next_attempt_at_ms);
            update.bindInt64(2, now_ms);
            update.bindText(3, last_error);
            update.bindText(4, claim.event_id);
            update.bindInt64(5, claim.attempt_count);

            if (update.step() != SqliteStepResult::Done) {
                throw std::runtime_error(
                    "Outbox retry update unexpectedly returned rows"
                );
            }

            if (sqlite3_changes(database_.get()) != 1) {
                throw std::runtime_error(
                    "Outbox retry did not update exactly one record"
                );
            }
        }
    }

    OutboxRetryResult result = OutboxRetrySuccess{
        static_cast<std::int64_t>(claims.size())
    };

    transaction.commit();

    return result;
}

OutboxDeadResult OutboxStore::markDead(
    const std::vector<OutboxClaimToken>& claims,
    std::int64_t now_ms,
    const std::string& reason
)
{
    if (now_ms <= 0 || claims.size() > 100) {
        throw std::invalid_argument(
            "Outbox terminal update requires a positive time "
            "and at most 100 claims"
        );
    }

    if (reason.empty()
        || reason.find('\0') != std::string::npos) {
        throw std::invalid_argument(
            "Outbox terminal reason must be non-empty "
            "and contain no null bytes"
        );
    }

    try {
        (void)nlohmann::json(reason).dump();
    }
    catch (const nlohmann::json::type_error&) {
        throw std::invalid_argument(
            "Outbox terminal reason must contain valid UTF-8"
        );
    }

    std::unordered_set<std::string> identities;
    identities.reserve(claims.size());

    for (const auto& claim : claims) {
        if (claim.event_id.empty()
            || claim.attempt_count <= 0
            || !identities.insert(claim.event_id).second) {
            throw std::invalid_argument(
                "Outbox terminal update requires non-empty unique IDs "
                "and positive attempt counts"
            );
        }
    }

    if (claims.empty()) {
        return OutboxDeadSuccess{0};
    }

    // 必须先取得门闩，再开启 SQLite 事务。
    // 声明顺序保证异常时先回滚事务，再释放门闩。
    auto access = lockAccess();
    SqliteTransaction transaction(database_);

    // 先核对整批领取，拒绝旧响应和错误状态。
    {
        SqliteStatement query(
            database_,
            "SELECT 1 FROM outbox "
            "WHERE event_id = ?1 "
            "AND state = 'SENDING' "
            "AND attempt_count = ?2;"
        );

        for (std::size_t index = 0;
             index < claims.size();
             ++index) {
            if (index != 0) {
                query.reset();
            }

            const auto& claim = claims[index];

            query.bindText(1, claim.event_id);
            query.bindInt64(2, claim.attempt_count);

            if (query.step() == SqliteStepResult::Done) {
                OutboxDeadResult result = OutboxClaimMismatch{
                    claim.event_id,
                    claim.attempt_count
                };

                transaction.rollback();

                return result;
            }

            if (query.step() != SqliteStepResult::Done) {
                throw std::runtime_error(
                    "Outbox terminal update matched multiple records"
                );
            }
        }
    }

    {
        SqliteStatement update(
            database_,
            "UPDATE outbox SET "
            "state = 'DEAD', "
            "updated_at_ms = ?1, "
            "last_error = ?2, "
            "acked_at_ms = NULL "
            "WHERE event_id = ?3 "
            "AND state = 'SENDING' "
            "AND attempt_count = ?4;"
        );

        for (std::size_t index = 0;
             index < claims.size();
             ++index) {
            if (index != 0) {
                update.reset();
            }

            const auto& claim = claims[index];

            update.bindInt64(1, now_ms);
            update.bindText(2, reason);
            update.bindText(3, claim.event_id);
            update.bindInt64(4, claim.attempt_count);

            if (update.step() != SqliteStepResult::Done) {
                throw std::runtime_error(
                    "Outbox terminal update unexpectedly returned rows"
                );
            }

            if (sqlite3_changes(database_.get()) != 1) {
                throw std::runtime_error(
                    "Outbox terminal update did not update exactly one record"
                );
            }
        }
    }

    OutboxDeadResult result = OutboxDeadSuccess{
        static_cast<std::int64_t>(claims.size())
    };

    transaction.commit();

    return result;
}

std::optional<std::int64_t> OutboxStore::nextReadyAfter(
    std::int64_t now_ms
)
{
    if (now_ms <= 0) {
        throw std::invalid_argument(
            "Outbox future ready query requires a positive time"
        );
    }
    
    auto access = lockAccess();
    
    SqliteStatement query(
        database_,
        "SELECT MIN(next_attempt_at_ms) "
        "FROM outbox "
        "WHERE state IN ('PENDING', 'RETRY') "
        "AND next_attempt_at_ms > ?1;"
    );

    query.bindInt64(1, now_ms);

    if (query.step() != SqliteStepResult::Row) {
        throw std::runtime_error(
            "Outbox future ready query returned no row"
        );
    }

    std::optional<std::int64_t> result;

    // MIN 在没有候选记录时返回一行 NULL。
    if (sqlite3_column_type(query.get(), 0) != SQLITE_NULL) {
        const auto value = readIntegerColumn(
            query.get(),
            0
        );

        if (value <= now_ms) {
            throw std::runtime_error(
                "Outbox future ready time is inconsistent"
            );
        }

        result = value;
    }

    if (query.step() != SqliteStepResult::Done) {
        throw std::runtime_error(
            "Outbox future ready query returned multiple rows"
        );
    }

    return result;
}

} // namespace edge