#pragma once

#include "edge/domain/EventJson.h"
#include "edge/domain/SensorEvent.h"
#include "edge/storage/OutboxAccessGate.h"

#include <memory>
#include <mutex>

#include <cstdint>
#include <string>
#include <variant>
#include <vector>
#include <optional>

namespace edge {

class SqliteDatabase;
struct StorageConfig;

enum class OutboxInsertStatus {
    Inserted,
    Duplicate,
    Conflict
};

struct OutboxInsertResult {
    OutboxInsertStatus status;
    std::string event_id;
};

enum class OutboxCapacityCode {
    PendingLimitReached,
    TotalLimitReached
};

struct OutboxCapacityError {
    OutboxCapacityCode code;
    std::string event_id;

    // 当前事务中检查到的实际记录数
    std::int64_t pending_records;
    std::int64_t total_records;

    // 本次触发的那项上限
    std::int64_t limit;
};

struct OutboxCleanupResult {
    std::int64_t expired_deleted = 0;
    std::int64_t excess_deleted = 0;
    std::int64_t remaining_acked = 0;
};

struct OutboxStateCounts {
    std::int64_t pending = 0;
    std::int64_t sending = 0;
    std::int64_t retry = 0;
    std::int64_t acked = 0;
    std::int64_t dead = 0;

    std::int64_t total = 0;

    // 对应 max_pending_records 的计数范围：
    // PENDING + SENDING + RETRY + DEAD
    std::int64_t unacked = 0;
};

struct OutboxReadyRecord {
    // 返回对象拥有自己的字符串, 不借用 SQLite 查询缓冲
    std::string event_id;
    std::string payload_json;

    std::int64_t attempt_count = 0;
};

struct OutboxClaimedRecord {
    // 返回对象拥有自己的内容, 不借用 SQLite 查询缓冲
    std::string event_id;
    std::string payload_json;

    // 本次领取提交后的尝试次数, 至少为 1
    std::int64_t attempt_count = 0;
};

struct OutboxClaimToken {
    std::string event_id;

    // 必须使用领取返回的次数, 不能自行增加或重新查询替换
    std::int64_t attempt_count = 0;
};

struct OutboxAckSuccess {
    std::int64_t acked_records = 0;
};

struct OutboxClaimMismatch {
    std::string event_id;

    // 调用方提交的预期领取次数
    std::int64_t attempt_count = 0;
};

using OutboxAckResult = std::variant<
    OutboxAckSuccess,
    OutboxClaimMismatch
>;

struct OutboxRetrySuccess {
    std::int64_t retry_records = 0;
};

using OutboxRetryResult = std::variant<
    OutboxRetrySuccess,
    OutboxClaimMismatch
>;

struct OutboxDeadSuccess {
    std::int64_t dead_records = 0;
};

using OutboxDeadResult = std::variant<
    OutboxDeadSuccess,
    OutboxClaimMismatch
>;

using OutboxEnqueueResult = std::variant<
    OutboxInsertResult,
    EventJsonError,
    OutboxCapacityError
>;

class OutboxStore {
public:
    // 使用 StorageConfig 的默认容量, 保留上一小节的调用方式
    explicit OutboxStore(SqliteDatabase& database);

    // 同一次运行的两个 store 传入同一个 gate。
    // gate 只协调 SQL 操作，不允许跨 HTTP 或报告交接持有。
    // 默认空值保留现有独立存储组件的调用方式。
    OutboxStore(
        SqliteDatabase& database,
        const StorageConfig& storage,
        std::shared_ptr<OutboxAccessGate> access_gate = {}
    );

    OutboxStore(const OutboxStore&) = delete;
    OutboxStore& operator=(const OutboxStore&) = delete;

    OutboxStore(OutboxStore&&) = delete;
    OutboxStore& operator=(OutboxStore&&) = delete;

    // event 必须已通过上游业务验证
    // 调用前完成 initializeOutboxSchema
    // database 在使用期间必须保持打开且不能移动
    // 本方法自行开启事务, 不能在外部事务中调用
    OutboxEnqueueResult enqueue(
        const SensorEvent& event,
        std::int64_t now_ms
    );

    // 先清理过期 ACKED, 再执行数量保留
    // 自行开启事务, 不能在外部事务中调用
    // 只有提交成功后才返回删除数量
    OutboxCleanupResult cleanupAcked(std::int64_t now_ms);

    OutboxStateCounts stateCounts();

    // 只能在取得数据库实例锁后、启动上传工作之前调用
    // 保留原身份和尝试次数, 让遗留 SENDING 立即具备重试资格
    std::int64_t recoverSending(std::int64_t now_ms);

    // 只查询 PENDING / RETRY 中已到发送时间的记录
    // 按创建时间、事件 ID 升序返回
    // 不修改状态, 不能代替上传阶段的领取操作
    std::vector<OutboxReadyRecord> listReady(
        std::int64_t now_ms,
        std::int32_t limit
    );

    // 只查询 PENDING / RETRY 中严格晚于 now_ms 的最早到期时间
    //
    // 已经到期的记录由 listReady 处理, 不参与本查询
    // 不修改状态、次数或时间, 不自行开启事务
    std::optional<std::int64_t> nextReadyAfter(
        std::int64_t now_ms
    );

    // 原子领取到期的 PENDING / RETRY
    // 按创建时间、事件 ID 升序, limit 必须为 1..100
    //
    // 同一事务内标记 SENDING、增加尝试次数并更新时间
    // 提交成功后才返回完整批次
    //
    // 自行开启事务, 不能在外部事务中调用
    // 同一数据库连接不能被其他线程并发使用
    std::vector<OutboxClaimedRecord> claimReady(
        std::int64_t now_ms,
        std::int32_t limit
    );

    // 确认完整核验响应中 accepted / duplicate 对应的领取令牌
    //
    // 最多 100 个令牌, ID 非空且不得重复, 次数必须为正
    // 空列表返回确认数量 0；now_ms 仍必须为正
    //
    // 全部令牌匹配当前 SENDING 和次数时才提交确认
    // 任意不匹配返回 OutboxClaimMismatch, 整批不修改
    //
    // SQL 或提交错误抛出异常
    // 自行开启事务, 不能在外部事务中调用
    OutboxAckResult acknowledge(
        const std::vector<OutboxClaimToken>& claims,
        std::int64_t now_ms
    );

    // 将本次仍有效的领取记录退回 RETRY
    //
    // 最多 100 个令牌, ID 非空且不得重复, 次数必须为正
    // now_ms 必须为正, next_attempt_at_ms 不得早于 now_ms
    // last_error 必须是非空、无内嵌零字节的有效 UTF-8
    //
    // 保留身份、载荷、创建时间和尝试次数
    // 全部令牌匹配才提交；任意不匹配则整批不修改
    //
    // 空列表返回重试数量 0, 但仍检查参数
    // 自行开启事务, 不能在外部事务中调用
    OutboxRetryResult scheduleRetry(
        const std::vector<OutboxClaimToken>& claims,
        std::int64_t now_ms,
        std::int64_t next_attempt_at_ms,
        const std::string& last_error
    );

    // 终止本次仍有效的领取记录
    //
    // 当前由调用方用于已完整核验的业务 conflict
    // 本方法不根据 HTTP 状态或失败次数自行判断终止
    //
    // 最多 100 个令牌, ID 非空且不得重复, 次数必须为正
    // now_ms 必须为正
    // reason 必须是非空、无内嵌零字节的有效 UTF-8
    //
    // 保留身份、载荷、创建时间、次数和原重试时间
    // 全部令牌匹配才提交；任意不匹配则整批不修改
    //
    // 空列表返回终止数量 0, 但仍检查参数
    // 自行开启事务, 不能在外部事务中调用
    OutboxDeadResult markDead(
        const std::vector<OutboxClaimToken>& claims,
        std::int64_t now_ms,
        const std::string& reason
    );

private:
    std::unique_lock<OutboxAccessGate> lockAccess();

    SqliteDatabase& database_;
    std::shared_ptr<OutboxAccessGate> access_gate_;

    std::int64_t max_pending_records_;
    std::int64_t max_total_records_;
    
    std::int32_t acked_keep_days_;
    std::int64_t acked_keep_max_records_;
};

} // namespace edge