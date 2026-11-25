#include "edge/upload/OutboxUploader.h"

#include <chrono>
#include <cmath>
#include <map>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "edge/config/ConfigValidator.h"
#include "edge/domain/EventId.h"
#include "edge/domain/EventJson.h"
#include "edge/storage/OutboxStore.h"
#include "edge/upload/RetryBackoff.h"

namespace edge {

namespace {

std::int64_t currentTimeMs()
{
    const auto elapsed =
        std::chrono::system_clock::now().time_since_epoch();

    return static_cast<std::int64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            elapsed
        ).count()
    );
}

std::int64_t readTime(const OutboxUploadClock& clock)
{
    const auto now_ms = clock();

    if (now_ms <= 0) {
        throw std::runtime_error(
            "Outbox upload clock returned a non-positive timestamp"
        );
    }

    return now_ms;
}

std::vector<SensorEvent> decodeClaimed(
    const std::vector<OutboxClaimedRecord>& records
)
{
    std::vector<SensorEvent> events;
    events.reserve(records.size());

    for (const auto& record : records) {
        auto parsed = parseSensorEventJson(record.payload_json);

        if (!std::holds_alternative<SensorEvent>(parsed)) {
            throw std::runtime_error(
                "Claimed Outbox payload is invalid"
            );
        }

        auto event = std::get<SensorEvent>(std::move(parsed));

        if (!std::isfinite(event.value)
            || makeEventId(event) != record.event_id) {
            throw std::runtime_error(
                "Claimed Outbox payload or identity is inconsistent"
            );
        }

        events.push_back(std::move(event));
    }

    return events;
}

template<typename Success>
const Success& requireMatched(
    const std::variant<Success, OutboxClaimMismatch>& result,
    const char* operation
)
{
    if (std::holds_alternative<OutboxClaimMismatch>(result)) {
        throw std::runtime_error(
            std::string{operation}
            + " rejected a stale or inconsistent Outbox claim"
        );
    }

    return std::get<Success>(result);
}

std::optional<BatchResponseError> partitionResponse(
    const std::vector<OutboxClaimedRecord>& records,
    const BatchResponse& response,
    std::vector<OutboxClaimToken>& acknowledgements,
    std::vector<OutboxClaimToken>& conflicts
)
{
    if (response.results.size() != records.size()) {
        return BatchResponseError{
            "Upload result count does not match the claimed batch."
        };
    }

    std::unordered_map<
        std::string,
        const OutboxClaimedRecord*
    > by_id;

    by_id.reserve(records.size());

    for (const auto& record : records) {
        by_id.emplace(record.event_id, &record);
    }

    std::unordered_set<std::string> seen;
    seen.reserve(records.size());

    for (const auto& item : response.results) {
        const auto found = by_id.find(item.event_id);

        if (found == by_id.end()
            || !seen.insert(item.event_id).second) {
            return BatchResponseError{
                "Upload result contains an unknown or repeated ID."
            };
        }

        const auto& record = *found->second;

        const OutboxClaimToken token{
            record.event_id,
            record.attempt_count
        };

        switch (item.status) {
        case BatchEventStatus::Accepted:
        case BatchEventStatus::Duplicate:
            acknowledgements.push_back(token);
            break;

        case BatchEventStatus::Conflict:
            conflicts.push_back(token);
            break;

        default:
            return BatchResponseError{
                "Upload result contains an unsupported status."
            };
        }
    }

    return std::nullopt;
}

std::string requestFailureSummary(
    const HttpRequestError& error
)
{
    switch (error.code) {
    case HttpRequestErrorCode::Timeout:
        return "HTTP request timed out";

    case HttpRequestErrorCode::Transport:
        return "HTTP transport failure";

    case HttpRequestErrorCode::ResponseTooLarge:
        return "HTTP response exceeded size limit";
    }

    return "HTTP request failed";
}

std::int64_t retryClaimed(
    OutboxStore& store,
    const UploadConfig& config,
    const std::vector<OutboxClaimedRecord>& records,
    std::int64_t now_ms,
    const std::string& reason
)
{
    // 不同领取次数可能对应不同时间。
    // 先计算完整计划，再开始数据库回写。
    std::map<
        std::int64_t,
        std::vector<OutboxClaimToken>
    > groups;

    for (const auto& record : records) {
        const auto schedule = calculateRetrySchedule(
            config,
            record.attempt_count,
            now_ms
        );

        groups[schedule.next_attempt_at_ms].push_back(
            OutboxClaimToken{
                record.event_id,
                record.attempt_count
            }
        );
    }

    std::int64_t retry_records = 0;

    for (const auto& group : groups) {
        const auto applied = store.scheduleRetry(
            group.second,
            now_ms,
            group.first,
            reason
        );

        retry_records += requireMatched(
            applied,
            "Retry update"
        ).retry_records;
    }

    return retry_records;
}

} // namespace

OutboxUploader::OutboxUploader(
    OutboxStore& store,
    const UploadConfig& config
)
    : OutboxUploader(
        store,
        config,
        uploadBatch,
        currentTimeMs
    )
{
}

OutboxUploader::OutboxUploader(
    OutboxStore& store,
    const UploadConfig& config,
    OutboxBatchSender sender,
    OutboxUploadClock clock
)
    : store_(store),
      config_(config),
      sender_(std::move(sender)),
      clock_(std::move(clock))
{
    if (!sender_ || !clock_) {
        throw std::invalid_argument(
            "Outbox uploader requires a sender and clock"
        );
    }

    if (validateUploadConfig(config_)) {
        throw std::invalid_argument(
            "Invalid Outbox upload configuration"
        );
    }
}

UploadCycleResult OutboxUploader::runOnce()
{
    const auto claim_time = readTime(clock_);

    const auto records = store_.claimReady(
        claim_time,
        config_.batch_size
    );

    UploadCycleResult report;
    report.claimed_records =
        static_cast<std::int64_t>(records.size());

    if (records.empty()) {
        return report;
    }

    const auto events = decodeClaimed(records);

    // claimReady 已提交并释放事务。
    // sender 调用期间不持有数据库事务。
    auto uploaded = sender_(config_, events);

    // 已领取批次无法编码属于本地问题，不能反复当网络失败重试。
    if (const auto* error =
            std::get_if<BatchRequestError>(&uploaded)) {
        throw std::runtime_error(
            std::string{"Cannot encode claimed Outbox batch: "}
            + error->message
        );
    }

    // 重试间隔从请求完成后开始计算。
    const auto completed_time = readTime(clock_);

    if (const auto* response =
            std::get_if<BatchResponse>(&uploaded)) {
        std::vector<OutboxClaimToken> acknowledgements;
        std::vector<OutboxClaimToken> conflicts;

        const auto error = partitionResponse(
            records,
            *response,
            acknowledgements,
            conflicts
        );

        if (error.has_value()) {
            report.failure = *error;

            report.retry_records = retryClaimed(
                store_,
                config_,
                records,
                completed_time,
                "Invalid batch response"
            );

            return report;
        }

        // 完整核对全部结果后，才开始任何确认回写。
        if (!acknowledgements.empty()) {
            const auto applied = store_.acknowledge(
                acknowledgements,
                completed_time
            );

            report.acked_records = requireMatched(
                applied,
                "Acknowledgement"
            ).acked_records;
        }

        if (!conflicts.empty()) {
            const auto applied = store_.markDead(
                conflicts,
                completed_time,
                "Server reported event identity conflict"
            );

            report.dead_records = requireMatched(
                applied,
                "Conflict update"
            ).dead_records;
        }

        return report;
    }

    std::string reason;

    if (auto* error =
            std::get_if<HttpRequestError>(&uploaded)) {
        reason = requestFailureSummary(*error);
        report.failure = std::move(*error);
    }
    else if (auto* error =
                 std::get_if<BatchHttpError>(&uploaded)) {
        reason =
            std::string{"HTTP status "}
            + std::to_string(error->status_code);

        report.failure = std::move(*error);
    }
    else {
        auto& protocol_error =
            std::get<BatchResponseError>(uploaded);

        reason = "Invalid batch response";
        report.failure = std::move(protocol_error);
    }

    report.retry_records = retryClaimed(
        store_,
        config_,
        records,
        completed_time,
        reason
    );

    return report;
}

} // namespace edge