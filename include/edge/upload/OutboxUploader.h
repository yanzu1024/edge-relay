#pragma once

#include <cstdint>
#include <functional>
#include <variant>
#include <vector>

#include "edge/config/AgentConfig.h"
#include "edge/upload/BatchUploader.h"

namespace edge {

class OutboxStore;

using OutboxBatchSender = std::function<
    BatchUploadResult(
        const UploadConfig&,
        const std::vector<SensorEvent>&
    )
>;

using OutboxUploadClock = std::function<std::int64_t()>;

struct UploadCycleResult {
    std::int64_t claimed_records = 0;
    std::int64_t acked_records = 0;
    std::int64_t retry_records = 0;
    std::int64_t dead_records = 0;

    // monostate 表示没有请求或协议错误。
    // 合法业务响应中的 conflict 由 dead_records 表示。
    std::variant<
        std::monostate,
        HttpRequestError,
        BatchHttpError,
        BatchResponseError
    > failure;
};

class OutboxUploader {
public:
    // 使用真实 uploadBatch 和系统时间。
    OutboxUploader(
        OutboxStore& store,
        const UploadConfig& config
    );

    // 测试入口：可以替换发送函数和时钟。
    OutboxUploader(
        OutboxStore& store,
        const UploadConfig& config,
        OutboxBatchSender sender,
        OutboxUploadClock clock
    );

    OutboxUploader(const OutboxUploader&) = delete;
    OutboxUploader& operator=(const OutboxUploader&) = delete;

    OutboxUploader(OutboxUploader&&) = delete;
    OutboxUploader& operator=(OutboxUploader&&) = delete;

    // 立即尝试领取并处理一个到期批次。
    //
    // 不等待凑批，不自动循环重试，不创建线程。
    // store 及其连接必须比本对象存活更久。
    // 不得与其他线程并发使用同一存储连接。
    //
    // 正常返回时：
    // claimed = acked + retry + dead。
    //
    // 本地错误、存储错误和领取不匹配继续抛出异常。
    UploadCycleResult runOnce();

private:
    OutboxStore& store_;
    UploadConfig config_;
    OutboxBatchSender sender_;
    OutboxUploadClock clock_;
};

} // namespace edge