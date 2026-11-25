#pragma once

#include "edge/upload/OutboxUploader.h"
#include "edge/upload/UploadWorker.h"

#include <functional>

namespace edge {

class OutboxStore;

// 在上传线程中调用。
// 回调应短小；需要保留报告时，应复制或汇总其中内容。
// 不能保存 report 引用到回调结束之后。
using OutboxCycleHandler =
    std::function<void(const UploadCycleResult&)>;

// 使用真实系统时间。
//
// store、uploader 必须属于当前工作线程。
// config 必须与构造 uploader 时使用的配置一致。
// 本函数不创建线程，不执行启动恢复，不负责关闭数据库。
void runOutboxUploadLoop(
    UploadWorker::Context& context,
    OutboxStore& store,
    OutboxUploader& uploader,
    const UploadConfig& config,
    OutboxCycleHandler on_cycle
);

// 测试入口：注入系统时间来源。
// 应与 uploader 使用同一个时间来源。
void runOutboxUploadLoop(
    UploadWorker::Context& context,
    OutboxStore& store,
    OutboxUploader& uploader,
    const UploadConfig& config,
    OutboxCycleHandler on_cycle,
    OutboxUploadClock clock
);

} // namespace edge