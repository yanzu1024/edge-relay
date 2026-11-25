#pragma once

#include "edge/config/AgentConfig.h"
#include "edge/storage/OutboxAccessGate.h"
#include "edge/upload/UploadWorker.h"

#include <functional>
#include <memory>

namespace edge {

class OutboxStore;
class OutboxUploader;

using OutboxWorkerBody = std::function<void(
    UploadWorker::Context&,
    OutboxStore&,
    OutboxUploader&
)>;

// storage.path 必须是已规范化的绝对路径。
//
// owner 应先完成数据库初始化、独占启动恢复和启动清理，
// 再启动任务；启动握手期间主连接不执行数据库操作。
//
// database/store/uploader 在工作线程内部创建和销毁。
// access_gate 只共享同步状态，不共享数据库连接。
//
// body 不得把借用引用交给其他线程。
UploadWorker::Task makeOutboxWorkerTask(
    const StorageConfig& storage,
    const UploadConfig& upload,
    OutboxWorkerBody body,
    std::shared_ptr<OutboxAccessGate> access_gate = {}
);

} // namespace edge