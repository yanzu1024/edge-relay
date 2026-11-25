#include "edge/upload/OutboxWorkerTask.h"

#include "edge/storage/OutboxDatabase.h"
#include "edge/storage/OutboxStore.h"
#include "edge/upload/OutboxUploader.h"

#include <stdexcept>
#include <utility>

namespace edge {

UploadWorker::Task makeOutboxWorkerTask(
    const StorageConfig& storage,
    const UploadConfig& upload,
    OutboxWorkerBody body,
    std::shared_ptr<OutboxAccessGate> access_gate
)
{
    if (!body) {
        throw std::invalid_argument(
            "Outbox worker requires a body"
        );
    }

    return [
        storage,
        upload,
        body = std::move(body),
        access_gate = std::move(access_gate)
    ](
        UploadWorker::Context& context
    ) {
        // 连接仍然只属于当前工作线程。
        auto database = openOutboxDatabase(storage.path);

        OutboxStore store(
            database,
            storage,
            access_gate
        );

        OutboxUploader uploader(store, upload);

        context.signalReady();

        // 不在 body 或 uploader.runOnce 外面持有门闩。
        // 各个 Store 方法自行管理短 SQL 操作的访问范围。
        body(context, store, uploader);

        // uploader -> store -> database，按相反顺序销毁。
    };
}

} // namespace edge