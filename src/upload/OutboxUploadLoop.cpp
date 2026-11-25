#include "edge/upload/OutboxUploadLoop.h"

#include "edge/storage/OutboxStore.h"
#include "edge/upload/UploadTrigger.h"
#include "edge/upload/UploadWaitPlan.h"
#include "edge/upload/HttpUploadPolicy.h"

#include <chrono>
#include <stdexcept>
#include <utility>

namespace edge {

namespace {

std::int64_t loopWallNowMs()
{
    return std::chrono::duration_cast<
        std::chrono::milliseconds
    >(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

} // namespace

void runOutboxUploadLoop(
    UploadWorker::Context& context,
    OutboxStore& store,
    OutboxUploader& uploader,
    const UploadConfig& config,
    OutboxCycleHandler on_cycle
)
{
    runOutboxUploadLoop(
        context,
        store,
        uploader,
        config,
        std::move(on_cycle),
        loopWallNowMs
    );
}

void runOutboxUploadLoop(
    UploadWorker::Context& context,
    OutboxStore& store,
    OutboxUploader& uploader,
    const UploadConfig& config,
    OutboxCycleHandler on_cycle,
    OutboxUploadClock clock
)
{
    if (!on_cycle || !clock) {
        throw std::invalid_argument(
            "Outbox upload loop requires a handler and clock"
        );
    }

    UploadTrigger trigger(config);

    while (!context.stopRequested()) {
        const auto now_ms = clock();

        if (now_ms <= 0) {
            throw std::runtime_error(
                "Outbox upload loop clock returned a non-positive time"
            );
        }

        const auto ready_count = store.listReady(
            now_ms,
            config.batch_size
        ).size();

        const auto next_future = store.nextReadyAfter(
            now_ms
        );

        const auto plan = makeUploadWaitPlan(
            trigger,
            ready_count,
            next_future,
            now_ms,
            UploadTrigger::Clock::now()
        );

        if (context.stopRequested()) {
            return;
        }

        if (plan.upload_now) {
            bool blocked = false;

            {
                // 正常返回意味着本轮状态回写已经完成。
                const auto report = uploader.runOnce();

                trigger.onCycleFinished();

                if (report.claimed_records > 0) {
                    if (const auto* error =
                            std::get_if<BatchHttpError>(
                                &report.failure)) {
                        blocked =
                            classifyUploadHttpStatus(
                                error->status_code
                            ) == HttpUploadAction::Block;
                    }

                    // 即使随后进入阻塞或停止，
                    // 也要交出已经完成的周期报告。
                    on_cycle(report);
                }
            }

            // 在长期等待前释放本轮报告及其响应体。
            if (blocked) {
                // 只接受停止，不因新事件通知恢复上传。
                // 不查询数据库，不领取，也不发送探测请求。
                context.waitForStop();
                return;
            }

            continue;
        }

        const auto reason = plan.deadline.has_value()
            ? context.waitUntil(*plan.deadline)
            : context.waitForWork();

        if (reason == UploadWorker::WaitResult::StopRequested) {
            return;
        }
    }
}

} // namespace edge