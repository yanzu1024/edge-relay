#include "RunAgent.h"
#include "RunOutput.h"
#include "ExportDead.h"

#include "edge/app/TcpEventProcessor.h"
#include "edge/domain/EventId.h"
#include "edge/net/TcpConnections.h"
#include "edge/net/TcpListener.h"
#include "edge/net/TcpPoller.h"
#include "edge/storage/OutboxDatabase.h"
#include "edge/storage/OutboxStore.h"
#include "edge/upload/OutboxUploader.h"
#include "edge/upload/UploadTrigger.h"
#include "edge/net/CurlRuntime.h"
#include "edge/upload/OutboxWorkerTask.h"
#include "edge/upload/OutboxUploadLoop.h"
#include "edge/upload/UploadReportChannel.h"
#include "edge/upload/UploadWorker.h"
#include "edge/upload/HttpUploadPolicy.h"
#include "edge/status/StatusOutput.h"
#include "edge/storage/OutboxAccessGate.h"
#include "edge/recording/RawRecorder.h"
#include "edge/status/TcpActivityTracker.h"

#include <memory>

#include <optional>
#include <pthread.h>
#include <thread>
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <system_error>
#include <utility>
#include <variant>
#include <exception>
#include <filesystem>

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>
#include <signal.h>
#include <stdexcept>

namespace {

// 此标志只用于信号处理器与主线程
// 上传线程屏蔽 SIGINT/SIGTERM, 不读取这个标志
// 主线程观察到信号后, 通过 UploadWorker 请求停止
volatile sig_atomic_t stop_requested = 0;

void requestStop(int) noexcept
{
    // 信号处理函数只设置标志 
    // 不打印、不分配内存、不操作连接集合 
    stop_requested = 1;
}

class SignalGuard {
public:
    SignalGuard()
    {
        stop_requested = 0;

        struct sigaction action{};
        action.sa_handler = requestStop;
        action.sa_flags = 0;

        if (::sigemptyset(&action.sa_mask) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "sigemptyset"
            );
        }

        if (::sigaction(SIGINT, &action, &old_int_) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "sigaction(SIGINT)"
            );
        }

        if (::sigaction(SIGTERM, &action, &old_term_) == -1) {
            const int error_code = errno;

            // 构造没有完成时, 析构函数不会执行, 
            // 因此必须在这里恢复已经修改的 SIGINT 
            (void)::sigaction(SIGINT, &old_int_, nullptr);

            throw std::system_error(
                error_code,
                std::generic_category(),
                "sigaction(SIGTERM)"
            );
        }
    }

    ~SignalGuard() noexcept
    {
        const int saved_errno = errno;

        (void)::sigaction(SIGTERM, &old_term_, nullptr);
        (void)::sigaction(SIGINT, &old_int_, nullptr);

        errno = saved_errno;
    }

    SignalGuard(const SignalGuard&) = delete;
    SignalGuard& operator=(const SignalGuard&) = delete;

private:
    struct sigaction old_int_{};
    struct sigaction old_term_{};
};

// 此锁约束使用同一个规范化数据库路径的 Agent 进程 
// SQLite 连接本身仍由后面的 SqliteDatabase 管理 
class StorageInstanceLock {
public:
    explicit StorageInstanceLock(
        const std::filesystem::path& database_path
    )
    {
        if (database_path.empty() ||
            !database_path.is_absolute() ||
            database_path.u8string().find('\0') != std::string::npos) {
            throw std::invalid_argument(
                "Invalid database path for Outbox lock"
            );
        }

        std::filesystem::create_directories(
            database_path.parent_path()
        );

        auto lock_path = database_path;
        lock_path += ".lock";

        const std::string filename = lock_path.string();

        const int descriptor = ::open(
            filename.c_str(),
            O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW,
            0600
        );

        if (descriptor == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "open Outbox lock"
            );
        }

        if (::flock(descriptor, LOCK_EX | LOCK_NB) == -1) {
            const int error_code = errno;

            // 构造失败时, 析构不会执行, 必须立即关闭 
            (void)::close(descriptor);

            throw std::system_error(
                error_code,
                std::generic_category(),
                "Outbox lock"
            );
        }

        fd_ = descriptor;
    }

    ~StorageInstanceLock() noexcept
    {
        const int saved_errno = errno;

        if (fd_ >= 0) {
            (void)::close(fd_);
        }

        errno = saved_errno;

        // 不删除锁文件, 避免并发进程分别锁住不同 inode 
        // 关闭描述符后, 内核会释放当前锁 
    }

    StorageInstanceLock(const StorageInstanceLock&) = delete;
    StorageInstanceLock& operator=(const StorageInstanceLock&) = delete;

private:
    int fd_ = -1;
};

struct StorageCounters {
    std::uint64_t stored = 0;
    std::uint64_t duplicates = 0;
    std::uint64_t conflicts = 0;
    std::uint64_t capacity_rejections = 0;
    std::uint64_t serialization_errors = 0;

    // 包括写入错误和运行期间的清理错误 
    std::uint64_t errors = 0;

    // 已通过验证, 但因本批次前面发生存储故障而未尝试写入 
    std::uint64_t skipped = 0;

    std::uint64_t acked_deleted = 0;

    std::uint64_t recovered_sending = 0;
};

struct UploadCounters {
    // 只累计正常返回、且实际领取了记录的协调周期 
    std::uint64_t cycles = 0;

    std::uint64_t claimed = 0;
    std::uint64_t acked = 0;
    std::uint64_t retry = 0;
    std::uint64_t dead = 0;

    // 按失败周期计数, 不按事件数量计数 
    std::uint64_t request_failures = 0;
    std::uint64_t http_failures = 0;
    std::uint64_t protocol_failures = 0;

    // 上传相关本地异常或数据库异常 
    std::uint64_t errors = 0;
    std::uint64_t warnings = 0;

    // 达到告警阈值后保持在阈值, 避免重复告警 
    std::uint64_t failure_streak = 0;

    // 当前进程是否进入上传阻塞状态, 不是累计次数 
    bool blocked = false;
    long blocked_http_status = 0;
};

enum class UploadStepResult {
    Ok,
    StorageFailure,
    LocalFailure
};

// 在线程创建前临时修改主线程的信号屏蔽集合 
// 子线程继承这个集合；随后恢复主线程原来的集合 
class WorkerSignalMask {
public:
    WorkerSignalMask()
    {
        sigset_t blocked{};

        if (::sigemptyset(&blocked) == -1
            || ::sigaddset(&blocked, SIGINT) == -1
            || ::sigaddset(&blocked, SIGTERM) == -1
            || ::sigaddset(&blocked, SIGPIPE) == -1) {
            const int error_code = errno;

            throw std::system_error(
                error_code,
                std::generic_category(),
                "prepare upload signal mask"
            );
        }

        const int result = ::pthread_sigmask(
            SIG_BLOCK,
            &blocked,
            &previous_
        );

        if (result != 0) {
            throw std::system_error(
                result,
                std::generic_category(),
                "pthread_sigmask block"
            );
        }

        active_ = true;
    }

    ~WorkerSignalMask() noexcept
    {
        if (active_) {
            const int saved_errno = errno;

            (void)::pthread_sigmask(
                SIG_SETMASK,
                &previous_,
                nullptr
            );

            errno = saved_errno;
        }
    }

    WorkerSignalMask(const WorkerSignalMask&) = delete;

    WorkerSignalMask& operator=(
        const WorkerSignalMask&
    ) = delete;

    void restore()
    {
        if (!active_) {
            return;
        }

        const int result = ::pthread_sigmask(
            SIG_SETMASK,
            &previous_,
            nullptr
        );

        if (result != 0) {
            throw std::system_error(
                result,
                std::generic_category(),
                "pthread_sigmask restore"
            );
        }

        active_ = false;
    }

private:
    sigset_t previous_{};
    bool active_ = false;
};

class BackgroundUpload {
public:
    explicit BackgroundUpload(
        const edge::AgentConfig& config,
        std::shared_ptr<edge::OutboxAccessGate> access_gate
    )
        : worker_(edge::makeOutboxWorkerTask(
            config.storage,
            config.upload,
            [this, upload = config.upload](
                edge::UploadWorker::Context& context,
                edge::OutboxStore& store,
                edge::OutboxUploader& uploader
            ) {
                // 初始化就绪后, 等待主线程发布 ready/Listening 
                if (context.waitForWork()
                    == edge::UploadWorker::WaitResult::StopRequested) {
                    return;
                }

                edge::runOutboxUploadLoop(
                    context,
                    store,
                    uploader,
                    upload,
                    [this](
                        const edge::UploadCycleResult& report
                    ) {
                        if (!reports_.publish(report)) {
                            throw std::runtime_error(
                                "Upload report channel is closed"
                            );
                        }
                    }
                );
            },
            std::move(access_gate)
        ))
    {
    }

    ~BackgroundUpload() noexcept
    {
        // 异常展开时, 先解除可能阻塞的报告发布, 
        // 再请求停止并回收线程 
        //
        // 正常停止路径会先收集报告并 join, 
        // 不依赖这里关闭通道来完成正常收尾 
        reports_.close();

        worker_.requestStop();
        worker_.join();
    }

    BackgroundUpload(const BackgroundUpload&) = delete;

    BackgroundUpload& operator=(
        const BackgroundUpload&
    ) = delete;

    BackgroundUpload(BackgroundUpload&&) = delete;

    BackgroundUpload& operator=(
        BackgroundUpload&&
    ) = delete;

    void start()
    {
        worker_.start();
    }

    void notifyWork()
    {
        worker_.notifyWork();
    }

    void requestStop()
    {
        worker_.requestStop();
    }

    void join()
    {
        worker_.join();
    }

    edge::UploadWorkerStatus status() const
    {
        return worker_.status();
    }

    std::optional<edge::UploadCycleResult> takeReport()
    {
        return reports_.tryTake();
    }

    std::optional<edge::UploadCycleResult> waitReportUntil(
        edge::UploadReportChannel::Clock::time_point deadline
    )
    {
        return reports_.waitTakeUntil(deadline);
    }

    void closeReports()
    {
        reports_.close();
    }

private:
    // 声明顺序保证通道比 worker 活得更久 
    edge::UploadReportChannel reports_;
    edge::UploadWorker worker_;
};

std::int64_t wallNowMs()
{
    return std::chrono::duration_cast<
        std::chrono::milliseconds
    >(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

void printRuntimeError(
    const std::string& operation,
    int error_code,
    const std::string& message
)
{
    std::cerr
        << "Runtime error [" << operation << "]: "
        << message
        << " (code " << error_code << ")\n";
}

bool storeResults(
    const edge::TcpEventBatch& results,
    edge::OutboxStore& store,
    StorageCounters& counters,
    std::int64_t now_ms,
    edge::RunOutput& output,
    bool storage_available = true
)
{
    for (const auto& result : results) {
        const auto* event =
            std::get_if<edge::SensorEvent>(&result.result);

        if (event == nullptr) {
            if (std::holds_alternative<edge::EventJsonError>(
                    result.result)) {
                std::cerr << "Event rejected: json\n";
            } else {
                std::cerr << "Event rejected: validation\n";
            }

            continue;
        }

        if (!storage_available) {
            ++counters.skipped;
            output.record(
                *event,
                edge::JsonlRecordDisposition::StorageSkipped
            );

            std::cerr
                << "Event rejected: storage_unavailable "
                << edge::makeEventId(*event)
                << '\n';

            continue;
        }

        try {
            const auto outcome = store.enqueue(*event, now_ms);

            if (const auto* inserted =
                    std::get_if<edge::OutboxInsertResult>(&outcome)) {
                switch (inserted->status) {
                case edge::OutboxInsertStatus::Inserted:
                    ++counters.stored;
                    output.record(
                        *event,
                        edge::JsonlRecordDisposition::Stored
                    );

                    // 只有 enqueue 内部 COMMIT 成功后才到这里 
                    std::cout
                        << "Event stored: "
                        << inserted->event_id
                        << std::endl;
                    break;

                case edge::OutboxInsertStatus::Duplicate:
                    ++counters.duplicates;
                    output.record(
                        *event,
                        edge::JsonlRecordDisposition::Duplicate
                    );

                    std::cout
                        << "Event duplicate: "
                        << inserted->event_id
                        << std::endl;
                    break;

                case edge::OutboxInsertStatus::Conflict:
                    ++counters.conflicts;
                    output.record(
                        *event,
                        edge::JsonlRecordDisposition::IdentityConflict
                    );

                    std::cerr
                        << "Event rejected: identity_conflict "
                        << inserted->event_id
                        << '\n';
                    break;
                }
            } else if (const auto* json_error =
                    std::get_if<edge::EventJsonError>(&outcome)) {
                ++counters.serialization_errors;

                std::cerr
                    << "Event rejected: serialization field="
                    << json_error->field
                    << ": " << json_error->message
                    << '\n';
            } else {
                const auto& capacity_error =
                    std::get<edge::OutboxCapacityError>(outcome);

                ++counters.capacity_rejections;
                output.record(
                    *event,
                    edge::JsonlRecordDisposition::CapacityRejected
                );

                std::cerr
                    << "Event rejected: storage_capacity "
                    << capacity_error.event_id
                    << " kind="
                    << (
                        capacity_error.code ==
                            edge::OutboxCapacityCode::PendingLimitReached
                        ? "pending"
                        : "total"
                    )
                    << " pending=" << capacity_error.pending_records
                    << " total=" << capacity_error.total_records
                    << " limit=" << capacity_error.limit
                    << '\n';
            }
        } catch (const edge::SqliteError& error) {
            ++counters.errors;
            output.record(
                *event,
                edge::JsonlRecordDisposition::StorageError
            );

            printRuntimeError(
                "storage",
                error.code(),
                error.what()
            );

            storage_available = false;
        } catch (const std::exception& error) {
            ++counters.errors;
            output.record(
                *event,
                edge::JsonlRecordDisposition::StorageError
            );

            printRuntimeError(
                "storage",
                0,
                error.what()
            );

            storage_available = false;
        }
    }

    return storage_available;
}

bool performCleanup(
    edge::OutboxStore& store,
    StorageCounters& counters,
    std::int64_t now_ms
)
{
    try {
        const auto result = store.cleanupAcked(now_ms);

        counters.acked_deleted +=
            static_cast<std::uint64_t>(result.expired_deleted);

        counters.acked_deleted +=
            static_cast<std::uint64_t>(result.excess_deleted);

        return true;
    } catch (const edge::SqliteError& error) {
        ++counters.errors;

        printRuntimeError(
            "storage_cleanup",
            error.code(),
            error.what()
        );
    } catch (const std::exception& error) {
        ++counters.errors;

        printRuntimeError(
            "storage_cleanup",
            0,
            error.what()
        );
    }

    return false;
}

void applyUploadReport(
    const edge::UploadCycleResult& report,
    const edge::UploadConfig& config,
    UploadCounters& counters
)
{
    if (report.claimed_records == 0) {
        return;
    }

    ++counters.cycles;

    counters.claimed +=
        static_cast<std::uint64_t>(report.claimed_records);

    counters.acked +=
        static_cast<std::uint64_t>(report.acked_records);

    counters.retry +=
        static_cast<std::uint64_t>(report.retry_records);

    counters.dead +=
        static_cast<std::uint64_t>(report.dead_records);

    const char* failure_kind = "none";

    if (std::holds_alternative<edge::HttpRequestError>(
            report.failure)) {
        ++counters.request_failures;
        failure_kind = "request";
    }
    else if (std::holds_alternative<edge::BatchHttpError>(
                 report.failure)) {
        ++counters.http_failures;
        failure_kind = "http";
    }
    else if (std::holds_alternative<edge::BatchResponseError>(
                 report.failure)) {
        ++counters.protocol_failures;
        failure_kind = "protocol";
    }

    std::cout
        << "Upload cycle: claimed=" << report.claimed_records
        << " acked=" << report.acked_records
        << " retry=" << report.retry_records
        << " dead=" << report.dead_records
        << " failure=" << failure_kind;

    if (const auto* error =
            std::get_if<edge::HttpRequestError>(&report.failure)) {
        std::cout
            << " curl_code=" << error->curl_code;
    }
    else if (const auto* error =
                 std::get_if<edge::BatchHttpError>(&report.failure)) {
        std::cout
            << " http_status=" << error->status_code;
    }

    std::cout << std::endl;

    if (const auto* error =
            std::get_if<edge::BatchHttpError>(
                &report.failure)) {
        if (edge::classifyUploadHttpStatus(
                error->status_code
            ) == edge::HttpUploadAction::Block
            && !counters.blocked) {
            counters.blocked = true;
            counters.blocked_http_status =
                error->status_code;

            std::cerr
                << "Upload blocked: http_status="
                << error->status_code
                << " recovery=restart\n";
        }
    }

    if (report.dead_records > 0) {
        std::cerr
            << "Upload conflict: records="
            << report.dead_records
            << '\n';
    }

    const bool failed =
        !std::holds_alternative<std::monostate>(
            report.failure
        );

    if (!failed) {
        counters.failure_streak = 0;
        return;
    }

    const auto threshold =
        static_cast<std::uint64_t>(
            config.failure_warn_attempts
        );

    if (counters.failure_streak < threshold) {
        ++counters.failure_streak;

        if (counters.failure_streak == threshold) {
            ++counters.warnings;

            std::cerr
                << "Upload warning: consecutive_failed_cycles="
                << counters.failure_streak
                << '\n';
        }
    }
}

UploadStepResult recordUploadFailure(
    const edge::UploadWorkerStatus& status,
    bool& reported,
    UploadCounters& counters
)
{
    if (!status.failure || reported) {
        return UploadStepResult::Ok;
    }

    reported = true;

    ++counters.errors;

    try {
        std::rethrow_exception(status.failure);
    }
    catch (const edge::SqliteError& error) {
        printRuntimeError(
            "upload_storage",
            error.code(),
            error.what()
        );

        return UploadStepResult::StorageFailure;
    }
    catch (const std::exception& error) {
        printRuntimeError(
            "upload",
            0,
            error.what()
        );

        return UploadStepResult::LocalFailure;
    }
    catch (...) {
        printRuntimeError(
            "upload",
            0,
            "Unknown upload worker exception"
        );

        return UploadStepResult::LocalFailure;
    }
}

struct StatusRuntimeState {
    using Clock = edge::TcpEventProcessor::Clock;

    Clock::time_point started_at = Clock::now();
    Clock::time_point next_sample = started_at;

    edge::StatusMonitoringCounters monitoring;

    // 同一次连续输出故障只打印一次诊断 
    bool output_failure_reported = false;
    
    std::optional<edge::StatusSnapshot> latest_snapshot;
};

edge::StatusCounters makeStatusCounters(
    const edge::EventStreamStats& stream,
    const StorageCounters& storage,
    const UploadCounters& upload
)
{
    edge::StatusCounters counters;

    counters.completed_frames = stream.framing.completed_frames;
    counters.valid_events = stream.valid_events;
    counters.json_errors = stream.json_errors;
    counters.validation_errors = stream.validation_errors;
    counters.partial_timeouts = stream.framing.partial_timeouts;

    counters.stored_events = storage.stored;
    counters.duplicate_events = storage.duplicates;
    counters.identity_conflicts = storage.conflicts;
    counters.capacity_rejections = storage.capacity_rejections;
    counters.serialization_errors = storage.serialization_errors;
    counters.storage_errors = storage.errors;
    counters.storage_skipped = storage.skipped;
    counters.acked_deleted = storage.acked_deleted;
    counters.recovered_sending = storage.recovered_sending;

    counters.upload_cycles = upload.cycles;
    counters.upload_claimed = upload.claimed;
    counters.upload_acked = upload.acked;
    counters.upload_retry = upload.retry;
    counters.upload_dead = upload.dead;
    counters.upload_request_failures = upload.request_failures;
    counters.upload_http_failures = upload.http_failures;
    counters.upload_protocol_failures = upload.protocol_failures;
    counters.upload_errors = upload.errors;
    counters.upload_warnings = upload.warnings;

    return counters;
}

void printStats(
    const edge::EventStreamStats& stats,
    const StorageCounters& storage,
    const UploadCounters& upload
)
{
    std::cout
        << "Stopped\n"
        << "completed_frames: "
        << stats.framing.completed_frames << '\n'
        << "valid_events: " << stats.valid_events << '\n'
        << "json_errors: " << stats.json_errors << '\n'
        << "validation_errors: "
        << stats.validation_errors << '\n'
        << "partial_timeouts: "
        << stats.framing.partial_timeouts << '\n'
        << "stored_events: " << storage.stored << '\n'
        << "duplicate_events: " << storage.duplicates << '\n'
        << "identity_conflicts: " << storage.conflicts << '\n'
        << "capacity_rejections: "
        << storage.capacity_rejections << '\n'
        << "serialization_errors: "
        << storage.serialization_errors << '\n'
        << "storage_errors: " << storage.errors << '\n'
        << "storage_skipped: " << storage.skipped << '\n'
        << "acked_deleted: " << storage.acked_deleted << '\n'
        << "recovered_sending: "
        << storage.recovered_sending << '\n'
        << "upload_cycles: " << upload.cycles << '\n'
        << "upload_claimed: " << upload.claimed << '\n'
        << "upload_acked: " << upload.acked << '\n'
        << "upload_retry: " << upload.retry << '\n'
        << "upload_dead: " << upload.dead << '\n'
        << "upload_request_failures: "
        << upload.request_failures << '\n'
        << "upload_http_failures: "
        << upload.http_failures << '\n'
        << "upload_protocol_failures: "
        << upload.protocol_failures << '\n'
        << "upload_errors: " << upload.errors << '\n'
        << "upload_warnings: " << upload.warnings << '\n'
        << "upload_blocked: "
        << (upload.blocked ? 1 : 0) << '\n'
        << "upload_blocked_http_status: "
        << upload.blocked_http_status << '\n'
        << std::flush;
}

} // namespace

namespace edge {

int runAgentImpl(
    const AgentConfig& config, 
    RunOutput& output, 
    bool drain_only
){
    try {
        const auto& tcp = config.tcp;

        StatusRuntimeState status_runtime;

        // SignalGuard signals;

        TcpConnections connections{
            tcp.max_connections
        };

        TcpEventProcessor processor{
            tcp
        };

        TcpActivityTracker activity{
            tcp
        };

        // drain-only 不创建监听 socket 
        // optional 不要求先构造一个无效 SocketFd 
        std::optional<SocketFd> listener;

        if (!drain_only) {
            auto listen_result = createTcpListener(
                tcp.bind,
                tcp.port,
                tcp.max_connections
            );

            if (const auto* error =
                    std::get_if<TcpListenError>(&listen_result)) {
                printRuntimeError(
                    error->operation,
                    error->error_code,
                    error->message
                );

                return 1;
            }

            listener.emplace(
                std::move(
                    std::get<SocketFd>(listen_result)
                )
            );
        }

        // 实例锁覆盖主连接、上传线程及其连接的完整生命周期 
        StorageInstanceLock storage_lock(
            config.storage.path
        );

        auto database = openOutboxDatabase(
            config.storage.path
        );

        auto access_gate = std::make_shared<OutboxAccessGate>();

        OutboxStore store(
            database,
            config.storage,
            access_gate
        );

        StorageCounters storage_stats;
        UploadCounters upload_stats;

        // 独占启动阶段只恢复一次 
        storage_stats.recovered_sending =
            static_cast<std::uint64_t>(
                store.recoverSending(wallNowMs())
            );

        if (!performCleanup(
                store,
                storage_stats,
                wallNowMs())) {
            return 1;
        }

        // 首次全局初始化必须在线程创建前完成 
        ensureCurlInitialized();

        RawRecorder recorder(
            config.recording,
            output.runId(),
            static_cast<std::size_t>(tcp.max_connections)
        );

        RawRecorderStats reported_recording;

        // 只在错误累计值变化时输出诊断 
        // 这些错误不计入业务存储或上传错误 
        const auto reportRecordingErrors = [&]() {
            const auto current = recorder.stats();

            if (
                current.startup_errors ==
                    reported_recording.startup_errors &&
                current.event_errors ==
                    reported_recording.event_errors &&
                current.session_errors ==
                    reported_recording.session_errors &&
                current.retention_errors ==
                    reported_recording.retention_errors
            ) {
                return;
            }

            std::cerr
                << "Raw recording warning:"
                << " startup_errors=" << current.startup_errors
                << " event_errors=" << current.event_errors
                << " session_errors=" << current.session_errors
                << " retention_errors=" << current.retention_errors
                << " code=" << current.last_error_code
                << " connection_id=" << current.last_error_connection
                << '\n';

            reported_recording = current;
        };

        reportRecordingErrors();

        // 在主连接之后构造, 退出时先结束上传线程 
        BackgroundUpload background(config, access_gate);

        {
            WorkerSignalMask blocked;

            // 等待工作线程内的 database/store/uploader 初始化完成 
            // 此期间主连接不执行数据库操作 
            background.start();

            // 只恢复主线程, 工作线程继续保持屏蔽 
            blocked.restore();
        }

        const int poll_timeout_ms = std::min(
            100,
            static_cast<int>(tcp.partial_timeout_ms)
        );

        auto next_cleanup =
            TcpEventProcessor::Clock::now()
            + std::chrono::seconds(60);

        int exit_code = 0;
        bool storage_available = true;
        bool upload_failure_reported = false;

        const auto observeUploadFailure = [&]() -> bool {
            const auto status = background.status();

            if (!status.failure || upload_failure_reported) {
                return false;
            }

            // failure 发布时工作线程已经结束 
            // 先收集此前正常完成的报告, 再记录异常 
            while (auto report = background.takeReport()) {
                applyUploadReport(
                    *report,
                    config.upload,
                    upload_stats
                );
            }

            const auto result = recordUploadFailure(
                status,
                upload_failure_reported,
                upload_stats
            );

            if (result == UploadStepResult::Ok) {
                return false;
            }

            if (result == UploadStepResult::StorageFailure) {
                ++storage_stats.errors;
                storage_available = false;
            }

            exit_code = 1;

            return true;
        };

        const auto updateStatus = [&](
            StatusProcessState process_state,
            bool worker_stopping,
            bool force
        ) {
            const auto steady_now =
                StatusRuntimeState::Clock::now();

            if (!force
                && steady_now < status_runtime.next_sample) {
                return;
            }

            try {
                StatusSnapshot snapshot;

                snapshot.agent_id = config.agent.id;
                snapshot.checked_at_ms = wallNowMs();

                snapshot.uptime_ms =
                    static_cast<std::uint64_t>(
                        std::chrono::duration_cast<
                            std::chrono::milliseconds
                        >(
                            steady_now
                            - status_runtime.started_at
                        ).count()
                    );

                snapshot.process_state = process_state;
                snapshot.active_connections = connections.size();

                snapshot.recording = recorder.stats();

                snapshot.tcp_activity = activity.snapshot(
                    TcpActivityTracker::Clock::now()
                );

                const auto worker_status = background.status();

                snapshot.upload = StatusUploadObservation{
                    worker_status.ready,
                    worker_status.finished,
                    static_cast<bool>(worker_status.failure),
                    worker_stopping,
                    upload_stats.blocked,
                    upload_stats.blocked_http_status
                };

                snapshot.counters = makeStatusCounters(
                    processor.stats(),
                    storage_stats,
                    upload_stats
                );

                sampleStatusResources(
                    snapshot,
                    store,
                    config.storage.path
                );

                if (snapshot.outbox.state
                    == StatusSampleState::Error) {
                    ++status_runtime.monitoring.outbox_query_errors;
                }

                if (snapshot.disk_available_bytes.state
                    == StatusSampleState::Error) {
                    ++status_runtime.monitoring.disk_query_errors;
                }

                snapshot.monitoring = status_runtime.monitoring;
                output.decorate(snapshot);

                // 状态文件失败与控制台发布独立处理 
                try {
                    writeStatusFileAtomically(
                        config.status.file,
                        config.storage.path,
                        serializeStatusSnapshot(
                            snapshot,
                            config.status
                        )
                    );

                    ++status_runtime.monitoring
                        .previous_successful_writes;

                    status_runtime.monitoring
                        .previous_successful_snapshot_at_ms =
                            snapshot.checked_at_ms;

                    if (status_runtime.output_failure_reported) {
                        std::cerr << "Status output recovered\n";
                        status_runtime.output_failure_reported = false;
                    }
                }
                catch (const std::exception& error) {
                    ++status_runtime.monitoring.output_errors;

                    snapshot.monitoring.output_errors =
                        status_runtime.monitoring.output_errors;

                    if (!status_runtime.output_failure_reported) {
                        std::cerr
                            << "Status output failed: "
                            << error.what()
                            << '\n';

                        status_runtime.output_failure_reported = true;
                    }
                }

                output.decorate(snapshot);
                output.status(snapshot);

                status_runtime.latest_snapshot =
                    std::move(snapshot);
            }
            catch (const std::exception& error) {
                ++status_runtime.monitoring.output_errors;

                if (!status_runtime.output_failure_reported) {
                    std::cerr
                        << "Status output failed: "
                        << error.what()
                        << '\n';

                    status_runtime.output_failure_reported = true;
                }
            }

            status_runtime.next_sample =
                StatusRuntimeState::Clock::now()
                + std::chrono::milliseconds(
                    config.status.interval_ms
                );
        };

        output.ready();

        if (drain_only) {
            std::cout
                << "Draining existing Outbox."
                << std::endl;
        } else {
            std::cout
                << "Listening on "
                << tcp.bind << ':' << tcp.port
                << std::endl;
        }
        
        updateStatus(
            stop_requested
                ? StatusProcessState::Stopping
                : StatusProcessState::Running,
            false,
            true
        );

        // 初始化就绪与开始上传分开 
        // ready 和初始状态发布后, 才放行首次查询与上传 
        if (!stop_requested) {
            background.notifyWork();
        }

        while (!stop_requested) {
            output.pump();

            // 每轮最多处理一份报告，保持既有消费预算。
            if (auto report = background.takeReport()) {
                applyUploadReport(
                    *report,
                    config.upload,
                    upload_stats
                );
            }

            if (observeUploadFailure()) {
                break;
            }

            if (stop_requested) {
                break;
            }

            const auto stored_before =
                storage_stats.stored;

            if (!drain_only) {
                const auto io = pollTcpOnce(
                    listener->get(),
                    connections,
                    poll_timeout_ms
                );

                // 在录制文件 I/O 和业务解析前记录网络活动。
                activity.consume(
                    io,
                    TcpActivityTracker::Clock::now()
                );

                recorder.consume(io);
                reportRecordingErrors();

                const auto now_ms = wallNowMs();

                const auto receive_now =
                    TcpEventProcessor::Clock::now();

                const auto results = processor.consume(
                    io,
                    now_ms,
                    receive_now
                );

                storage_available = storeResults(
                    results,
                    store,
                    storage_stats,
                    now_ms,
                    output,
                    storage_available
                );

                for (const auto& event : io.events) {
                    if (event.kind == TcpIoKind::ClientError) {
                        std::cerr
                            << "Client error: slot="
                            << *event.slot
                            << " code="
                            << event.error_code
                            << '\n';
                    }
                    else if (
                        event.kind ==
                            TcpIoKind::RejectedAtCapacity
                    ) {
                        std::cerr
                            << "Connection rejected: capacity\n";
                    }
                }

                if (io.error) {
                    printRuntimeError(
                        io.error->operation,
                        io.error->error_code,
                        io.error->message
                    );

                    exit_code = 1;
                    break;
                }
            } else {
                // 上传由后台线程执行。
                // 主线程定期消费报告、采样并响应停止，避免空转。
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(poll_timeout_ms)
                );
            }

            if (!storage_available) {
                exit_code = 1;
                break;
            }

            if (observeUploadFailure()) {
                break;
            }

            if (stop_requested) {
                break;
            }

            const auto steady_now =
                TcpEventProcessor::Clock::now();

            if (steady_now >= next_cleanup) {
                storage_available = performCleanup(
                    store,
                    storage_stats,
                    wallNowMs()
                );

                if (!storage_available) {
                    exit_code = 1;
                    break;
                }

                next_cleanup =
                    TcpEventProcessor::Clock::now()
                    + std::chrono::seconds(60);
            }

            // 普通运行模式只有新事件提交成功后才通知。
            // drain-only 的首次通知已经在进入循环前发出。
            if (storage_stats.stored != stored_before) {
                background.notifyWork();
            }

            updateStatus(
                StatusProcessState::Running,
                false,
                false
            );

            if (
                drain_only
                && status_runtime.latest_snapshot.has_value()
            ) {
                const auto& snapshot =
                    *status_runtime.latest_snapshot;

                if (
                    snapshot.outbox.state ==
                        StatusSampleState::Valid
                    && snapshot.outbox.value.has_value()
                ) {
                    const auto& counts =
                        *snapshot.outbox.value;

                    // 三种未完成状态都清空后，结束补传。
                    // DEAD 不参与判断，RETRY 即使尚未到期也继续等待。
                    if (
                        counts.pending == 0
                        && counts.sending == 0
                        && counts.retry == 0
                    ) {
                        break;
                    }
                }
            }
        }

        // 先停止新的上传, 再停止接收 
        // 当前已开始的请求仍完成回写与报告 
        background.requestStop();

        listener.reset();

        // 如果上传线程已报告存储故障, 关闭阶段不再尝试入库 
        observeUploadFailure();

        TcpPollBatch closing;

        for (std::size_t slot = 0;
             slot < connections.capacity();
             ++slot) {
            if (connections.fdAt(slot) >= 0) {
                closing.events.push_back({
                    TcpIoKind::Closed,
                    slot,
                    {},
                    0
                });

                connections.closeAt(slot);
            }
        }

        // closing 中包含仍然活动的连接
        // 必须先让录制器消费, 再结束整个管理器
        activity.consume(
            closing,
            TcpActivityTracker::Clock::now()
        );

        recorder.consume(closing);
        recorder.finish();
        reportRecordingErrors();

        const auto closing_now = wallNowMs();

        storage_available = storeResults(
            processor.consume(closing, closing_now),
            store,
            storage_stats,
            closing_now,
            output,
            storage_available
        );

        if (!storage_available) {
            exit_code = 1;
        }

        updateStatus(
            exit_code == 0
                ? StatusProcessState::Stopping
                : StatusProcessState::Failed,
            true,
            true
        );

        // 不能先 join：工作线程可能正在等待报告槽位 
        // 持续消费报告, 让当前周期完成并退出 
        while (!background.status().finished) {
            const auto deadline =
                UploadReportChannel::Clock::now()
                + std::chrono::milliseconds(100);
            
            if (auto report =
                    background.waitReportUntil(deadline)) {
                applyUploadReport(
                    *report,
                    config.upload,
                    upload_stats
                );
            }

            output.pump();
        }

        background.join();

        // 线程结束时, 槽位里仍可能留有最后一份报告 
        while (auto report = background.takeReport()) {
            applyUploadReport(
                *report,
                config.upload,
                upload_stats
            );
        }

        observeUploadFailure();

        background.closeReports();

        updateStatus(
            exit_code == 0
                ? StatusProcessState::Stopped
                : StatusProcessState::Failed,
            true,
            true
        );

        if (status_runtime.latest_snapshot.has_value()) {
            output.stopped(
                *status_runtime.latest_snapshot,
                exit_code
            );
        }

        printStats(
            processor.stats(),
            storage_stats,
            upload_stats
        );

        // 此时报告已收集、工作线程已 join 
        // 控制台只获得有限的收尾时间 
        output.finish();

        // 把收尾期间产生的展示丢弃计数写回最终状态文件 
        // 复用已有快照, 不再查询数据库 
        if (status_runtime.latest_snapshot.has_value()) {
            try {
                auto& snapshot =
                    *status_runtime.latest_snapshot;

                snapshot.monitoring = status_runtime.monitoring;
                output.decorate(snapshot);

                writeStatusFileAtomically(
                    config.status.file,
                    config.storage.path,
                    serializeStatusSnapshot(
                        snapshot,
                        config.status
                    )
                );
            }
            catch (const std::exception& error) {
                ++status_runtime.monitoring.output_errors;

                std::cerr
                    << "Final status output failed: "
                    << error.what()
                    << '\n';

                output.pump();
            }
        }
        
        return exit_code;
    }
    catch (const SqliteError& error) {
        printRuntimeError(
            "storage",
            error.code(),
            error.what()
        );

        return 1;
    }
    catch (const std::system_error& error) {
        printRuntimeError(
            "runtime",
            error.code().value(),
            error.what()
        );

        return 1;
    }
    catch (const std::exception& error) {
        printRuntimeError(
            "runtime",
            0,
            error.what()
        );

        return 1;
    }
}

int runAgent(
    const AgentConfig& config,
    const AgentRunOptions& options
)
{
    try {
        // signals 比 output 活得更久, 
        // 覆盖输出收尾期间的 SIGINT/SIGTERM 处理 
        SignalGuard signals;

        AgentConfig effective_config = config;

        if (options.drain_only) {
            effective_config.recording.enabled = false;
        }

        RunOutput output(effective_config, options);

        return runAgentImpl(
            effective_config,
            output,
            options.drain_only
        );
    }
    catch (const std::exception& error) {
        reportRunOutputSetupError(error.what());
        return 1;
    }
}

int exportDead(
    const AgentConfig& config,
    const std::filesystem::path& output_path
)
{
    try {
        // 导出模式不能创建新的业务数据库。
        if (
            !std::filesystem::is_regular_file(
                config.storage.path
            )
        ) {
            throw std::invalid_argument(
                "Existing Outbox database required"
            );
        }

        // 与普通运行及 drain-only 使用同一把锁。
        StorageInstanceLock storage_lock(
            config.storage.path
        );

        const auto count = exportDeadFile(
            config.storage.path,
            output_path
        );

        std::cout
            << "Exported DEAD records: "
            << count << '\n'
            << "Output: "
            << std::filesystem::absolute(
                output_path
            ).lexically_normal().string()
            << '\n';

        return 0;
    }
    catch (const std::exception& error) {
        std::cerr
            << "Export error: "
            << error.what()
            << '\n';

        return 2;
    }
}

} // namespace edge