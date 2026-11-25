#include "edge/recording/RawRecorder.h"

#include <cerrno>
#include <charconv>
#include <filesystem>
#include <new>
#include <queue>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace edge {
namespace {

namespace fs = std::filesystem;

// 只识别 RawRecordingSession 生成的规范文件名：
//
// edge-raw-v1-<32 hex>-c<20 digits>-p<20 digits>.bin
//
// 不使用简单的 *.bin 匹配，避免误删目录中的其他文件。
bool isManagedFileName(const std::string& name) noexcept
{
    if (name.size() != 92 ||
        name.compare(0, 12, "edge-raw-v1-") != 0 ||
        name.compare(44, 2, "-c") != 0 ||
        name.compare(66, 2, "-p") != 0 ||
        name.compare(88, 4, ".bin") != 0) {
        return false;
    }

    for (std::size_t i = 12; i < 44; ++i) {
        const char ch = name[i];

        if (!((ch >= '0' && ch <= '9') ||
              (ch >= 'a' && ch <= 'f'))) {
            return false;
        }
    }

    const auto positiveNumber = [&name](
        std::size_t offset
    ) noexcept {
        std::uint64_t value = 0;

        const char* begin = name.data() + offset;
        const char* end = begin + 20;

        const auto result = std::from_chars(begin, end, value);

        return result.ec == std::errc{} &&
               result.ptr == end &&
               value != 0;
    };

    return positiveNumber(46) && positiveNumber(68);
}

struct ClosedCandidate {
    fs::file_time_type modified;
    fs::path path;
    std::string name;
};

// priority_queue 的堆顶是最旧的候选文件。
// 修改时间相同时，按规范文件名确定顺序。
struct NewerCandidate {
    bool operator()(
        const ClosedCandidate& lhs,
        const ClosedCandidate& rhs
    ) const noexcept
    {
        if (lhs.modified != rhs.modified) {
            return lhs.modified > rhs.modified;
        }

        return lhs.name > rhs.name;
    }
};

} // namespace

struct RawRecorder::Impl {
    struct Slot {
        bool active = false;

        std::uint64_t connection_id = 0;

        bool failure_reported = false;

        std::unique_ptr<RawRecordingSession> session;
    };

    RecordingConfig config;
    std::string run_id;

    std::vector<Slot> slots;

    // 已结束会话的累计值保存在这里。
    // stats() 再叠加当前仍持有的会话统计。
    RawRecorderStats totals;

    int lock_fd = -1;

    Impl(
        const RecordingConfig& recording_config,
        std::string recording_run_id,
        std::size_t max_connections
    )
        : config(recording_config),
          run_id(std::move(recording_run_id)),
          slots(max_connections)
    {
        totals.enabled = config.enabled;

        // 禁用时不检查目录，也不创建目录或锁文件。
        if (!config.enabled) {
            return;
        }

        try {
            initializeDirectory();
            totals.operational = true;
        } catch (const std::system_error& error) {
            startupFailed(error.code().value());
        } catch (const std::invalid_argument&) {
            startupFailed(EINVAL);
        } catch (const std::bad_alloc&) {
            startupFailed(ENOMEM);
        } catch (...) {
            startupFailed(EIO);
        }

        if (totals.operational) {
            pruneClosed();
        }
    }

    ~Impl() noexcept
    {
        // 必须在成员析构前结束会话：
        // 会话的关闭回调仍会使用当前管理器。
        finish();
    }

    void recordError(
        RawRecorderErrorStage stage,
        int code,
        std::uint64_t connection_id = 0,
        RawRecordingOperation operation =
            RawRecordingOperation::None
    ) noexcept
    {
        totals.last_error_stage = stage;
        totals.last_error_code = code != 0 ? code : EIO;
        totals.last_error_connection = connection_id;
        totals.last_session_operation = operation;

        switch (stage) {
        case RawRecorderErrorStage::Startup:
            ++totals.startup_errors;
            break;

        case RawRecorderErrorStage::Event:
            ++totals.event_errors;
            break;

        case RawRecorderErrorStage::Session:
            ++totals.session_errors;
            break;

        case RawRecorderErrorStage::Retention:
            ++totals.retention_errors;
            break;

        case RawRecorderErrorStage::None:
            break;
        }
    }

    void releaseDirectory() noexcept
    {
        if (lock_fd < 0) {
            return;
        }

        const int fd = std::exchange(lock_fd, -1);

        ::flock(fd, LOCK_UN);
        ::close(fd);

        // 不删除锁文件。
        // 删除后重新创建可能产生两个不同 inode，
        // 从而破坏跨进程互斥。
    }

    void startupFailed(int code) noexcept
    {
        releaseDirectory();

        totals.operational = false;

        recordError(
            RawRecorderErrorStage::Startup,
            code
        );
    }

    void initializeDirectory()
    {
        if (config.directory.empty() ||
            !config.directory.is_absolute() ||
            config.directory.native().find('\0') !=
                std::string::npos ||
            config.max_file_bytes <= 0 ||
            config.keep_closed_files < 0) {
            throw std::invalid_argument(
                "Invalid raw recording configuration"
            );
        }

        // 复用底层文件名生成器校验 run_id。
        (void)makeRawRecordingFileName(run_id, 1, 1);

        fs::create_directories(config.directory);

        const fs::path lock_path =
            config.directory / ".edge-raw-v1.lock";

        lock_fd = ::open(
            lock_path.c_str(),
            O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW,
            0600
        );

        if (lock_fd < 0) {
            const int code = errno;

            throw std::system_error(
                code,
                std::generic_category(),
                "Open raw recording directory lock"
            );
        }

        struct stat info {};

        if (::fstat(lock_fd, &info) != 0) {
            const int code = errno;

            throw std::system_error(
                code,
                std::generic_category(),
                "Inspect raw recording directory lock"
            );
        }

        if (!S_ISREG(info.st_mode)) {
            throw std::system_error(
                EINVAL,
                std::generic_category(),
                "Raw recording lock is not a regular file"
            );
        }

        if (::flock(lock_fd, LOCK_EX | LOCK_NB) != 0) {
            const int code = errno;

            throw std::system_error(
                code,
                std::generic_category(),
                "Lock raw recording directory"
            );
        }
    }

    bool isActivePath(const fs::path& path) const noexcept
    {
        for (const auto& slot : slots) {
            if (slot.session &&
                slot.session->activePath() == path) {
                return true;
            }
        }

        return false;
    }

    void pruneClosed() noexcept
    {
        if (!totals.operational || lock_fd < 0) {
            return;
        }

        totals.retained_closed_files.reset();

        try {
            std::priority_queue<
                ClosedCandidate,
                std::vector<ClosedCandidate>,
                NewerCandidate
            > survivors;

            const std::size_t keep_count =
                static_cast<std::size_t>(
                    config.keep_closed_files
                );

            bool all_removed = true;

            for (const auto& entry :
                 fs::directory_iterator(config.directory)) {
                const std::string name =
                    entry.path().filename().string();

                if (!isManagedFileName(name)) {
                    continue;
                }

                // 不跟随符号链接，也不递归处理子目录。
                if (!fs::is_regular_file(
                        entry.symlink_status())) {
                    continue;
                }

                if (isActivePath(entry.path())) {
                    continue;
                }

                survivors.push(ClosedCandidate{
                    entry.last_write_time(),
                    entry.path(),
                    name
                });

                // 只保留最新的 keep_count 个候选。
                // 堆最多暂存 keep_count + 1 个元素。
                if (survivors.size() > keep_count) {
                    const fs::path victim =
                        survivors.top().path;

                    survivors.pop();

                    if (::unlink(victim.c_str()) == 0) {
                        ++totals.files_deleted;
                    } else {
                        const int code = errno;

                        all_removed = false;

                        recordError(
                            RawRecorderErrorStage::Retention,
                            code
                        );
                    }
                }
            }

            if (all_removed) {
                totals.retained_closed_files =
                    static_cast<std::uint64_t>(
                        survivors.size()
                    );
            }
        } catch (const std::system_error& error) {
            recordError(
                RawRecorderErrorStage::Retention,
                error.code().value()
            );
        } catch (const std::bad_alloc&) {
            recordError(
                RawRecorderErrorStage::Retention,
                ENOMEM
            );
        } catch (...) {
            recordError(
                RawRecorderErrorStage::Retention,
                EIO
            );
        }

        // 清理失败仅记录管理错误。
        // 不能通过关闭回调抛回 RawRecordingSession，
        // 否则会把清理失败误判为当前连接录制失败。
    }

    void noticeSessionFailure(Slot& slot) noexcept
    {
        if (!slot.session ||
            !slot.session->stats().failed ||
            slot.failure_reported) {
            return;
        }

        const auto& stats = slot.session->stats();

        slot.failure_reported = true;

        recordError(
            RawRecorderErrorStage::Session,
            stats.error_code,
            slot.connection_id,
            stats.error_operation
        );
    }

    void retire(Slot& slot) noexcept
    {
        if (slot.session) {
            const bool closed = slot.session->close();

            noticeSessionFailure(slot);

            const auto& stats = slot.session->stats();

            totals.bytes_written += stats.bytes_written;
            totals.opened_files += stats.opened_files;
            totals.closed_files += stats.closed_files;

            slot.session.reset();

            // 异常关闭可能留下部分文件，
            // 这种关闭不会触发正常的 on_closed 回调。
            if (!closed) {
                pruneClosed();
            }
        }

        slot.active = false;
    }

    void accept(Slot& slot) noexcept
    {
        if (slot.active) {
            recordError(
                RawRecorderErrorStage::Event,
                EPROTO,
                slot.connection_id
            );

            retire(slot);
        }

        slot.active = true;
        slot.failure_reported = false;

        slot.connection_id = ++totals.connections_seen;

        if (!totals.operational) {
            return;
        }

        try {
            slot.session =
                std::make_unique<RawRecordingSession>(
                    config.directory,
                    run_id,
                    slot.connection_id,
                    static_cast<std::uint64_t>(
                        config.max_file_bytes
                    ),
                    [this](const RawClosedSegment&) noexcept {
                        // 底层先清空 activePath，再调用这里。
                        pruneClosed();
                    }
                );
        } catch (const std::bad_alloc&) {
            slot.failure_reported = true;

            recordError(
                RawRecorderErrorStage::Session,
                ENOMEM,
                slot.connection_id,
                RawRecordingOperation::Metadata
            );
        } catch (...) {
            slot.failure_reported = true;

            recordError(
                RawRecorderErrorStage::Session,
                EIO,
                slot.connection_id,
                RawRecordingOperation::Metadata
            );
        }
    }

    void consume(const TcpPollBatch& batch) noexcept
    {
        if (!totals.enabled || totals.finished) {
            return;
        }

        // 不因 batch.error 跳过已经生成的事件。
        for (const auto& event : batch.events) {
            if (event.kind == TcpIoKind::RejectedAtCapacity) {
                continue;
            }

            if (event.kind == TcpIoKind::Data) {
                totals.bytes_received +=
                    static_cast<std::uint64_t>(
                        event.bytes.size()
                    );
            }

            if (!event.slot ||
                *event.slot >= slots.size()) {
                recordError(
                    RawRecorderErrorStage::Event,
                    EPROTO
                );

                continue;
            }

            Slot& slot = slots[*event.slot];

            if (event.kind == TcpIoKind::Accepted) {
                accept(slot);
                continue;
            }

            if (!slot.active) {
                recordError(
                    RawRecorderErrorStage::Event,
                    EPROTO
                );

                continue;
            }

            switch (event.kind) {
            case TcpIoKind::Data:
                if (!slot.session ||
                    slot.session->stats().failed) {
                    break;
                }

                if (!slot.session->append(
                        event.bytes.data(),
                        event.bytes.size())) {
                    noticeSessionFailure(slot);

                    // 将异常关闭后留下的部分文件
                    // 重新纳入已关闭文件清理。
                    pruneClosed();
                }
                break;

            case TcpIoKind::Closed:
            case TcpIoKind::ClientError:
                retire(slot);
                break;

            case TcpIoKind::Accepted:
            case TcpIoKind::RejectedAtCapacity:
                break;
            }
        }
    }

    void finish() noexcept
    {
        if (totals.finished) {
            return;
        }

        for (auto& slot : slots) {
            retire(slot);
        }

        pruneClosed();

        totals.finished = true;
        totals.operational = false;

        releaseDirectory();
    }

    RawRecorderStats stats() const noexcept
    {
        RawRecorderStats result = totals;

        for (const auto& slot : slots) {
            if (!slot.session) {
                continue;
            }

            const auto& current = slot.session->stats();

            result.bytes_written += current.bytes_written;
            result.opened_files += current.opened_files;
            result.closed_files += current.closed_files;

            if (!current.failed && !current.finished) {
                ++result.active_sessions;
            }

            if (!slot.session->activePath().empty()) {
                ++result.active_files;
            }
        }

        result.bytes_skipped =
            result.bytes_received - result.bytes_written;

        return result;
    }
};

RawRecorder::RawRecorder(
    const RecordingConfig& config,
    std::string run_id,
    std::size_t max_connections
)
{
    if (max_connections == 0 ||
        max_connections >
            static_cast<std::size_t>(kMaxTcpConnections)) {
        throw std::invalid_argument(
            "Invalid raw recording connection capacity"
        );
    }

    impl_ = std::make_unique<Impl>(
        config,
        std::move(run_id),
        max_connections
    );
}

RawRecorder::~RawRecorder() noexcept = default;

void RawRecorder::consume(
    const TcpPollBatch& batch
) noexcept
{
    impl_->consume(batch);
}

void RawRecorder::finish() noexcept
{
    impl_->finish();
}

RawRecorderStats RawRecorder::stats() const noexcept
{
    return impl_->stats();
}

} // namespace edge