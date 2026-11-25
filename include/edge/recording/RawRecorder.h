#pragma once

#include "edge/config/AgentConfig.h"
#include "edge/net/TcpPoller.h"
#include "edge/recording/RawRecordingSession.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace edge {

enum class RawRecorderErrorStage {
    None,
    Startup,
    Event,
    Session,
    Retention
};

struct RawRecorderStats {
    bool enabled = false;

    // 表示目录已经成功初始化并被当前管理器占用。
    // 个别连接录制失败，不会将此字段改为 false。
    bool operational = false;

    bool finished = false;

    std::uint64_t connections_seen = 0;

    std::uint64_t bytes_received = 0;
    std::uint64_t bytes_written = 0;
    std::uint64_t bytes_skipped = 0;

    std::uint64_t opened_files = 0;
    std::uint64_t closed_files = 0;

    std::uint64_t active_sessions = 0;
    std::uint64_t active_files = 0;

    std::uint64_t files_deleted = 0;

    // 最近一次成功清理后，保留的已关闭文件数量。
    // 未启用、初始化失败或最近一次清理失败时为 nullopt。
    std::optional<std::uint64_t> retained_closed_files;

    std::uint64_t startup_errors = 0;
    std::uint64_t event_errors = 0;

    // 同一连接的录制失败只累计一次。
    std::uint64_t session_errors = 0;

    std::uint64_t retention_errors = 0;

    RawRecorderErrorStage last_error_stage =
        RawRecorderErrorStage::None;

    int last_error_code = 0;

    // 全局错误使用 0。
    std::uint64_t last_error_connection = 0;

    // 仅 last_error_stage == Session 时有意义。
    RawRecordingOperation last_session_operation =
        RawRecordingOperation::None;
};

// 单线程调用。
//
// 调用方必须按原顺序传入每一批 TcpPollBatch。
// 即使 batch.error 有值，已经生成的 events 也仍会被消费。
//
// finish() 关闭全部录制会话并释放目录锁。
// finish() 之后的 consume() 不再处理事件。
class RawRecorder {
public:
    RawRecorder(
        const RecordingConfig& config,
        std::string run_id,
        std::size_t max_connections
    );

    ~RawRecorder() noexcept;

    RawRecorder(const RawRecorder&) = delete;
    RawRecorder& operator=(const RawRecorder&) = delete;

    RawRecorder(RawRecorder&&) = delete;
    RawRecorder& operator=(RawRecorder&&) = delete;

    void consume(const TcpPollBatch& batch) noexcept;

    void finish() noexcept;

    [[nodiscard]] RawRecorderStats stats() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace edge