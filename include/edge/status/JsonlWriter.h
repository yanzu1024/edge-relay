#pragma once

#include "edge/status/JsonlProtocol.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <optional>
#include <string>

namespace edge {

inline constexpr std::size_t kMaxPendingJsonlRecords = 64;
inline constexpr std::size_t kMaxJsonlDrainOperations = 32;

enum class JsonlEnqueueResult {
    Queued,
    Coalesced,
    Dropped,
    Disabled
};

enum class JsonlDrainResult {
    Idle,
    Pending,
    WouldBlock,
    Disabled
};

struct JsonlWriterStats {
    std::uint64_t written_bytes = 0;
    std::uint64_t completed_lines = 0;

    std::uint64_t display_dropped = 0;
    std::uint64_t record_queue_drops = 0;
    std::uint64_t record_rate_drops = 0;

    std::uint64_t status_coalesced = 0;

    // 因停用或主动放弃而未交付的行，包含生命周期消息。
    std::uint64_t abandoned_lines = 0;
    std::uint64_t partial_lines_abandoned = 0;

    std::uint64_t io_errors = 0;
    int last_error = 0;
};

// 单线程使用，不拥有线程，也不拥有描述符。
//
// owner 必须：
// 1. 在本对象存活期间忽略 SIGPIPE。
// 2. 保证描述符始终有效。
// 3. 独占该输出流的写入和描述符状态标志管理。
// 4. 决定何时 drain，以及收尾时何时 abandon。
//
// enqueue 接收 JsonlProtocol 编码成功的完整行。
// 本组件检查行边界、协议元数据和消息顺序，
// 不重新校验 SensorEvent 的业务规则。
class JsonlWriter {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    explicit JsonlWriter(int descriptor);
    ~JsonlWriter() noexcept;

    JsonlWriter(const JsonlWriter&) = delete;
    JsonlWriter& operator=(const JsonlWriter&) = delete;

    JsonlWriter(JsonlWriter&&) = delete;
    JsonlWriter& operator=(JsonlWriter&&) = delete;

    JsonlEnqueueResult enqueue(std::string line);

    // 不等待描述符变为可写。
    // byte_budget 会被限制在 64 KiB 以内。
    JsonlDrainResult drain(
        TimePoint now = Clock::now(),
        std::size_t byte_budget = kMaxJsonlLineBytes
    );

    // 放弃全部未交付内容，永久停用本对象。
    // 不关闭描述符，不等待，不追加用于“补齐”的换行。
    void abandon() noexcept;

    // 先放弃剩余内容，再恢复原始状态标志。
    // 返回 0 表示成功，否则返回 errno。
    // 析构时会再次尝试尚未成功的恢复。
    int restoreFlags() noexcept;

    bool enabled() const noexcept;
    bool empty() const noexcept;

    const JsonlWriterStats& stats() const noexcept;

private:
    enum class Kind {
        Ready,
        Status,
        Record,
        Stopped
    };

    struct Frame {
        Kind kind = Kind::Status;
        std::string run_id;
        std::uint64_t sequence = 0;
        std::string line;
    };

    static Frame parseLine(std::string line);

    std::optional<Frame> takeNext();
    void expireRecordStarts(TimePoint now) noexcept;
    void countAbandoned(const Frame& frame, bool partial) noexcept;
    void failIo(int error_code) noexcept;

    int descriptor_ = -1;
    int original_flags_ = 0;
    bool need_restore_ = false;
    bool enabled_ = true;

    bool ready_seen_ = false;
    bool stopped_seen_ = false;

    std::string run_id_;
    std::uint64_t last_sequence_ = 0;

    std::optional<Frame> ready_;
    std::optional<Frame> status_;
    std::optional<Frame> stopped_;
    std::deque<Frame> records_;

    std::optional<Frame> active_;
    std::size_t active_offset_ = 0;

    // 固定容量，避免在已经写出字节后再为限流记录分配内存。
    std::array<TimePoint, kMaxJsonlRecordsPerSecond> record_starts_{};
    std::size_t rate_head_ = 0;
    std::size_t rate_size_ = 0;

    std::optional<TimePoint> last_drain_time_;

    JsonlWriterStats stats_;
};

} // namespace edge