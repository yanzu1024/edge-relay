#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>
#include <string>
#include <chrono>
#include <optional>

#include "edge/protocol/FrameFormat.h"


namespace edge {

struct StreamDecoderStats{
    std::uint64_t completed_frames = 0;
    std::uint64_t invalid_lengths = 0;
    std::uint64_t resync_bytes = 0;
    std::uint64_t partial_timeouts = 0;
};


// 有状态的流式解码器，支持分块解码
// 每条 TCP 连接应拥有独立实例, 避免不同连接的数据混在一起, 导致多线程并发访问
class StreamDecoder{
public:

    // 超时使用单调时钟, 不受系统日期调整影响
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    explicit StreamDecoder(
        std::chrono::milliseconds partial_timeout = 
            std::chrono::milliseconds{3000},
        std::size_t max_payload_bytes = kMaxFramePayloadBytes
    );

    // 输入一段字节, 返回解码出的完整数据块, 不完整的数据块会被缓存, 保留到下一次 feed
    std::vector<std::string> feed(
        const std::uint8_t* data,
        // size 为 0 时允许 data 为空, size > 0 时 data 不允许为空, 否则抛出 std::invalid_argument
        std::size_t size,
        TimePoint now = Clock::now()    // 同一实例传入的时间应按单调顺序推进
    );

    // 没有新数据时, 检查超时, 超时重同步后, 可能发现缓冲区内已有的完整帧
    // 因此调用方也需要处理这里返回的载荷
    std::vector<std::string> checkTimeout(
        TimePoint now = Clock::now()
    );

    // 清除半帧和等待时间, 保留累计超时次数
    void reset() noexcept;

    // 当前保留的尚未完整处理的数据块的总字节数, 用于判断缓存是否过大
    std::size_t bufferedBytes() const noexcept;
    std::uint64_t partialTimeoutCount() const noexcept; // 保留原有接口, 已有超时测试仍可继续使用

    StreamDecoderStats stats() const noexcept;

private:
    // 从缓冲区尽可能提取完整数据块, 返回提取出的完整数据块, 并从缓冲区移除这些数据块
    void extractFrames(
        std::vector<std::string>& payloads,
        TimePoint now
    );

    // 首次等待时记录起点, 达到截止时间时计数并返回 true
    bool partialFrameExpired(TimePoint now);

    std::vector<std::uint8_t> buffer_;  // 缓冲区, 用于存储尚未完整处理的数据块

    std::chrono::milliseconds partial_timeout_;

    // 已经过范围检查的实际载荷上限
    std::size_t max_payload_bytes_;
    
    std::optional<TimePoint> partial_since_;

    StreamDecoderStats stats_;
};

}   // namespace edge