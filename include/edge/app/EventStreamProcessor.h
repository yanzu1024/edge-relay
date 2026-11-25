#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include "edge/domain/EventJson.h"
#include "edge/protocol/StreamDecoder.h"
#include "edge/validation/EventValidator.h"
#include "edge/config/AgentConfig.h"
#include "edge/protocol/FrameFormat.h"


namespace edge {

// 每个完整帧对应一种处理结果。
// 同时保留 JSON 错误和业务验证错误的具体类型。
using EventProcessResult = std::variant<
    SensorEvent,
    EventJsonError,
    EventValidationError
>;

using EventProcessBatch = std::vector<EventProcessResult>;

struct EventStreamStats {
    StreamDecoderStats framing;

    std::uint64_t valid_events = 0;
    std::uint64_t json_errors = 0;
    std::uint64_t validation_errors = 0;
};

// 组合帧解码、JSON 解析和业务验证。
// 后续每条 TCP 连接可以拥有一个独立实例。
class EventStreamProcessor {
public:
    using Clock = StreamDecoder::Clock;
    using TimePoint = StreamDecoder::TimePoint;

    explicit EventStreamProcessor(
        std::chrono::milliseconds partial_timeout =
            std::chrono::milliseconds{3000},
        std::size_t max_payload_bytes = kMaxFramePayloadBytes
    );

    // 从 TCP 配置中取得解码参数
    explicit EventStreamProcessor(const TcpConfig& config);

    // now_ms：Unix 毫秒时间，用于检查事件观测时间。
    // steady_now：单调时间，用于检查半帧超时。
    EventProcessBatch feed(
        const std::uint8_t* data,
        std::size_t size,
        std::int64_t now_ms,
        TimePoint steady_now = Clock::now()
    );

    // 超时重同步可能释放出完整帧，也需要执行解析和验证。
    EventProcessBatch checkTimeout(
        std::int64_t now_ms,
        TimePoint steady_now = Clock::now()
    );

    // 清除连接中的半帧状态，保留累计统计。
    void reset() noexcept;

    std::size_t bufferedBytes() const noexcept;
    EventStreamStats stats() const noexcept;

private:
    EventProcessBatch processPayloads(
        const std::vector<std::string>& payloads,
        std::int64_t now_ms
    );

    StreamDecoder decoder_;

    std::uint64_t valid_events_ = 0;
    std::uint64_t json_errors_ = 0;
    std::uint64_t validation_errors_ = 0;
};

} // namespace edge