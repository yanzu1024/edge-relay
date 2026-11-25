#include "edge/app/EventStreamProcessor.h"

#include <utility>
#include <stdexcept>

namespace edge {

namespace {

// TcpConfig 使用有符号整数保存配置输入。
// 先检查范围，再转换为 size_t，避免负数转换成巨大无符号值。
std::size_t checkedPayloadLimit(std::int32_t value)
{
    if (value < 1
        || value > static_cast<std::int32_t>(kMaxFramePayloadBytes)) {
        throw std::invalid_argument(
            "TCP payload limit must be between 1 and 8192"
        );
    }

    return static_cast<std::size_t>(value);
}

} // namespace

EventStreamProcessor::EventStreamProcessor(
    std::chrono::milliseconds partial_timeout,
    std::size_t max_payload_bytes
)
    : decoder_(partial_timeout, max_payload_bytes)
{
}

EventStreamProcessor::EventStreamProcessor(
    const TcpConfig& config
)
    : EventStreamProcessor(
          std::chrono::milliseconds{config.partial_timeout_ms},
          checkedPayloadLimit(config.max_payload_bytes)
      )
{
}

EventProcessBatch EventStreamProcessor::feed(
    const std::uint8_t* data,
    std::size_t size,
    std::int64_t now_ms,
    TimePoint steady_now
)
{
    const auto payloads = decoder_.feed(
        data,
        size,
        steady_now
    );

    return processPayloads(payloads, now_ms);
}

EventProcessBatch EventStreamProcessor::checkTimeout(
    std::int64_t now_ms,
    TimePoint steady_now
)
{
    const auto payloads = decoder_.checkTimeout(steady_now);

    return processPayloads(payloads, now_ms);
}

EventProcessBatch EventStreamProcessor::processPayloads(
    const std::vector<std::string>& payloads,
    std::int64_t now_ms
)
{
    EventProcessBatch results;

    // 每个完整载荷产生一个结果，提前预留容量。
    results.reserve(payloads.size());

    for (const auto& payload : payloads) {
        auto parsed = parseSensorEventJson(payload);

        if (const auto* error =
                std::get_if<EventJsonError>(&parsed)) {
            results.emplace_back(*error);
            ++json_errors_;

            // 当前载荷失败，继续处理后面的载荷。
            continue;
        }

        auto& event = std::get<SensorEvent>(parsed);

        if (const auto error =
                validateSensorEvent(event, now_ms)) {
            results.emplace_back(*error);
            ++validation_errors_;
            continue;
        }

        // 验证通过后，将事件移动到返回结果中。
        // 此后不再使用 parsed 中的这个事件。
        results.emplace_back(std::move(event));
        ++valid_events_;
    }

    return results;
}

void EventStreamProcessor::reset() noexcept
{
    decoder_.reset();
}

std::size_t EventStreamProcessor::bufferedBytes() const noexcept
{
    return decoder_.bufferedBytes();
}

EventStreamStats EventStreamProcessor::stats() const noexcept
{
    return EventStreamStats{
        decoder_.stats(),
        valid_events_,
        json_errors_,
        validation_errors_
    };
}

} // namespace edge