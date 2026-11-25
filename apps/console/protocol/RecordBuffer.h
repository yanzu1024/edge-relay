#ifndef EDGE_CONSOLE_RECORD_BUFFER_H
#define EDGE_CONSOLE_RECORD_BUFFER_H

#include "JsonlMessageParser.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <vector>

namespace edge::console {

// 同一线程使用。
// 输入应当来自 JsonlReceiver 已经校验通过的 record 消息。
class RecordBuffer final {
public:
    static constexpr std::size_t kCapacity = 64;

    RecordBuffer() = default;

    RecordBuffer(const RecordBuffer&) = delete;
    RecordBuffer& operator=(const RecordBuffer&) = delete;

    void push(JsonlMessage message);

    // 按接收顺序取走最多 maximum 条消息。
    [[nodiscard]]
    std::vector<JsonlMessage> take(std::size_t maximum);

    void reset() noexcept;

    [[nodiscard]]
    std::size_t size() const noexcept;

    [[nodiscard]]
    std::uint64_t dropped() const noexcept;

private:
    std::deque<JsonlMessage> records_;

    std::uint64_t dropped_ = 0;
};

} // namespace edge::console

#endif