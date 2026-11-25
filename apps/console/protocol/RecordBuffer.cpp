#include "RecordBuffer.h"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace edge::console {

void RecordBuffer::push(JsonlMessage message)
{
    if (message.kind != JsonlMessageKind::Record) {
        throw std::invalid_argument(
            "RecordBuffer accepts record messages only."
        );
    }

    // 先插入成功，再淘汰旧消息。
    // 插入失败时不会提前丢掉已有记录。
    records_.push_back(std::move(message));

    if (records_.size() > kCapacity) {
        records_.pop_front();

        if (dropped_
            != std::numeric_limits<std::uint64_t>::max()) {
            ++dropped_;
        }
    }
}

std::vector<JsonlMessage> RecordBuffer::take(
    std::size_t maximum
)
{
    const std::size_t count =
        std::min(maximum, records_.size());

    std::vector<JsonlMessage> result;

    // maximum 再大，也只按实际缓存数量分配。
    result.reserve(count);

    for (std::size_t index = 0; index < count; ++index) {
        result.push_back(
            std::move(records_.front())
        );

        records_.pop_front();
    }

    return result;
}

void RecordBuffer::reset() noexcept
{
    records_.clear();
    dropped_ = 0;
}

std::size_t RecordBuffer::size() const noexcept
{
    return records_.size();
}

std::uint64_t RecordBuffer::dropped() const noexcept
{
    return dropped_;
}

} // namespace edge::console