#include "edge/protocol/StreamDecoder.h"

#include <algorithm>
#include <stdexcept>

#include "edge/protocol/FrameFormat.h"

namespace edge {

namespace {

constexpr std::size_t kFeedChunkBytes = 4096;

// 找不到完整 magic 时，只保留末尾可能属于其开头的字节。
std::size_t possibleMagicPrefixSize(
    const std::vector<std::uint8_t>& buffer,
    std::size_t offset
)
{
    const auto available = buffer.size() - offset;

    const auto max_keep = std::min(
        available,
        kFrameMagic.size() - 1
    );

    for (std::size_t keep = max_keep; keep > 0; --keep) {
        if (std::equal(
                buffer.end() - keep,
                buffer.end(),
                kFrameMagic.begin())) {
            return keep;
        }
    }

    return 0;
}

} // namespace

StreamDecoder::StreamDecoder(
    std::chrono::milliseconds partial_timeout,
    std::size_t max_payload_bytes
)
    : partial_timeout_(partial_timeout),
      max_payload_bytes_(max_payload_bytes)
{
    if (partial_timeout_.count() <= 0) {
        throw std::invalid_argument(
            "StreamDecoder partial timeout must be positive"
        );
    }

    // 配置只能收紧协议限制，不能突破协议硬上限。
    if (max_payload_bytes_ == 0
        || max_payload_bytes_ > kMaxFramePayloadBytes) {
        throw std::invalid_argument(
            "StreamDecoder payload limit must be between 1 and 8192"
        );
    }
}

std::vector<std::string> StreamDecoder::feed(
    const std::uint8_t* data,
    std::size_t size,
    TimePoint now
)
{
    if (size > 0 && data == nullptr) {
        throw std::invalid_argument(
            "StreamDecoder::feed requires non-null data when size > 0"
        );
    }

    std::vector<std::string> payloads;

    // 先检查原来等待的候选是否过期。
    // 避免迟到的数据补全一个已经超时的旧候选。
    extractFrames(payloads, now);

    while (size > 0) {
        const auto count = std::min(size, kFeedChunkBytes);

        buffer_.insert(
            buffer_.end(),
            data,
            data + count
        );

        extractFrames(payloads, now);

        data += count;
        size -= count;
    }

    return payloads;
}

std::vector<std::string> StreamDecoder::checkTimeout(
    TimePoint now
)
{
    // 不追加新字节，只执行已有缓冲的检查和解析。
    return feed(nullptr, 0, now);
}

bool StreamDecoder::partialFrameExpired(TimePoint now){
    if (!partial_since_) {
        partial_since_ = now;
        return false;
    }

    if (now - *partial_since_ < partial_timeout_) {
        return false;
    }

    ++stats_.partial_timeouts;

    partial_since_.reset();

    return true;
}

void StreamDecoder::extractFrames(
    std::vector<std::string>& payloads,
    TimePoint now
){
    std::size_t read_offset = 0;

    while (read_offset < buffer_.size()) {
        const auto magic_position = std::search(
            buffer_.begin() + read_offset,
            buffer_.end(),
            kFrameMagic.begin(),
            kFrameMagic.end()
        );

        if (magic_position == buffer_.end()) {
            partial_since_.reset();

            const auto keep = possibleMagicPrefixSize(
                buffer_,
                read_offset
            );

            // 只统计本次真正丢弃的未处理字节。
            // 可能属于 magic 前缀的末尾字节继续保留。
            const auto discard =
                buffer_.size() - keep - read_offset;

            stats_.resync_bytes += discard;

            read_offset = buffer_.size() - keep;
            break;
        }

        const auto magic_offset = static_cast<std::size_t>(
            magic_position - buffer_.begin()
        );

        // 搜索到的 magic 之前是噪声。
        stats_.resync_bytes += magic_offset - read_offset;
        read_offset = magic_offset;

        const auto available = buffer_.size() - read_offset;

        if (available < kFrameHeaderSize) {
            if (partialFrameExpired(now)) {
                // 超时后先放弃候选起点，再继续搜索。
                ++stats_.resync_bytes;
                ++read_offset;
                continue;
            }

            break;
        }

        const std::uint32_t payload_length =
            (static_cast<std::uint32_t>(
                buffer_[read_offset + 4]) << 24U)
            | (static_cast<std::uint32_t>(
                buffer_[read_offset + 5]) << 16U)
            | (static_cast<std::uint32_t>(
                buffer_[read_offset + 6]) << 8U)
            | static_cast<std::uint32_t>(
                buffer_[read_offset + 7]);

        if (payload_length == 0
            || payload_length > max_payload_bytes_) {
            ++stats_.invalid_lengths;

            partial_since_.reset();

            // 当前只明确丢弃一个字节。
            // 其余字节在后续搜索中按实际丢弃数量统计。
            ++stats_.resync_bytes;
            ++read_offset;
            continue;
        }

        const auto frame_size =
            kFrameHeaderSize
            + static_cast<std::size_t>(payload_length);

        if (available < frame_size) {
            if (partialFrameExpired(now)) {
                ++stats_.resync_bytes;
                ++read_offset;
                continue;
            }

            break;
        }

        partial_since_.reset();

        payloads.emplace_back(
            reinterpret_cast<const char*>(
                buffer_.data() + read_offset + kFrameHeaderSize
            ),
            static_cast<std::size_t>(payload_length)
        );

        ++stats_.completed_frames;

        // 正常消费的帧不属于重同步丢弃。
        read_offset += frame_size;
    }

    if (read_offset > 0) {
        buffer_.erase(
            buffer_.begin(),
            buffer_.begin() + read_offset
        );
    }
}

void StreamDecoder::reset() noexcept
{
    buffer_.clear();
    partial_since_.reset();
}

std::size_t StreamDecoder::bufferedBytes() const noexcept
{
    return buffer_.size();
}

std::uint64_t StreamDecoder::partialTimeoutCount() const noexcept
{
    return stats_.partial_timeouts;
}

StreamDecoderStats StreamDecoder::stats() const noexcept
{
    return stats_;
}

} // namespace edge