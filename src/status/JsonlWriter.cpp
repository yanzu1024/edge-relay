#include "edge/status/JsonlWriter.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <cerrno>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <signal.h>
#include <unistd.h>

namespace edge {

namespace {

using Json = nlohmann::json;

bool validRunId(const std::string& value)
{
    return value.size() == 32
        && std::all_of(
            value.begin(),
            value.end(),
            [](char character) {
                return (character >= '0' && character <= '9')
                    || (character >= 'a' && character <= 'f');
            }
        );
}

} // namespace

JsonlWriter::JsonlWriter(int descriptor)
    : descriptor_(descriptor)
{
    original_flags_ = ::fcntl(descriptor_, F_GETFL);

    if (original_flags_ == -1) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "read JSONL descriptor flags"
        );
    }

    if ((original_flags_ & O_ACCMODE) == O_RDONLY) {
        throw std::invalid_argument(
            "JSONL descriptor is not writable"
        );
    }

    struct sigaction action{};

    if (::sigaction(SIGPIPE, nullptr, &action) == -1) {
        throw std::system_error(
            errno,
            std::generic_category(),
            "read SIGPIPE disposition"
        );
    }

    if (action.sa_handler != SIG_IGN) {
        throw std::logic_error(
            "JSONL writer requires SIGPIPE to be ignored by its owner"
        );
    }

    if ((original_flags_ & O_NONBLOCK) == 0) {
        if (::fcntl(
                descriptor_,
                F_SETFL,
                original_flags_ | O_NONBLOCK
            ) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "enable nonblocking JSONL output"
            );
        }

        need_restore_ = true;
    }
}

JsonlWriter::~JsonlWriter() noexcept
{
    (void)restoreFlags();
}

JsonlWriter::Frame JsonlWriter::parseLine(std::string line)
{
    if (line.empty()
        || line.size() > kMaxJsonlLineBytes
        || line.back() != '\n'
        || line.find('\n') != line.size() - 1
        || line.find('\r') != std::string::npos) {
        throw std::invalid_argument(
            "JSONL input must be one complete bounded line"
        );
    }

    Json message;

    try {
        message = Json::parse(line);
    }
    catch (const Json::exception&) {
        throw std::invalid_argument(
            "Malformed JSONL input"
        );
    }

    if (!message.is_object()) {
        throw std::invalid_argument(
            "JSONL input must contain an object"
        );
    }

    const std::array<const char*, 6> required{
        "protocol_version",
        "type",
        "run_id",
        "sequence",
        "emitted_at_ms",
        "data"
    };

    for (const auto* name : required) {
        if (!message.contains(name)) {
            throw std::invalid_argument(
                "Missing JSONL envelope field"
            );
        }
    }

    if (!message["protocol_version"].is_number_integer()
        || message["protocol_version"] != kJsonlProtocolVersion
        || !message["type"].is_string()
        || !message["run_id"].is_string()
        || !message["sequence"].is_number_unsigned()
        || !message["emitted_at_ms"].is_number_unsigned()
        || !message["data"].is_object()) {
        throw std::invalid_argument(
            "Invalid JSONL envelope field types"
        );
    }

    Frame frame;

    frame.run_id = message["run_id"].get<std::string>();
    frame.sequence = message["sequence"].get<std::uint64_t>();

    const auto timestamp =
        message["emitted_at_ms"].get<std::uint64_t>();

    if (!validRunId(frame.run_id)
        || frame.sequence == 0
        || timestamp == 0
        || timestamp > static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max()
        )) {
        throw std::invalid_argument(
            "Invalid JSONL envelope values"
        );
    }

    const auto type = message["type"].get<std::string>();

    if (type == "ready") {
        frame.kind = Kind::Ready;
    } else if (type == "status") {
        frame.kind = Kind::Status;
    } else if (type == "record") {
        frame.kind = Kind::Record;
    } else if (type == "stopped") {
        frame.kind = Kind::Stopped;
    } else {
        throw std::invalid_argument(
            "Unknown JSONL message type"
        );
    }

    frame.line = std::move(line);
    return frame;
}

JsonlEnqueueResult JsonlWriter::enqueue(std::string line)
{
    auto frame = parseLine(std::move(line));

    if (!ready_seen_) {
        if (frame.kind != Kind::Ready || frame.sequence != 1) {
            throw std::invalid_argument(
                "JSONL stream must begin with ready sequence 1"
            );
        }

        run_id_ = frame.run_id;
    } else {
        if (stopped_seen_) {
            throw std::invalid_argument(
                "Cannot enqueue after stopped"
            );
        }

        if (frame.kind == Kind::Ready
            || frame.run_id != run_id_
            || frame.sequence <= last_sequence_) {
            throw std::invalid_argument(
                "JSONL run ID or sequence order is inconsistent"
            );
        }
    }

    ready_seen_ = true;
    last_sequence_ = frame.sequence;

    if (frame.kind == Kind::Stopped) {
        stopped_seen_ = true;
    }

    // 即使消息被丢弃，序号也不能重新使用。
    if (!enabled_) {
        countAbandoned(frame, false);
        return JsonlEnqueueResult::Disabled;
    }

    switch (frame.kind) {
    case Kind::Ready:
        ready_ = std::move(frame);
        return JsonlEnqueueResult::Queued;

    case Kind::Status: {
        const bool replaced = status_.has_value();

        if (replaced) {
            ++stats_.status_coalesced;
        }

        status_ = std::move(frame);

        return replaced
            ? JsonlEnqueueResult::Coalesced
            : JsonlEnqueueResult::Queued;
    }

    case Kind::Record:
        if (records_.size() >= kMaxPendingJsonlRecords) {
            ++stats_.display_dropped;
            ++stats_.record_queue_drops;
            return JsonlEnqueueResult::Dropped;
        }

        records_.push_back(std::move(frame));
        return JsonlEnqueueResult::Queued;

    case Kind::Stopped:
        stopped_ = std::move(frame);
        return JsonlEnqueueResult::Queued;
    }

    throw std::logic_error("Unreachable JSONL kind");
}

std::optional<JsonlWriter::Frame> JsonlWriter::takeNext()
{
    std::optional<Frame>* selected = nullptr;

    const auto consider = [&](std::optional<Frame>& candidate) {
        if (candidate.has_value()
            && (selected == nullptr
                || candidate->sequence < (*selected)->sequence)) {
            selected = &candidate;
        }
    };

    consider(ready_);
    consider(status_);
    consider(stopped_);

    // 在剩余消息中按序号选择，避免状态合并造成输出倒序。
    if (!records_.empty()
        && (selected == nullptr
            || records_.front().sequence < (*selected)->sequence)) {
        Frame frame = std::move(records_.front());
        records_.pop_front();

        return std::optional<Frame>{std::move(frame)};
    }

    if (selected != nullptr) {
        Frame frame = std::move(**selected);
        selected->reset();

        return std::optional<Frame>{std::move(frame)};
    }

    return std::nullopt;
}

void JsonlWriter::expireRecordStarts(TimePoint now) noexcept
{
    while (rate_size_ > 0
        && now - record_starts_[rate_head_]
            >= std::chrono::seconds(1)) {
        rate_head_ = (rate_head_ + 1) % record_starts_.size();
        --rate_size_;
    }
}

JsonlDrainResult JsonlWriter::drain(
    TimePoint now,
    std::size_t byte_budget
)
{
    if (!enabled_) {
        return JsonlDrainResult::Disabled;
    }

    if (last_drain_time_.has_value()
        && now < *last_drain_time_) {
        throw std::invalid_argument(
            "JSONL drain requires monotonic time"
        );
    }

    last_drain_time_ = now;
    expireRecordStarts(now);

    byte_budget = std::min(
        byte_budget,
        kMaxJsonlLineBytes
    );

    for (std::size_t operation = 0;
         operation < kMaxJsonlDrainOperations
             && byte_budget > 0;
         ++operation) {
        if (!active_.has_value()) {
            active_ = takeNext();
            active_offset_ = 0;

            if (!active_.has_value()) {
                return JsonlDrainResult::Idle;
            }
        }

        auto& frame = *active_;

        if (frame.kind == Kind::Record
            && active_offset_ == 0
            && rate_size_ >= record_starts_.size()) {
            ++stats_.display_dropped;
            ++stats_.record_rate_drops;

            active_.reset();
            continue;
        }

        const auto length = std::min(
            frame.line.size() - active_offset_,
            byte_budget
        );

        const auto written = ::write(
            descriptor_,
            frame.line.data() + active_offset_,
            length
        );

        if (written < 0) {
            const int error_code = errno;

            if (error_code == EINTR) {
                // 本次操作预算已经消耗，不无限重试。
                continue;
            }

            if (error_code == EAGAIN
                || error_code == EWOULDBLOCK) {
                return JsonlDrainResult::WouldBlock;
            }

            failIo(error_code);
            return JsonlDrainResult::Disabled;
        }

        if (written == 0) {
            failIo(EIO);
            return JsonlDrainResult::Disabled;
        }

        if (frame.kind == Kind::Record
            && active_offset_ == 0) {
            const auto position =
                (rate_head_ + rate_size_)
                % record_starts_.size();

            record_starts_[position] = now;
            ++rate_size_;
        }

        const auto count = static_cast<std::size_t>(written);

        active_offset_ += count;
        byte_budget -= count;
        stats_.written_bytes += count;

        if (active_offset_ == frame.line.size()) {
            ++stats_.completed_lines;

            active_.reset();
            active_offset_ = 0;
        }
    }

    return empty()
        ? JsonlDrainResult::Idle
        : JsonlDrainResult::Pending;
}

void JsonlWriter::countAbandoned(
    const Frame& frame,
    bool partial
) noexcept
{
    ++stats_.abandoned_lines;

    if (frame.kind == Kind::Record) {
        ++stats_.display_dropped;
    }

    if (partial) {
        ++stats_.partial_lines_abandoned;
    }
}

void JsonlWriter::abandon() noexcept
{
    enabled_ = false;

    if (active_.has_value()) {
        countAbandoned(*active_, active_offset_ > 0);
        active_.reset();
    }

    active_offset_ = 0;

    const auto discard = [&](std::optional<Frame>& frame) {
        if (frame.has_value()) {
            countAbandoned(*frame, false);
            frame.reset();
        }
    };

    discard(ready_);
    discard(status_);
    discard(stopped_);

    for (const auto& frame : records_) {
        countAbandoned(frame, false);
    }

    records_.clear();
}

void JsonlWriter::failIo(int error_code) noexcept
{
    ++stats_.io_errors;
    stats_.last_error = error_code;

    abandon();
}

int JsonlWriter::restoreFlags() noexcept
{
    abandon();

    if (!need_restore_) {
        return 0;
    }

    if (::fcntl(
            descriptor_,
            F_SETFL,
            original_flags_
        ) == -1) {
        const int error_code = errno;

        ++stats_.io_errors;
        stats_.last_error = error_code;

        return error_code;
    }

    need_restore_ = false;
    return 0;
}

bool JsonlWriter::enabled() const noexcept
{
    return enabled_;
}

bool JsonlWriter::empty() const noexcept
{
    return !active_.has_value()
        && !ready_.has_value()
        && !status_.has_value()
        && !stopped_.has_value()
        && records_.empty();
}

const JsonlWriterStats& JsonlWriter::stats() const noexcept
{
    return stats_;
}

} // namespace edge