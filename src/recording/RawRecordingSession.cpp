#include "edge/recording/RawRecordingSession.h"

#include <algorithm>
#include <cerrno>
#include <limits>
#include <new>
#include <stdexcept>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace edge {

namespace {

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

std::string decimal20(std::uint64_t value)
{
    const auto text = std::to_string(value);

    return std::string(20 - text.size(), '0') + text;
}

} // namespace

std::string makeRawRecordingFileName(
    const std::string& run_id,
    std::uint64_t connection_id,
    std::uint64_t part_index
)
{
    if (!validRunId(run_id)
        || connection_id == 0
        || part_index == 0) {
        throw std::invalid_argument(
            "Invalid raw recording file identity"
        );
    }

    return "edge-raw-v1-"
        + run_id
        + "-c" + decimal20(connection_id)
        + "-p" + decimal20(part_index)
        + ".bin";
}

RawRecordingSession::RawRecordingSession(
    std::filesystem::path directory,
    std::string run_id,
    std::uint64_t connection_id,
    std::uint64_t max_file_bytes,
    RawClosedSegmentHandler on_closed
)
    : directory_(std::move(directory)),
      run_id_(std::move(run_id)),
      connection_id_(connection_id),
      max_file_bytes_(max_file_bytes),
      on_closed_(std::move(on_closed))
{
    if (directory_.empty()
        || !directory_.is_absolute()
        || directory_.u8string().find('\0') != std::string::npos) {
        throw std::invalid_argument(
            "Raw recording directory must be an absolute path without NUL"
        );
    }

    if (max_file_bytes_ == 0
        || max_file_bytes_
            > static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max()
            )) {
        throw std::invalid_argument(
            "Invalid raw recording file size limit"
        );
    }

    // 只检查身份，不创建文件。
    (void)makeRawRecordingFileName(
        run_id_,
        connection_id_,
        1
    );
}

RawRecordingSession::~RawRecordingSession() noexcept
{
    (void)close();
}

void RawRecordingSession::fail(
    RawRecordingOperation operation,
    int error_code
) noexcept
{
    const int saved_errno = errno;

    if (!stats_.failed) {
        stats_.failed = true;
        stats_.error_operation = operation;
        stats_.error_code = error_code;
    }

    const int descriptor = std::exchange(fd_, -1);

    if (descriptor >= 0) {
        // 故障后的资源回收，不重试 Linux close。
        (void)::close(descriptor);
    }

    active_path_.clear();
    current_bytes_ = 0;

    errno = saved_errno;
}

bool RawRecordingSession::openNext() noexcept
{
    if (part_index_
        == std::numeric_limits<std::uint64_t>::max()) {
        fail(RawRecordingOperation::Limit, EOVERFLOW);
        return false;
    }

    try {
        const auto next_part = part_index_ + 1;

        auto candidate = directory_
            / makeRawRecordingFileName(
                run_id_,
                connection_id_,
                next_part
            );

        const int descriptor = ::open(
            candidate.c_str(),
            O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW,
            0600
        );

        if (descriptor == -1) {
            const int code = errno;
            fail(RawRecordingOperation::Open, code);
            return false;
        }

        // 立即接管描述符，后续失败也能回收。
        fd_ = descriptor;
        active_path_ = std::move(candidate);

        part_index_ = next_part;
        current_bytes_ = 0;

        ++stats_.opened_files;
        return true;
    }
    catch (const std::bad_alloc&) {
        fail(RawRecordingOperation::Metadata, ENOMEM);
    }
    catch (...) {
        fail(RawRecordingOperation::Metadata, EIO);
    }

    return false;
}

bool RawRecordingSession::closeCurrent() noexcept
{
    if (fd_ < 0) {
        return true;
    }

    RawClosedSegment segment;

    try {
        segment.path = active_path_;
        segment.connection_id = connection_id_;
        segment.part_index = part_index_;
        segment.bytes = current_bytes_;
    }
    catch (const std::bad_alloc&) {
        fail(RawRecordingOperation::Metadata, ENOMEM);
        return false;
    }
    catch (...) {
        fail(RawRecordingOperation::Metadata, EIO);
        return false;
    }

    const int descriptor = std::exchange(fd_, -1);

    if (::close(descriptor) == -1) {
        const int code = errno;
        fail(RawRecordingOperation::Close, code);
        return false;
    }

    active_path_.clear();
    current_bytes_ = 0;

    ++stats_.closed_files;

    if (on_closed_) {
        try {
            on_closed_(segment);
        }
        catch (...) {
            // 文件已经关闭，通知失败不能伪装成写入仍在继续。
            fail(RawRecordingOperation::Notify, EIO);
            return false;
        }
    }

    return true;
}

bool RawRecordingSession::append(
    const std::uint8_t* data,
    std::size_t size
) noexcept
{
    if (stats_.failed || stats_.finished) {
        return false;
    }

    if (size == 0) {
        return true;
    }

    if (data == nullptr) {
        fail(RawRecordingOperation::Input, EINVAL);
        return false;
    }

    if (size
        > std::numeric_limits<std::uint64_t>::max()
            - stats_.bytes_written) {
        fail(RawRecordingOperation::Limit, EOVERFLOW);
        return false;
    }

    std::size_t offset = 0;
    unsigned consecutive_interruptions = 0;

    while (offset < size) {
        if (fd_ < 0 && !openNext()) {
            return false;
        }

        const auto length = std::min<std::uint64_t>({
            static_cast<std::uint64_t>(size - offset),
            max_file_bytes_ - current_bytes_,
            static_cast<std::uint64_t>(
                std::numeric_limits<ssize_t>::max()
            )
        });

        const auto written = ::write(
            fd_,
            data + offset,
            static_cast<std::size_t>(length)
        );

        if (written < 0) {
            const int code = errno;

            if (code == EINTR
                && ++consecutive_interruptions < 32) {
                continue;
            }

            fail(RawRecordingOperation::Write, code);
            return false;
        }

        if (written == 0) {
            fail(RawRecordingOperation::Write, EIO);
            return false;
        }

        consecutive_interruptions = 0;

        const auto count =
            static_cast<std::uint64_t>(written);

        offset += static_cast<std::size_t>(written);
        current_bytes_ += count;
        stats_.bytes_written += count;

        // 达到上限就关闭；下次确实还有字节时才创建下一文件。
        if (current_bytes_ == max_file_bytes_) {
            if (!closeCurrent()) {
                return false;
            }
        }
    }

    return true;
}

bool RawRecordingSession::close() noexcept
{
    if (stats_.finished) {
        return !stats_.failed;
    }

    stats_.finished = true;

    if (stats_.failed) {
        return false;
    }

    return closeCurrent();
}

const RawRecordingSessionStats&
RawRecordingSession::stats() const noexcept
{
    return stats_;
}

const std::filesystem::path&
RawRecordingSession::activePath() const noexcept
{
    return active_path_;
}

} // namespace edge