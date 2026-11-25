#include "edge/status/StatusOutput.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <unistd.h>

namespace edge {

namespace {

namespace fs = std::filesystem;

template <typename T, typename Query>
StatusSample<T> captureSample(
    std::int64_t checked_at_ms,
    Query query
)
{
    StatusSample<T> sample;
    sample.checked_at_ms = checked_at_ms;

    try {
        sample.value = query();
        sample.state = StatusSampleState::Valid;
    }
    catch (const std::exception& error) {
        sample.value.reset();
        sample.state = StatusSampleState::Error;
        sample.error = std::string("Query failed: ") + error.what();
    }
    catch (...) {
        sample.value.reset();
        sample.state = StatusSampleState::Error;
        sample.error = "Query failed: unknown exception";
    }

    return sample;
}

std::uint64_t queryDiskAvailable(const fs::path& path)
{
    const auto information = fs::space(path);

    // filesystem::space 用 uintmax_t(-1) 表达无法确定的值。
    // 不能把它当成“磁盘空间非常充足”。
    if (information.available
        == std::numeric_limits<std::uintmax_t>::max()) {
        throw std::runtime_error(
            "Filesystem available space is unknown"
        );
    }

    if (information.available
        > std::numeric_limits<std::uint64_t>::max()) {
        throw std::overflow_error(
            "Filesystem available space exceeds uint64_t"
        );
    }

    return static_cast<std::uint64_t>(
        information.available
    );
}

void validateAbsoluteFilePath(const fs::path& path)
{
    if (path.empty()
        || !path.is_absolute()
        || !path.has_filename()
        || path.filename() == "."
        || path.filename() == ".."
        || path.u8string().find('\0') != std::string::npos) {
        throw std::invalid_argument(
            "Status output requires absolute file paths without NUL"
        );
    }
}

void protectOutboxPaths(
    const fs::path& status_path,
    const fs::path& outbox_path
)
{
    validateAbsoluteFilePath(status_path);
    validateAbsoluteFilePath(outbox_path);

    const auto status_resolved = fs::weakly_canonical(status_path);
    const auto outbox_resolved = fs::weakly_canonical(outbox_path);

    // 同时考虑配置路径和解析符号链接后的数据库路径。
    const std::array<fs::path, 2> bases{
        outbox_path,
        outbox_resolved
    };

    const std::array<const char*, 5> suffixes{
        "",
        ".lock",
        "-journal",
        "-wal",
        "-shm"
    };

    for (const auto& base : bases) {
        for (const auto* suffix : suffixes) {
            auto protected_path = base;
            protected_path += suffix;

            if (status_resolved
                == fs::weakly_canonical(protected_path)) {
                throw std::invalid_argument(
                    "Status file must not replace Outbox data or sidecars"
                );
            }

            // canonical 不能识别不同文件名指向同一 inode 的硬链接。
            if (fs::exists(status_path)
                && fs::exists(protected_path)
                && fs::equivalent(status_path, protected_path)) {
                throw std::invalid_argument(
                    "Status file aliases Outbox data or sidecars"
                );
            }
        }
    }
}

class TemporaryStatusFile {
public:
    explicit TemporaryStatusFile(std::string name_template)
        : name_(std::move(name_template))
    {
        // mkostemp 排他创建，权限为 0600。
        // edge-core 已经定义 _GNU_SOURCE。
        fd_ = ::mkostemp(name_.data(), O_CLOEXEC);

        if (fd_ == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "create temporary status file"
            );
        }
    }

    ~TemporaryStatusFile() noexcept
    {
        const int saved_errno = errno;

        if (fd_ >= 0) {
            (void)::close(fd_);
        }

        if (!published_) {
            (void)::unlink(name_.c_str());
        }

        errno = saved_errno;
    }

    TemporaryStatusFile(const TemporaryStatusFile&) = delete;

    TemporaryStatusFile& operator=(
        const TemporaryStatusFile&
    ) = delete;

    void writeAll(const std::string& contents)
    {
        std::size_t offset = 0;

        while (offset < contents.size()) {
            const auto length = std::min(
                contents.size() - offset,
                static_cast<std::size_t>(
                    std::numeric_limits<ssize_t>::max()
                )
            );

            const auto written = ::write(
                fd_,
                contents.data() + offset,
                length
            );

            if (written < 0) {
                if (errno == EINTR) {
                    continue;
                }

                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "write temporary status file"
                );
            }

            if (written == 0) {
                throw std::system_error(
                    EIO,
                    std::generic_category(),
                    "status file write made no progress"
                );
            }

            offset += static_cast<std::size_t>(written);
        }
    }

    void closeChecked()
    {
        const int descriptor = std::exchange(fd_, -1);

        // Linux 的 close 失败后也不能盲目重试，
        // 以免关闭一个后来复用的描述符。
        if (::close(descriptor) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "close temporary status file"
            );
        }
    }

    void publish(const fs::path& destination)
    {
        if (::rename(
                name_.c_str(),
                destination.c_str()
            ) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "rename status file"
            );
        }

        // rename 后原临时名称已释放，不再尝试 unlink 它。
        published_ = true;
    }

private:
    std::string name_;
    int fd_ = -1;
    bool published_ = false;
};

} // namespace

void sampleStatusResources(
    StatusSnapshot& snapshot,
    OutboxStore& store,
    const std::filesystem::path& outbox_path,
    StatusDiskQuery disk_query
)
{
    if (snapshot.checked_at_ms <= 0) {
        throw std::invalid_argument(
            "Resource sampling requires a positive timestamp"
        );
    }

    snapshot.outbox = captureSample<OutboxStateCounts>(
        snapshot.checked_at_ms,
        [&store] {
            return store.stateCounts();
        }
    );

    snapshot.disk_available_bytes =
        captureSample<std::uint64_t>(
            snapshot.checked_at_ms,
            [&] {
                return disk_query
                    ? disk_query(outbox_path)
                    : queryDiskAvailable(outbox_path);
            }
        );
}

void writeStatusFileAtomically(
    const std::filesystem::path& status_path,
    const std::filesystem::path& outbox_path,
    const std::string& contents
)
{
    protectOutboxPaths(status_path, outbox_path);

    fs::create_directories(status_path.parent_path());

    const auto temporary_path =
        status_path.parent_path()
        / (
            "." + status_path.filename().string()
            + ".tmp.XXXXXX"
        );

    TemporaryStatusFile temporary(
        temporary_path.string()
    );

    temporary.writeAll(contents);
    temporary.closeChecked();
    temporary.publish(status_path);
}

} // namespace edge