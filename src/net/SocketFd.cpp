#include "edge/net/SocketFd.h"

#include <cerrno>
#include <utility>

#include <unistd.h>

namespace edge {

SocketFd::SocketFd(int fd) noexcept
    : fd_(fd >= 0 ? fd : -1)
{
}

SocketFd::~SocketFd() noexcept
{
    reset();
}

SocketFd::SocketFd(SocketFd&& other) noexcept
    : fd_(other.release())
{
}

SocketFd& SocketFd::operator=(SocketFd&& other) noexcept
{
    // 自移动时保留原来的资源。
    if (this != &other) {
        // 先从 other 取得所有权；
        // reset 会关闭本对象原来持有的资源。
        reset(other.release());
    }

    return *this;
}

int SocketFd::get() const noexcept
{
    return fd_;
}

bool SocketFd::valid() const noexcept
{
    return fd_ >= 0;
}

SocketFd::operator bool() const noexcept
{
    return valid();
}

int SocketFd::release() noexcept
{
    // 返回原值，同时将当前对象清空。
    return std::exchange(fd_, -1);
}

void SocketFd::reset(int new_fd) noexcept
{
    // 统一所有空状态的表示。
    if (new_fd < 0) {
        new_fd = -1;
    }

    // 避免 reset(get()) 关闭自身仍要持有的描述符。
    if (new_fd == fd_) {
        return;
    }

    const int old_fd = std::exchange(fd_, new_fd);

    if (old_fd >= 0) {
        // 自动清理不能覆盖调用方正在处理的系统调用错误。
        const int saved_errno = errno;

        // Linux 上 close 失败后也不能盲目重试，
        // 包括 EINTR；原描述符编号可能已经被复用。
        ::close(old_fd);

        errno = saved_errno;
    }
}

} // namespace edge