#include "edge/net/TcpConnections.h"

#include "edge/config/AgentConfig.h"

#include <cerrno>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <sys/socket.h>

namespace edge {

namespace {

std::size_t checkedLimit(std::int32_t limit)
{
    if (limit < 1 || limit > kMaxTcpConnections) {
        throw std::invalid_argument(
            "max_connections must be between 1 and " +
            std::to_string(kMaxTcpConnections)
        );
    }

    return static_cast<std::size_t>(limit);
}

bool canRetryAccept(int error_code) noexcept
{
    switch (error_code) {
    case EINTR:
    case ECONNABORTED:
    case ENETDOWN:
    case EPROTO:
    case ENOPROTOOPT:
    case EHOSTDOWN:
    case ENONET:
    case EHOSTUNREACH:
    case EOPNOTSUPP:
    case ENETUNREACH:
        return true;

    default:
        return false;
    }
}

} // namespace

TcpConnections::TcpConnections(std::int32_t max_connections)
    : slots_(checkedLimit(max_connections))
{
    // 每个元素默认构造为空 SocketFd。
    // 后续只替换槽位内容，不改变槽位数量。
}

TcpAcceptResult TcpConnections::acceptOne(int listening_fd)
{
    // 新连接需要显式设置非阻塞和 close-on-exec。
    // 不依赖它继承监听 Socket 的标志。
    SocketFd incoming{
        ::accept4(
            listening_fd,
            nullptr,
            nullptr,
            SOCK_NONBLOCK | SOCK_CLOEXEC
        )
    };

    if (!incoming) {
        // 必须在其他操作之前保存本次系统调用的错误。
        const int error_code = errno;

        if (error_code == EAGAIN ||
            error_code == EWOULDBLOCK) {
            return {
                TcpAcceptStatus::WouldBlock,
                std::nullopt,
                error_code,
                {}
            };
        }

        if (canRetryAccept(error_code)) {
            // 本函数不在内部无限重试。
            // 后续事件循环根据每轮预算决定何时再次调用。
            return {
                TcpAcceptStatus::RetryLater,
                std::nullopt,
                error_code,
                {}
            };
        }

        return {
            TcpAcceptStatus::Error,
            std::nullopt,
            error_code,
            std::error_code(
                error_code,
                std::generic_category()
            ).message()
        };
    }

    for (std::size_t slot = 0; slot < slots_.size(); ++slot) {
        if (!slots_[slot]) {
            slots_[slot] = std::move(incoming);

            return {
                TcpAcceptStatus::Accepted,
                slot,
                0,
                {}
            };
        }
    }

    // 没有空槽。incoming 离开作用域时自动关闭，
    // 不会加入当前维护的连接集合。
    return {
        TcpAcceptStatus::RejectedAtCapacity,
        std::nullopt,
        0,
        {}
    };
}

std::size_t TcpConnections::size() const noexcept
{
    std::size_t count = 0;

    for (const auto& socket : slots_) {
        if (socket.valid()) {
            ++count;
        }
    }

    return count;
}

std::size_t TcpConnections::capacity() const noexcept
{
    return slots_.size();
}

int TcpConnections::fdAt(std::size_t slot) const
{
    return slots_.at(slot).get();
}

void TcpConnections::closeAt(std::size_t slot)
{
    slots_.at(slot).reset();
}

} // namespace edge