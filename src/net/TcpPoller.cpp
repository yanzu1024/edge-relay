#include "edge/net/TcpPoller.h"

#include <array>
#include <cerrno>
#include <stdexcept>
#include <system_error>
#include <utility>

#include <poll.h>
#include <sys/socket.h>

namespace edge {

namespace {

TcpPollError makeError(const char* operation, int error_code)
{
    return {
        operation,
        error_code,
        std::error_code(
            error_code,
            std::generic_category()
        ).message()
    };
}

// 在 poll 报告错误、但 recv 没有直接给出错误码时使用。
int pendingSocketError(int fd)
{
    int error_code = 0;
    socklen_t size = sizeof(error_code);

    if (::getsockopt(
            fd,
            SOL_SOCKET,
            SO_ERROR,
            &error_code,
            &size
        ) == -1) {
        return errno;
    }

    return error_code != 0 ? error_code : EIO;
}

void closeConnection(
    TcpConnections& connections,
    std::size_t slot,
    TcpPollBatch& batch,
    int error_code
)
{
    batch.events.push_back({
        error_code == 0 ? TcpIoKind::Closed
                        : TcpIoKind::ClientError,
        slot,
        {},
        error_code
    });

    connections.closeAt(slot);
}

void readConnection(
    TcpConnections& connections,
    std::size_t slot,
    const pollfd& ready,
    TcpPollBatch& batch
)
{
    std::array<std::uint8_t, kTcpReadChunkBytes> buffer{};

    for (std::size_t attempt = 0;
         attempt < kTcpReadCallsPerRound;
         ++attempt) {
        const auto count = ::recv(
            ready.fd,
            buffer.data(),
            buffer.size(),
            MSG_DONTWAIT
        );

        if (count > 0) {
            std::vector<std::uint8_t> bytes(
                buffer.data(),
                buffer.data() + static_cast<std::size_t>(count)
            );

            batch.events.push_back({
                TcpIoKind::Data,
                slot,
                std::move(bytes),
                0
            });

            // 短读不表示 EOF，继续尝试，直到无数据或预算耗尽。
            continue;
        }

        if (count == 0) {
            closeConnection(connections, slot, batch, 0);
            return;
        }

        const int error_code = errno;

        if (error_code == EINTR) {
            // 中断也消耗一次尝试，避免信号持续到来时无限重试。
            continue;
        }

        if (error_code == EAGAIN ||
            error_code == EWOULDBLOCK) {
            if (ready.revents & POLLERR) {
                closeConnection(
                    connections,
                    slot,
                    batch,
                    pendingSocketError(ready.fd)
                );
            } else if (ready.revents & POLLHUP) {
                // 已无可读数据，且对端已挂断。
                closeConnection(connections, slot, batch, 0);
            }

            return;
        }

        closeConnection(connections, slot, batch, error_code);
        return;
    }

    // 预算耗尽，保留连接。
    // 即使本轮带 POLLHUP，也要留到下一轮继续读取剩余数据。
}

} // namespace

TcpPollBatch pollTcpOnce(
    int listening_fd,
    TcpConnections& connections,
    int timeout_ms
)
{
    if (timeout_ms < 0) {
        throw std::invalid_argument(
            "poll timeout must be nonnegative"
        );
    }

    TcpPollBatch batch;

    if (listening_fd < 0) {
        batch.error = makeError("validate_listener", EBADF);
        return batch;
    }

    // 下标 0 是监听 Socket；下标 slot + 1 对应客户端槽位。
    // 这是本轮快照，后续接入不能参与本轮已有事件的处理。
    std::vector<pollfd> descriptors(
        connections.capacity() + 1
    );

    descriptors[0] = {listening_fd, POLLIN, 0};

    for (std::size_t slot = 0;
         slot < connections.capacity();
         ++slot) {
        descriptors[slot + 1] = {
            connections.fdAt(slot),
            POLLIN,
            0
        };
    }

    const int ready_count = ::poll(
        descriptors.data(),
        static_cast<nfds_t>(descriptors.size()),
        timeout_ms
    );

    if (ready_count == -1) {
        const int error_code = errno;

        if (error_code == EINTR) {
            batch.interrupted = true;
        } else {
            batch.error = makeError("poll", error_code);
        }

        return batch;
    }

    if (ready_count == 0) {
        return batch;
    }

    // 先处理快照里的客户端，再接入新连接。
    // 防止槽位释放、描述符复用后，把旧事件套在新连接上。
    for (std::size_t slot = 0;
         slot < connections.capacity();
         ++slot) {
        const auto& ready = descriptors[slot + 1];

        if (ready.fd < 0 || ready.revents == 0) {
            continue;
        }

        if (ready.revents & POLLNVAL) {
            // 正常的资源所有权规则下不应出现无效描述符。
            // 报告给调用方，不能忽略后持续空转。
            batch.error = makeError("poll_client", EBADF);
            return batch;
        }

        if (ready.revents & (POLLIN | POLLHUP | POLLERR)) {
            readConnection(connections, slot, ready, batch);
        }
    }

    const auto& listener = descriptors[0];

    if (listener.revents & (POLLNVAL | POLLERR | POLLHUP)) {
        const int error_code =
            (listener.revents & POLLNVAL)
                ? EBADF
                : pendingSocketError(listening_fd);

        batch.error = makeError("poll_listener", error_code);
        return batch;
    }

    if (!(listener.revents & POLLIN)) {
        return batch;
    }

    for (std::size_t attempt = 0;
         attempt < kTcpAcceptAttemptsPerRound;
         ++attempt) {
        const auto result = connections.acceptOne(listening_fd);

        if (result.status == TcpAcceptStatus::Accepted) {
            batch.events.push_back({
                TcpIoKind::Accepted,
                result.slot,
                {},
                0
            });
        } else if (
            result.status == TcpAcceptStatus::RejectedAtCapacity
        ) {
            batch.events.push_back({
                TcpIoKind::RejectedAtCapacity,
                std::nullopt,
                {},
                0
            });
        } else if (result.status == TcpAcceptStatus::Error) {
            batch.error = makeError(
                "accept4",
                result.error_code
            );
            break;
        } else {
            // WouldBlock：队列已空。
            // RetryLater：把控制权交回调用方。
            break;
        }
    }

    return batch;
}

} // namespace edge