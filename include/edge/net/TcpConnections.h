#pragma once

#include "edge/net/SocketFd.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace edge {

enum class TcpAcceptStatus {
    Accepted,            // 成功接入，slot 有值
    WouldBlock,          // 当前没有等待接入的连接
    RejectedAtCapacity,  // 已达到上限，新连接已关闭
    RetryLater,          // 被信号打断或发生可重试的网络错误
    Error                // 其他系统调用错误
};

struct TcpAcceptResult {
    TcpAcceptStatus status = TcpAcceptStatus::Error;

    // 仅 Accepted 时有值。
    std::optional<std::size_t> slot;

    // 系统调用失败时保存 errno。
    int error_code = 0;

    std::string message;
};

// 单线程使用的客户端连接集合。
// 每个槽位独立持有一个 SocketFd，槽位数量在构造时确定。
class TcpConnections {
public:
    // 上限必须为 1～kMaxTcpConnections，否则抛 invalid_argument。
    explicit TcpConnections(std::int32_t max_connections);

    TcpConnections(const TcpConnections&) = delete;
    TcpConnections& operator=(const TcpConnections&) = delete;

    // listening_fd 必须是处于非阻塞模式的 TCP 监听 Socket。
    // 只借用描述符，不接管监听 Socket 的所有权。
    [[nodiscard]] TcpAcceptResult acceptOne(int listening_fd);

    // 当前持有的客户端连接数。
    [[nodiscard]] std::size_t size() const noexcept;

    // 可用槽位总数，即连接数上限。
    [[nodiscard]] std::size_t capacity() const noexcept;

    // 借用槽位中的描述符，空槽返回 -1。
    // 下标越界时抛 std::out_of_range。
    [[nodiscard]] int fdAt(std::size_t slot) const;

    // 关闭对应连接，槽位重新变为空闲。
    // 重复关闭空槽安全；下标越界时抛 std::out_of_range。
    void closeAt(std::size_t slot);

private:
    std::vector<SocketFd> slots_;
};

} // namespace edge