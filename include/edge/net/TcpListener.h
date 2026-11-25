#pragma once

#include "edge/net/SocketFd.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

namespace edge {

struct TcpListenError {
    // 出错步骤，例如 socket、bind、listen。
    std::string operation;

    // 参数错误使用 EINVAL；系统调用失败保存对应 errno。
    int error_code = 0;

    std::string message;
};

// SocketFd 不可复制，因此这个结果也需要通过移动传递所有权。
using TcpListenResult = std::variant<SocketFd, TcpListenError>;

// 创建 IPv4 TCP 监听 Socket。
//
// bind_address：IPv4 字面地址，例如 127.0.0.1。
// port：0～65535；0 表示交给内核选择临时端口。
// backlog：必须大于 0，表示等待接入的连接队列长度。
//
// 成功结果持有 Socket 所有权，失败路径自动释放已创建的资源。
[[nodiscard]] TcpListenResult createTcpListener(
    std::string_view bind_address,
    std::int32_t port,
    std::int32_t backlog
);

} // namespace edge