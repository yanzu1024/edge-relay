#include "edge/net/TcpListener.h"

#include <cerrno>
#include <system_error>
#include <utility>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace edge {

namespace {

TcpListenError systemError(
    const char* operation,
    int error_code
)
{
    return TcpListenError{
        operation,
        error_code,
        std::error_code(
            error_code,
            std::generic_category()
        ).message()
    };
}

} // namespace

TcpListenResult createTcpListener(
    std::string_view bind_address,
    std::int32_t port,
    std::int32_t backlog
)
{
    // 必须在转换为 uint16_t 前检查，避免截断或负数回绕。
    if (port < 0 || port > 65535) {
        return TcpListenError{
            "validate_port",
            EINVAL,
            "port must be between 0 and 65535"
        };
    }

    if (backlog <= 0) {
        return TcpListenError{
            "validate_backlog",
            EINVAL,
            "backlog must be positive"
        };
    }

    // inet_pton 接收 C 字符串，必须拒绝内嵌 NUL，
    // 避免只解析输入前半部分。
    if (bind_address.empty() ||
        bind_address.find('\0') != std::string_view::npos) {
        return TcpListenError{
            "validate_address",
            EINVAL,
            "bind address must be a nonempty IPv4 literal"
        };
    }

    // string_view 不保证末尾有 NUL，转换成 string 后再传给 C API。
    const std::string address_text{bind_address};

    // 清零整个结构，避免未初始化字段。
    sockaddr_in address{};
    address.sin_family = AF_INET;

    // 网络协议中的端口使用网络字节序。
    address.sin_port = htons(
        static_cast<std::uint16_t>(port)
    );

    const int parse_result = ::inet_pton(
        AF_INET,
        address_text.c_str(),
        &address.sin_addr
    );

    if (parse_result == 0) {
        // 返回 0 表示文本无效，此时不能直接使用 errno。
        return TcpListenError{
            "inet_pton",
            EINVAL,
            "bind address must be an IPv4 literal"
        };
    }

    if (parse_result == -1) {
        return systemError("inet_pton", errno);
    }

    // 创建时一次设置非阻塞和 close-on-exec 标志。
    SocketFd socket{
        ::socket(
            AF_INET,
            SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
            0
        )
    };

    if (!socket) {
        return systemError("socket", errno);
    }

    const int reuse_address = 1;

    if (::setsockopt(
            socket.get(),
            SOL_SOCKET,
            SO_REUSEADDR,
            &reuse_address,
            sizeof(reuse_address)
        ) == -1) {
        return systemError("setsockopt", errno);
    }

    if (::bind(
            socket.get(),
            reinterpret_cast<const sockaddr*>(&address),
            sizeof(address)
        ) == -1) {
        return systemError("bind", errno);
    }

    if (::listen(
            socket.get(),
            static_cast<int>(backlog)
        ) == -1) {
        return systemError("listen", errno);
    }

    // 转移 Socket 所有权，局部对象随后析构不会关闭成功结果。
    return TcpListenResult{std::move(socket)};
}

} // namespace edge