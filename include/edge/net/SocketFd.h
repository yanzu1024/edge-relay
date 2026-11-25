#pragma once

namespace edge {

// Linux socket 文件描述符的单一所有权封装。
// 传入非负描述符后，由本对象负责关闭，调用方不能再自行关闭。
class SocketFd {
public:
    // 默认构造为空，不持有资源。
    SocketFd() noexcept = default;

    // explicit 防止普通整数被隐式转换为资源所有者。
    explicit SocketFd(int fd) noexcept;

    ~SocketFd() noexcept;

    // 同一个描述符只能由一个对象负责关闭。
    SocketFd(const SocketFd&) = delete;
    SocketFd& operator=(const SocketFd&) = delete;

    // 移动只转移所有权，不复制内核资源。
    SocketFd(SocketFd&& other) noexcept;
    SocketFd& operator=(SocketFd&& other) noexcept;

    // 借用描述符，供 bind、accept、recv、poll 等接口使用。
    // 调用方不能通过这个返回值直接 close。
    [[nodiscard]] int get() const noexcept;

    [[nodiscard]] bool valid() const noexcept;

    explicit operator bool() const noexcept;

    // 交出所有权，不关闭资源。
    // 调用方必须接管返回值，或负责关闭它。
    [[nodiscard]] int release() noexcept;

    // 关闭旧资源，再接管新资源。
    // 不传参数表示仅关闭并清空。
    void reset(int new_fd = -1) noexcept;

private:
    // -1 表示空；0 也可能是有效描述符。
    int fd_ = -1;
};

} // namespace edge