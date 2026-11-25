#include "edge/net/TcpListener.h"

#include <cerrno>
#include <string>
#include <utility>
#include <variant>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <gtest/gtest.h>

namespace {

std::string errorText(const edge::TcpListenResult& result)
{
    const auto* error =
        std::get_if<edge::TcpListenError>(&result);

    if (error == nullptr) {
        return {};
    }

    return error->operation + ": " + error->message;
}

void expectInvalid(const edge::TcpListenResult& result)
{
    const auto* error =
        std::get_if<edge::TcpListenError>(&result);

    ASSERT_NE(error, nullptr);

    EXPECT_EQ(error->error_code, EINVAL);
    EXPECT_FALSE(error->operation.empty());
    EXPECT_FALSE(error->message.empty());
}

// 每个 TEST_F 都会获得一个独立的监听 Socket。
class TcpListenerTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        // 使用临时端口，避免占用 demo.yml 中的固定端口。
        auto result = edge::createTcpListener(
            "127.0.0.1",
            0,
            8
        );

        ASSERT_TRUE(
            std::holds_alternative<edge::SocketFd>(result)
        ) << errorText(result);

        listener_ =
            std::move(std::get<edge::SocketFd>(result));

        socklen_t size = sizeof(address_);

        // 查询内核实际分配的地址和端口。
        ASSERT_EQ(
            ::getsockname(
                listener_.get(),
                reinterpret_cast<sockaddr*>(&address_),
                &size
            ),
            0
        );

        ASSERT_EQ(
            size,
            static_cast<socklen_t>(sizeof(address_))
        );
    }

    edge::SocketFd listener_;
    sockaddr_in address_{};
};

} // namespace

TEST_F(TcpListenerTest, CreatesConfiguredListener)
{
    ASSERT_TRUE(listener_.valid());

    EXPECT_EQ(address_.sin_family, AF_INET);
    EXPECT_EQ(
        ntohl(address_.sin_addr.s_addr),
        INADDR_LOOPBACK
    );
    EXPECT_GT(ntohs(address_.sin_port), 0);

    const int status_flags =
        ::fcntl(listener_.get(), F_GETFL);

    ASSERT_NE(status_flags, -1);
    EXPECT_NE(status_flags & O_NONBLOCK, 0);

    const int descriptor_flags =
        ::fcntl(listener_.get(), F_GETFD);

    ASSERT_NE(descriptor_flags, -1);
    EXPECT_NE(descriptor_flags & FD_CLOEXEC, 0);

    // 验证监听状态和地址复用选项确实设置成功。
    for (const int option : {SO_ACCEPTCONN, SO_REUSEADDR}) {
        SCOPED_TRACE(option);

        int value = 0;
        socklen_t size = sizeof(value);

        ASSERT_EQ(
            ::getsockopt(
                listener_.get(),
                SOL_SOCKET,
                option,
                &value,
                &size
            ),
            0
        );

        EXPECT_NE(value, 0);
    }
}

TEST_F(TcpListenerTest, EmptyAcceptReturnsWithoutWaiting)
{
    // 没有客户端时，非阻塞监听应立即返回。
    // 如果意外接入成功，也用 SocketFd 接管，避免测试泄漏资源。
    edge::SocketFd accepted{
        ::accept(listener_.get(), nullptr, nullptr)
    };

    const int error_code = errno;

    EXPECT_FALSE(accepted.valid());
    EXPECT_TRUE(
        error_code == EAGAIN ||
        error_code == EWOULDBLOCK
    );
}

TEST_F(TcpListenerTest, ReportsOccupiedAddress)
{
    // 第一个监听保持存活，第二个尝试绑定同一地址和端口。
    auto result = edge::createTcpListener(
        "127.0.0.1",
        ntohs(address_.sin_port),
        8
    );

    const auto* error =
        std::get_if<edge::TcpListenError>(&result);

    ASSERT_NE(error, nullptr);

    EXPECT_EQ(error->operation, "bind");
    EXPECT_EQ(error->error_code, EADDRINUSE);
    EXPECT_FALSE(error->message.empty());
}

TEST(TcpListenerInputTest, RejectsInvalidAddress)
{
    std::string embedded_nul{"127.0.0.1"};
    embedded_nul.push_back('\0');
    embedded_nul += "extra";

    for (const std::string& address : {
            std::string{},
            std::string{"localhost"},
            std::string{"999.0.0.1"},
            embedded_nul
        }) {
        auto result = edge::createTcpListener(
            address,
            0,
            8
        );

        expectInvalid(result);
    }
}

TEST(TcpListenerInputTest, RejectsInvalidPort)
{
    for (const int port : {-1, 65536}) {
        auto result = edge::createTcpListener(
            "127.0.0.1",
            port,
            8
        );

        expectInvalid(result);
    }
}

TEST(TcpListenerInputTest, RejectsInvalidBacklog)
{
    for (const int backlog : {0, -1}) {
        auto result = edge::createTcpListener(
            "127.0.0.1",
            0,
            backlog
        );

        expectInvalid(result);
    }
}