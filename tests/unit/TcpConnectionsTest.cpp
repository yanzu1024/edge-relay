#include "edge/net/TcpConnections.h"
#include "edge/net/TcpListener.h"

#include <cerrno>
#include <stdexcept>
#include <utility>
#include <variant>

#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>

#include <gtest/gtest.h>

namespace {

class TcpConnectionsTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        auto result = edge::createTcpListener(
            "127.0.0.1",
            0,
            8
        );

        if (const auto* error =
                std::get_if<edge::TcpListenError>(&result)) {
            FAIL() << error->operation << ": " << error->message;
        }

        listener_ =
            std::move(std::get<edge::SocketFd>(result));

        socklen_t size = sizeof(address_);

        ASSERT_EQ(
            ::getsockname(
                listener_.get(),
                reinterpret_cast<sockaddr*>(&address_),
                &size
            ),
            0
        );
    }

    void connectClient(edge::SocketFd& client)
    {
        // 测试客户端连接本机临时端口。
        client.reset(
            ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)
        );

        ASSERT_TRUE(client.valid());

        ASSERT_EQ(
            ::connect(
                client.get(),
                reinterpret_cast<const sockaddr*>(&address_),
                sizeof(address_)
            ),
            0
        );

        // 有界等待监听就绪，避免通过 sleep 猜测接入时机。
        pollfd event{};
        event.fd = listener_.get();
        event.events = POLLIN;

        ASSERT_EQ(::poll(&event, 1, 1000), 1);
        ASSERT_NE(event.revents & POLLIN, 0);
    }

    edge::SocketFd listener_;
    sockaddr_in address_{};
};

} // namespace

TEST_F(TcpConnectionsTest, EmptyQueueReturnsWouldBlock)
{
    edge::TcpConnections connections{2};

    const auto result =
        connections.acceptOne(listener_.get());

    EXPECT_EQ(result.status, edge::TcpAcceptStatus::WouldBlock);
    EXPECT_FALSE(result.slot.has_value());

    EXPECT_EQ(connections.size(), 0u);
    EXPECT_EQ(connections.capacity(), 2u);
    EXPECT_EQ(connections.fdAt(0), -1);
    EXPECT_EQ(connections.fdAt(1), -1);
}

TEST_F(TcpConnectionsTest, AcceptsIndependentNonblockingSockets)
{
    edge::TcpConnections connections{2};

    edge::SocketFd first_client;
    ASSERT_NO_FATAL_FAILURE(connectClient(first_client));

    const auto first =
        connections.acceptOne(listener_.get());

    ASSERT_EQ(first.status, edge::TcpAcceptStatus::Accepted);
    ASSERT_TRUE(first.slot.has_value());

    edge::SocketFd second_client;
    ASSERT_NO_FATAL_FAILURE(connectClient(second_client));

    const auto second =
        connections.acceptOne(listener_.get());

    ASSERT_EQ(second.status, edge::TcpAcceptStatus::Accepted);
    ASSERT_TRUE(second.slot.has_value());

    EXPECT_NE(*first.slot, *second.slot);
    EXPECT_EQ(connections.size(), 2u);

    const int first_fd = connections.fdAt(*first.slot);
    const int second_fd = connections.fdAt(*second.slot);

    ASSERT_GE(first_fd, 0);
    ASSERT_GE(second_fd, 0);

    EXPECT_NE(first_fd, second_fd);
    EXPECT_NE(first_fd, listener_.get());
    EXPECT_NE(second_fd, listener_.get());

    for (const int fd : {first_fd, second_fd}) {
        const int status_flags = ::fcntl(fd, F_GETFL);

        ASSERT_NE(status_flags, -1);
        EXPECT_NE(status_flags & O_NONBLOCK, 0);

        const int descriptor_flags = ::fcntl(fd, F_GETFD);

        ASSERT_NE(descriptor_flags, -1);
        EXPECT_NE(descriptor_flags & FD_CLOEXEC, 0);
    }

    EXPECT_TRUE(listener_.valid());
}

TEST_F(TcpConnectionsTest, RejectsAndClosesExcessConnection)
{
    edge::TcpConnections connections{1};

    edge::SocketFd first_client;
    ASSERT_NO_FATAL_FAILURE(connectClient(first_client));

    const auto first =
        connections.acceptOne(listener_.get());

    ASSERT_EQ(first.status, edge::TcpAcceptStatus::Accepted);
    ASSERT_TRUE(first.slot.has_value());

    const int retained_fd = connections.fdAt(*first.slot);

    edge::SocketFd excess_client;
    ASSERT_NO_FATAL_FAILURE(connectClient(excess_client));

    const auto excess =
        connections.acceptOne(listener_.get());

    EXPECT_EQ(
        excess.status,
        edge::TcpAcceptStatus::RejectedAtCapacity
    );
    EXPECT_FALSE(excess.slot.has_value());

    EXPECT_EQ(connections.size(), 1u);
    EXPECT_EQ(connections.fdAt(*first.slot), retained_fd);

    // 超额客户端应观察到服务端关闭。
    // 本测试没有发送任何数据，正常关闭表现为 EOF。
    pollfd event{};
    event.fd = excess_client.get();
    event.events = POLLIN;

    ASSERT_EQ(::poll(&event, 1, 1000), 1);

    char byte = 0;

    EXPECT_EQ(
        ::recv(excess_client.get(), &byte, 1, MSG_DONTWAIT),
        0
    );

    // 被拒绝的连接已从监听队列取走。
    const auto empty =
        connections.acceptOne(listener_.get());

    EXPECT_EQ(empty.status, edge::TcpAcceptStatus::WouldBlock);
}

TEST_F(TcpConnectionsTest, ClosedSlotCanBeReused)
{
    edge::TcpConnections connections{1};

    edge::SocketFd first_client;
    ASSERT_NO_FATAL_FAILURE(connectClient(first_client));

    const auto first =
        connections.acceptOne(listener_.get());

    ASSERT_EQ(first.status, edge::TcpAcceptStatus::Accepted);
    ASSERT_TRUE(first.slot.has_value());

    connections.closeAt(*first.slot);

    EXPECT_EQ(connections.size(), 0u);
    EXPECT_EQ(connections.fdAt(*first.slot), -1);

    // 空槽重复关闭安全。
    connections.closeAt(*first.slot);

    edge::SocketFd next_client;
    ASSERT_NO_FATAL_FAILURE(connectClient(next_client));

    const auto next =
        connections.acceptOne(listener_.get());

    ASSERT_EQ(next.status, edge::TcpAcceptStatus::Accepted);
    ASSERT_TRUE(next.slot.has_value());

    EXPECT_EQ(next.slot, first.slot);
    EXPECT_EQ(connections.size(), 1u);
}

TEST_F(TcpConnectionsTest, InvalidListenerReturnsError)
{
    edge::TcpConnections connections{1};

    const auto result = connections.acceptOne(-1);

    EXPECT_EQ(result.status, edge::TcpAcceptStatus::Error);
    EXPECT_EQ(result.error_code, EBADF);
    EXPECT_FALSE(result.message.empty());
    EXPECT_FALSE(result.slot.has_value());

    EXPECT_EQ(connections.size(), 0u);
}

TEST_F(TcpConnectionsTest, RejectsInvalidConnectionLimit)
{
    EXPECT_THROW(
        edge::TcpConnections{0},
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::TcpConnections{-1},
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::TcpConnections{9},
        std::invalid_argument
    );
}