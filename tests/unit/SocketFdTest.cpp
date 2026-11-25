#include "edge/net/SocketFd.h"

#include <cerrno>
#include <type_traits>
#include <utility>

#include <fcntl.h>
#include <sys/socket.h>

#include <gtest/gtest.h>

namespace {

// 在编译阶段检查所有权规则。
static_assert(!std::is_copy_constructible_v<edge::SocketFd>);
static_assert(!std::is_copy_assignable_v<edge::SocketFd>);

static_assert(
    std::is_nothrow_move_constructible_v<edge::SocketFd>
);
static_assert(
    std::is_nothrow_move_assignable_v<edge::SocketFd>
);

edge::SocketFd makeSocket()
{
    return edge::SocketFd{
        ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0)
    };
}

void expectClosed(int fd)
{
    // 这里只查询描述符状态，不创建新描述符。
    const int result = ::fcntl(fd, F_GETFD);
    const int error = errno;

    EXPECT_EQ(result, -1);
    EXPECT_EQ(error, EBADF);
}

} // namespace

TEST(SocketFdTest, DefaultConstructedObjectIsEmpty)
{
    edge::SocketFd socket;

    EXPECT_FALSE(socket.valid());
    EXPECT_FALSE(static_cast<bool>(socket));
    EXPECT_EQ(socket.get(), -1);
    EXPECT_EQ(socket.release(), -1);
}

TEST(SocketFdTest, DestructorClosesOwnedDescriptor)
{
    int raw_fd = -1;

    {
        auto socket = makeSocket();
        ASSERT_TRUE(socket.valid());

        raw_fd = socket.get();

        EXPECT_TRUE(static_cast<bool>(socket));
        EXPECT_NE(::fcntl(raw_fd, F_GETFD), -1);
    }

    expectClosed(raw_fd);
}

TEST(SocketFdTest, MoveConstructorTransfersOwnership)
{
    auto source = makeSocket();
    ASSERT_TRUE(source.valid());

    const int raw_fd = source.get();

    edge::SocketFd destination{std::move(source)};

    EXPECT_FALSE(source.valid());
    EXPECT_EQ(source.get(), -1);

    EXPECT_TRUE(destination.valid());
    EXPECT_EQ(destination.get(), raw_fd);
    EXPECT_NE(::fcntl(raw_fd, F_GETFD), -1);
}

TEST(SocketFdTest, MoveAssignmentClosesOldDescriptor)
{
    auto source = makeSocket();
    auto destination = makeSocket();

    ASSERT_TRUE(source.valid());
    ASSERT_TRUE(destination.valid());

    const int source_fd = source.get();
    const int old_destination_fd = destination.get();

    destination = std::move(source);

    EXPECT_FALSE(source.valid());
    EXPECT_EQ(destination.get(), source_fd);
    EXPECT_NE(::fcntl(source_fd, F_GETFD), -1);

    expectClosed(old_destination_fd);
}

TEST(SocketFdTest, ReleaseTransfersWithoutClosing)
{
    edge::SocketFd destination;
    int raw_fd = -1;

    {
        auto source = makeSocket();
        ASSERT_TRUE(source.valid());

        raw_fd = source.release();

        EXPECT_FALSE(source.valid());
        EXPECT_NE(::fcntl(raw_fd, F_GETFD), -1);

        // 立即交给新的所有者，避免裸描述符泄漏。
        destination.reset(raw_fd);
    }

    // source 已析构，资源仍由 destination 持有。
    EXPECT_EQ(destination.get(), raw_fd);
    EXPECT_NE(::fcntl(raw_fd, F_GETFD), -1);
}

TEST(SocketFdTest, ResetClosesAndPreservesErrno)
{
    auto socket = makeSocket();
    ASSERT_TRUE(socket.valid());

    const int raw_fd = socket.get();

    errno = EACCES;
    socket.reset();
    const int preserved_errno = errno;

    EXPECT_EQ(preserved_errno, EACCES);
    EXPECT_FALSE(socket.valid());
    EXPECT_EQ(socket.get(), -1);

    expectClosed(raw_fd);

    // 重复清理空对象应当安全。
    socket.reset();
    EXPECT_FALSE(socket.valid());
}

TEST(SocketFdTest, ResetReplacesOwnedDescriptor)
{
    auto socket = makeSocket();
    auto replacement = makeSocket();

    ASSERT_TRUE(socket.valid());
    ASSERT_TRUE(replacement.valid());

    const int old_fd = socket.get();
    const int new_fd = replacement.get();

    socket.reset(replacement.release());

    EXPECT_FALSE(replacement.valid());
    EXPECT_EQ(socket.get(), new_fd);
    EXPECT_NE(::fcntl(new_fd, F_GETFD), -1);

    expectClosed(old_fd);
}

TEST(SocketFdTest, ResetWithSameDescriptorKeepsItOpen)
{
    auto socket = makeSocket();
    ASSERT_TRUE(socket.valid());

    const int raw_fd = socket.get();

    socket.reset(socket.get());

    EXPECT_EQ(socket.get(), raw_fd);
    EXPECT_NE(::fcntl(raw_fd, F_GETFD), -1);
}