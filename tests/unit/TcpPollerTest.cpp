#include "edge/net/TcpPoller.h"
#include "edge/net/TcpListener.h"

#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <netinet/in.h>
#include <sys/socket.h>

#include <gtest/gtest.h>

#include "edge/app/TcpEventProcessor.h"
#include "edge/protocol/FrameEncoder.h"

namespace {

class TcpPollerTest : public ::testing::Test {
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

    void openClient(edge::SocketFd& client)
    {
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
    }

    void admitClient(
        edge::SocketFd& client,
        std::size_t& slot
    )
    {
        ASSERT_NO_FATAL_FAILURE(openClient(client));

        const auto batch = edge::pollTcpOnce(
            listener_.get(),
            connections_,
            1000
        );

        ASSERT_FALSE(batch.error.has_value());
        ASSERT_FALSE(batch.interrupted);
        ASSERT_EQ(batch.events.size(), 1u);

        const auto& event = batch.events.front();

        ASSERT_EQ(event.kind, edge::TcpIoKind::Accepted);
        ASSERT_TRUE(event.slot.has_value());

        slot = *event.slot;
    }

    void sendAll(int fd, std::string_view bytes)
    {
        while (!bytes.empty()) {
            const auto count = ::send(
                fd,
                bytes.data(),
                bytes.size(),
                MSG_NOSIGNAL
            );

            ASSERT_GT(count, 0);

            bytes.remove_prefix(
                static_cast<std::size_t>(count)
            );
        }
    }

    void collect(
        const edge::TcpPollBatch& batch,
        std::size_t slot,
        std::string& received,
        bool& closed
    )
    {
        ASSERT_FALSE(batch.error.has_value());

        for (const auto& event : batch.events) {
            EXPECT_NE(event.kind, edge::TcpIoKind::ClientError);

            if (event.slot != slot) {
                continue;
            }

            if (event.kind == edge::TcpIoKind::Data) {
                // 同一连接不能在 Closed 之后继续产生 Data。
                EXPECT_FALSE(closed);

                received.append(
                    event.bytes.begin(),
                    event.bytes.end()
                );
            } else if (event.kind == edge::TcpIoKind::Closed) {
                closed = true;
            }
        }
    }

    edge::SocketFd listener_;
    sockaddr_in address_{};
    edge::TcpConnections connections_{2};
};

} // namespace

TEST_F(TcpPollerTest, IdleRoundHasNoEvents)
{
    const auto batch = edge::pollTcpOnce(
        listener_.get(),
        connections_,
        0
    );

    EXPECT_TRUE(batch.events.empty());
    EXPECT_FALSE(batch.error.has_value());
    EXPECT_FALSE(batch.interrupted);
}

TEST_F(TcpPollerTest, ReceivesBytesBeforeClosingConnection)
{
    edge::SocketFd client;
    std::size_t slot = 0;

    ASSERT_NO_FATAL_FAILURE(admitClient(client, slot));

    // 包含 NUL，验证按字节长度传递，而不是按 C 字符串处理。
    const std::string payload{"abc\0xyz", 7};

    ASSERT_NO_FATAL_FAILURE(sendAll(client.get(), payload));
    ASSERT_EQ(::shutdown(client.get(), SHUT_WR), 0);

    std::string received;
    bool closed = false;

    for (int round = 0; round < 20 && !closed; ++round) {
        const auto batch = edge::pollTcpOnce(
            listener_.get(),
            connections_,
            50
        );

        ASSERT_NO_FATAL_FAILURE(
            collect(batch, slot, received, closed)
        );
    }

    EXPECT_TRUE(closed);
    EXPECT_EQ(received, payload);
    EXPECT_EQ(connections_.size(), 0u);
}

TEST_F(TcpPollerTest, KeepsClientBytesSeparate)
{
    edge::SocketFd first_client;
    edge::SocketFd second_client;

    std::size_t first_slot = 0;
    std::size_t second_slot = 0;

    ASSERT_NO_FATAL_FAILURE(
        admitClient(first_client, first_slot)
    );
    ASSERT_NO_FATAL_FAILURE(
        admitClient(second_client, second_slot)
    );

    ASSERT_NE(first_slot, second_slot);

    const std::string first_payload{"first-client"};
    const std::string second_payload{"second-client"};

    ASSERT_NO_FATAL_FAILURE(
        sendAll(first_client.get(), first_payload)
    );
    ASSERT_NO_FATAL_FAILURE(
        sendAll(second_client.get(), second_payload)
    );

    std::string first_received;
    std::string second_received;

    bool first_closed = false;
    bool second_closed = false;

    for (int round = 0;
         round < 20 &&
         (first_received.size() < first_payload.size() ||
          second_received.size() < second_payload.size());
         ++round) {
        const auto batch = edge::pollTcpOnce(
            listener_.get(),
            connections_,
            50
        );

        ASSERT_NO_FATAL_FAILURE(
            collect(
                batch,
                first_slot,
                first_received,
                first_closed
            )
        );

        ASSERT_NO_FATAL_FAILURE(
            collect(
                batch,
                second_slot,
                second_received,
                second_closed
            )
        );
    }

    EXPECT_EQ(first_received, first_payload);
    EXPECT_EQ(second_received, second_payload);
    EXPECT_FALSE(first_closed);
    EXPECT_FALSE(second_closed);

    // 数据暂时读完不能被误判为断开。
    EXPECT_EQ(connections_.size(), 2u);
}

TEST_F(TcpPollerTest, ReadBudgetPreservesRemainingBytes)
{
    edge::SocketFd client;
    std::size_t slot = 0;

    ASSERT_NO_FATAL_FAILURE(admitClient(client, slot));

    // 数据量大于单轮读取预算。
    const std::string payload(
        10 * edge::kTcpReadChunkBytes,
        'x'
    );

    ASSERT_NO_FATAL_FAILURE(sendAll(client.get(), payload));
    ASSERT_EQ(::shutdown(client.get(), SHUT_WR), 0);

    std::string received;
    bool closed = false;

    const auto first = edge::pollTcpOnce(
        listener_.get(),
        connections_,
        1000
    );

    ASSERT_NO_FATAL_FAILURE(
        collect(first, slot, received, closed)
    );

    EXPECT_LE(
        received.size(),
        edge::kTcpReadChunkBytes *
            edge::kTcpReadCallsPerRound
    );

    EXPECT_FALSE(closed);
    EXPECT_EQ(connections_.size(), 1u);

    for (int round = 0; round < 20 && !closed; ++round) {
        const auto batch = edge::pollTcpOnce(
            listener_.get(),
            connections_,
            50
        );

        ASSERT_NO_FATAL_FAILURE(
            collect(batch, slot, received, closed)
        );
    }

    EXPECT_TRUE(closed);
    EXPECT_EQ(received, payload);
    EXPECT_EQ(connections_.size(), 0u);
}

TEST_F(TcpPollerTest, BoundsAcceptAttemptsAndRejectsExcess)
{
    std::vector<edge::SocketFd> clients(
        edge::kTcpAcceptAttemptsPerRound + 1
    );

    for (auto& client : clients) {
        ASSERT_NO_FATAL_FAILURE(openClient(client));
    }

    const auto first = edge::pollTcpOnce(
        listener_.get(),
        connections_,
        1000
    );

    ASSERT_FALSE(first.error.has_value());

    // 两个接入、两个因容量不足被拒绝。
    EXPECT_EQ(
        first.events.size(),
        edge::kTcpAcceptAttemptsPerRound
    );
    EXPECT_EQ(connections_.size(), 2u);

    std::size_t accepted = 0;
    std::size_t rejected = 0;

    for (const auto& event : first.events) {
        if (event.kind == edge::TcpIoKind::Accepted) {
            ++accepted;
        } else if (
            event.kind == edge::TcpIoKind::RejectedAtCapacity
        ) {
            ++rejected;
        }
    }

    EXPECT_EQ(accepted, 2u);
    EXPECT_EQ(rejected, 2u);

    // 剩余一个连接留到下一轮处理。
    const auto next = edge::pollTcpOnce(
        listener_.get(),
        connections_,
        1000
    );

    ASSERT_FALSE(next.error.has_value());
    ASSERT_EQ(next.events.size(), 1u);

    EXPECT_EQ(
        next.events.front().kind,
        edge::TcpIoKind::RejectedAtCapacity
    );
}

TEST_F(TcpPollerTest, RejectsNegativeTimeout)
{
    EXPECT_THROW(
        (void)edge::pollTcpOnce(
            listener_.get(),
            connections_,
            -1
        ),
        std::invalid_argument
    );
}


TEST_F(TcpPollerTest, PassesTcpBytesThroughEventProcessing)
{
    constexpr std::int64_t now_ms = 1700000000000;

    edge::TcpConfig config;
    config.max_connections = 2;

    edge::TcpEventProcessor processor{config};

    edge::SocketFd client;
    ASSERT_NO_FATAL_FAILURE(openClient(client));

    const auto accepted = edge::pollTcpOnce(
        listener_.get(),
        connections_,
        1000
    );

    ASSERT_FALSE(accepted.error.has_value());

    EXPECT_TRUE(
        processor.consume(accepted, now_ms).empty()
    );
    ASSERT_EQ(processor.activeCount(), 1u);

    const std::string payload = R"({
        "schema_version": 1,
        "device_id": "tcp_sensor",
        "session_id": "12345678-1234-1234-1234-123456789abc",
        "sequence": 1,
        "observed_at_ms": 1700000000000,
        "metric": "temperature",
        "value": 23.5,
        "unit": "degC"
    })";

    const auto encoded = edge::encodeFrame(payload);

    ASSERT_TRUE(
        std::holds_alternative<edge::EncodedFrame>(encoded)
    );

    const auto& frame =
        std::get<edge::EncodedFrame>(encoded);

    ASSERT_NO_FATAL_FAILURE(
        sendAll(
            client.get(),
            std::string_view{
                reinterpret_cast<const char*>(frame.data()),
                frame.size()
            }
        )
    );

    ASSERT_EQ(::shutdown(client.get(), SHUT_WR), 0);

    std::size_t valid_events = 0;

    for (int round = 0;
         round < 20 && connections_.size() != 0;
         ++round) {
        const auto io = edge::pollTcpOnce(
            listener_.get(),
            connections_,
            50
        );

        ASSERT_FALSE(io.error.has_value());

        const auto results = processor.consume(io, now_ms);

        for (const auto& result : results) {
            const auto* event =
                std::get_if<edge::SensorEvent>(&result.result);

            ASSERT_NE(event, nullptr);

            EXPECT_EQ(event->device_id, "tcp_sensor");
            EXPECT_EQ(event->sequence, 1);
            EXPECT_NE(result.connection_id, 0u);

            ++valid_events;
        }
    }

    EXPECT_EQ(valid_events, 1u);
    EXPECT_EQ(connections_.size(), 0u);
    EXPECT_EQ(processor.activeCount(), 0u);

    // 连接关闭后，累计统计仍然保留。
    EXPECT_EQ(processor.stats().valid_events, 1u);
}