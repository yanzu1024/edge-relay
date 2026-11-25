#include "edge/status/JsonlWriter.h"

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

#include <fcntl.h>
#include <signal.h>
#include <unistd.h>

namespace {

using Json = nlohmann::json;

class SigpipeDisposition {
public:
    using Handler = void (*)(int);

    explicit SigpipeDisposition(Handler handler)
    {
        struct sigaction action{};
        action.sa_handler = handler;

        if (::sigemptyset(&action.sa_mask) == -1
            || ::sigaction(SIGPIPE, &action, &previous_) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "configure test SIGPIPE"
            );
        }
    }

    ~SigpipeDisposition() noexcept
    {
        (void)::sigaction(SIGPIPE, &previous_, nullptr);
    }

    SigpipeDisposition(const SigpipeDisposition&) = delete;

    SigpipeDisposition& operator=(
        const SigpipeDisposition&
    ) = delete;

private:
    struct sigaction previous_{};
};

class TestPipe {
public:
    TestPipe()
    {
        if (::pipe(descriptors_.data()) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "pipe"
            );
        }

        const int flags = ::fcntl(readFd(), F_GETFL);

        if (flags == -1
            || ::fcntl(readFd(), F_SETFL, flags | O_NONBLOCK) == -1) {
            const int error_code = errno;

            (void)::close(descriptors_[0]);
            (void)::close(descriptors_[1]);

            throw std::system_error(
                error_code,
                std::generic_category(),
                "configure test pipe"
            );
        }
    }

    ~TestPipe() noexcept
    {
        for (const int descriptor : descriptors_) {
            if (descriptor >= 0) {
                (void)::close(descriptor);
            }
        }
    }

    TestPipe(const TestPipe&) = delete;
    TestPipe& operator=(const TestPipe&) = delete;

    int readFd() const noexcept
    {
        return descriptors_[0];
    }

    int writeFd() const noexcept
    {
        return descriptors_[1];
    }

    void closeReader() noexcept
    {
        (void)::close(descriptors_[0]);
        descriptors_[0] = -1;
    }

private:
    std::array<int, 2> descriptors_{-1, -1};
};

std::string requireLine(edge::JsonlEncodeResult result)
{
    if (auto* line = std::get_if<std::string>(&result)) {
        return std::move(*line);
    }

    throw std::runtime_error(
        std::get<edge::JsonlEncodeError>(result).message
    );
}

edge::JsonlMessageMeta metadata(std::uint64_t sequence)
{
    return {
        "0123456789abcdef0123456789abcdef",
        sequence,
        1700000000200LL
    };
}

edge::StatusSnapshot snapshot()
{
    edge::StatusSnapshot value;

    value.agent_id = "demo-agent";
    value.checked_at_ms = 1700000000100LL;
    value.process_state = edge::StatusProcessState::Running;
    value.active_connections = 0;

    edge::StatusUploadObservation upload;
    upload.initialized = true;
    value.upload = upload;

    return value;
}

std::string readyLine()
{
    return requireLine(
        edge::encodeJsonlReady(
            metadata(1),
            {"demo-agent", "127.0.0.1", 19090, true}
        )
    );
}

std::string statusLine(std::uint64_t sequence)
{
    return requireLine(
        edge::encodeJsonlStatus(metadata(sequence), snapshot(), {})
    );
}

std::string recordLine(std::uint64_t sequence)
{
    edge::JsonlRecordData record;

    record.event.device_id = "sensor-01";
    record.event.session_id =
        "550e8400-e29b-41d4-a716-446655440000";
    record.event.sequence = sequence;
    record.event.observed_at_ms = 1700000000000LL;
    record.event.metric = "temperature";
    record.event.value = 21.5;
    record.event.unit = "degC";
    record.disposition = edge::JsonlRecordDisposition::Stored;

    return requireLine(
        edge::encodeJsonlRecord(metadata(sequence), record)
    );
}

std::string stoppedLine(std::uint64_t sequence)
{
    auto value = snapshot();
    value.process_state = edge::StatusProcessState::Stopped;
    value.upload->finished = true;
    value.upload->stop_requested = true;

    return requireLine(
        edge::encodeJsonlStopped(metadata(sequence), 0, value, {})
    );
}

std::string readAvailable(int descriptor)
{
    std::string result;
    std::array<char, 4096> buffer{};

    for (;;) {
        const auto count = ::read(
            descriptor,
            buffer.data(),
            buffer.size()
        );

        if (count > 0) {
            result.append(
                buffer.data(),
                static_cast<std::size_t>(count)
            );
            continue;
        }

        if (count == 0) {
            return result;
        }

        if (errno == EINTR) {
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return result;
        }

        throw std::system_error(
            errno,
            std::generic_category(),
            "read test pipe"
        );
    }
}

std::string flushAvailable(
    edge::JsonlWriter& writer,
    const TestPipe& pipe,
    edge::JsonlWriter::TimePoint now
)
{
    std::string result;

    for (int attempt = 0; attempt < 1000; ++attempt) {
        const auto state = writer.drain(now);

        result += readAvailable(pipe.readFd());

        if (writer.empty()) {
            return result;
        }

        if (state == edge::JsonlDrainResult::Disabled) {
            throw std::runtime_error(
                "Test writer unexpectedly disabled"
            );
        }
    }

    throw std::runtime_error("Test writer did not drain");
}

std::vector<std::uint64_t> sequences(const std::string& text)
{
    std::vector<std::uint64_t> result;
    std::size_t offset = 0;

    while (offset < text.size()) {
        const auto end = text.find('\n', offset);

        if (end == std::string::npos) {
            throw std::runtime_error("Incomplete test JSONL");
        }

        const auto message = Json::parse(
            text.substr(offset, end - offset)
        );

        result.push_back(
            message["sequence"].get<std::uint64_t>()
        );

        offset = end + 1;
    }

    return result;
}

class JsonlWriterTest : public ::testing::Test {
protected:
    SigpipeDisposition sigpipe_{SIG_IGN};
    TestPipe pipe_;

    edge::JsonlWriter::TimePoint now_ =
        edge::JsonlWriter::TimePoint{}
        + std::chrono::seconds(10);
};

TEST_F(JsonlWriterTest, RestoresFlagsAndKeepsBorrowedDescriptorOpen)
{
    const int original = ::fcntl(pipe_.writeFd(), F_GETFL);
    ASSERT_NE(original, -1);

    {
        edge::JsonlWriter writer(pipe_.writeFd());

        EXPECT_NE(
            ::fcntl(pipe_.writeFd(), F_GETFL) & O_NONBLOCK,
            0
        );

        writer.enqueue(readyLine());

        EXPECT_EQ(
            flushAvailable(writer, pipe_, now_),
            readyLine()
        );
    }

    EXPECT_EQ(::fcntl(pipe_.writeFd(), F_GETFL), original);
}

TEST_F(JsonlWriterTest, CoalescesStatusAndPreservesSequenceOrder)
{
    edge::JsonlWriter writer(pipe_.writeFd());

    writer.enqueue(readyLine());
    writer.enqueue(statusLine(2));
    writer.enqueue(recordLine(3));

    EXPECT_EQ(
        writer.enqueue(statusLine(4)),
        edge::JsonlEnqueueResult::Coalesced
    );

    writer.enqueue(recordLine(5));
    writer.enqueue(stoppedLine(6));

    EXPECT_EQ(
        flushAvailable(writer, pipe_, now_),
        readyLine() + recordLine(3) + statusLine(4)
            + recordLine(5) + stoppedLine(6)
    );

    EXPECT_EQ(writer.stats().status_coalesced, 1);
    EXPECT_EQ(writer.stats().display_dropped, 0);
}

TEST_F(JsonlWriterTest, BoundsRecordQueueWithoutDroppingLifecycle)
{
    edge::JsonlWriter writer(pipe_.writeFd());

    writer.enqueue(readyLine());

    for (std::uint64_t sequence = 2; sequence <= 65; ++sequence) {
        EXPECT_EQ(
            writer.enqueue(recordLine(sequence)),
            edge::JsonlEnqueueResult::Queued
        );
    }

    EXPECT_EQ(
        writer.enqueue(recordLine(66)),
        edge::JsonlEnqueueResult::Dropped
    );

    writer.enqueue(statusLine(67));
    writer.enqueue(stoppedLine(68));

    const auto delivered = sequences(
        flushAvailable(writer, pipe_, now_)
    );

    ASSERT_EQ(delivered.size(), 67);
    EXPECT_EQ(delivered.front(), 1);
    EXPECT_EQ(delivered.back(), 68);

    EXPECT_EQ(writer.stats().record_queue_drops, 1);
    EXPECT_EQ(writer.stats().display_dropped, 1);
}

TEST_F(JsonlWriterTest, KeepsPartialLineIntact)
{
    edge::JsonlWriter writer(pipe_.writeFd());

    writer.enqueue(readyLine());
    std::string output = flushAvailable(writer, pipe_, now_);

    writer.enqueue(statusLine(2));

    EXPECT_EQ(
        writer.drain(now_, 1),
        edge::JsonlDrainResult::Pending
    );

    output += readAvailable(pipe_.readFd());

    writer.enqueue(statusLine(3));
    writer.enqueue(statusLine(4));
    writer.enqueue(stoppedLine(5));

    output += flushAvailable(writer, pipe_, now_);

    EXPECT_EQ(
        output,
        readyLine() + statusLine(2)
            + statusLine(4) + stoppedLine(5)
    );

    EXPECT_EQ(writer.stats().status_coalesced, 1);
}

TEST_F(JsonlWriterTest, ReturnsWhenPipeIsFull)
{
    edge::JsonlWriter writer(pipe_.writeFd());
    std::array<char, 1024> filler{};

    // 此时写端已经由 writer 设置为非阻塞。
    for (;;) {
        const auto count = ::write(
            pipe_.writeFd(),
            filler.data(),
            filler.size()
        );

        if (count > 0) {
            continue;
        }

        if (count < 0 && errno == EINTR) {
            continue;
        }

        ASSERT_EQ(count, -1);
        ASSERT_TRUE(errno == EAGAIN || errno == EWOULDBLOCK);
        break;
    }

    writer.enqueue(readyLine());

    EXPECT_EQ(
        writer.drain(now_),
        edge::JsonlDrainResult::WouldBlock
    );

    EXPECT_TRUE(writer.enabled());
    EXPECT_FALSE(writer.empty());
    EXPECT_EQ(writer.stats().written_bytes, 0);
    EXPECT_EQ(writer.stats().io_errors, 0);
}

TEST_F(JsonlWriterTest, AppliesRateLimitToRecordsOnly)
{
    edge::JsonlWriter writer(pipe_.writeFd());

    writer.enqueue(readyLine());
    (void)flushAvailable(writer, pipe_, now_);

    for (std::uint64_t sequence = 2; sequence <= 101; ++sequence) {
        writer.enqueue(recordLine(sequence));
        (void)flushAvailable(writer, pipe_, now_);
    }

    writer.enqueue(recordLine(102));
    writer.enqueue(statusLine(103));
    writer.enqueue(stoppedLine(104));

    EXPECT_EQ(
        flushAvailable(writer, pipe_, now_),
        statusLine(103) + stoppedLine(104)
    );

    EXPECT_EQ(writer.stats().record_rate_drops, 1);
    EXPECT_EQ(writer.stats().display_dropped, 1);
}

TEST_F(JsonlWriterTest, SlidingWindowRecoversAtOneSecond)
{
    edge::JsonlWriter writer(pipe_.writeFd());

    writer.enqueue(readyLine());
    (void)flushAvailable(writer, pipe_, now_);

    for (std::uint64_t sequence = 2; sequence <= 101; ++sequence) {
        writer.enqueue(recordLine(sequence));
        (void)flushAvailable(writer, pipe_, now_);
    }

    writer.enqueue(recordLine(102));

    EXPECT_TRUE(
        flushAvailable(
            writer,
            pipe_,
            now_ + std::chrono::milliseconds(999)
        ).empty()
    );

    writer.enqueue(recordLine(103));

    EXPECT_EQ(
        flushAvailable(
            writer,
            pipe_,
            now_ + std::chrono::seconds(1)
        ),
        recordLine(103)
    );

    EXPECT_EQ(writer.stats().record_rate_drops, 1);
}

TEST_F(JsonlWriterTest, HandlesClosedReaderAsError)
{
    edge::JsonlWriter writer(pipe_.writeFd());

    writer.enqueue(readyLine());
    writer.enqueue(recordLine(2));

    pipe_.closeReader();

    EXPECT_EQ(
        writer.drain(now_),
        edge::JsonlDrainResult::Disabled
    );

    EXPECT_FALSE(writer.enabled());
    EXPECT_TRUE(writer.empty());

    EXPECT_EQ(writer.stats().io_errors, 1);
    EXPECT_EQ(writer.stats().last_error, EPIPE);
    EXPECT_EQ(writer.stats().abandoned_lines, 2);
    EXPECT_EQ(writer.stats().display_dropped, 1);

    EXPECT_EQ(
        writer.enqueue(recordLine(3)),
        edge::JsonlEnqueueResult::Disabled
    );

    EXPECT_EQ(writer.stats().display_dropped, 2);
}

TEST_F(JsonlWriterTest, AbandonsPartialLineWithoutAppending)
{
    edge::JsonlWriter writer(pipe_.writeFd());

    writer.enqueue(readyLine());
    (void)writer.drain(now_, 1);

    writer.enqueue(recordLine(2));
    writer.abandon();

    EXPECT_EQ(readAvailable(pipe_.readFd()), "{");

    EXPECT_EQ(writer.stats().abandoned_lines, 2);
    EXPECT_EQ(writer.stats().partial_lines_abandoned, 1);
    EXPECT_EQ(writer.stats().display_dropped, 1);

    EXPECT_EQ(
        writer.drain(now_),
        edge::JsonlDrainResult::Disabled
    );
}

TEST_F(JsonlWriterTest, RejectsInvalidMessageOrderAndRun)
{
    edge::JsonlWriter writer(pipe_.writeFd());

    EXPECT_THROW(
        writer.enqueue(statusLine(2)),
        std::invalid_argument
    );

    writer.enqueue(readyLine());

    EXPECT_THROW(
        writer.enqueue(readyLine()),
        std::invalid_argument
    );

    // 展示序号允许有间隔。
    writer.enqueue(statusLine(3));

    EXPECT_THROW(
        writer.enqueue(recordLine(2)),
        std::invalid_argument
    );

    auto other_run = Json::parse(recordLine(4));
    other_run["run_id"] = std::string(32, 'a');

    EXPECT_THROW(
        writer.enqueue(other_run.dump() + "\n"),
        std::invalid_argument
    );

    writer.enqueue(stoppedLine(4));

    EXPECT_THROW(
        writer.enqueue(statusLine(5)),
        std::invalid_argument
    );
}

TEST_F(JsonlWriterTest, RejectsMalformedLines)
{
    edge::JsonlWriter writer(pipe_.writeFd());

    EXPECT_THROW(
        writer.enqueue(""),
        std::invalid_argument
    );

    auto missing_newline = readyLine();
    missing_newline.pop_back();

    EXPECT_THROW(
        writer.enqueue(missing_newline),
        std::invalid_argument
    );

    EXPECT_THROW(
        writer.enqueue(readyLine() + "\n"),
        std::invalid_argument
    );

    EXPECT_THROW(
        writer.enqueue("{}\n"),
        std::invalid_argument
    );

    EXPECT_THROW(
        writer.enqueue(
            std::string(edge::kMaxJsonlLineBytes, 'x') + "\n"
        ),
        std::invalid_argument
    );

    // 拒绝错误输入后，发送器仍可接收合法首消息。
    writer.enqueue(readyLine());

    EXPECT_EQ(
        flushAvailable(writer, pipe_, now_),
        readyLine()
    );
}

TEST_F(JsonlWriterTest, BoundsBytesPerDrain)
{
    edge::JsonlWriter writer(pipe_.writeFd());

    writer.enqueue(readyLine());

    EXPECT_EQ(
        writer.drain(now_, 7),
        edge::JsonlDrainResult::Pending
    );

    auto output = readAvailable(pipe_.readFd());

    EXPECT_EQ(output.size(), 7);
    EXPECT_EQ(writer.stats().written_bytes, 7);

    output += flushAvailable(writer, pipe_, now_);

    EXPECT_EQ(output, readyLine());
}

TEST_F(JsonlWriterTest, ExplicitRestoreDisablesWriter)
{
    const int original = ::fcntl(pipe_.writeFd(), F_GETFL);
    ASSERT_NE(original, -1);

    edge::JsonlWriter writer(pipe_.writeFd());
    writer.enqueue(readyLine());

    EXPECT_EQ(writer.restoreFlags(), 0);

    EXPECT_FALSE(writer.enabled());
    EXPECT_TRUE(writer.empty());
    EXPECT_EQ(writer.stats().abandoned_lines, 1);

    EXPECT_EQ(
        ::fcntl(pipe_.writeFd(), F_GETFL),
        original
    );

    EXPECT_EQ(
        writer.drain(now_),
        edge::JsonlDrainResult::Disabled
    );
}

TEST(JsonlWriterSignalTest, RequiresIgnoredSigpipe)
{
    SigpipeDisposition disposition(SIG_DFL);
    TestPipe pipe;

    EXPECT_THROW(
        edge::JsonlWriter(pipe.writeFd()),
        std::logic_error
    );
}

} // namespace