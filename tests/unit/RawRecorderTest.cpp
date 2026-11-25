#include "edge/recording/RawRecorder.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iterator>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <unistd.h>

namespace {

namespace fs = std::filesystem;

constexpr char kRun[] =
    "0123456789abcdef0123456789abcdef";

constexpr char kPreviousRun[] =
    "abcdef0123456789abcdef0123456789";

edge::TcpIoEvent event(
    edge::TcpIoKind kind,
    std::size_t slot,
    const std::string& bytes = {}
)
{
    return edge::TcpIoEvent{
        kind,
        slot,
        std::vector<std::uint8_t>(
            bytes.begin(),
            bytes.end()
        ),
        0
    };
}

void send(
    edge::RawRecorder& recorder,
    std::initializer_list<edge::TcpIoEvent> events
)
{
    edge::TcpPollBatch batch;
    batch.events.assign(events.begin(), events.end());

    recorder.consume(batch);
}

void writeFile(
    const fs::path& path,
    const std::string& bytes
)
{
    std::ofstream output(
        path,
        std::ios::binary | std::ios::trunc
    );

    output.write(
        bytes.data(),
        static_cast<std::streamsize>(bytes.size())
    );

    output.close();

    if (!output) {
        throw std::runtime_error("Cannot write test file");
    }
}

std::string readFile(const fs::path& path)
{
    std::ifstream input(path, std::ios::binary);

    if (!input) {
        throw std::runtime_error("Cannot read test file");
    }

    return std::string(
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()
    );
}

// 即使测试中途退出，也恢复目录权限。
class PermissionRestore {
public:
    explicit PermissionRestore(fs::path path)
        : path_(std::move(path)),
          permissions_(fs::status(path_).permissions())
    {
    }

    ~PermissionRestore()
    {
        std::error_code error;

        fs::permissions(
            path_,
            permissions_,
            fs::perm_options::replace,
            error
        );
    }

private:
    fs::path path_;
    fs::perms permissions_;
};

class RawRecorderTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        std::string pattern =
            "/tmp/edge-raw-recorder-XXXXXX";

        std::vector<char> buffer(
            pattern.begin(),
            pattern.end()
        );

        buffer.push_back('\0');

        const char* created = ::mkdtemp(buffer.data());

        ASSERT_NE(created, nullptr);

        root_ = created;

        config_.enabled = true;
        config_.directory = root_ / "raw";
        config_.max_file_bytes = 64;
        config_.keep_closed_files = 8;
    }

    void TearDown() override
    {
        std::error_code error;
        fs::remove_all(root_, error);
    }

    fs::path segment(
        std::uint64_t connection,
        std::uint64_t part = 1,
        const std::string& run = kRun
    ) const
    {
        return config_.directory /
            edge::makeRawRecordingFileName(
                run,
                connection,
                part
            );
    }

    fs::path root_;
    edge::RecordingConfig config_;
};

TEST_F(RawRecorderTest, DisabledDoesNotCreateDirectory)
{
    config_.enabled = false;

    edge::RawRecorder recorder(config_, kRun, 2);

    send(recorder, {
        event(edge::TcpIoKind::Accepted, 0),
        event(edge::TcpIoKind::Data, 0, "ignored"),
        event(edge::TcpIoKind::Closed, 0)
    });

    recorder.finish();

    const auto stats = recorder.stats();

    EXPECT_FALSE(fs::exists(config_.directory));
    EXPECT_FALSE(stats.enabled);
    EXPECT_TRUE(stats.finished);

    EXPECT_EQ(stats.connections_seen, 0U);
    EXPECT_EQ(stats.bytes_received, 0U);
    EXPECT_EQ(stats.opened_files, 0U);
}

TEST_F(RawRecorderTest, CapturesSlotReuseInEventOrder)
{
    edge::RawRecorder recorder(config_, kRun, 2);

    ASSERT_TRUE(recorder.stats().operational);

    const std::string first("\0\xff" "A", 3);
    const std::string second = "second";

    edge::TcpPollBatch batch;

    batch.events = {
        event(edge::TcpIoKind::Accepted, 0),
        event(edge::TcpIoKind::Data, 0, first),
        event(edge::TcpIoKind::Closed, 0),

        event(edge::TcpIoKind::Accepted, 0),
        event(edge::TcpIoKind::Data, 0, second),
        event(edge::TcpIoKind::ClientError, 0)
    };

    // 即使本批最后出现 poll 错误，已有事件仍要消费。
    batch.error = edge::TcpPollError{
        "poll",
        EIO,
        "test error"
    };

    recorder.consume(batch);

    ASSERT_TRUE(fs::exists(segment(1)));
    ASSERT_TRUE(fs::exists(segment(2)));

    EXPECT_EQ(readFile(segment(1)), first);
    EXPECT_EQ(readFile(segment(2)), second);

    const auto stats = recorder.stats();

    EXPECT_EQ(stats.connections_seen, 2U);
    EXPECT_EQ(stats.bytes_received, 9U);
    EXPECT_EQ(stats.bytes_written, 9U);
    EXPECT_EQ(stats.bytes_skipped, 0U);

    EXPECT_EQ(stats.closed_files, 2U);
    EXPECT_EQ(stats.active_sessions, 0U);
    EXPECT_EQ(stats.active_files, 0U);

    EXPECT_EQ(stats.event_errors, 0U);
    EXPECT_EQ(stats.session_errors, 0U);
}

TEST_F(RawRecorderTest, KeepZeroProtectsOtherActiveFile)
{
    config_.keep_closed_files = 0;

    edge::RawRecorder recorder(config_, kRun, 2);

    ASSERT_TRUE(recorder.stats().operational);

    send(recorder, {
        event(edge::TcpIoKind::Accepted, 0),
        event(edge::TcpIoKind::Accepted, 1),
        event(edge::TcpIoKind::Data, 0, "active"),
        event(edge::TcpIoKind::Data, 1, "other"),
        event(edge::TcpIoKind::Closed, 1)
    });

    EXPECT_TRUE(fs::exists(segment(1)));
    EXPECT_FALSE(fs::exists(segment(2)));

    send(recorder, {
        event(edge::TcpIoKind::Data, 0, "tail")
    });

    EXPECT_EQ(readFile(segment(1)), "activetail");

    auto stats = recorder.stats();

    EXPECT_EQ(stats.active_files, 1U);
    EXPECT_EQ(stats.files_deleted, 1U);

    recorder.finish();

    stats = recorder.stats();

    EXPECT_FALSE(fs::exists(segment(1)));

    EXPECT_EQ(stats.bytes_received, 15U);
    EXPECT_EQ(stats.bytes_written, 15U);
    EXPECT_EQ(stats.files_deleted, 2U);

    ASSERT_TRUE(stats.retained_closed_files.has_value());
    EXPECT_EQ(*stats.retained_closed_files, 0U);
}

TEST_F(RawRecorderTest, RotationCanPruneInsideAppend)
{
    config_.max_file_bytes = 3;
    config_.keep_closed_files = 0;

    edge::RawRecorder recorder(config_, kRun, 2);

    ASSERT_TRUE(recorder.stats().operational);

    send(recorder, {
        event(edge::TcpIoKind::Accepted, 0),
        event(edge::TcpIoKind::Data, 0, "abcdefg")
    });

    EXPECT_FALSE(fs::exists(segment(1, 1)));
    EXPECT_FALSE(fs::exists(segment(1, 2)));

    ASSERT_TRUE(fs::exists(segment(1, 3)));
    EXPECT_EQ(readFile(segment(1, 3)), "g");

    auto stats = recorder.stats();

    EXPECT_EQ(stats.bytes_written, 7U);
    EXPECT_EQ(stats.opened_files, 3U);
    EXPECT_EQ(stats.closed_files, 2U);
    EXPECT_EQ(stats.active_files, 1U);
    EXPECT_EQ(stats.files_deleted, 2U);

    recorder.finish();

    stats = recorder.stats();

    EXPECT_FALSE(fs::exists(segment(1, 3)));
    EXPECT_EQ(stats.closed_files, 3U);
    EXPECT_EQ(stats.files_deleted, 3U);
    EXPECT_EQ(stats.session_errors, 0U);
}

TEST_F(RawRecorderTest, StartupRetentionIncludesPreviousRuns)
{
    config_.keep_closed_files = 2;

    fs::create_directories(config_.directory);

    const auto base =
        fs::file_time_type::clock::now() -
        std::chrono::hours(1);

    for (std::uint64_t id = 1; id <= 3; ++id) {
        const auto path = segment(id, 1, kPreviousRun);

        writeFile(path, "old");

        fs::last_write_time(
            path,
            base + std::chrono::seconds(id)
        );
    }

    const fs::path unrelated =
        config_.directory / "user.bin";

    const fs::path malformed =
        config_.directory / "edge-raw-v1-invalid.bin";

    const fs::path directory =
        segment(4, 1, kPreviousRun);

    const fs::path symlink =
        segment(5, 1, kPreviousRun);

    writeFile(unrelated, "user data");
    writeFile(malformed, "unmanaged");

    fs::create_directory(directory);
    fs::create_symlink(unrelated, symlink);

    edge::RawRecorder recorder(config_, kRun, 2);

    ASSERT_TRUE(recorder.stats().operational);

    EXPECT_FALSE(
        fs::exists(segment(1, 1, kPreviousRun))
    );

    EXPECT_TRUE(
        fs::exists(segment(2, 1, kPreviousRun))
    );

    EXPECT_TRUE(
        fs::exists(segment(3, 1, kPreviousRun))
    );

    EXPECT_EQ(readFile(unrelated), "user data");
    EXPECT_EQ(readFile(malformed), "unmanaged");

    EXPECT_TRUE(fs::is_directory(directory));
    EXPECT_TRUE(fs::is_symlink(symlink));

    const auto stats = recorder.stats();

    EXPECT_EQ(stats.files_deleted, 1U);

    ASSERT_TRUE(stats.retained_closed_files.has_value());
    EXPECT_EQ(*stats.retained_closed_files, 2U);
}

TEST_F(RawRecorderTest, DirectoryLockPreventsSecondRecorder)
{
    config_.keep_closed_files = 0;

    edge::RawRecorder first(config_, kRun, 2);

    ASSERT_TRUE(first.stats().operational);

    send(first, {
        event(edge::TcpIoKind::Accepted, 0),
        event(edge::TcpIoKind::Data, 0, "active")
    });

    edge::RawRecorder second(
        config_,
        kPreviousRun,
        2
    );

    EXPECT_FALSE(second.stats().operational);
    EXPECT_EQ(second.stats().startup_errors, 1U);

    // 第二个管理器不能把第一个的活动文件当成历史文件。
    EXPECT_TRUE(fs::exists(segment(1)));

    send(second, {
        event(edge::TcpIoKind::Accepted, 0),
        event(edge::TcpIoKind::Data, 0, "lost"),
        event(edge::TcpIoKind::Closed, 0)
    });

    EXPECT_EQ(second.stats().bytes_received, 4U);
    EXPECT_EQ(second.stats().bytes_written, 0U);
    EXPECT_EQ(second.stats().bytes_skipped, 4U);

    first.finish();

    // finish 后即可由新的管理器取得同一目录。
    edge::RawRecorder third(
        config_,
        kPreviousRun,
        2
    );

    EXPECT_TRUE(third.stats().operational);

    EXPECT_TRUE(fs::exists(
        config_.directory / ".edge-raw-v1.lock"
    ));
}

TEST_F(RawRecorderTest, StartupFailureIsVisibleAndSkipsBytes)
{
    writeFile(config_.directory, "marker");

    edge::RawRecorder recorder(config_, kRun, 2);

    EXPECT_FALSE(recorder.stats().operational);
    EXPECT_EQ(recorder.stats().startup_errors, 1U);

    send(recorder, {
        event(edge::TcpIoKind::Accepted, 0),
        event(edge::TcpIoKind::Data, 0, "lost"),
        event(edge::TcpIoKind::Closed, 0)
    });

    const auto stats = recorder.stats();

    EXPECT_EQ(stats.bytes_received, 4U);
    EXPECT_EQ(stats.bytes_written, 0U);
    EXPECT_EQ(stats.bytes_skipped, 4U);

    EXPECT_EQ(stats.session_errors, 0U);
    EXPECT_EQ(readFile(config_.directory), "marker");
}

TEST_F(RawRecorderTest, FailedSessionDoesNotBlockOtherConnection)
{
    edge::RawRecorder recorder(config_, kRun, 2);

    ASSERT_TRUE(recorder.stats().operational);

    // 模拟文件名冲突，底层必须拒绝覆盖。
    writeFile(segment(1), "marker");

    send(recorder, {
        event(edge::TcpIoKind::Accepted, 0),
        event(edge::TcpIoKind::Data, 0, "lost"),
        event(edge::TcpIoKind::Data, 0, "again"),

        event(edge::TcpIoKind::Accepted, 1),
        event(edge::TcpIoKind::Data, 1, "good"),

        event(edge::TcpIoKind::Closed, 0),
        event(edge::TcpIoKind::Closed, 1)
    });

    EXPECT_EQ(readFile(segment(1)), "marker");
    EXPECT_EQ(readFile(segment(2)), "good");

    const auto stats = recorder.stats();

    EXPECT_TRUE(stats.operational);

    EXPECT_EQ(stats.bytes_received, 13U);
    EXPECT_EQ(stats.bytes_written, 4U);
    EXPECT_EQ(stats.bytes_skipped, 9U);

    EXPECT_EQ(stats.session_errors, 1U);
    EXPECT_EQ(stats.last_error_connection, 1U);

    EXPECT_EQ(
        stats.last_error_stage,
        edge::RawRecorderErrorStage::Session
    );

    EXPECT_EQ(
        stats.last_session_operation,
        edge::RawRecordingOperation::Open
    );

    EXPECT_EQ(stats.last_error_code, EEXIST);
}

TEST_F(RawRecorderTest, FinishIsIdempotentAndTerminal)
{
    edge::RawRecorder recorder(config_, kRun, 2);

    send(recorder, {
        event(edge::TcpIoKind::Accepted, 0),
        event(edge::TcpIoKind::Data, 0, "abc")
    });

    recorder.finish();
    recorder.finish();

    send(recorder, {
        event(edge::TcpIoKind::Accepted, 1),
        event(edge::TcpIoKind::Data, 1, "ignored")
    });

    const auto stats = recorder.stats();

    EXPECT_TRUE(stats.finished);
    EXPECT_FALSE(stats.operational);

    EXPECT_EQ(stats.connections_seen, 1U);
    EXPECT_EQ(stats.bytes_received, 3U);
    EXPECT_EQ(stats.bytes_written, 3U);

    EXPECT_EQ(stats.closed_files, 1U);
    EXPECT_EQ(stats.active_sessions, 0U);
    EXPECT_EQ(stats.active_files, 0U);

    EXPECT_EQ(readFile(segment(1)), "abc");
}

TEST_F(RawRecorderTest, RetentionFailureDoesNotFailSession)
{
    if (::geteuid() == 0) {
        GTEST_SKIP()
            << "Permission failure requires a non-root user";
    }

    config_.keep_closed_files = 0;

    edge::RawRecorder recorder(config_, kRun, 2);

    ASSERT_TRUE(recorder.stats().operational);

    send(recorder, {
        event(edge::TcpIoKind::Accepted, 0),
        event(edge::TcpIoKind::Data, 0, "abc")
    });

    {
        PermissionRestore restore(config_.directory);

        fs::permissions(
            config_.directory,
            fs::perms::none,
            fs::perm_options::replace
        );

        send(recorder, {
            event(edge::TcpIoKind::Closed, 0)
        });

        const auto stats = recorder.stats();

        EXPECT_TRUE(stats.operational);
        EXPECT_EQ(stats.bytes_written, 3U);
        EXPECT_EQ(stats.closed_files, 1U);

        EXPECT_EQ(stats.session_errors, 0U);
        EXPECT_GT(stats.retention_errors, 0U);

        EXPECT_FALSE(
            stats.retained_closed_files.has_value()
        );
    }

    // 权限恢复后，finish 会再尝试清理。
    recorder.finish();

    const auto stats = recorder.stats();

    EXPECT_FALSE(fs::exists(segment(1)));

    ASSERT_TRUE(stats.retained_closed_files.has_value());
    EXPECT_EQ(*stats.retained_closed_files, 0U);
}

TEST_F(RawRecorderTest, UnexpectedDataIsAccountedAsSkipped)
{
    edge::RawRecorder recorder(config_, kRun, 2);

    // 尚未收到 Accepted。
    send(recorder, {
        event(edge::TcpIoKind::Data, 0, "orphan")
    });

    const auto stats = recorder.stats();

    EXPECT_EQ(stats.event_errors, 1U);

    EXPECT_EQ(stats.bytes_received, 6U);
    EXPECT_EQ(stats.bytes_written, 0U);
    EXPECT_EQ(stats.bytes_skipped, 6U);

    EXPECT_EQ(stats.opened_files, 0U);
    EXPECT_FALSE(fs::exists(segment(1)));
}

} // namespace