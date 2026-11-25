#include "edge/recording/RawRecordingSession.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

#include <signal.h>
#include <sys/resource.h>

namespace {

namespace fs = std::filesystem;

constexpr const char* kRunId =
    "0123456789abcdef0123456789abcdef";

class TemporaryDirectory {
public:
    TemporaryDirectory()
        : name_(
            (
                fs::temp_directory_path()
                / "edge-raw-session-XXXXXX"
            ).string()
        )
    {
        if (::mkdtemp(name_.data()) == nullptr) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "mkdtemp"
            );
        }
    }

    ~TemporaryDirectory() noexcept
    {
        std::error_code error;
        fs::remove_all(name_, error);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;

    TemporaryDirectory& operator=(
        const TemporaryDirectory&
    ) = delete;

    fs::path path() const
    {
        return name_;
    }

private:
    std::string name_;
};

std::string readFile(const fs::path& path)
{
    std::ifstream input(path, std::ios::binary);

    if (!input) {
        throw std::runtime_error("Cannot read test recording");
    }

    return std::string(
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>()
    );
}

void seedFile(
    const fs::path& path,
    const std::string& contents
)
{
    std::ofstream output;
    output.exceptions(
        std::ios::badbit | std::ios::failbit
    );

    output.open(path, std::ios::binary);
    output << contents;
    output.close();
}

bool appendText(
    edge::RawRecordingSession& session,
    const std::string& text
)
{
    return session.append(
        reinterpret_cast<const std::uint8_t*>(text.data()),
        text.size()
    );
}

// 仅改变当前测试进程的软限制和 SIGXFSZ 处理。
// 离开作用域后恢复，制造真实的文件写入失败。
class ScopedFileSizeLimit {
public:
    explicit ScopedFileSizeLimit(rlim_t bytes)
    {
        if (::getrlimit(RLIMIT_FSIZE, &previous_limit_) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "getrlimit"
            );
        }

        if (previous_limit_.rlim_max != RLIM_INFINITY
            && previous_limit_.rlim_max < bytes) {
            throw std::runtime_error(
                "Test file size exceeds hard resource limit"
            );
        }

        struct sigaction action{};
        action.sa_handler = SIG_IGN;

        if (::sigemptyset(&action.sa_mask) == -1
            || ::sigaction(
                SIGXFSZ,
                &action,
                &previous_signal_
            ) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "configure SIGXFSZ"
            );
        }

        auto limit = previous_limit_;
        limit.rlim_cur = bytes;

        if (::setrlimit(RLIMIT_FSIZE, &limit) == -1) {
            const int code = errno;

            (void)::sigaction(
                SIGXFSZ,
                &previous_signal_,
                nullptr
            );

            throw std::system_error(
                code,
                std::generic_category(),
                "setrlimit"
            );
        }
    }

    ~ScopedFileSizeLimit() noexcept
    {
        (void)::setrlimit(RLIMIT_FSIZE, &previous_limit_);

        (void)::sigaction(
            SIGXFSZ,
            &previous_signal_,
            nullptr
        );
    }

    ScopedFileSizeLimit(const ScopedFileSizeLimit&) = delete;

    ScopedFileSizeLimit& operator=(
        const ScopedFileSizeLimit&
    ) = delete;

private:
    struct rlimit previous_limit_{};
    struct sigaction previous_signal_{};
};

class RawRecordingSessionTest : public ::testing::Test {
protected:
    edge::RawRecordingSession makeSession(
        std::uint64_t limit = 64,
        std::uint64_t connection_id = 1
    )
    {
        return edge::RawRecordingSession(
            directory_.path(),
            kRunId,
            connection_id,
            limit,
            [this](const edge::RawClosedSegment& segment) {
                closed_.push_back(segment);
            }
        );
    }

    TemporaryDirectory directory_;
    std::vector<edge::RawClosedSegment> closed_;
};

TEST_F(RawRecordingSessionTest, EmptyInputCreatesNoFile)
{
    auto session = makeSession();

    EXPECT_TRUE(session.append(nullptr, 0));
    EXPECT_TRUE(session.close());

    EXPECT_TRUE(fs::is_empty(directory_.path()));
    EXPECT_TRUE(closed_.empty());

    EXPECT_EQ(session.stats().opened_files, 0);
    EXPECT_EQ(session.stats().bytes_written, 0);
    EXPECT_TRUE(session.stats().finished);
}

TEST_F(RawRecordingSessionTest, PreservesAllByteValuesAcrossCalls)
{
    auto session = makeSession(1024);

    std::string input;

    for (unsigned value = 0; value < 256; ++value) {
        input.push_back(static_cast<char>(value));
    }

    ASSERT_TRUE(appendText(session, input.substr(0, 97)));
    ASSERT_TRUE(appendText(session, input.substr(97)));
    ASSERT_TRUE(session.close());

    ASSERT_EQ(closed_.size(), 1);
    EXPECT_EQ(readFile(closed_[0].path), input);

    EXPECT_EQ(session.stats().bytes_written, 256);
    EXPECT_FALSE(session.stats().failed);
}

TEST_F(RawRecordingSessionTest, SplitsInputAtConfiguredLimit)
{
    auto session = makeSession(5);

    ASSERT_TRUE(appendText(session, "abcdefghijkl"));

    // 两个完整分段已关闭，第三个分段仍打开。
    EXPECT_EQ(session.stats().opened_files, 3);
    EXPECT_EQ(session.stats().closed_files, 2);
    EXPECT_FALSE(session.activePath().empty());

    ASSERT_TRUE(session.close());
    ASSERT_EQ(closed_.size(), 3);

    EXPECT_EQ(readFile(closed_[0].path), "abcde");
    EXPECT_EQ(readFile(closed_[1].path), "fghij");
    EXPECT_EQ(readFile(closed_[2].path), "kl");

    for (std::size_t index = 0; index < closed_.size(); ++index) {
        EXPECT_EQ(closed_[index].connection_id, 1);
        EXPECT_EQ(closed_[index].part_index, index + 1);
        EXPECT_LE(closed_[index].bytes, 5);
        EXPECT_LE(fs::file_size(closed_[index].path), 5);
    }

    EXPECT_EQ(session.stats().bytes_written, 12);
    EXPECT_EQ(session.stats().closed_files, 3);
}

TEST_F(RawRecordingSessionTest, ExactBoundaryCreatesNoEmptySuccessor)
{
    auto session = makeSession(5);

    ASSERT_TRUE(appendText(session, "abcdefghij"));

    EXPECT_TRUE(session.activePath().empty());
    EXPECT_EQ(session.stats().opened_files, 2);
    EXPECT_EQ(session.stats().closed_files, 2);

    ASSERT_TRUE(session.close());

    EXPECT_EQ(closed_.size(), 2);

    EXPECT_EQ(
        std::distance(
            fs::directory_iterator(directory_.path()),
            fs::directory_iterator()
        ),
        2
    );
}

TEST_F(RawRecordingSessionTest, SeparatesConnections)
{
    auto first = makeSession(64, 1);
    auto second = makeSession(64, 2);

    ASSERT_TRUE(appendText(first, "first"));
    ASSERT_TRUE(appendText(second, "second"));

    EXPECT_NE(first.activePath(), second.activePath());

    ASSERT_TRUE(first.close());
    ASSERT_TRUE(second.close());

    ASSERT_EQ(closed_.size(), 2);
    EXPECT_EQ(readFile(closed_[0].path), "first");
    EXPECT_EQ(readFile(closed_[1].path), "second");

    EXPECT_EQ(closed_[0].connection_id, 1);
    EXPECT_EQ(closed_[1].connection_id, 2);
}

TEST_F(RawRecordingSessionTest, ExistingFileIsNeverOverwritten)
{
    const auto path = directory_.path()
        / edge::makeRawRecordingFileName(kRunId, 1, 1);

    seedFile(path, "keep-existing");

    auto session = makeSession();

    EXPECT_FALSE(appendText(session, "replacement"));
    EXPECT_TRUE(session.stats().failed);

    EXPECT_EQ(
        session.stats().error_operation,
        edge::RawRecordingOperation::Open
    );

    EXPECT_EQ(session.stats().error_code, EEXIST);
    EXPECT_EQ(session.stats().opened_files, 0);
    EXPECT_EQ(readFile(path), "keep-existing");
}

TEST_F(RawRecordingSessionTest, WriteFailureStopsSession)
{
    auto session = makeSession();

    bool written = true;

    {
        ScopedFileSizeLimit limit(3);
        written = appendText(session, "abcdef");
    }

    // 资源限制已经恢复，再执行测试断言。
    EXPECT_FALSE(written);
    EXPECT_TRUE(session.stats().failed);

    EXPECT_EQ(
        session.stats().error_operation,
        edge::RawRecordingOperation::Write
    );

    EXPECT_EQ(session.stats().error_code, EFBIG);
    EXPECT_EQ(session.stats().bytes_written, 3);

    const auto path = directory_.path()
        / edge::makeRawRecordingFileName(kRunId, 1, 1);

    EXPECT_EQ(readFile(path), "abc");

    // 即使外部故障已消失，本连接也不偷偷恢复录制。
    EXPECT_FALSE(appendText(session, "more"));
    EXPECT_EQ(readFile(path), "abc");
    EXPECT_FALSE(session.close());
}

TEST_F(RawRecordingSessionTest, NotificationFailureIsVisible)
{
    int notifications = 0;

    edge::RawRecordingSession session(
        directory_.path(),
        kRunId,
        1,
        3,
        [&](const edge::RawClosedSegment&) {
            ++notifications;
            throw std::runtime_error("notification failure");
        }
    );

    EXPECT_FALSE(appendText(session, "abc"));

    EXPECT_TRUE(session.stats().failed);
    EXPECT_EQ(session.stats().closed_files, 1);

    EXPECT_EQ(
        session.stats().error_operation,
        edge::RawRecordingOperation::Notify
    );

    EXPECT_EQ(notifications, 1);
    EXPECT_FALSE(appendText(session, "more"));
    EXPECT_FALSE(session.close());
    EXPECT_EQ(notifications, 1);

    const auto path = directory_.path()
        / edge::makeRawRecordingFileName(kRunId, 1, 1);

    EXPECT_EQ(readFile(path), "abc");
}

TEST_F(RawRecordingSessionTest, CloseIsIdempotentAndTerminal)
{
    auto session = makeSession();

    ASSERT_TRUE(appendText(session, "abc"));
    ASSERT_TRUE(session.close());

    EXPECT_TRUE(session.close());
    EXPECT_FALSE(appendText(session, "more"));

    EXPECT_TRUE(session.stats().finished);
    EXPECT_FALSE(session.stats().failed);
    EXPECT_EQ(session.stats().bytes_written, 3);
    EXPECT_EQ(closed_.size(), 1);
}

TEST_F(RawRecordingSessionTest, RejectsInvalidParameters)
{
    EXPECT_THROW(
        edge::RawRecordingSession(
            directory_.path(), "bad-id", 1, 64
        ),
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::RawRecordingSession(
            directory_.path(), kRunId, 0, 64
        ),
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::RawRecordingSession(
            directory_.path(), kRunId, 1, 0
        ),
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::RawRecordingSession(
            "relative-directory", kRunId, 1, 64
        ),
        std::invalid_argument
    );

    EXPECT_TRUE(fs::is_empty(directory_.path()));
}

TEST_F(RawRecordingSessionTest, NullInputIsReportedWithoutCreatingFile)
{
    auto session = makeSession();

    EXPECT_FALSE(session.append(nullptr, 1));

    EXPECT_TRUE(session.stats().failed);
    EXPECT_EQ(session.stats().error_code, EINVAL);

    EXPECT_EQ(
        session.stats().error_operation,
        edge::RawRecordingOperation::Input
    );

    EXPECT_TRUE(fs::is_empty(directory_.path()));
}

} // namespace