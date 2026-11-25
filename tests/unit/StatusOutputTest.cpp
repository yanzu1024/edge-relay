#include "edge/status/StatusOutput.h"
#include "edge/storage/OutboxSchema.h"
#include "edge/storage/SqliteDatabase.h"

#include <gtest/gtest.h>
#include <sqlite3.h>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <stdexcept>
#include <string>
#include <system_error>

namespace {

namespace fs = std::filesystem;

class TemporaryDirectory {
public:
    TemporaryDirectory()
        : name_(
            (
                fs::temp_directory_path()
                / "edge-status-XXXXXX"
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
        throw std::runtime_error("Cannot read test file");
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

class StatusOutputTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        edge::initializeOutboxSchema(database_);
    }

    fs::path root() const
    {
        return directory_.path();
    }

    edge::StatusSnapshot snapshot() const
    {
        edge::StatusSnapshot value;
        value.agent_id = "test-agent";
        value.checked_at_ms = 1700000000100LL;
        return value;
    }

    TemporaryDirectory directory_;
    edge::SqliteDatabase database_{":memory:"};
};

TEST_F(StatusOutputTest, ReadsRealOutboxAndValidZeroDisk)
{
    edge::OutboxStore store(database_);
    auto value = snapshot();

    edge::sampleStatusResources(
        value,
        store,
        root() / "outbox.db",
        [](const fs::path&) -> std::uint64_t {
            return 0;
        }
    );

    ASSERT_EQ(
        value.outbox.state,
        edge::StatusSampleState::Valid
    );
    ASSERT_TRUE(value.outbox.value.has_value());
    EXPECT_EQ(value.outbox.value->total, 0);

    ASSERT_EQ(
        value.disk_available_bytes.state,
        edge::StatusSampleState::Valid
    );
    ASSERT_TRUE(value.disk_available_bytes.value.has_value());
    EXPECT_EQ(*value.disk_available_bytes.value, 0);

    EXPECT_EQ(sqlite3_next_stmt(database_.get(), nullptr), nullptr);
}

TEST_F(StatusOutputTest, CapturesQueryFailureAndRecovers)
{
    edge::OutboxStore store(database_);
    auto value = snapshot();

    value.counters.storage_errors = 7;

    const auto deny_read = [](
        void*,
        int action,
        const char* first,
        const char*,
        const char*,
        const char*
    ) -> int {
        if (action == SQLITE_READ
            && first != nullptr
            && std::strcmp(first, "outbox") == 0) {
            return SQLITE_DENY;
        }

        return SQLITE_OK;
    };

    ASSERT_EQ(
        sqlite3_set_authorizer(
            database_.get(),
            deny_read,
            nullptr
        ),
        SQLITE_OK
    );

    edge::sampleStatusResources(
        value,
        store,
        root(),
        [](const fs::path&) -> std::uint64_t {
            return 123;
        }
    );

    const int restored = sqlite3_set_authorizer(
        database_.get(),
        nullptr,
        nullptr
    );

    ASSERT_EQ(restored, SQLITE_OK);

    EXPECT_EQ(value.outbox.state, edge::StatusSampleState::Error);
    EXPECT_FALSE(value.outbox.value.has_value());
    EXPECT_FALSE(value.outbox.error.empty());

    ASSERT_TRUE(value.disk_available_bytes.value.has_value());
    EXPECT_EQ(*value.disk_available_bytes.value, 123);

    EXPECT_EQ(value.counters.storage_errors, 7);
    EXPECT_EQ(sqlite3_next_stmt(database_.get(), nullptr), nullptr);

    edge::sampleStatusResources(value, store, root());

    EXPECT_EQ(value.outbox.state, edge::StatusSampleState::Valid);
    EXPECT_TRUE(value.outbox.error.empty());
    ASSERT_TRUE(value.outbox.value.has_value());
    EXPECT_EQ(value.outbox.value->total, 0);
}

TEST_F(StatusOutputTest, ReportsRealDiskQueryFailure)
{
    edge::OutboxStore store(database_);
    auto value = snapshot();

    const auto missing = root() / "missing" / "outbox.db";

    edge::sampleStatusResources(value, store, missing);

    EXPECT_EQ(value.outbox.state, edge::StatusSampleState::Valid);

    EXPECT_EQ(
        value.disk_available_bytes.state,
        edge::StatusSampleState::Error
    );
    EXPECT_FALSE(value.disk_available_bytes.value.has_value());
    EXPECT_FALSE(value.disk_available_bytes.error.empty());

    EXPECT_FALSE(fs::exists(missing.parent_path()));
}

TEST_F(StatusOutputTest, QueriesRealFilesystemWithoutCreatingFiles)
{
    edge::OutboxStore store(database_);
    auto value = snapshot();

    edge::sampleStatusResources(value, store, root());

    EXPECT_EQ(
        value.disk_available_bytes.state,
        edge::StatusSampleState::Valid
    );
    EXPECT_TRUE(value.disk_available_bytes.value.has_value());
    EXPECT_TRUE(fs::is_empty(root()));
}

TEST_F(StatusOutputTest, ReplacesCompleteFileWithoutTemporaryRemains)
{
    const auto destination = root() / "nested" / "status.json";
    const auto database_path = root() / "outbox.db";

    edge::writeStatusFileAtomically(
        destination,
        database_path,
        R"({"value":1})"
    );

    EXPECT_EQ(readFile(destination), R"({"value":1})");

    edge::writeStatusFileAtomically(
        destination,
        database_path,
        R"({"value":2,"complete":true})"
    );

    EXPECT_EQ(
        readFile(destination),
        R"({"value":2,"complete":true})"
    );

    EXPECT_EQ(
        std::distance(
            fs::directory_iterator(destination.parent_path()),
            fs::directory_iterator()
        ),
        1
    );
}

TEST_F(StatusOutputTest, RemovesTemporaryFileWhenRenameFails)
{
    const auto destination = root() / "occupied";
    fs::create_directory(destination);
    seedFile(destination / "marker", "keep");

    EXPECT_THROW(
        edge::writeStatusFileAtomically(
            destination,
            root() / "outbox.db",
            R"({"value":1})"
        ),
        std::system_error
    );

    EXPECT_EQ(readFile(destination / "marker"), "keep");

    EXPECT_EQ(
        std::distance(
            fs::directory_iterator(root()),
            fs::directory_iterator()
        ),
        1
    );
}

TEST_F(StatusOutputTest, ProtectsDatabaseAndLockFile)
{
    const auto database_path = root() / "outbox.db";
    const auto lock_path = root() / "outbox.db.lock";

    seedFile(database_path, "database");
    seedFile(lock_path, "lock");

    EXPECT_THROW(
        edge::writeStatusFileAtomically(
            database_path,
            database_path,
            "replacement"
        ),
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::writeStatusFileAtomically(
            lock_path,
            database_path,
            "replacement"
        ),
        std::invalid_argument
    );

    EXPECT_EQ(readFile(database_path), "database");
    EXPECT_EQ(readFile(lock_path), "lock");
}

TEST_F(StatusOutputTest, ProtectsHardlinkAndSymlinkAliases)
{
    const auto database_path = root() / "outbox.db";
    const auto hardlink = root() / "hardlink.json";
    const auto symlink = root() / "symlink.json";

    seedFile(database_path, "database");

    fs::create_hard_link(database_path, hardlink);
    fs::create_symlink(database_path, symlink);

    EXPECT_THROW(
        edge::writeStatusFileAtomically(
            hardlink,
            database_path,
            "replacement"
        ),
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::writeStatusFileAtomically(
            symlink,
            database_path,
            "replacement"
        ),
        std::invalid_argument
    );

    EXPECT_EQ(readFile(database_path), "database");
    EXPECT_EQ(readFile(hardlink), "database");
    EXPECT_TRUE(fs::is_symlink(symlink));
}

TEST_F(StatusOutputTest, RejectsRelativeAndNulPaths)
{
    const auto database_path = root() / "outbox.db";

    EXPECT_THROW(
        edge::writeStatusFileAtomically(
            "relative-status.json",
            database_path,
            "{}"
        ),
        std::invalid_argument
    );

    std::string invalid = (root() / "status").string();
    invalid.push_back('\0');
    invalid += ".json";

    EXPECT_THROW(
        edge::writeStatusFileAtomically(
            fs::path(invalid),
            database_path,
            "{}"
        ),
        std::invalid_argument
    );

    EXPECT_TRUE(fs::is_empty(root()));
}

} // namespace