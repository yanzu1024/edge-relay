#include "ExportDead.h"
#include "edge/storage/OutboxSchema.h"

#include <nlohmann/json.hpp>
#include <sqlite3.h>

#include <cerrno>
#include <cstdlib>
#include <initializer_list>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include <unistd.h>

namespace edge {
namespace {

using Database =
    std::unique_ptr<sqlite3, decltype(&sqlite3_close_v2)>;

using Statement =
    std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)>;

void execute(sqlite3* database, const char* sql)
{
    if (
        sqlite3_exec(
            database,
            sql,
            nullptr,
            nullptr,
            nullptr
        ) != SQLITE_OK
    ) {
        throw std::runtime_error(
            sqlite3_errmsg(database)
        );
    }
}

Statement prepare(sqlite3* database, const char* sql)
{
    sqlite3_stmt* raw = nullptr;

    const int result = sqlite3_prepare_v2(
        database,
        sql,
        -1,
        &raw,
        nullptr
    );

    Statement statement(raw, sqlite3_finalize);

    if (result != SQLITE_OK) {
        throw std::runtime_error(
            sqlite3_errmsg(database)
        );
    }

    return statement;
}

std::int64_t integer(
    sqlite3_stmt* statement,
    int column
)
{
    if (
        sqlite3_column_type(statement, column)
        != SQLITE_INTEGER
    ) {
        throw std::runtime_error(
            "Unexpected non-integer Outbox field"
        );
    }

    return sqlite3_column_int64(statement, column);
}

std::string text(
    sqlite3_stmt* statement,
    int column
)
{
    if (
        sqlite3_column_type(statement, column)
        != SQLITE_TEXT
    ) {
        throw std::runtime_error(
            "Unexpected non-text Outbox field"
        );
    }

    const auto* value =
        sqlite3_column_text(statement, column);

    const int length =
        sqlite3_column_bytes(statement, column);

    if (value == nullptr) {
        throw std::runtime_error(
            "Cannot read Outbox text field"
        );
    }

    return std::string(
        reinterpret_cast<const char*>(value),
        length
    );
}

std::int64_t pragma(
    sqlite3* database,
    const char* sql
)
{
    auto statement = prepare(database, sql);

    if (sqlite3_step(statement.get()) != SQLITE_ROW) {
        throw std::runtime_error(
            "Cannot read Outbox identity"
        );
    }

    const auto value = integer(statement.get(), 0);

    if (sqlite3_step(statement.get()) != SQLITE_DONE) {
        throw std::runtime_error(
            "Unexpected Outbox identity result"
        );
    }

    return value;
}

class AtomicOutput {
public:
    explicit AtomicOutput(
        const std::filesystem::path& destination
    )
        : destination_(destination)
    {
        std::filesystem::create_directories(
            destination.parent_path()
        );

        temporary_ = (
            destination.parent_path()
            / ".edge-dead-XXXXXX"
        ).string();

        fd_ = ::mkstemp(temporary_.data());

        if (fd_ == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "create export file"
            );
        }
    }

    ~AtomicOutput() noexcept
    {
        const int saved = errno;

        if (fd_ >= 0) {
            (void)::close(fd_);
        }

        (void)::unlink(temporary_.c_str());

        errno = saved;
    }

    AtomicOutput(const AtomicOutput&) = delete;
    AtomicOutput& operator=(const AtomicOutput&) = delete;

    void write(const std::string& value)
    {
        std::size_t offset = 0;

        while (offset < value.size()) {
            const auto count = ::write(
                fd_,
                value.data() + offset,
                value.size() - offset
            );

            if (count < 0 && errno == EINTR) {
                continue;
            }

            if (count < 0) {
                throw std::system_error(
                    errno,
                    std::generic_category(),
                    "write export file"
                );
            }

            if (count == 0) {
                throw std::runtime_error(
                    "Zero-byte export write"
                );
            }

            offset += static_cast<std::size_t>(count);
        }
    }

    void publish()
    {
        if (::fsync(fd_) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "flush export file"
            );
        }

        const int descriptor =
            std::exchange(fd_, -1);

        if (::close(descriptor) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "close export file"
            );
        }

        // 同目录硬链接原子发布。
        // 目标已存在时失败，不覆盖已有文件。
        if (
            ::link(
                temporary_.c_str(),
                destination_.c_str()
            ) == -1
        ) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "publish export file"
            );
        }
    }

private:
    std::filesystem::path destination_;
    std::string temporary_;
    int fd_ = -1;
};

} // namespace

std::uint64_t exportDeadFile(
    const std::filesystem::path& database_path,
    const std::filesystem::path& output_path
)
{
    namespace fs = std::filesystem;

    if (output_path.empty()) {
        throw std::invalid_argument(
            "Export path must not be empty"
        );
    }

    const auto destination =
        fs::absolute(output_path).lexically_normal();

    if (fs::exists(fs::symlink_status(destination))) {
        throw std::runtime_error(
            "Export target already exists"
        );
    }

    const auto source =
        fs::weakly_canonical(database_path);

    const auto normalized_output =
        fs::weakly_canonical(destination);

    for (
        const char* suffix :
        {"", ".lock", "-wal", "-shm", "-journal"}
    ) {
        if (
            normalized_output
            == fs::path(source.string() + suffix)
        ) {
            throw std::invalid_argument(
                "Export target overlaps database files"
            );
        }
    }

    sqlite3* raw = nullptr;

    const int opened = sqlite3_open_v2(
        database_path.c_str(),
        &raw,
        SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX,
        nullptr
    );

    Database database(raw, sqlite3_close_v2);

    if (opened != SQLITE_OK) {
        throw std::runtime_error(
            raw
                ? sqlite3_errmsg(raw)
                : "Cannot open Outbox"
        );
    }

    if (
        sqlite3_busy_timeout(database.get(), 2000)
        != SQLITE_OK
    ) {
        throw std::runtime_error(
            sqlite3_errmsg(database.get())
        );
    }

    execute(database.get(), "PRAGMA query_only = ON");
    execute(database.get(), "BEGIN");

    if (
        pragma(database.get(), "PRAGMA application_id")
            != kOutboxApplicationId
        || pragma(database.get(), "PRAGMA user_version")
            != kOutboxSchemaVersion
    ) {
        throw std::runtime_error(
            "Outbox identity or schema version mismatch"
        );
    }

    auto statement = prepare(
        database.get(),
        R"sql(
            SELECT event_id, payload_json, state,
                   attempt_count, next_attempt_at_ms,
                   created_at_ms, updated_at_ms,
                   acked_at_ms, last_error
            FROM outbox
            WHERE state = 'DEAD'
            ORDER BY created_at_ms, event_id
        )sql"
    );

    AtomicOutput output(destination);

    std::uint64_t count = 0;
    int result = SQLITE_OK;

    while (
        (result = sqlite3_step(statement.get()))
        == SQLITE_ROW
    ) {
        nlohmann::json row = {
            {"export_version", 1},
            {"event_id", text(statement.get(), 0)},
            {"payload_json", text(statement.get(), 1)},
            {"state", text(statement.get(), 2)},
            {"attempt_count", integer(statement.get(), 3)},
            {"next_attempt_at_ms", integer(statement.get(), 4)},
            {"created_at_ms", integer(statement.get(), 5)},
            {"updated_at_ms", integer(statement.get(), 6)},
            {"acked_at_ms", nullptr},
            {"last_error", text(statement.get(), 8)}
        };

        if (
            sqlite3_column_type(statement.get(), 7)
            != SQLITE_NULL
        ) {
            row["acked_at_ms"] =
                integer(statement.get(), 7);
        }

        output.write(row.dump() + "\n");
        ++count;
    }

    if (result != SQLITE_DONE) {
        throw std::runtime_error(
            sqlite3_errmsg(database.get())
        );
    }

    statement.reset();

    // 结束只读事务后，再发布完整输出文件。
    execute(database.get(), "ROLLBACK");
    output.publish();

    return count;
}

} // namespace edge