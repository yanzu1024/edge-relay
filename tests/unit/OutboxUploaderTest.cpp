#include "edge/upload/OutboxUploader.h"

#include "edge/domain/EventId.h"
#include "edge/storage/OutboxSchema.h"
#include "edge/storage/OutboxStore.h"
#include "edge/storage/SqliteDatabase.h"
#include "edge/storage/SqliteStatement.h"

#include <sqlite3.h>

#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include <gtest/gtest.h>

namespace {

edge::SensorEvent makeEvent(std::int64_t sequence)
{
    edge::SensorEvent event;

    event.schema_version = 1;
    event.device_id = "device-001";
    event.session_id = "session-001";
    event.sequence = sequence;
    event.observed_at_ms = 1794441600000LL;
    event.metric = "temperature";
    event.value = 25.5;
    event.unit = "degC";

    return event;
}

std::string eventId(std::int64_t sequence)
{
    return edge::makeEventId(makeEvent(sequence));
}

edge::OutboxUploadClock testClock()
{
    return [calls = 0]() mutable -> std::int64_t {
        return calls++ == 0 ? 2000 : 2500;
    };
}

edge::BatchUploadResult acceptedResponse(
    const std::vector<edge::SensorEvent>& events
)
{
    edge::BatchResponse response;

    for (const auto& event : events) {
        response.results.push_back(edge::BatchEventResult{
            edge::makeEventId(event),
            edge::BatchEventStatus::Accepted
        });
    }

    return response;
}

struct RecordState {
    std::string state;
    std::int64_t attempt_count;
    std::int64_t next_attempt_at_ms;
    std::int64_t updated_at_ms;
    std::int64_t acked_at_ms;
    std::string last_error;
};

class OutboxUploaderTest : public ::testing::Test {
protected:
    edge::SqliteDatabase database_{":memory:"};
    edge::OutboxStore store_{database_};
    edge::UploadConfig config_;

    void SetUp() override
    {
        edge::initializeOutboxSchema(database_);
    }

    void execute(const std::string& sql)
    {
        edge::SqliteStatement statement(database_, sql);

        if (statement.step() != edge::SqliteStepResult::Done) {
            throw std::runtime_error("Unexpected test SQL rows");
        }
    }

    void seed(std::int64_t count = 2)
    {
        for (std::int64_t sequence = 1;
             sequence <= count;
             ++sequence) {
            const auto result = store_.enqueue(
                makeEvent(sequence),
                1000
            );

            const auto* inserted =
                std::get_if<edge::OutboxInsertResult>(&result);

            if (inserted == nullptr
                || inserted->status
                    != edge::OutboxInsertStatus::Inserted) {
                throw std::runtime_error("Cannot prepare test event");
            }
        }
    }

    RecordState read(std::int64_t sequence)
    {
        edge::SqliteStatement query(
            database_,
            "SELECT state, attempt_count, next_attempt_at_ms, "
            "updated_at_ms, COALESCE(acked_at_ms, 0), last_error "
            "FROM outbox WHERE event_id = ?1;"
        );

        query.bindText(1, eventId(sequence));

        if (query.step() != edge::SqliteStepResult::Row) {
            throw std::runtime_error("Test event not found");
        }

        const auto text = [&query](int column) {
            const auto* value =
                sqlite3_column_text(query.get(), column);

            if (value == nullptr) {
                throw std::runtime_error("Cannot read test text");
            }

            return std::string(
                reinterpret_cast<const char*>(value),
                static_cast<std::size_t>(
                    sqlite3_column_bytes(query.get(), column)
                )
            );
        };

        const RecordState result{
            text(0),
            static_cast<std::int64_t>(
                sqlite3_column_int64(query.get(), 1)
            ),
            static_cast<std::int64_t>(
                sqlite3_column_int64(query.get(), 2)
            ),
            static_cast<std::int64_t>(
                sqlite3_column_int64(query.get(), 3)
            ),
            static_cast<std::int64_t>(
                sqlite3_column_int64(query.get(), 4)
            ),
            text(5)
        };

        if (query.step() != edge::SqliteStepResult::Done) {
            throw std::runtime_error("Unexpected duplicate test event");
        }

        return result;
    }
};

TEST_F(OutboxUploaderTest, IdleDoesNotCallSender)
{
    int sends = 0;
    int clock_calls = 0;

    edge::OutboxUploader uploader(
        store_,
        config_,
        [&sends](
            const edge::UploadConfig&,
            const std::vector<edge::SensorEvent>& events
        ) -> edge::BatchUploadResult {
            ++sends;
            return acceptedResponse(events);
        },
        [&clock_calls]() -> std::int64_t {
            ++clock_calls;
            return 2000;
        }
    );

    const auto report = uploader.runOnce();

    EXPECT_EQ(sends, 0);
    EXPECT_EQ(clock_calls, 1);
    EXPECT_EQ(report.claimed_records, 0);
    EXPECT_EQ(report.acked_records, 0);
    EXPECT_EQ(report.retry_records, 0);
    EXPECT_EQ(report.dead_records, 0);
    EXPECT_TRUE(
        std::holds_alternative<std::monostate>(report.failure)
    );
}

TEST_F(OutboxUploaderTest, AppliesMixedResponseOutsideTransaction)
{
    seed(3);

    int sends = 0;

    edge::OutboxUploader uploader(
        store_,
        config_,
        [this, &sends](
            const edge::UploadConfig&,
            const std::vector<edge::SensorEvent>& events
        ) -> edge::BatchUploadResult {
            ++sends;

            EXPECT_EQ(events.size(), 3U);
            EXPECT_EQ(sqlite3_get_autocommit(database_.get()), 1);
            EXPECT_EQ(store_.stateCounts().sending, 3);

            // 故意乱序返回。
            return edge::BatchResponse{
                {
                    {eventId(3), edge::BatchEventStatus::Conflict},
                    {eventId(1), edge::BatchEventStatus::Accepted},
                    {eventId(2), edge::BatchEventStatus::Duplicate}
                }
            };
        },
        testClock()
    );

    const auto report = uploader.runOnce();

    EXPECT_EQ(sends, 1);
    EXPECT_EQ(report.claimed_records, 3);
    EXPECT_EQ(report.acked_records, 2);
    EXPECT_EQ(report.retry_records, 0);
    EXPECT_EQ(report.dead_records, 1);

    EXPECT_TRUE(
        std::holds_alternative<std::monostate>(report.failure)
    );

    EXPECT_EQ(read(1).state, "ACKED");
    EXPECT_EQ(read(1).acked_at_ms, 2500);
    EXPECT_EQ(read(2).state, "ACKED");

    EXPECT_EQ(read(3).state, "DEAD");
    EXPECT_EQ(read(3).attempt_count, 1);
    EXPECT_EQ(read(3).updated_at_ms, 2500);
    EXPECT_EQ(
        read(3).last_error,
        "Server reported event identity conflict"
    );
}

TEST_F(OutboxUploaderTest, TimeoutUsesCompletionTimeAndPerRecordAttempts)
{
    seed();

    execute(
        "UPDATE outbox SET "
        "state = 'RETRY', attempt_count = 2, "
        "next_attempt_at_ms = 1500 "
        "WHERE event_id = 'device-001/session-001/2';"
    );

    edge::OutboxUploader uploader(
        store_,
        config_,
        [](
            const edge::UploadConfig&,
            const std::vector<edge::SensorEvent>&
        ) -> edge::BatchUploadResult {
            return edge::HttpRequestError{
                edge::HttpRequestErrorCode::Timeout,
                28,
                "simulated timeout"
            };
        },
        testClock()
    );

    const auto report = uploader.runOnce();

    EXPECT_EQ(report.claimed_records, 2);
    EXPECT_EQ(report.acked_records, 0);
    EXPECT_EQ(report.retry_records, 2);
    EXPECT_EQ(report.dead_records, 0);

    const auto* failure =
        std::get_if<edge::HttpRequestError>(&report.failure);

    ASSERT_NE(failure, nullptr);
    EXPECT_EQ(failure->code, edge::HttpRequestErrorCode::Timeout);
    EXPECT_EQ(failure->curl_code, 28);

    // 请求结束为 2500，首次领取等待 500ms。
    EXPECT_EQ(read(1).attempt_count, 1);
    EXPECT_EQ(read(1).next_attempt_at_ms, 3000);

    // 第二条本次领取后次数为 3，等待 2000ms。
    EXPECT_EQ(read(2).attempt_count, 3);
    EXPECT_EQ(read(2).next_attempt_at_ms, 4500);

    EXPECT_EQ(read(1).state, "RETRY");
    EXPECT_EQ(read(2).state, "RETRY");
    EXPECT_EQ(read(1).last_error, "HTTP request timed out");
}

TEST_F(OutboxUploaderTest, RetriesOtherFailureCategories)
{
    struct FailureCase {
        edge::BatchUploadResult outcome;
        std::string summary;
    };

    const std::vector<FailureCase> cases{
        {
            edge::HttpRequestError{
                edge::HttpRequestErrorCode::Transport,
                7,
                "connection failed"
            },
            "HTTP transport failure"
        },
        {
            edge::HttpRequestError{
                edge::HttpRequestErrorCode::ResponseTooLarge,
                23,
                "response too large"
            },
            "HTTP response exceeded size limit"
        },
        {
            edge::BatchHttpError{
                503,
                std::string(1, static_cast<char>(0xFF))
            },
            "HTTP status 503"
        },
        {
            edge::BatchResponseError{"broken response"},
            "Invalid batch response"
        }
    };

    for (const auto& entry : cases) {
        execute("DELETE FROM outbox;");
        seed();

        edge::OutboxUploader uploader(
            store_,
            config_,
            [outcome = entry.outcome](
                const edge::UploadConfig&,
                const std::vector<edge::SensorEvent>&
            ) -> edge::BatchUploadResult {
                return outcome;
            },
            testClock()
        );

        const auto report = uploader.runOnce();

        EXPECT_EQ(report.claimed_records, 2);
        EXPECT_EQ(report.retry_records, 2);
        EXPECT_EQ(report.acked_records, 0);
        EXPECT_EQ(report.dead_records, 0);

        EXPECT_FALSE(
            std::holds_alternative<std::monostate>(report.failure)
        );

        EXPECT_EQ(read(1).state, "RETRY");
        EXPECT_EQ(read(2).state, "RETRY");
        EXPECT_EQ(read(1).next_attempt_at_ms, 3000);
        EXPECT_EQ(read(1).last_error, entry.summary);

        if (const auto* original =
                std::get_if<edge::BatchHttpError>(&entry.outcome)) {
            const auto* returned =
                std::get_if<edge::BatchHttpError>(&report.failure);

            ASSERT_NE(returned, nullptr);
            EXPECT_EQ(returned->status_code, original->status_code);
            EXPECT_EQ(returned->body, original->body);
        }
    }
}

TEST_F(OutboxUploaderTest, InvalidTypedResponseRetriesWholeBatch)
{
    for (int mode = 0; mode < 4; ++mode) {
        execute("DELETE FROM outbox;");
        seed();

        edge::OutboxUploader uploader(
            store_,
            config_,
            [mode](
                const edge::UploadConfig&,
                const std::vector<edge::SensorEvent>& events
            ) -> edge::BatchUploadResult {
                auto result = acceptedResponse(events);

                auto response =
                    std::get<edge::BatchResponse>(result);

                if (mode == 0) {
                    response.results[1].event_id = eventId(999);
                }
                else if (mode == 1) {
                    response.results[1].event_id = eventId(1);
                }
                else if (mode == 2) {
                    response.results.pop_back();
                }
                else {
                    response.results[1].status =
                        static_cast<edge::BatchEventStatus>(99);
                }

                return response;
            },
            testClock()
        );

        const auto report = uploader.runOnce();

        EXPECT_EQ(report.retry_records, 2);
        EXPECT_EQ(report.acked_records, 0);
        EXPECT_EQ(report.dead_records, 0);

        EXPECT_TRUE(
            std::holds_alternative<edge::BatchResponseError>(
                report.failure
            )
        );

        // 即使第一条结果看起来合法，也不能提前确认。
        EXPECT_EQ(read(1).state, "RETRY");
        EXPECT_EQ(read(2).state, "RETRY");
    }
}

TEST_F(OutboxUploaderTest, LocalUploadErrorsLeaveClaimsForRecovery)
{
    for (const bool throws : {false, true}) {
        execute("DELETE FROM outbox;");
        seed();

        edge::OutboxUploader uploader(
            store_,
            config_,
            [throws](
                const edge::UploadConfig&,
                const std::vector<edge::SensorEvent>&
            ) -> edge::BatchUploadResult {
                if (throws) {
                    throw std::runtime_error(
                        "simulated local sender failure"
                    );
                }

                return edge::BatchRequestError{
                    edge::BatchRequestErrorCode::RequestTooLarge,
                    std::nullopt,
                    std::nullopt,
                    "simulated encoding failure"
                };
            },
            testClock()
        );

        EXPECT_THROW(
            uploader.runOnce(),
            std::runtime_error
        );

        EXPECT_EQ(store_.stateCounts().sending, 2);
        EXPECT_EQ(store_.stateCounts().retry, 0);
        EXPECT_EQ(store_.stateCounts().dead, 0);
    }
}

TEST_F(OutboxUploaderTest, StorageFailureIsNotConvertedToRetry)
{
    seed();

    execute(
        "CREATE TEMP TRIGGER reject_second_cycle_ack "
        "BEFORE UPDATE OF state ON main.outbox "
        "WHEN NEW.state = 'ACKED' "
        "AND NEW.event_id = 'device-001/session-001/2' "
        "BEGIN "
            "SELECT RAISE(ABORT, 'simulated storage failure'); "
        "END;"
    );

    int sends = 0;

    edge::OutboxUploader uploader(
        store_,
        config_,
        [&sends](
            const edge::UploadConfig&,
            const std::vector<edge::SensorEvent>& events
        ) -> edge::BatchUploadResult {
            ++sends;
            return acceptedResponse(events);
        },
        testClock()
    );

    EXPECT_THROW(
        uploader.runOnce(),
        edge::SqliteError
    );

    EXPECT_EQ(sends, 1);
    EXPECT_EQ(store_.stateCounts().sending, 2);
    EXPECT_EQ(store_.stateCounts().acked, 0);
    EXPECT_EQ(store_.stateCounts().retry, 0);
}

TEST_F(OutboxUploaderTest, RejectsEmptyDependenciesAndInvalidClock)
{
    seed();

    int sends = 0;

    const auto sender = [&sends](
        const edge::UploadConfig&,
        const std::vector<edge::SensorEvent>& events
    ) -> edge::BatchUploadResult {
        ++sends;
        return acceptedResponse(events);
    };

    EXPECT_THROW(
        (edge::OutboxUploader{
            store_,
            config_,
            edge::OutboxBatchSender{},
            testClock()
        }),
        std::invalid_argument
    );

    EXPECT_THROW(
        (edge::OutboxUploader{
            store_,
            config_,
            sender,
            edge::OutboxUploadClock{}
        }),
        std::invalid_argument
    );

    edge::OutboxUploader bad_before(
        store_,
        config_,
        sender,
        []() -> std::int64_t {
            return 0;
        }
    );

    EXPECT_THROW(
        bad_before.runOnce(),
        std::runtime_error
    );

    EXPECT_EQ(sends, 0);
    EXPECT_EQ(store_.stateCounts().pending, 2);

    int calls = 0;

    edge::OutboxUploader bad_after(
        store_,
        config_,
        sender,
        [&calls]() -> std::int64_t {
            return calls++ == 0 ? 2000 : -1;
        }
    );

    EXPECT_THROW(
        bad_after.runOnce(),
        std::runtime_error
    );

    EXPECT_EQ(sends, 1);
    EXPECT_EQ(store_.stateCounts().sending, 2);
    EXPECT_EQ(store_.stateCounts().acked, 0);
}

TEST_F(OutboxUploaderTest, ClaimMismatchIsReportedAsLocalFailure)
{
    seed();

    edge::OutboxUploader uploader(
        store_,
        config_,
        [this](
            const edge::UploadConfig&,
            const std::vector<edge::SensorEvent>& events
        ) -> edge::BatchUploadResult {
            // 模拟发送期间领取身份已失效。
            EXPECT_EQ(store_.recoverSending(2200), 2);
            return acceptedResponse(events);
        },
        testClock()
    );

    EXPECT_THROW(
        uploader.runOnce(),
        std::runtime_error
    );

    EXPECT_EQ(store_.stateCounts().acked, 0);
    EXPECT_EQ(store_.stateCounts().retry, 2);
    EXPECT_EQ(read(1).next_attempt_at_ms, 2200);
    EXPECT_EQ(read(1).attempt_count, 1);
}

} // namespace