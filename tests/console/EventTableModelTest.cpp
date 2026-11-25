#include "presentation/EventTableModel.h"

#include <gtest/gtest.h>

#include <QPersistentModelIndex>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

using namespace edge::console;

JsonlMessage displayRecord(std::uint64_t sequence)
{
    return JsonlMessage{
        JsonlMessageKind::Record,
        std::string(32, 'a'),
        sequence,
        1700000000000LL,
        nlohmann::json{
            {"event_id", "display-test-event"},
            {"disposition", "stored"},
            {"event", {
                {"name", u8"中文事件"},
                {"probe",
                    std::numeric_limits<std::uint64_t>::max()}
            }}
        }
    };
}

void appendRange(
    EventTableModel& model,
    std::uint64_t first,
    int count
)
{
    while (count > 0) {
        const int batchSize = std::min(
            count,
            EventTableModel::kBatchLimit
        );

        std::vector<JsonlMessage> batch;

        for (int index = 0; index < batchSize; ++index) {
            batch.push_back(displayRecord(first));
            ++first;
        }

        model.appendBatch(std::move(batch));

        count -= batchSize;
    }
}

TEST(EventTableModelTest, EmptyModelAndReadOnlyRows)
{
    EventTableModel model;

    EXPECT_EQ(model.rowCount(), 0);
    EXPECT_EQ(model.columnCount(), 5);
    EXPECT_EQ(model.evicted(), 0U);
    EXPECT_TRUE(model.details(0).isEmpty());

    model.appendBatch({displayRecord(2)});

    const auto index = model.index(0, 0);

    EXPECT_EQ(model.rowCount(index), 0);
    EXPECT_EQ(model.columnCount(index), 0);

    EXPECT_FALSE(
        model.flags(index).testFlag(Qt::ItemIsEditable)
    );
}

TEST(EventTableModelTest, PreservesIntegerPrecisionAndUnicode)
{
    EventTableModel model;

    const auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    model.appendBatch({displayRecord(maximum)});

    EXPECT_EQ(
        model.data(model.index(0, 0)).toString(),
        QStringLiteral("18446744073709551615")
    );

    const QString details = model.details(0);

    EXPECT_TRUE(
        details.contains(
            QStringLiteral("18446744073709551615")
        )
    );

    EXPECT_TRUE(
        details.contains(QStringLiteral("中文事件"))
    );
}

TEST(EventTableModelTest, SameEventIdKeepsSeparateDispositions)
{
    EventTableModel model;

    auto stored = displayRecord(2);
    auto duplicate = displayRecord(3);

    duplicate.data["disposition"] = "duplicate";

    model.appendBatch({
        std::move(stored),
        std::move(duplicate)
    });

    ASSERT_EQ(model.rowCount(), 2);

    EXPECT_EQ(
        model.data(model.index(0, 2)).toString(),
        QStringLiteral("stored")
    );

    EXPECT_EQ(
        model.data(model.index(1, 2)).toString(),
        QStringLiteral("duplicate")
    );
}

TEST(EventTableModelTest, HistoryKeepsLatestTwoHundred)
{
    EventTableModel model;

    appendRange(
        model,
        2,
        EventTableModel::kHistoryLimit + 5
    );

    ASSERT_EQ(
        model.rowCount(),
        EventTableModel::kHistoryLimit
    );

    EXPECT_EQ(model.evicted(), 5U);

    EXPECT_EQ(
        model.data(model.index(0, 0)).toString(),
        QStringLiteral("7")
    );

    EXPECT_EQ(
        model.data(model.index(model.rowCount() - 1, 0))
            .toString(),
        QStringLiteral("206")
    );
}

TEST(EventTableModelTest, PersistentIndexesFollowRetainedRows)
{
    EventTableModel model;

    appendRange(model, 2, EventTableModel::kHistoryLimit);

    QPersistentModelIndex removed(
        model.index(0, 0)
    );

    QPersistentModelIndex retained(
        model.index(100, 0)
    );

    appendRange(model, 202, EventTableModel::kBatchLimit);

    EXPECT_FALSE(removed.isValid());

    ASSERT_TRUE(retained.isValid());

    EXPECT_EQ(retained.row(), 68);
    EXPECT_EQ(retained.data().toString(), QStringLiteral("102"));

    EXPECT_EQ(model.evicted(), 32U);
}

TEST(EventTableModelTest, ResetClearsHistoryAndIndexes)
{
    EventTableModel model;

    appendRange(
        model,
        2,
        EventTableModel::kHistoryLimit + 5
    );

    QPersistentModelIndex previous(
        model.index(10, 0)
    );

    model.resetForRun();

    EXPECT_EQ(model.rowCount(), 0);
    EXPECT_EQ(model.evicted(), 0U);
    EXPECT_FALSE(previous.isValid());
    EXPECT_TRUE(model.details(0).isEmpty());

    auto nextRun = displayRecord(2);
    nextRun.run_id = std::string(32, 'b');

    model.appendBatch({std::move(nextRun)});

    EXPECT_TRUE(
        model.details(0).contains(
            QString(32, QLatin1Char('b'))
        )
    );
}

TEST(EventTableModelTest, RejectsInvalidBatchBeforeChangingHistory)
{
    EventTableModel model;

    appendRange(model, 2, EventTableModel::kHistoryLimit);

    QPersistentModelIndex first(model.index(0, 0));

    auto invalid = displayRecord(203);
    invalid.kind = JsonlMessageKind::Status;

    std::vector<JsonlMessage> batch;

    batch.push_back(displayRecord(202));
    batch.push_back(std::move(invalid));

    EXPECT_THROW(
        model.appendBatch(std::move(batch)),
        std::invalid_argument
    );

    std::vector<JsonlMessage> oversized(
        EventTableModel::kBatchLimit + 1,
        displayRecord(202)
    );

    EXPECT_THROW(
        model.appendBatch(std::move(oversized)),
        std::invalid_argument
    );

    EXPECT_EQ(model.rowCount(), EventTableModel::kHistoryLimit);
    EXPECT_EQ(model.evicted(), 0U);

    ASSERT_TRUE(first.isValid());
    EXPECT_EQ(first.data().toString(), QStringLiteral("2"));
}

TEST(EventTableModelTest, BoundsPreviewAndDetailText)
{
    EventTableModel model;

    auto message = displayRecord(2);

    // 直接构造大值，验证显示层的长度边界。
    message.data["event_id"] = std::string(500, 'i');
    message.data["event"]["large"] = std::string(70000, 'x');

    model.appendBatch({std::move(message)});

    const QString preview =
        model.data(model.index(0, 4)).toString();

    const QString identifier =
        model.data(model.index(0, 3)).toString();

    const QString details = model.details(0);

    EXPECT_LE(preview.size(), EventTableModel::kPreviewChars);
    EXPECT_LE(identifier.size(), 128);
    EXPECT_LE(details.size(), EventTableModel::kDetailChars);

    EXPECT_TRUE(preview.contains(QStringLiteral("已截断")));
    EXPECT_TRUE(details.contains(QStringLiteral("已截断")));
}

} // namespace