#include "EventTableModel.h"

#include <QStringList>

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <string>
#include <utility>

namespace edge::console {
namespace {

QString utf8Text(const std::string& value)
{
    return QString::fromUtf8(
        value.data(),
        static_cast<qsizetype>(value.size())
    );
}

QString limitedText(QString text, qsizetype limit)
{
    if (text.size() <= limit) {
        return text;
    }

    const QString suffix = QStringLiteral("…[已截断]");

    qsizetype keep = limit - suffix.size();

    if (keep <= 0) {
        return suffix.left(limit);
    }

    // 避免截断在 UTF-16 代理对中间。
    if (text.at(keep - 1).isHighSurrogate()) {
        --keep;
    }

    return text.left(keep) + suffix;
}

QString cellText(QString text, qsizetype limit)
{
    text.replace(QLatin1Char('\r'), QStringLiteral("\\r"));
    text.replace(QLatin1Char('\n'), QStringLiteral("\\n"));

    text.replace(
        QChar(0x2028),
        QStringLiteral("\\u2028")
    );

    text.replace(
        QChar(0x2029),
        QStringLiteral("\\u2029")
    );

    return limitedText(std::move(text), limit);
}

QString jsonText(const nlohmann::json& value)
{
    // 直接序列化原始 JSON 值，不经过 QJsonValue/double。
    return utf8Text(value.dump());
}

} // namespace

EventTableModel::EventTableModel(QObject* parent)
    : QAbstractTableModel(parent)
{
    entries_.reserve(kHistoryLimit);
}

int EventTableModel::rowCount(
    const QModelIndex& parent
) const
{
    return parent.isValid()
        ? 0
        : static_cast<int>(entries_.size());
}

int EventTableModel::columnCount(
    const QModelIndex& parent
) const
{
    return parent.isValid() ? 0 : kColumns;
}

QVariant EventTableModel::data(
    const QModelIndex& index,
    int role
) const
{
    if (!index.isValid()
        || index.model() != this
        || index.row() < 0
        || index.row() >= rowCount()
        || index.column() < 0
        || index.column() >= kColumns) {
        return {};
    }

    if (role != Qt::DisplayRole
        && role != Qt::ToolTipRole) {
        return {};
    }

    const auto& entry =
        entries_[static_cast<std::size_t>(index.row())];

    return entry.cells[
        static_cast<std::size_t>(index.column())
    ];
}

QVariant EventTableModel::headerData(
    int section,
    Qt::Orientation orientation,
    int role
) const
{
    if (role != Qt::DisplayRole || section < 0) {
        return {};
    }

    if (orientation == Qt::Vertical) {
        return section < rowCount()
            ? QVariant(section + 1)
            : QVariant{};
    }

    static const QStringList headers = {
        QStringLiteral("序号"),
        QStringLiteral("发出时间 ms"),
        QStringLiteral("处置"),
        QStringLiteral("事件标识"),
        QStringLiteral("事件 JSON 摘要")
    };

    if (section >= headers.size()) {
        return {};
    }

    return headers.at(section);
}

EventTableModel::Entry EventTableModel::prepare(
    JsonlMessage message
)
{
    if (message.kind != JsonlMessageKind::Record) {
        throw std::invalid_argument(
            "EventTableModel accepts record messages only."
        );
    }

    std::array<QString, kColumns> cells{
        QString::number(
            static_cast<qulonglong>(message.sequence)
        ),

        QString::number(
            static_cast<qlonglong>(message.emitted_at_ms)
        ),

        cellText(
            utf8Text(
                message.data.at("disposition")
                    .get<std::string>()
            ),
            64
        ),

        cellText(
            utf8Text(
                message.data.at("event_id")
                    .get<std::string>()
            ),
            128
        ),

        cellText(
            jsonText(message.data.at("event")),
            kPreviewChars
        )
    };

    return Entry{
        std::move(message),
        std::move(cells)
    };
}

void EventTableModel::appendBatch(
    std::vector<JsonlMessage> batch
)
{
    if (batch.empty()) {
        return;
    }

    if (batch.size()
        > static_cast<std::size_t>(kBatchLimit)) {
        throw std::invalid_argument(
            "Event display batch exceeds its limit."
        );
    }

    // 先完成校验和文本准备，再修改模型。
    std::vector<Entry> prepared;
    prepared.reserve(batch.size());

    for (auto& message : batch) {
        prepared.push_back(
            prepare(std::move(message))
        );
    }

    const int incoming =
        static_cast<int>(prepared.size());

    const int removeCount = std::max(
        0,
        rowCount() + incoming - kHistoryLimit
    );

    if (removeCount > 0) {
        beginRemoveRows(
            QModelIndex(),
            0,
            removeCount - 1
        );

        entries_.erase(
            entries_.begin(),
            entries_.begin() + removeCount
        );

        const quint64 remaining =
            std::numeric_limits<quint64>::max()
            - evicted_;

        evicted_ += std::min(
            remaining,
            static_cast<quint64>(removeCount)
        );

        endRemoveRows();
    }

    const int first = rowCount();

    beginInsertRows(
        QModelIndex(),
        first,
        first + incoming - 1
    );

    // 历史容量已经预留，插入后仍不超过上限。
    for (auto& entry : prepared) {
        entries_.push_back(std::move(entry));
    }

    endInsertRows();
}

void EventTableModel::resetForRun()
{
    beginResetModel();

    entries_.clear();
    evicted_ = 0;

    endResetModel();
}

quint64 EventTableModel::evicted() const noexcept
{
    return evicted_;
}

QString EventTableModel::details(int row) const
{
    if (row < 0 || row >= rowCount()) {
        return {};
    }

    const auto& message =
        entries_[static_cast<std::size_t>(row)].message;

    QString text =
        QStringLiteral("run_id: ")
        + utf8Text(message.run_id)
        + QStringLiteral("\nsequence: ")
        + QString::number(
            static_cast<qulonglong>(message.sequence)
        )
        + QStringLiteral("\nemitted_at_ms: ")
        + QString::number(
            static_cast<qlonglong>(message.emitted_at_ms)
        )
        + QStringLiteral("\ndata:\n")
        + jsonText(message.data);

    return limitedText(
        std::move(text),
        kDetailChars
    );
}

} // namespace edge::console