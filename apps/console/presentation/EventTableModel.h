#ifndef EDGE_CONSOLE_EVENT_TABLE_MODEL_H
#define EDGE_CONSOLE_EVENT_TABLE_MODEL_H

#include "protocol/JsonlMessageParser.h"

#include <QAbstractTableModel>
#include <QString>
#include <QVariant>

#include <array>
#include <vector>

namespace edge::console {

class EventTableModel final : public QAbstractTableModel {
    Q_OBJECT

public:
    static constexpr int kColumns = 5;
    static constexpr int kHistoryLimit = 200;
    static constexpr int kBatchLimit = 32;

    static constexpr qsizetype kPreviewChars = 256;
    static constexpr qsizetype kDetailChars = 65536;

    explicit EventTableModel(QObject* parent = nullptr);

    int rowCount(
        const QModelIndex& parent = QModelIndex()
    ) const override;

    int columnCount(
        const QModelIndex& parent = QModelIndex()
    ) const override;

    QVariant data(
        const QModelIndex& index,
        int role = Qt::DisplayRole
    ) const override;

    QVariant headerData(
        int section,
        Qt::Orientation orientation,
        int role = Qt::DisplayRole
    ) const override;

    // 输入为已经校验过的 record，每批最多 32 条。
    void appendBatch(std::vector<JsonlMessage> batch);

    void resetForRun();

    [[nodiscard]]
    quint64 evicted() const noexcept;

    [[nodiscard]]
    QString details(int row) const;

private:
    struct Entry {
        JsonlMessage message;
        std::array<QString, kColumns> cells;
    };

    static Entry prepare(JsonlMessage message);

    std::vector<Entry> entries_;

    quint64 evicted_ = 0;
};

} // namespace edge::console

#endif