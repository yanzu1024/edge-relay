#ifndef EDGE_CONSOLE_STATUS_TEXT_H
#define EDGE_CONSOLE_STATUS_TEXT_H

#include "protocol/StatusSummary.h"

#include <QString>

#include <optional>

namespace edge::console {

struct StatusText {
    QString scope;

    QString identity = QStringLiteral("未知");
    QString runtime = QStringLiteral("未知");
    QString counters = QStringLiteral("未知");

    QString upload = QStringLiteral("未知");
    QString recording = QStringLiteral("未知");

    QString outbox = QStringLiteral("未知");
    QString disk = QStringLiteral("未知");
    QString warnings = QStringLiteral("未知");
};

// 只根据传入值生成文本，不保存上一次运行的数据。
[[nodiscard]]
StatusText makeStatusText(
    const std::optional<StatusSummary>& snapshot,
    bool processActive
);

} // namespace edge::console

#endif