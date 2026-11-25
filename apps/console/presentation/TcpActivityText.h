#ifndef EDGE_CONSOLE_TCP_ACTIVITY_TEXT_H
#define EDGE_CONSOLE_TCP_ACTIVITY_TEXT_H

#include "protocol/StatusSummary.h"

#include <QString>

#include <array>
#include <cstddef>
#include <optional>
#include <vector>

namespace edge::console {

inline constexpr std::size_t kTcpActivityColumns = 6;

using TcpConnectionText =
    std::array<QString, kTcpActivityColumns>;

struct TcpActivityText {
    QString scope;
    QString totals = QStringLiteral("未知");

    // 只包含当前快照中的连接，不保留历史行。
    std::vector<TcpConnectionText> rows;
};

[[nodiscard]]
TcpActivityText makeTcpActivityText(
    const std::optional<StatusSummary>& snapshot,
    bool processActive
);

} // namespace edge::console

#endif