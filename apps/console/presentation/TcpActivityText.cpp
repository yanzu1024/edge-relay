#include "TcpActivityText.h"

#include "edge/config/AgentConfig.h"

#include <cstdint>

namespace edge::console {
namespace {

QString unsignedText(std::uint64_t value)
{
    return QString::number(
        static_cast<qulonglong>(value)
    );
}

QString connectionStateText(TcpConnectionState state)
{
    switch (state) {
    case TcpConnectionState::Waiting:
        return QStringLiteral("WAITING");

    case TcpConnectionState::Active:
        return QStringLiteral("ACTIVE");

    case TcpConnectionState::Silent:
        return QStringLiteral("SILENT");
    }

    return QStringLiteral("UNKNOWN");
}

} // namespace

TcpActivityText makeTcpActivityText(
    const std::optional<StatusSummary>& snapshot,
    bool processActive
)
{
    TcpActivityText text;

    if (!snapshot.has_value()) {
        text.scope = processActive
            ? QStringLiteral("等待本次运行的 TCP 观察")
            : QStringLiteral("暂无 TCP 观察");

        return text;
    }

    if (processActive) {
        text.scope = snapshot->exit_code.has_value()
            ? QStringLiteral(
                "已收到 stopped 快照；等待进程退出"
            )
            : QStringLiteral("最近接收的 TCP 观察快照");
    } else {
        text.scope = snapshot->exit_code.has_value()
            ? QStringLiteral(
                "进程已退出；最后收到 stopped 快照"
            )
            : QStringLiteral(
                "进程已退出；保留最后 TCP 快照"
                "（未确认最终状态）"
            );
    }

    if (!snapshot->tcp_activity.has_value()) {
        text.scope += QStringLiteral("；TCP 观察未知");
        return text;
    }

    const auto& activity = *snapshot->tcp_activity;

    // 协议解码已检查上限，显示层再做一次防御性检查。
    // 使用与 Agent 一致的连接上限，不另写一个业务常量。
    const auto maximumRows =
        static_cast<std::size_t>(
            ::edge::kMaxTcpConnections
        );

    if (activity.connections.size() > maximumRows) {
        text.totals = QStringLiteral(
            "连接数量超出协议上限，未生成表格"
        );

        return text;
    }

    if (activity.connected_count == 0) {
        text.scope += QStringLiteral("；当前快照没有连接");
    }

    text.totals = QStringLiteral(
        "当前连接=%1；WAITING=%2；ACTIVE=%3；"
        "SILENT=%4；静默阈值=%5 ms"
    )
        .arg(unsignedText(activity.connected_count))
        .arg(unsignedText(activity.waiting_count))
        .arg(unsignedText(activity.active_count))
        .arg(unsignedText(activity.silent_count))
        .arg(QString::number(activity.silent_after_ms));

    text.totals += QStringLiteral("\n");

    // 累计值直接来自快照，包含已经关闭的连接。
    text.totals += QStringLiteral(
        "接入累计=%1；关闭累计=%2；拒绝累计=%3；"
        "客户端错误=%4；收到字节累计=%5 B"
    )
        .arg(unsignedText(activity.accepted_connections))
        .arg(unsignedText(activity.closed_connections))
        .arg(unsignedText(activity.rejected_connections))
        .arg(unsignedText(activity.client_errors))
        .arg(unsignedText(activity.bytes_received));

    text.rows.reserve(activity.connections.size());

    // 解码后的连接已经按槽位递增排列。
    for (const auto& connection : activity.connections) {
        const QString dataAge =
            connection.last_data_age_ms.has_value()
            ? unsignedText(*connection.last_data_age_ms)
            : QStringLiteral("尚未收到数据");

        text.rows.push_back(TcpConnectionText{
            unsignedText(connection.slot),
            unsignedText(connection.connection_id),
            connectionStateText(connection.state),
            unsignedText(connection.connected_ms),
            dataAge,
            unsignedText(connection.bytes_received)
        });
    }

    return text;
}

} // namespace edge::console