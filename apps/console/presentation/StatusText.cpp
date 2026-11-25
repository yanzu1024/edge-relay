#include "StatusText.h"

#include <QDateTime>
#include <QTimeZone>
#include <QStringList>

#include <cstdint>
#include <string>

namespace edge::console {
namespace {

QString unsignedText(std::uint64_t value)
{
    // 直接使用整数重载，避免经过 double。
    return QString::number(
        static_cast<qulonglong>(value)
    );
}

QString signedText(std::int64_t value)
{
    return QString::number(
        static_cast<qlonglong>(value)
    );
}

QString shortText(const std::string& value)
{
    QString text = QString::fromUtf8(
        value.data(),
        static_cast<qsizetype>(value.size())
    );

    text.replace(QLatin1Char('\r'), QStringLiteral("\\r"));
    text.replace(QLatin1Char('\n'), QStringLiteral("\\n"));

    return text.left(256);
}

QString timestampText(std::int64_t milliseconds)
{
    const QDateTime date = QDateTime::fromMSecsSinceEpoch(
        static_cast<qint64>(milliseconds),
        QTimeZone(QTimeZone::UTC)
    );

    const QString formatted =
        date.toString(Qt::ISODateWithMs);

    if (!formatted.isEmpty()) {
        return formatted;
    }

    // 超出常见日期文本范围时，仍保留原始时间戳。
    return QStringLiteral("Unix ms=")
        + signedText(milliseconds);
}

QString processStateText(ProcessState state)
{
    switch (state) {
    case ProcessState::Unknown:
        return QStringLiteral("UNKNOWN");
    case ProcessState::Starting:
        return QStringLiteral("STARTING");
    case ProcessState::Running:
        return QStringLiteral("RUNNING");
    case ProcessState::Stopping:
        return QStringLiteral("STOPPING");
    case ProcessState::Stopped:
        return QStringLiteral("STOPPED");
    case ProcessState::Failed:
        return QStringLiteral("FAILED");
    }

    return QStringLiteral("UNKNOWN");
}

QString sampleStateText(SampleState state)
{
    switch (state) {
    case SampleState::Unknown:
        return QStringLiteral("未知");
    case SampleState::Valid:
        return QStringLiteral("有效");
    case SampleState::Stale:
        return QStringLiteral("过期（保留旧值）");
    case SampleState::Error:
        return QStringLiteral("失败");
    }

    return QStringLiteral("未知");
}

template<class T, class Formatter>
QString sampleText(
    const SampleValue<T>& sample,
    Formatter formatter
)
{
    QString text = sampleStateText(sample.state);

    if (sample.checked_at_ms.has_value()) {
        text += QStringLiteral("；采样=")
            + timestampText(*sample.checked_at_ms);
    }

    if (sample.state == SampleState::Error) {
        return text
            + QStringLiteral("；错误=")
            + shortText(sample.error);
    }

    if ((sample.state == SampleState::Valid
            || sample.state == SampleState::Stale)
        && sample.value.has_value()) {
        return text
            + QStringLiteral("\n")
            + formatter(*sample.value);
    }

    return text + QStringLiteral("；值不可用");
}

QString outboxValueText(const OutboxCounts& counts)
{
    return QStringLiteral(
        "PENDING=%1；SENDING=%2；RETRY=%3；ACKED=%4；DEAD=%5\n"
        "总量=%6；未确认=%7；积压（不含DEAD）=%8"
    )
        .arg(signedText(counts.pending))
        .arg(signedText(counts.sending))
        .arg(signedText(counts.retry))
        .arg(signedText(counts.acked))
        .arg(signedText(counts.dead))
        .arg(signedText(counts.total))
        .arg(signedText(counts.unacked))
        .arg(signedText(counts.backlog));
}

QString diskValueText(std::uint64_t bytes)
{
    constexpr std::uint64_t bytesPerMiB = 1024ULL * 1024ULL;

    return QStringLiteral("%1 B（约 %2 MiB）")
        .arg(unsignedText(bytes))
        .arg(unsignedText(bytes / bytesPerMiB));
}

QString counterText(
    const StatusSummary& snapshot,
    const char* name
)
{
    const auto found = snapshot.counters.find(name);

    if (found == snapshot.counters.end()) {
        return QStringLiteral("未知");
    }

    return unsignedText(found->second);
}

QString warningText(
    SampleState sampleState,
    const std::optional<bool>& warning
)
{
    // 过期、失败或未知采样不能显示成当前“正常”。
    if (sampleState != SampleState::Valid
        || !warning.has_value()) {
        return QStringLiteral("无法判定");
    }

    return *warning
        ? QStringLiteral("告警")
        : QStringLiteral("正常");
}

QString boolText(bool value)
{
    return value
        ? QStringLiteral("是")
        : QStringLiteral("否");
}

QString uploadStateText(UploadState state)
{
    switch (state) {
    case UploadState::Initializing:
        return QStringLiteral("初始化中（INITIALIZING）");

    case UploadState::Enabled:
        return QStringLiteral("已启用（ENABLED）");

    case UploadState::Blocked:
        return QStringLiteral("阻塞（BLOCKED）");

    case UploadState::Stopping:
        return QStringLiteral("停止中（STOPPING）");

    case UploadState::Stopped:
        return QStringLiteral("已结束（STOPPED）");

    case UploadState::Failed:
        return QStringLiteral("失败（FAILED）");
    }

    return QStringLiteral("未知");
}

QString uploadText(
    const std::optional<UploadObservation>& observation
)
{
    if (!observation.has_value()) {
        return QStringLiteral("未知");
    }

    const auto& value = *observation;

    QString httpStatus = QStringLiteral("不适用");

    if (value.blocked_http_status.has_value()) {
        httpStatus = QString::number(
            *value.blocked_http_status
        );
    } else if (value.blocked) {
        httpStatus = QStringLiteral("未知");
    }

    // 状态和阻塞信息分别显示。
    // 线程结束后仍可能保留本次运行的阻塞原因。
    return QStringLiteral(
        "状态=%1；已初始化=%2；已结束=%3；失败=%4；"
        "已请求停止=%5\n"
        "阻塞标志=%6；阻塞HTTP状态=%7"
    )
        .arg(uploadStateText(value.state))
        .arg(boolText(value.initialized))
        .arg(boolText(value.finished))
        .arg(boolText(value.failed))
        .arg(boolText(value.stop_requested))
        .arg(boolText(value.blocked))
        .arg(httpStatus);
}

QString recordingStateText(RecordingState state)
{
    switch (state) {
    case RecordingState::Disabled:
        return QStringLiteral("未启用（DISABLED）");

    case RecordingState::Active:
        return QStringLiteral("正常运行（ACTIVE）");

    case RecordingState::Degraded:
        return QStringLiteral("降级运行（DEGRADED）");

    case RecordingState::Failed:
        return QStringLiteral("失败（FAILED）");

    case RecordingState::Stopped:
        return QStringLiteral("已结束（STOPPED）");
    }

    return QStringLiteral("未知");
}

QString recordingErrorStageText(
    RecordingErrorStage stage
)
{
    switch (stage) {
    case RecordingErrorStage::None:
        return QStringLiteral("NONE");

    case RecordingErrorStage::Startup:
        return QStringLiteral("STARTUP");

    case RecordingErrorStage::Event:
        return QStringLiteral("EVENT");

    case RecordingErrorStage::Session:
        return QStringLiteral("SESSION");

    case RecordingErrorStage::Retention:
        return QStringLiteral("RETENTION");
    }

    return QStringLiteral("UNKNOWN");
}

QString recordingOperationText(
    RecordingOperation operation
)
{
    switch (operation) {
    case RecordingOperation::None:
        return QStringLiteral("NONE");

    case RecordingOperation::Input:
        return QStringLiteral("INPUT");

    case RecordingOperation::Metadata:
        return QStringLiteral("METADATA");

    case RecordingOperation::Open:
        return QStringLiteral("OPEN");

    case RecordingOperation::Write:
        return QStringLiteral("WRITE");

    case RecordingOperation::Close:
        return QStringLiteral("CLOSE");

    case RecordingOperation::Notify:
        return QStringLiteral("NOTIFY");

    case RecordingOperation::Limit:
        return QStringLiteral("LIMIT");
    }

    return QStringLiteral("UNKNOWN");
}

QString recordingText(
    const std::optional<RecordingObservation>& observation
)
{
    if (!observation.has_value()) {
        return QStringLiteral("未知");
    }

    const auto& value = *observation;

    const QString retained =
        value.retained_closed_files.has_value()
        ? unsignedText(*value.retained_closed_files)
        : QStringLiteral("未知");

    // 会话操作仅在最近错误阶段为 SESSION 时有意义。
    const QString operation =
        value.last_error_stage == RecordingErrorStage::Session
        ? recordingOperationText(value.last_session_operation)
        : QStringLiteral("不适用");

    QStringList lines;

    lines.append(
        QStringLiteral(
            "状态=%1；已启用=%2；目录已就绪=%3；已结束=%4"
        )
            .arg(recordingStateText(value.state))
            .arg(boolText(value.enabled))
            .arg(boolText(value.operational))
            .arg(boolText(value.finished))
    );

    lines.append(
        QStringLiteral(
            "接入连接累计=%1；活动会话=%2；活动文件=%3"
        )
            .arg(unsignedText(value.connections_seen))
            .arg(unsignedText(value.active_sessions))
            .arg(unsignedText(value.active_files))
    );

    lines.append(
        QStringLiteral(
            "收到字节=%1 B；写入字节=%2 B；跳过字节=%3 B"
        )
            .arg(unsignedText(value.bytes_received))
            .arg(unsignedText(value.bytes_written))
            .arg(unsignedText(value.bytes_skipped))
    );

    lines.append(
        QStringLiteral(
            "打开文件累计=%1；关闭文件累计=%2；"
            "保留已关闭文件=%3；已删除文件=%4"
        )
            .arg(unsignedText(value.opened_files))
            .arg(unsignedText(value.closed_files))
            .arg(retained)
            .arg(unsignedText(value.files_deleted))
    );

    // 分项显示，不相加，避免总和溢出或掩盖错误来源。
    lines.append(
        QStringLiteral(
            "错误累计：STARTUP=%1；EVENT=%2；"
            "SESSION=%3；RETENTION=%4"
        )
            .arg(unsignedText(value.startup_errors))
            .arg(unsignedText(value.event_errors))
            .arg(unsignedText(value.session_errors))
            .arg(unsignedText(value.retention_errors))
    );

    lines.append(
        QStringLiteral(
            "最近错误：阶段=%1；errno=%2；"
            "连接=%3；会话操作=%4"
        )
            .arg(recordingErrorStageText(value.last_error_stage))
            .arg(QString::number(value.last_error_code))
            .arg(unsignedText(value.last_error_connection))
            .arg(operation)
    );

    return lines.join(QStringLiteral("\n"));
}

} // namespace

StatusText makeStatusText(
    const std::optional<StatusSummary>& snapshot,
    bool processActive
)
{
    StatusText text;

    if (!snapshot.has_value()) {
        text.scope = processActive
            ? QStringLiteral("等待本次运行的状态快照")
            : QStringLiteral("暂无状态快照");

        return text;
    }

    const auto& value = *snapshot;

    if (value.exit_code.has_value()) {
        text.scope = processActive
            ? QStringLiteral("已收到 stopped 快照；等待进程退出")
            : QStringLiteral("进程已退出；最后收到 stopped 快照");
    } else {
        text.scope = processActive
            ? QStringLiteral("最近接收的状态快照")
            : QStringLiteral(
                "进程已退出；保留最后状态快照（未确认最终状态）"
            );
    }

    text.identity =
        QStringLiteral("Agent=")
        + shortText(value.agent_id)
        + QStringLiteral("；sequence=")
        + unsignedText(value.sequence)
        + QStringLiteral("\n快照采样（UTC）=")
        + timestampText(value.checked_at_ms);

    const QString connections =
        value.active_connections.has_value()
        ? unsignedText(*value.active_connections)
        : QStringLiteral("未知");

    text.runtime = QStringLiteral(
        "快照状态=%1；运行时长=%2 ms；连接数=%3"
    )
        .arg(processStateText(value.process_state))
        .arg(unsignedText(value.uptime_ms))
        .arg(connections);

    text.counters = QStringLiteral(
        "有效事件=%1；入库=%2；重复=%3；身份冲突=%4\n"
        "容量拒绝=%5；存储错误=%6；本次上传ACK=%7；"
        "Agent展示丢弃=%8"
    )
        .arg(counterText(value, "valid_events"))
        .arg(counterText(value, "stored_events"))
        .arg(counterText(value, "duplicate_events"))
        .arg(counterText(value, "identity_conflicts"))
        .arg(counterText(value, "capacity_rejections"))
        .arg(counterText(value, "storage_errors"))
        .arg(counterText(value, "upload_acked"))
        .arg(unsignedText(value.display_dropped));

    text.outbox = sampleText(
        value.outbox,
        outboxValueText
    );

    text.disk = sampleText(
        value.disk_available_bytes,
        diskValueText
    );

    // 使用已校验快照中的告警结果，不在显示层另建计算规则。
    text.warnings = QStringLiteral(
        "未确认记录：%1（阈值 %2 条）；"
        "磁盘可用空间：%3（阈值 %4 B）"
    )
        .arg(warningText(
            value.outbox.state,
            value.unacked_warning
        ))
        .arg(signedText(value.unacked_warn_records))
        .arg(warningText(
            value.disk_available_bytes.state,
            value.disk_warning
        ))
        .arg(unsignedText(value.disk_available_warn_bytes));

    text.upload = uploadText(value.upload);
    text.recording = recordingText(value.recording);        

    return text;
}

} // namespace edge::console