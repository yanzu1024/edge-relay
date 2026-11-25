#ifndef EDGE_CONSOLE_AGENT_PROCESS_H
#define EDGE_CONSOLE_AGENT_PROCESS_H

#include "protocol/JsonlReceiver.h"
#include "protocol/RecordBuffer.h"

#include <cstddef>
#include <vector>

#include <QByteArray>
#include <QObject>
#include <QProcess>
#include <QString>
#include <QStringList>
#include <QTimer>

#include <optional>

namespace edge::console {

// 本次运行的观察结果。
// 操作系统退出情况与 Agent 的 stopped 消息分别保存。
struct AgentProcessObservation {
    bool processStarted = false;
    bool readySeen = false;
    bool stoppedSeen = false;

    bool failedToStart = false;
    bool crashed = false;

    bool stopRequested = false;
    bool forceKillRequested = false;

    // 只有 NormalExit 时，操作系统退出码才有意义。
    std::optional<int> exitCode;

    // 只有收到并通过校验的 stopped，才填写该字段。
    std::optional<int> declaredExitCode;

    QString runId;

    std::optional<StatusSummary> latestStatus;

    quint64 recordsReceived = 0;
    quint64 recordsPending = 0;
    quint64 recordsDropped = 0;

    quint64 protocolErrors = 0;

    // 控制台诊断缓存的丢弃数，与 Agent 的 display_dropped 分开。
    quint64 diagnosticsDropped = 0;
};

class AgentProcess final : public QObject {
    Q_OBJECT

public:
    explicit AgentProcess(QObject* parent = nullptr);
    ~AgentProcess() override;

    // 全部接口均在对象所属线程使用。
    //
    // program、workingDirectory 必须是绝对路径。
    // 返回 true 仅表示接受启动请求，启动结果通过观察和信号报告。
    bool start(
        const QString& program,
        const QStringList& arguments,
        const QString& workingDirectory
    );

    void requestStop();
    void forceKill();

    // 包括启动中、运行中，以及退出后的管道排空阶段。
    [[nodiscard]] bool active() const noexcept;

    [[nodiscard]]
    const AgentProcessObservation& observation() const noexcept;

    [[nodiscard]]
    std::vector<JsonlMessage> takeRecords(std::size_t maximum);

    // 取走当前有界诊断缓存。
    QStringList takeDiagnostics();

signals:
    // 不逐条发送 JSON 对象，避免形成消息历史队列。
    void observationChanged();

    // 启动失败也会发出此信号。
    // 发出前已完成通道排空，可以开始下一次运行。
    void runFinished();

private:
    void schedulePump();
    void pump();

    bool drainChannel(QProcess::ProcessChannel channel);

    void receive(JsonlReceiveResult result);

    void consumeStderr(const QByteArray& bytes);
    void flushStderr(bool incompleteTail);

    void addDiagnostic(QString text);
    void finishRun();

    QProcess* process_ = nullptr;

    QTimer pumpTimer_;

    JsonlReceiver receiver_;
    RecordBuffer records_;
    AgentProcessObservation observation_;

    QStringList diagnostics_;

    QByteArray stderrPending_;
    bool stderrTruncated_ = false;

    bool exitSeen_ = false;
    bool finishing_ = false;
};

} // namespace edge::console

#endif