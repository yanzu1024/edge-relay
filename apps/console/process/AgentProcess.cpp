#include "AgentProcess.h"

#include <QDir>

#include <algorithm>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

namespace edge::console {
namespace {

constexpr qint64 kReadChunkBytes = 8192;

// 每轮每个通道最多读取 32 KiB，随后让出事件循环。
constexpr int kChunksPerTurn = 4;

constexpr qsizetype kMaxStderrLineBytes = 4096;
constexpr qsizetype kMaxDiagnosticChars = 1024;
constexpr qsizetype kMaxDiagnostics = 64;

void incrementSaturated(quint64& value)
{
    if (value != std::numeric_limits<quint64>::max()) {
        ++value;
    }
}

QString shortText(const std::string& text)
{
    // 错误说明也只保留有限长度。
    const auto size = std::min<std::size_t>(text.size(), 256);

    return QString::fromUtf8(
        text.data(),
        static_cast<qsizetype>(size)
    );
}

} // namespace

AgentProcess::AgentProcess(QObject* parent)
    : QObject(parent)
{
    pumpTimer_.setSingleShot(true);

    connect(
        &pumpTimer_,
        &QTimer::timeout,
        this,
        &AgentProcess::pump
    );
}

AgentProcess::~AgentProcess()
{
    // 接入窗口后，必须先异步停止，等 runFinished 再销毁。
    Q_ASSERT_X(
        process_ == nullptr,
        "AgentProcess::~AgentProcess",
        "Stop the child and wait for runFinished before destruction."
    );
}

bool AgentProcess::start(
    const QString& program,
    const QStringList& arguments,
    const QString& workingDirectory
)
{
    if (process_ != nullptr
        || finishing_
        || !QDir::isAbsolutePath(program)
        || !QDir::isAbsolutePath(workingDirectory)) {
        return false;
    }

    pumpTimer_.stop();

    receiver_.reset();
    records_.reset();

    observation_ = AgentProcessObservation{};

    diagnostics_.clear();

    stderrPending_.clear();
    stderrTruncated_ = false;

    exitSeen_ = false;

    // 每次启动创建新 QProcess，旧进程信号不能更新新运行。
    auto* process = new QProcess(this);
    process_ = process;

    process->setProcessChannelMode(QProcess::SeparateChannels);
    process->setWorkingDirectory(workingDirectory);

    connect(
        process,
        &QProcess::started,
        this,
        [this, process] {
            if (process_ != process) {
                return;
            }

            observation_.processStarted = true;

            // Agent 不通过 stdin 接收控制命令。
            process->closeWriteChannel();

            // 处理在 Starting 阶段提出的停止请求。
            if (observation_.forceKillRequested) {
                process->kill();
            } else if (observation_.stopRequested) {
                process->terminate();
            }

            emit observationChanged();
        }
    );

    connect(
        process,
        &QProcess::readyReadStandardOutput,
        this,
        [this, process] {
            if (process_ == process) {
                schedulePump();
            }
        }
    );

    connect(
        process,
        &QProcess::readyReadStandardError,
        this,
        [this, process] {
            if (process_ == process) {
                schedulePump();
            }
        }
    );

    connect(
        process,
        &QProcess::errorOccurred,
        this,
        [this, process](QProcess::ProcessError error) {
            if (process_ != process) {
                return;
            }

            addDiagnostic(
                QStringLiteral("[process] ")
                + process->errorString()
            );

            if (error == QProcess::FailedToStart) {
                observation_.failedToStart = true;

                // FailedToStart 不依赖 finished 信号完成回收。
                exitSeen_ = true;
            }

            // Crashed 等错误仍等待 finished，保留实际退出信息。
            schedulePump();
        }
    );

    connect(
        process,
        &QProcess::finished,
        this,
        [this, process](
            int exitCode,
            QProcess::ExitStatus exitStatus
        ) {
            if (process_ != process) {
                return;
            }

            observation_.crashed =
                exitStatus == QProcess::CrashExit;

            if (exitStatus == QProcess::NormalExit) {
                observation_.exitCode = exitCode;
            }

            exitSeen_ = true;

            // finished 后仍需读取 QProcess 中剩余的数据。
            schedulePump();
        }
    );

    // 参数以列表传递，不拼接 shell 命令。
    process->start(program, arguments);

    emit observationChanged();

    return true;
}

void AgentProcess::requestStop()
{
    if (process_ == nullptr
        || process_->state() == QProcess::NotRunning
        || observation_.stopRequested) {
        return;
    }

    observation_.stopRequested = true;

    if (process_->state() == QProcess::Running) {
        process_->terminate();
    }

    // Starting 阶段先记住请求，started 回调中再执行。
    emit observationChanged();
}

void AgentProcess::forceKill()
{
    if (process_ == nullptr
        || process_->state() == QProcess::NotRunning
        || observation_.forceKillRequested) {
        return;
    }

    observation_.stopRequested = true;
    observation_.forceKillRequested = true;

    addDiagnostic(
        QStringLiteral(
            "[process] 已请求强制终止，不能视为完整正常收尾。"
        )
    );

    if (process_->state() == QProcess::Running) {
        process_->kill();
    }

    emit observationChanged();
}

bool AgentProcess::active() const noexcept
{
    return process_ != nullptr;
}

const AgentProcessObservation&
AgentProcess::observation() const noexcept
{
    return observation_;
}

QStringList AgentProcess::takeDiagnostics()
{
    QStringList result;
    result.swap(diagnostics_);
    return result;
}

void AgentProcess::schedulePump()
{
    if (process_ != nullptr && !pumpTimer_.isActive()) {
        pumpTimer_.start(0);
    }
}

bool AgentProcess::drainChannel(
    QProcess::ProcessChannel channel
)
{
    process_->setReadChannel(channel);

    for (int index = 0; index < kChunksPerTurn; ++index) {
        if (process_->bytesAvailable() <= 0) {
            break;
        }

        const QByteArray bytes = process_->read(kReadChunkBytes);

        if (bytes.isEmpty()) {
            break;
        }

        if (channel == QProcess::StandardOutput) {
            receiver_.feed(
                std::string_view(
                    bytes.constData(),
                    static_cast<std::size_t>(bytes.size())
                ),
                [this](JsonlReceiveResult result) {
                    receive(std::move(result));
                }
            );
        } else {
            consumeStderr(bytes);
        }
    }

    return process_->bytesAvailable() > 0;
}

void AgentProcess::pump()
{
    if (process_ == nullptr) {
        return;
    }

    // 两个通道均获得本轮读取预算，避免只消费 stdout。
    const bool stdoutRemaining =
        drainChannel(QProcess::StandardOutput);

    const bool stderrRemaining =
        drainChannel(QProcess::StandardError);

    if (exitSeen_
        && !stdoutRemaining
        && !stderrRemaining) {
        receiver_.finish(
            [this](JsonlReceiveResult result) {
                receive(std::move(result));
            }
        );

        flushStderr(true);

        finishRun();
        return;
    }

    if (stdoutRemaining || stderrRemaining) {
        schedulePump();
    }

    // 回调发生在 receiver.feed 返回后，避免接收器重入。
    emit observationChanged();
}

void AgentProcess::receive(JsonlReceiveResult result)
{
    if (auto* received =
            std::get_if<JsonlReceivedMessage>(&result)) {
        const auto kind = received->message.kind;

        if (kind == JsonlMessageKind::Ready) {
            observation_.readySeen = true;

            const auto& runId = received->message.run_id;

            observation_.runId = QString::fromLatin1(
                runId.data(),
                static_cast<qsizetype>(runId.size())
            );
        }

        if (kind == JsonlMessageKind::Record) {
            records_.push(
                std::move(received->message)
            );

            incrementSaturated(
                observation_.recordsReceived
            );

            observation_.recordsPending =
                static_cast<quint64>(records_.size());

            observation_.recordsDropped =
                static_cast<quint64>(records_.dropped());

            // record 没有状态快照，处理完直接返回。
            return;
        }

        if (kind == JsonlMessageKind::Stopped) {
            observation_.stoppedSeen = true;

            if (received->status.has_value()) {
                observation_.declaredExitCode =
                    received->status->exit_code;
            }
        }

        if (received->status.has_value()) {
            observation_.latestStatus =
                std::move(received->status);
        }

        return;
    }

    incrementSaturated(observation_.protocolErrors);

    if (const auto* framing =
            std::get_if<JsonlReceiveFramingError>(&result)) {
        if (framing->kind == JsonlFrameKind::IncompleteTail) {
            addDiagnostic(
                QStringLiteral("[protocol] stdout 存在不完整尾行。")
            );
        } else {
            addDiagnostic(
                QStringLiteral("[protocol] stdout 行超过长度限制。")
            );
        }

        return;
    }

    if (const auto* error =
            std::get_if<JsonlParseError>(&result)) {
        addDiagnostic(
            QStringLiteral("[protocol] ")
            + shortText(error->message)
        );

        return;
    }

    const auto& error = std::get<StatusDecodeError>(result);

    addDiagnostic(
        QStringLiteral("[status] ")
        + shortText(error.path)
        + QStringLiteral(": ")
        + shortText(error.message)
    );
}

void AgentProcess::consumeStderr(const QByteArray& bytes)
{
    for (const char byte : bytes) {
        if (byte == '\n') {
            flushStderr(false);
            continue;
        }

        if (stderrPending_.size() < kMaxStderrLineBytes) {
            stderrPending_.append(byte);
        } else {
            // 超长诊断保留前缀，其余丢弃到下一次 LF。
            stderrTruncated_ = true;
        }
    }
}

void AgentProcess::flushStderr(bool incompleteTail)
{
    if (incompleteTail
        && stderrPending_.isEmpty()
        && !stderrTruncated_) {
        return;
    }

    QString prefix = QStringLiteral("[stderr");

    if (stderrTruncated_) {
        prefix += QStringLiteral(" truncated");
    }

    if (incompleteTail) {
        prefix += QStringLiteral(" EOF");
    }

    prefix += QStringLiteral("] ");

    // 普通行在收齐字节后再解码，支持 UTF-8 跨读取边界。
    addDiagnostic(
        prefix + QString::fromUtf8(stderrPending_)
    );

    stderrPending_.clear();
    stderrTruncated_ = false;
}

void AgentProcess::addDiagnostic(QString text)
{
    if (text.size() > kMaxDiagnosticChars) {
        text = text.left(kMaxDiagnosticChars);
    }

    if (diagnostics_.size() >= kMaxDiagnostics) {
        diagnostics_.removeFirst();

        incrementSaturated(
            observation_.diagnosticsDropped
        );
    }

    diagnostics_.append(std::move(text));
}

void AgentProcess::finishRun()
{
    Q_ASSERT(process_ != nullptr);
    Q_ASSERT(process_->state() == QProcess::NotRunning);

    if (!observation_.failedToStart
        && !observation_.stoppedSeen) {
        addDiagnostic(
            QStringLiteral(
                "[process] 子进程已退出，但未观察到有效 stopped；"
                "保留实际退出结果，不补造最终快照。"
            )
        );
    }

    if (observation_.exitCode.has_value()
        && observation_.declaredExitCode.has_value()
        && *observation_.exitCode
            != *observation_.declaredExitCode) {
        incrementSaturated(observation_.protocolErrors);

        addDiagnostic(
            QStringLiteral(
                "[protocol] stopped 声明的退出码与实际退出码不一致。"
            )
        );
    }

    pumpTimer_.stop();

    auto* finishedProcess = process_;

    QObject::disconnect(
        finishedProcess,
        nullptr,
        this,
        nullptr
    );

    process_ = nullptr;

    finishedProcess->deleteLater();

    // 最后一次观察通知期间禁止重启，保证通知顺序明确。
    finishing_ = true;
    emit observationChanged();
    finishing_ = false;

    // 此信号的槽中可以启动下一次运行。
    emit runFinished();
}

std::vector<JsonlMessage> AgentProcess::takeRecords(
    std::size_t maximum
)
{
    auto result = records_.take(maximum);

    if (!result.empty()) {
        observation_.recordsPending =
            static_cast<quint64>(records_.size());

        emit observationChanged();
    }

    return result;
}

} // namespace edge::console