#include "process/AgentProcess.h"
#include "protocol/RecordBuffer.h"
#include "presentation/EventTableModel.h"

#include <QObject>
#include <utility>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include <QCoreApplication>
#include <QDir>
#include <QEventLoop>
#include <QTemporaryDir>
#include <QTimer>

#include <csignal>
#include <cstdint>
#include <functional>
#include <iostream>
#include <limits>
#include <optional>
#include <string>

namespace {

using edge::console::AgentProcess;
using Json = nlohmann::json;

volatile std::sig_atomic_t childStopRequested = 0;

void childSignalHandler(int)
{
    childStopRequested = 1;
}

std::string readyLine(char runCharacter)
{
    const Json message = {
        {"protocol_version", 1},
        {"type", "ready"},
        {"run_id", std::string(32, runCharacter)},
        {"sequence", 1},
        {"emitted_at_ms", 1700000000000LL},
        {"data", {
            {"agent_id", "process-test-agent"},
            {"tcp_bind", "127.0.0.1"},
            {"tcp_port", 19090},
            {"events_jsonl", true},
            {"snapshot_version", 1},
            {"limits", {
                {"line_bytes", 65536},
                {"record_rate_per_second", 100}
            }}
        }}
    };

    return message.dump() + "\n";
}

std::string recordLine()
{
    const Json message = {
        {"protocol_version", 1},
        {"type", "record"},
        {"run_id", std::string(32, 'a')},
        {"sequence", 7},
        {"emitted_at_ms", 1700000000001LL},
        {"data", {
            {"event_id", "process-test-event"},
            {"disposition", "stored"},
            {"event", {
                {"probe",
                    std::numeric_limits<std::uint64_t>::max()}
            }}
        }}
    };

    return message.dump() + "\n";
}

// 同一个测试可执行文件作为模拟 Agent 使用。
int runChild(const QString& mode)
{
    childStopRequested = 0;

    if (mode == QStringLiteral("ignore-stop")) {
        std::signal(SIGTERM, SIG_IGN);
    } else {
        std::signal(SIGTERM, childSignalHandler);
    }

    if (mode == QStringLiteral("no-ready")) {
        std::cerr << "child started without ready\n";
        std::cerr.flush();
    } else {
        const char runCharacter =
            mode == QStringLiteral("once-b") ? 'b' : 'a';

        std::cout << readyLine(runCharacter);
        std::cout.flush();
    }

    if (mode == QStringLiteral("pipeline-stream")) {
        Json message = Json::parse(recordLine());

        std::uint64_t sequence = 2;

        const std::string diagnostic(2048, 'd');

        QTimer producer;

        QObject::connect(
            &producer,
            &QTimer::timeout,
            &producer,
            [&] {
                if (childStopRequested != 0) {
                    QCoreApplication::quit();
                    return;
                }

                // 每轮同时产生事件和诊断。
                for (int index = 0; index < 3; ++index) {
                    message["sequence"] = sequence;
                    ++sequence;

                    std::cout << message.dump() << '\n';
                    std::cerr << diagnostic << '\n';
                }

                std::cout.flush();
                std::cerr.flush();
            }
        );

        producer.start(40);

        return QCoreApplication::exec();
    }

    if (mode == QStringLiteral("record-burst")) {
        Json message = Json::parse(recordLine());

        const std::size_t count =
            edge::console::RecordBuffer::kCapacity + 5;

        for (std::size_t index = 0;
             index < count;
             ++index) {
            message["sequence"] =
                static_cast<std::uint64_t>(index) + 2;

            std::cout << message.dump() << '\n';
        }

        std::cout.flush();

        return 0;
    }

    if (mode == QStringLiteral("exit-with-tail")) {
        std::cout << recordLine();
        std::cout << "{\"unfinished\":";
        std::cout.flush();

        std::cerr << u8"诊断尾部";
        std::cerr.flush();

        return 7;
    }

    if (mode == QStringLiteral("burst")) {
        const std::string longLine(6000, 'x');

        for (int index = 0; index < 300; ++index) {
            std::cerr << longLine << '\n';
        }

        std::cerr.flush();

        std::cout << recordLine();
        std::cout.flush();

        return 0;
    }

    if (mode == QStringLiteral("once-a")
        || mode == QStringLiteral("once-b")) {
        return 0;
    }

    QTimer stopPoll;

    QObject::connect(
        &stopPoll,
        &QTimer::timeout,
        [] {
            if (childStopRequested != 0) {
                QCoreApplication::quit();
            }
        }
    );

    stopPoll.start(10);

    return QCoreApplication::exec();
}

bool spinUntil(
    const std::function<bool()>& predicate,
    int timeoutMs = 5000
)
{
    if (predicate()) {
        return true;
    }

    QEventLoop loop;

    QTimer poll;
    QTimer deadline;

    deadline.setSingleShot(true);

    QObject::connect(
        &poll,
        &QTimer::timeout,
        &loop,
        [&] {
            if (predicate()) {
                loop.quit();
            }
        }
    );

    QObject::connect(
        &deadline,
        &QTimer::timeout,
        &loop,
        &QEventLoop::quit
    );

    poll.start(2);
    deadline.start(timeoutMs);

    loop.exec();

    return predicate();
}

class AgentProcessTest : public ::testing::Test {
protected:
    bool launch(const QString& mode)
    {
        return owner.start(
            QCoreApplication::applicationFilePath(),
            {
                QStringLiteral("--agent-process-child"),
                mode
            },
            QDir::tempPath()
        );
    }

    void TearDown() override
    {
        // 即使断言失败，也先回收本测试创建的子进程。
        if (owner.active()) {
            owner.forceKill();

            const bool finished = spinUntil(
                [this] {
                    return !owner.active();
                }
            );

            EXPECT_TRUE(finished);
        }
    }

    AgentProcess owner;
};

struct RecordDisplayProbe {
    AgentProcess& process;

    edge::console::EventTableModel model;

    quint64 consumed = 0;
    int heartbeats = 0;

    bool enabled = true;
    bool onlyAfterExit = false;

    // 放在最后：析构时先停止定时器、断开相关连接。
    QTimer timer;

    explicit RecordDisplayProbe(AgentProcess& owner)
        : process(owner)
    {
        QObject::connect(
            &timer,
            &QTimer::timeout,
            &timer,
            [this] {
                ++heartbeats;

                if (!enabled) {
                    return;
                }

                if (onlyAfterExit && process.active()) {
                    return;
                }

                if (process.observation().recordsPending == 0) {
                    return;
                }

                auto batch = process.takeRecords(
                    edge::console::EventTableModel::kBatchLimit
                );

                if (batch.empty()) {
                    return;
                }

                consumed +=
                    static_cast<quint64>(batch.size());

                model.appendBatch(std::move(batch));
            }
        );

        timer.start(100);
    }
};

void expectPipelineCounts(
    const AgentProcess& process,
    const RecordDisplayProbe& probe
)
{
    const auto& observation = process.observation();

    // 本测试没有清空或重置运行，数量应当守恒。
    EXPECT_EQ(
        observation.recordsReceived,
        observation.recordsDropped
            + observation.recordsPending
            + probe.consumed
    );

    EXPECT_EQ(
        probe.consumed,
        static_cast<quint64>(probe.model.rowCount())
            + probe.model.evicted()
    );

    EXPECT_LE(
        observation.recordsPending,
        static_cast<quint64>(
            edge::console::RecordBuffer::kCapacity
        )
    );

    EXPECT_LE(
        probe.model.rowCount(),
        edge::console::EventTableModel::kHistoryLimit
    );
}

TEST_F(AgentProcessTest, ReportsFailedStart)
{
    QTemporaryDir directory;
    ASSERT_TRUE(directory.isValid());

    ASSERT_TRUE(owner.start(
        directory.filePath(QStringLiteral("missing-agent")),
        {},
        directory.path()
    ));

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    const auto& result = owner.observation();

    EXPECT_TRUE(result.failedToStart);
    EXPECT_FALSE(result.processStarted);
    EXPECT_FALSE(result.readySeen);
    EXPECT_FALSE(result.exitCode.has_value());
}

TEST_F(AgentProcessTest, StartedDoesNotImplyReady)
{
    ASSERT_TRUE(launch(QStringLiteral("no-ready")));

    ASSERT_TRUE(spinUntil([this] {
        return owner.observation().processStarted;
    }));

    EXPECT_TRUE(owner.active());
    EXPECT_FALSE(owner.observation().readySeen);

    owner.requestStop();

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    EXPECT_TRUE(owner.observation().stopRequested);
    EXPECT_FALSE(owner.observation().readySeen);
}

TEST_F(AgentProcessTest, StopsReadyChildAsynchronously)
{
    ASSERT_TRUE(launch(QStringLiteral("wait")));

    ASSERT_TRUE(spinUntil([this] {
        return owner.observation().readySeen;
    }));

    EXPECT_TRUE(owner.active());

    owner.requestStop();

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    const auto& result = owner.observation();

    EXPECT_TRUE(result.processStarted);
    EXPECT_TRUE(result.readySeen);
    EXPECT_TRUE(result.stopRequested);
    EXPECT_FALSE(result.crashed);
    EXPECT_FALSE(result.forceKillRequested);

    ASSERT_TRUE(result.exitCode.has_value());
    EXPECT_EQ(*result.exitCode, 0);

    // 模拟进程没有发送 stopped，不能补造。
    EXPECT_FALSE(result.stoppedSeen);
    EXPECT_FALSE(result.declaredExitCode.has_value());
    EXPECT_FALSE(result.latestStatus.has_value());
}

TEST_F(AgentProcessTest, DrainsBothChannelsAndReportsTail)
{
    ASSERT_TRUE(launch(QStringLiteral("exit-with-tail")));

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    const auto& result = owner.observation();

    EXPECT_TRUE(result.readySeen);
    EXPECT_EQ(result.recordsReceived, 1U);
    EXPECT_EQ(result.protocolErrors, 1U);
    EXPECT_FALSE(result.stoppedSeen);

    ASSERT_TRUE(result.exitCode.has_value());
    EXPECT_EQ(*result.exitCode, 7);

    const QString diagnostics =
        owner.takeDiagnostics().join(QStringLiteral("\n"));

    EXPECT_TRUE(
        diagnostics.contains(
            QString::fromUtf8(u8"诊断尾部")
        )
    );
}

TEST_F(AgentProcessTest, ResetsProtocolContextOnRestart)
{
    ASSERT_TRUE(launch(QStringLiteral("once-a")));

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    EXPECT_EQ(
        owner.observation().runId,
        QString(32, QLatin1Char('a'))
    );

    ASSERT_TRUE(launch(QStringLiteral("once-b")));

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    const auto& result = owner.observation();

    EXPECT_TRUE(result.readySeen);
    EXPECT_EQ(result.runId, QString(32, QLatin1Char('b')));
    EXPECT_EQ(result.protocolErrors, 0U);
    EXPECT_EQ(result.recordsReceived, 0U);
}

TEST_F(AgentProcessTest, RejectsOverlappingStart)
{
    ASSERT_TRUE(launch(QStringLiteral("wait")));

    ASSERT_TRUE(spinUntil([this] {
        return owner.observation().readySeen;
    }));

    EXPECT_FALSE(launch(QStringLiteral("once-b")));

    EXPECT_TRUE(owner.active());

    EXPECT_EQ(
        owner.observation().runId,
        QString(32, QLatin1Char('a'))
    );
}

TEST_F(AgentProcessTest, RemembersStopRequestedDuringStarting)
{
    ASSERT_TRUE(launch(QStringLiteral("wait")));

    owner.requestStop();

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    EXPECT_TRUE(owner.observation().stopRequested);

    // 极早停止可能发生在子进程安装 SIGTERM 处理器之前，
    // 因而这里不强行要求退出码为 0。
}

TEST_F(AgentProcessTest, ReportsExplicitForceKill)
{
    ASSERT_TRUE(launch(QStringLiteral("ignore-stop")));

    ASSERT_TRUE(spinUntil([this] {
        return owner.observation().readySeen;
    }));

    owner.requestStop();
    owner.forceKill();

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    const auto& result = owner.observation();

    EXPECT_TRUE(result.stopRequested);
    EXPECT_TRUE(result.forceKillRequested);
    EXPECT_TRUE(result.crashed);
    EXPECT_FALSE(result.exitCode.has_value());
    EXPECT_FALSE(result.stoppedSeen);
}

TEST_F(AgentProcessTest, BoundsDiagnosticStorage)
{
    ASSERT_TRUE(launch(QStringLiteral("burst")));

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    const auto& result = owner.observation();

    EXPECT_TRUE(result.readySeen);
    EXPECT_EQ(result.recordsReceived, 1U);
    EXPECT_EQ(result.protocolErrors, 0U);
    EXPECT_GT(result.diagnosticsDropped, 0U);

    const auto diagnostics = owner.takeDiagnostics();

    EXPECT_LE(diagnostics.size(), 64);

    for (const auto& line : diagnostics) {
        EXPECT_LE(line.size(), 1024);
    }

    EXPECT_TRUE(owner.takeDiagnostics().isEmpty());
}

TEST_F(AgentProcessTest, BuffersLatestRecordsAfterProcessExit)
{
    ASSERT_TRUE(launch(QStringLiteral("record-burst")));

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    const auto capacity =
        edge::console::RecordBuffer::kCapacity;

    EXPECT_EQ(
        owner.observation().recordsReceived,
        static_cast<quint64>(capacity + 5)
    );

    EXPECT_EQ(
        owner.observation().recordsPending,
        static_cast<quint64>(capacity)
    );

    EXPECT_EQ(owner.observation().recordsDropped, 5U);

    const auto first = owner.takeRecords(32);

    ASSERT_EQ(first.size(), 32U);
    EXPECT_EQ(first.front().sequence, 7U);

    EXPECT_EQ(
        owner.observation().recordsPending,
        static_cast<quint64>(capacity - 32)
    );

    const auto second = owner.takeRecords(1000);

    ASSERT_EQ(second.size(), capacity - 32);

    EXPECT_EQ(
        second.back().sequence,
        static_cast<std::uint64_t>(capacity) + 6
    );

    EXPECT_EQ(
        second.back().data.at("event").at("probe")
            .get<std::uint64_t>(),
        std::numeric_limits<std::uint64_t>::max()
    );

    EXPECT_EQ(owner.observation().recordsPending, 0U);
    EXPECT_EQ(owner.observation().recordsDropped, 5U);
}

TEST_F(AgentProcessTest, RestartClearsPendingRecordsAndDrops)
{
    ASSERT_TRUE(launch(QStringLiteral("record-burst")));

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    ASSERT_GT(owner.observation().recordsPending, 0U);
    ASSERT_EQ(owner.observation().recordsDropped, 5U);

    ASSERT_TRUE(launch(QStringLiteral("once-b")));

    ASSERT_TRUE(spinUntil([this] {
        return !owner.active();
    }));

    EXPECT_EQ(owner.observation().recordsReceived, 0U);
    EXPECT_EQ(owner.observation().recordsPending, 0U);
    EXPECT_EQ(owner.observation().recordsDropped, 0U);

    EXPECT_TRUE(owner.takeRecords(10).empty());

    EXPECT_EQ(
        owner.observation().runId,
        QString(32, QLatin1Char('b'))
    );
}

TEST_F(
    AgentProcessTest,
    SlowDisplayKeepsBothChannelsDraining
)
{
    bool stopSent = false;

    RecordDisplayProbe probe(owner);

    // 先暂停展示消费，接收层仍照常运行。
    probe.enabled = false;

    QObject::connect(
        &owner,
        &AgentProcess::observationChanged,
        &probe.timer,
        [this, &probe, &stopSent] {
            const auto received =
                owner.observation().recordsReceived;

            const auto unlockAt =
                static_cast<quint64>(
                    edge::console::RecordBuffer::kCapacity + 9
                );

            // 确认输入已经超过缓存容量，再恢复展示。
            if (received >= unlockAt) {
                probe.enabled = true;
            }

            if (received >= 270 && !stopSent) {
                // requestStop 会发观察信号，先设置标志防止重入。
                stopSent = true;
                owner.requestStop();
            }
        }
    );

    ASSERT_TRUE(
        launch(QStringLiteral("pipeline-stream"))
    );

    ASSERT_TRUE(spinUntil(
        [this] {
            return !owner.active()
                && owner.observation().recordsPending == 0;
        },
        10000
    ));

    probe.timer.stop();

    const auto& result = owner.observation();

    EXPECT_TRUE(stopSent);
    EXPECT_TRUE(result.readySeen);
    EXPECT_TRUE(result.stopRequested);

    EXPECT_FALSE(result.crashed);
    EXPECT_FALSE(result.forceKillRequested);

    ASSERT_TRUE(result.exitCode.has_value());
    EXPECT_EQ(*result.exitCode, 0);

    EXPECT_GE(result.recordsReceived, 270U);
    EXPECT_GT(result.recordsDropped, 0U);

    // 测试不消费诊断历史，但仍必须持续排空 stderr。
    EXPECT_GT(result.diagnosticsDropped, 0U);

    EXPECT_EQ(result.protocolErrors, 0U);
    EXPECT_GT(probe.heartbeats, 0);

    expectPipelineCounts(owner, probe);

    ASSERT_GT(probe.model.rowCount(), 0);

    const int last = probe.model.rowCount() - 1;

    // 模拟流的 ready 为 1，record 从 2 连续递增。
    EXPECT_EQ(
        probe.model.data(probe.model.index(last, 0)).toString(),
        QString::number(
            static_cast<qulonglong>(
                result.recordsReceived + 1
            )
        )
    );

    EXPECT_TRUE(
        probe.model.details(last).contains(
            QStringLiteral("18446744073709551615")
        )
    );
}

TEST_F(
    AgentProcessTest,
    FinalRecordsReachModelAfterStop
)
{
    bool stopSent = false;

    RecordDisplayProbe probe(owner);

    // 进程退出前完全不消费，验证退出后的剩余缓存。
    probe.onlyAfterExit = true;

    QObject::connect(
        &owner,
        &AgentProcess::observationChanged,
        &probe.timer,
        [this, &stopSent] {
            if (owner.observation().recordsReceived >= 27
                && !stopSent) {
                stopSent = true;
                owner.requestStop();
            }
        }
    );

    ASSERT_TRUE(
        launch(QStringLiteral("pipeline-stream"))
    );

    ASSERT_TRUE(spinUntil(
        [this] {
            return !owner.active()
                && owner.observation().recordsPending == 0;
        },
        10000
    ));

    probe.timer.stop();

    const auto& result = owner.observation();

    EXPECT_TRUE(stopSent);
    EXPECT_TRUE(result.stopRequested);
    EXPECT_FALSE(result.crashed);

    ASSERT_TRUE(result.exitCode.has_value());
    EXPECT_EQ(*result.exitCode, 0);

    EXPECT_GE(result.recordsReceived, 27U);
    EXPECT_GT(probe.consumed, 0U);
    EXPECT_GT(probe.model.rowCount(), 0);

    // 模拟进程没有发送 stopped，不能补造最终状态。
    EXPECT_FALSE(result.stoppedSeen);
    EXPECT_FALSE(result.declaredExitCode.has_value());

    EXPECT_EQ(result.protocolErrors, 0U);

    expectPipelineCounts(owner, probe);
}

TEST_F(
    AgentProcessTest,
    ForcedExitRetainsValidReceivedRecords
)
{
    bool killSent = false;

    RecordDisplayProbe probe(owner);
    probe.onlyAfterExit = true;

    QObject::connect(
        &owner,
        &AgentProcess::observationChanged,
        &probe.timer,
        [this, &killSent] {
            if (owner.observation().recordsReceived >= 12
                && !killSent) {
                killSent = true;

                // 直接注入强制终止，用于验证异常退出路径。
                owner.forceKill();
            }
        }
    );

    ASSERT_TRUE(
        launch(QStringLiteral("pipeline-stream"))
    );

    ASSERT_TRUE(spinUntil(
        [this] {
            return !owner.active()
                && owner.observation().recordsPending == 0;
        },
        10000
    ));

    probe.timer.stop();

    const auto& result = owner.observation();

    EXPECT_TRUE(killSent);
    EXPECT_TRUE(result.forceKillRequested);
    EXPECT_TRUE(result.crashed);

    EXPECT_FALSE(result.exitCode.has_value());
    EXPECT_FALSE(result.stoppedSeen);

    EXPECT_GE(result.recordsReceived, 12U);
    EXPECT_GT(probe.consumed, 0U);

    expectPipelineCounts(owner, probe);

    // 强杀可能产生不完整尾行，不能要求 protocolErrors 为 0。
    // 这里只核对已经完整接收、校验通过的事件。
}

} // namespace

int main(int argc, char* argv[])
{
    QCoreApplication application(argc, argv);

    if (argc == 3
        && QString::fromLocal8Bit(argv[1])
            == QStringLiteral("--agent-process-child")) {
        return runChild(QString::fromLocal8Bit(argv[2]));
    }

    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}