#include "mainwindow.h"
#include "ui_mainwindow.h"
#include "presentation/StatusText.h"
#include "presentation/TcpActivityText.h"
#include "presentation/EventTableModel.h"

#include <QAbstractItemView>
#include <QHeaderView>
#include <QItemSelectionModel>
#include <QPlainTextEdit>
#include <QScrollBar>
#include <QSplitter>
#include <QTableView>
#include <QTabWidget>
#include <QVBoxLayout>
#include <QSignalBlocker>
#include <QTableWidget>
#include <QTableWidgetItem>

#include <utility>

#include <QFormLayout>
#include <QGroupBox>
#include <QLabel>
#include <QLayout>
#include <QScrollArea>
#include <QSizePolicy>

#include <QCloseEvent>
#include <QDir>
#include <QFileDialog>
#include <QFileInfo>
#include <QLineEdit>
#include <QMessageBox>
#include <QPushButton>
#include <QStringList>

namespace {

QString boundedDiagnostic(QString text)
{
    // 一条诊断只占一个文本块，并限制显示长度。
    text.replace(QLatin1Char('\r'), QStringLiteral("\\r"));
    text.replace(QLatin1Char('\n'), QStringLiteral("\\n"));

    return text.left(1024);
}

bool confirmedSuccess(
    const edge::console::AgentProcessObservation& result
    )
{
    return !result.failedToStart
           && !result.crashed
           && !result.forceKillRequested
           && result.stoppedSeen
           && result.exitCode.has_value()
           && result.declaredExitCode.has_value()
           && *result.exitCode == 0
           && *result.declaredExitCode == 0;
}

} // namespace

MainWindow::MainWindow(QWidget* parent)
    : QMainWindow(parent)
    , ui(new Ui::MainWindow)
{
    ui->setupUi(this);

    setupStatusPanel();

    setupEventPanel();

    connect(
        ui->browseAgentButton,
        &QPushButton::clicked,
        this,
        &MainWindow::chooseAgent
        );

    connect(
        ui->browseConfigButton,
        &QPushButton::clicked,
        this,
        &MainWindow::chooseConfig
        );

    connect(
        ui->browseDataDirButton,
        &QPushButton::clicked,
        this,
        &MainWindow::chooseDataDirectory
        );

    connect(
        ui->startButton,
        &QPushButton::clicked,
        this,
        &MainWindow::startAgent
        );

    connect(
        ui->stopButton,
        &QPushButton::clicked,
        this,
        &MainWindow::stopAgent
        );

    connect(
        ui->forceKillButton,
        &QPushButton::clicked,
        this,
        &MainWindow::forceKillAgent
        );

    connect(
        ui->clearDiagnosticsButton,
        &QPushButton::clicked,
        this,
        [this] {
            // 用户主动清空时，同时取走尚未展示的诊断。
            (void)agent_.takeDiagnostics();

            ui->diagnosticsEdit->clear();
        }
        );

    const auto markDirty = [this] {
        dirty_ = true;
    };

    connect(
        ui->agentPathEdit,
        &QLineEdit::textChanged,
        this,
        markDirty
        );

    connect(
        ui->configPathEdit,
        &QLineEdit::textChanged,
        this,
        markDirty
        );

    connect(
        ui->dataDirEdit,
        &QLineEdit::textChanged,
        this,
        markDirty
        );

    connect(
        &agent_,
        &edge::console::AgentProcess::observationChanged,
        this,
        markDirty
        );

    connect(
        &agent_,
        &edge::console::AgentProcess::runFinished,
        this,
        &MainWindow::onRunFinished
        );

    // 接收层继续及时排空管道，界面最多每 100ms 合并刷新一次。
    connect(
        &refreshTimer_,
        &QTimer::timeout,
        this,
        [this] {
            // 即使进程已经退出，也继续消费剩余缓存。
            drainRecordBatch();

            if (dirty_) {
                refreshUi();
            }
        }
    );

    refreshTimer_.start(100);

    refreshUi();
}

MainWindow::~MainWindow()
{
    refreshTimer_.stop();

    Q_ASSERT(!agent_.active());

    delete ui;
}

void MainWindow::chooseAgent()
{
    if (agent_.active() || closing_) {
        return;
    }

    const QString initialPath =
        ui->agentPathEdit->text().isEmpty()
            ? QDir::homePath()
            : ui->agentPathEdit->text();

    const QString selected = QFileDialog::getOpenFileName(
        this,
        tr("选择 edge-agent 程序"),
        initialPath,
        tr("所有文件 (*)")
        );

    if (!selected.isEmpty()) {
        ui->agentPathEdit->setText(selected);
    }
}

void MainWindow::chooseConfig()
{
    if (agent_.active() || closing_) {
        return;
    }

    const QString initialPath =
        ui->configPathEdit->text().isEmpty()
            ? QDir::homePath()
            : ui->configPathEdit->text();

    const QString selected = QFileDialog::getOpenFileName(
        this,
        tr("选择 Agent 配置文件"),
        initialPath,
        tr("YAML 文件 (*.yml *.yaml);;所有文件 (*)")
        );

    if (!selected.isEmpty()) {
        ui->configPathEdit->setText(selected);
    }
}

void MainWindow::chooseDataDirectory()
{
    if (agent_.active() || closing_) {
        return;
    }

    const QString initialPath =
        ui->dataDirEdit->text().isEmpty()
            ? QDir::homePath()
            : ui->dataDirEdit->text();

    const QString selected =
        QFileDialog::getExistingDirectory(
            this,
            tr("选择 Agent 数据目录"),
            initialPath
            );

    if (!selected.isEmpty()) {
        ui->dataDirEdit->setText(selected);
    }
}

void MainWindow::startAgent()
{
    if (agent_.active() || closing_) {
        return;
    }

    // 不按空格拆分路径，也不拼接 shell 命令。
    const QString programPath = ui->agentPathEdit->text();
    const QString configPath = ui->configPathEdit->text();
    const QString dataDirectory = ui->dataDirEdit->text();

    if (!QDir::isAbsolutePath(programPath)
        || !QDir::isAbsolutePath(configPath)) {
        QMessageBox::warning(
            this,
            tr("无法启动"),
            tr("Agent 程序和配置文件必须使用绝对路径。")
            );

        return;
    }

    const QFileInfo programInfo(programPath);
    const QFileInfo configInfo(configPath);

    if (!programInfo.isFile()
        || !programInfo.isExecutable()) {
        QMessageBox::warning(
            this,
            tr("无法启动"),
            tr("所选 Agent 程序不存在或没有执行权限。")
            );

        return;
    }

    if (!configInfo.isFile()
        || !configInfo.isReadable()) {
        QMessageBox::warning(
            this,
            tr("无法启动"),
            tr("所选配置文件不存在或不可读取。")
            );

        return;
    }

    if (!dataDirectory.isEmpty()) {
        if (!QDir::isAbsolutePath(dataDirectory)) {
            QMessageBox::warning(
                this,
                tr("无法启动"),
                tr("数据目录必须使用绝对路径，或留空。")
                );

            return;
        }

        const QFileInfo dataInfo(dataDirectory);

        if (dataInfo.exists() && !dataInfo.isDir()) {
            QMessageBox::warning(
                this,
                tr("无法启动"),
                tr("数据目录指向了一个普通文件。")
                );

            return;
        }
    }

    QStringList arguments = {
        QStringLiteral("--config"),
        configInfo.absoluteFilePath(),
        QStringLiteral("--run"),
        QStringLiteral("--events-jsonl")
    };

    if (!dataDirectory.isEmpty()) {
        arguments
            << QStringLiteral("--data-dir")
            << dataDirectory;
    }

    // 工作目录明确设为配置文件所在目录，
    // 不依赖 Qt Creator 启动 GUI 时所设置的目录。
    const bool accepted = agent_.start(
        programInfo.absoluteFilePath(),
        arguments,
        configInfo.absolutePath()
        );

    if (!accepted) {
        QMessageBox::warning(
            this,
            tr("无法启动"),
            tr("启动请求未被接受，请等待当前运行完成。")
            );

        return;
    }

    hasRun_ = true;

    eventModel_->resetForRun();
    eventDetails_->clear();

    // 每次运行重新展示，避免与上一次诊断混在一起。
    ui->diagnosticsEdit->clear();

    appendDiagnostic(
        tr("启动请求：%1")
            .arg(programInfo.absoluteFilePath())
        );

    refreshUi();
}

void MainWindow::stopAgent()
{
    if (!agent_.active()) {
        return;
    }

    agent_.requestStop();

    refreshUi();
}

void MainWindow::forceKillAgent()
{
    if (!agent_.active()
        || !agent_.observation().stopRequested) {
        return;
    }

    agent_.forceKill();

    refreshUi();
}

void MainWindow::setupStatusPanel()
{
    auto* scroll = new QScrollArea(ui->centralwidget);

    scroll->setWidgetResizable(true);
    scroll->setMinimumHeight(140);
    scroll->setMaximumHeight(260);

    auto* group = new QGroupBox(
        tr("状态快照"),
        scroll
    );

    auto* form = new QFormLayout(group);

    form->setLabelAlignment(
        Qt::AlignLeft | Qt::AlignTop
    );

    form->setFieldGrowthPolicy(
        QFormLayout::AllNonFixedFieldsGrow
    );

    form->setSizeConstraint(
        QLayout::SetMinAndMaxSize
    );

    const auto makeValueLabel = [group] {
        auto* label = new QLabel(group);

        label->setTextFormat(Qt::PlainText);
        label->setWordWrap(true);

        label->setTextInteractionFlags(
            Qt::TextSelectableByMouse
        );

        label->setSizePolicy(
            QSizePolicy::Ignored,
            QSizePolicy::Preferred
        );

        return label;
    };

    snapshotScopeLabel_ = makeValueLabel();
    snapshotIdentityLabel_ = makeValueLabel();
    snapshotRuntimeLabel_ = makeValueLabel();
    snapshotCountersLabel_ = makeValueLabel();
    snapshotOutboxLabel_ = makeValueLabel();
    snapshotDiskLabel_ = makeValueLabel();
    snapshotWarningsLabel_ = makeValueLabel();
    snapshotUploadLabel_ = makeValueLabel();
    snapshotRecordingLabel_ = makeValueLabel();

    form->addRow(tr("观察范围"), snapshotScopeLabel_);
    form->addRow(tr("快照信息"), snapshotIdentityLabel_);
    form->addRow(tr("运行观察"), snapshotRuntimeLabel_);
    form->addRow(tr("上传观察"), snapshotUploadLabel_);
    form->addRow(tr("原始录制"), snapshotRecordingLabel_);
    form->addRow(tr("累计计数"), snapshotCountersLabel_);
    form->addRow(tr("Outbox"), snapshotOutboxLabel_);
    form->addRow(tr("磁盘采样"), snapshotDiskLabel_);
    form->addRow(tr("告警"), snapshotWarningsLabel_);

    tcpScopeLabel_ = makeValueLabel();
    tcpTotalsLabel_ = makeValueLabel();

    tcpTable_ = new QTableWidget(
        0,
        static_cast<int>(
            edge::console::kTcpActivityColumns
        ),
        group
    );

    tcpTable_->setHorizontalHeaderLabels({
        tr("槽位"),
        tr("连接编号"),
        tr("字节活动状态"),
        tr("已连接 ms"),
        tr("距最近数据 ms"),
        tr("收到字节 B")
    });

    tcpTable_->setEditTriggers(
        QAbstractItemView::NoEditTriggers
    );

    // 保留协议中的槽位顺序，避免字符串数值排序。
    tcpTable_->setSortingEnabled(false);

    tcpTable_->setSelectionBehavior(
        QAbstractItemView::SelectRows
    );

    tcpTable_->setSelectionMode(
        QAbstractItemView::SingleSelection
    );

    tcpTable_->verticalHeader()->setVisible(false);

    tcpTable_->horizontalHeader()->setSectionResizeMode(
        QHeaderView::ResizeToContents
    );

    tcpTable_->horizontalHeader()->setStretchLastSection(true);

    tcpTable_->setMinimumHeight(140);
    tcpTable_->setMaximumHeight(240);

    form->addRow(tr("TCP 观察"), tcpScopeLabel_);
    form->addRow(tr("TCP 累计"), tcpTotalsLabel_);
    form->addRow(tcpTable_);

    scroll->setWidget(group);

    // 插入到现有“退出结果”标签之前。
    const int index =
        ui->mainLayout->indexOf(ui->exitResultLabel);

    ui->mainLayout->insertWidget(index, scroll);
}

void MainWindow::refreshStatusPanel()
{
    const auto text = edge::console::makeStatusText(
        agent_.observation().latestStatus,
        agent_.active()
    );

    snapshotScopeLabel_->setText(text.scope);
    snapshotIdentityLabel_->setText(text.identity);
    snapshotRuntimeLabel_->setText(text.runtime);
    snapshotUploadLabel_->setText(text.upload);
    snapshotRecordingLabel_->setText(text.recording);
    snapshotCountersLabel_->setText(text.counters);
    snapshotOutboxLabel_->setText(text.outbox);
    snapshotDiskLabel_->setText(text.disk);
    snapshotWarningsLabel_->setText(text.warnings);

    refreshTcpTable();
}

void MainWindow::setupEventPanel()
{
    eventModel_ = new edge::console::EventTableModel(this);

    auto* tabs = new QTabWidget(ui->centralwidget);
    auto* page = new QWidget(tabs);
    auto* layout = new QVBoxLayout(page);

    layout->setContentsMargins(8, 8, 8, 8);

    eventStatsLabel_ = new QLabel(page);
    eventStatsLabel_->setWordWrap(true);

    layout->addWidget(eventStatsLabel_);

    auto* splitter = new QSplitter(
        Qt::Vertical,
        page
    );

    eventTable_ = new QTableView;
    eventTable_->setModel(eventModel_);

    eventTable_->setEditTriggers(
        QAbstractItemView::NoEditTriggers
    );

    eventTable_->setSelectionBehavior(
        QAbstractItemView::SelectRows
    );

    eventTable_->setSelectionMode(
        QAbstractItemView::SingleSelection
    );

    eventTable_->setSortingEnabled(false);
    eventTable_->setWordWrap(false);

    eventTable_->horizontalHeader()->setSectionResizeMode(
        QHeaderView::Interactive
    );

    eventTable_->horizontalHeader()->setStretchLastSection(true);

    eventTable_->setColumnWidth(0, 110);
    eventTable_->setColumnWidth(1, 160);
    eventTable_->setColumnWidth(2, 150);
    eventTable_->setColumnWidth(3, 220);
    eventTable_->setColumnWidth(4, 350);

    eventTable_->verticalHeader()->setDefaultSectionSize(26);

    eventDetails_ = new QPlainTextEdit;

    eventDetails_->setReadOnly(true);
    eventDetails_->setUndoRedoEnabled(false);

    eventDetails_->setPlaceholderText(
        tr("选择一行查看事件 JSON；过长详情会标记截断。")
    );

    splitter->addWidget(eventTable_);
    splitter->addWidget(eventDetails_);

    splitter->setStretchFactor(0, 2);
    splitter->setStretchFactor(1, 1);

    layout->addWidget(splitter, 1);

    // 将原诊断框移到页签中，原来的 ui 指针继续有效。
    const int index =
        ui->mainLayout->indexOf(ui->diagnosticsEdit);

    ui->mainLayout->removeWidget(ui->diagnosticsEdit);

    tabs->addTab(page, tr("事件"));
    tabs->addTab(ui->diagnosticsEdit, tr("诊断"));

    ui->mainLayout->insertWidget(index, tabs, 1);

    connect(
        eventTable_->selectionModel(),
        &QItemSelectionModel::currentRowChanged,
        this,
        [this] {
            refreshEventDetails();
        }
    );
}

void MainWindow::drainRecordBatch()
{
    if (agent_.observation().recordsPending == 0) {
        return;
    }

    auto batch = agent_.takeRecords(
        edge::console::EventTableModel::kBatchLimit
    );

    if (batch.empty()) {
        return;
    }

    const auto* scrollbar =
        eventTable_->verticalScrollBar();

    const bool followTail =
        scrollbar->value() >= scrollbar->maximum();

    eventModel_->appendBatch(std::move(batch));

    // 用户正在查看较早记录时，不强行滚回底部。
    if (followTail) {
        eventTable_->scrollToBottom();
    }

    // 历史淘汰可能改变选中行，同步刷新或清空详情。
    refreshEventDetails();

    dirty_ = true;
}

void MainWindow::refreshEventDetails()
{
    const QModelIndex current =
        eventTable_->currentIndex();

    const QString text = current.isValid()
        ? eventModel_->details(current.row())
        : QString{};

    // 同一条事件内容未变时，保留详情区的滚动和选择。
    if (eventDetails_->toPlainText() != text) {
        eventDetails_->setPlainText(text);
    }
}

QString MainWindow::processStateText() const
{
    const auto& result = agent_.observation();

    if (agent_.active()) {
        if (result.forceKillRequested) {
            return tr("已请求强制终止，等待退出及通道排空");
        }

        if (result.stoppedSeen) {
            return tr("已收到 Agent 收尾消息，等待进程退出");
        }

        if (result.stopRequested) {
            return tr("已请求正常停止，等待收尾");
        }

        if (result.readySeen) {
            return tr("Agent 已就绪");
        }

        if (result.processStarted) {
            return tr("进程已启动，等待 Agent 就绪");
        }

        return tr("正在启动进程");
    }

    if (!hasRun_) {
        return tr("尚未启动");
    }

    if (result.failedToStart) {
        return tr("启动失败");
    }

    if (result.crashed) {
        return tr("进程异常终止");
    }

    if (result.forceKillRequested) {
        return tr("进程已结束，曾请求强制终止");
    }

    if (result.exitCode.has_value()
        && result.declaredExitCode.has_value()
        && *result.exitCode != *result.declaredExitCode) {
        return tr("进程已退出，退出信息不一致");
    }

    if (confirmedSuccess(result)) {
        return tr("已确认正常结束");
    }

    if ((result.exitCode.has_value()
         && *result.exitCode != 0)
        || (result.declaredExitCode.has_value()
            && *result.declaredExitCode != 0)) {
        return tr("进程失败退出");
    }

    return tr("进程已退出，最终业务状态未确认");
}

QString MainWindow::exitResultText() const
{
    if (!hasRun_) {
        return tr("退出结果：—");
    }

    if (agent_.active()) {
        return tr("退出结果：等待进程退出");
    }

    const auto& result = agent_.observation();

    if (result.failedToStart) {
        return tr("退出结果：程序未成功启动，详见诊断");
    }

    QStringList parts;

    if (result.crashed) {
        parts.append(tr("操作系统报告异常终止"));
    } else if (result.exitCode.has_value()) {
        parts.append(
            tr("实际退出码：%1").arg(*result.exitCode)
            );
    } else {
        parts.append(tr("未取得有效退出码"));
    }

    if (result.declaredExitCode.has_value()) {
        parts.append(
            tr("Agent stopped 声明退出码：%1")
                .arg(*result.declaredExitCode)
            );
    } else {
        parts.append(tr("未收到有效 stopped，最终业务状态未确认"));
    }

    if (!result.readySeen) {
        parts.append(tr("未观察到 Agent ready"));
    }

    if (result.forceKillRequested) {
        parts.append(tr("曾请求强制终止"));
    }

    return tr("退出结果：%1")
        .arg(parts.join(QStringLiteral("；")));
}

void MainWindow::refreshUi()
{
    dirty_ = false;

    refreshStatusPanel();

    eventStatsLabel_->setText(
        tr("历史行：%1/%2；历史淘汰：%3；选中行查看事件详情")
            .arg(eventModel_->rowCount())
            .arg(edge::console::EventTableModel::kHistoryLimit)
            .arg(eventModel_->evicted())
    );

    const bool active = agent_.active();
    const bool canConfigure = !active && !closing_;

    const auto& result = agent_.observation();

    ui->agentPathEdit->setEnabled(canConfigure);
    ui->configPathEdit->setEnabled(canConfigure);
    ui->dataDirEdit->setEnabled(canConfigure);

    ui->browseAgentButton->setEnabled(canConfigure);
    ui->browseConfigButton->setEnabled(canConfigure);
    ui->browseDataDirButton->setEnabled(canConfigure);

    ui->startButton->setEnabled(
        canConfigure
        && !ui->agentPathEdit->text().isEmpty()
        && !ui->configPathEdit->text().isEmpty()
        );

    ui->stopButton->setEnabled(
        active && !result.stopRequested
        );

    // 先提出正常停止请求，再允许显式强制终止。
    // 等待关闭窗口时，也保留这个按钮。
    ui->forceKillButton->setEnabled(
        active
        && result.stopRequested
        && !result.forceKillRequested
        );

    const QString configPath = ui->configPathEdit->text();

    if (!configPath.isEmpty()
        && QDir::isAbsolutePath(configPath)) {
        ui->workingDirectoryLabel->setText(
            QFileInfo(configPath).absolutePath()
            );
    } else {
        ui->workingDirectoryLabel->setText(
            tr("选择配置文件后确定")
            );
    }

    const QString state = processStateText();

    ui->processStateLabel->setText(
        tr("进程状态：%1").arg(state)
        );

    ui->runIdLabel->setText(
        tr("运行标识：%1").arg(
            result.runId.isEmpty()
                ? tr("未取得")
                : result.runId
            )
        );

    ui->exitResultLabel->setText(exitResultText());

    ui->diagnosticStatsLabel->setText(
        tr(
            "协议错误：%1；诊断缓存丢弃：%2；"
            "已接收事件：%3；待展示：%4；"
            "控制台事件缓存丢弃：%5；"
            "诊断区保留最近 500 行"
        )
            .arg(result.protocolErrors)
            .arg(result.diagnosticsDropped)
            .arg(result.recordsReceived)
            .arg(result.recordsPending)
            .arg(result.recordsDropped)
    );

    // 每批最多取走 owner 中保存的 64 条诊断，
    // 合并后只调用一次控件追加接口。
    QStringList diagnostics = agent_.takeDiagnostics();

    for (QString& line : diagnostics) {
        line = boundedDiagnostic(line);
    }

    if (!diagnostics.isEmpty()) {
        ui->diagnosticsEdit->appendPlainText(
            diagnostics.join(QStringLiteral("\n"))
            );
    }

    if (closing_ && active) {
        ui->statusbar->showMessage(
            tr(
                "正在关闭：等待 Agent 收尾。"
                "若无法完成，可点击“强制终止”。"
                )
            );
    } else {
        ui->statusbar->showMessage(state);
    }
}

void MainWindow::appendDiagnostic(const QString& text)
{
    ui->diagnosticsEdit->appendPlainText(
        boundedDiagnostic(text)
        );
}

void MainWindow::closeEvent(QCloseEvent* event)
{
    if (!agent_.active()) {
        event->accept();
        return;
    }

    // 保持窗口和事件循环存活，继续读取子进程输出。
    event->ignore();

    if (!closing_) {
        closing_ = true;

        appendDiagnostic(
            tr("收到关闭请求，等待 Agent 正常停止。")
            );

        agent_.requestStop();
    }

    refreshUi();
}

void MainWindow::onRunFinished()
{
    refreshUi();

    if (!closing_) {
        return;
    }

    // 等 owner 的 runFinished 信号调用栈返回后再关闭，
    // 避免在 owner 的成员函数执行期间销毁它。
    QTimer::singleShot(
        0,
        this,
        [this] {
            if (closing_ && !agent_.active()) {
                close();
            }
        }
        );
}

void MainWindow::refreshTcpTable()
{
    const auto text = edge::console::makeTcpActivityText(
        agent_.observation().latestStatus,
        agent_.active()
    );

    tcpScopeLabel_->setText(text.scope);
    tcpTotalsLabel_->setText(text.totals);

    const QSignalBlocker blocker(tcpTable_);

    tcpTable_->clearContents();

    tcpTable_->setRowCount(
        static_cast<int>(text.rows.size())
    );

    for (int row = 0; row < tcpTable_->rowCount(); ++row) {
        const auto& values =
            text.rows[static_cast<std::size_t>(row)];

        for (int column = 0;
             column < tcpTable_->columnCount();
             ++column) {
            const QString& value =
                values[static_cast<std::size_t>(column)];

            auto* item = new QTableWidgetItem(value);

            item->setToolTip(value);

            item->setTextAlignment(
                Qt::AlignLeft | Qt::AlignVCenter
            );

            tcpTable_->setItem(row, column, item);
        }
    }
}