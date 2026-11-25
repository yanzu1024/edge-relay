#ifndef EDGE_CONSOLE_MAINWINDOW_H
#define EDGE_CONSOLE_MAINWINDOW_H

#include "process/AgentProcess.h"

#include <QMainWindow>
#include <QString>
#include <QTimer>

class QCloseEvent;
class QLabel;
class QTableWidget;
class QPlainTextEdit;
class QTableView;

namespace edge::console {
class EventTableModel;
}

namespace Ui {
class MainWindow;
}

class MainWindow final : public QMainWindow {
    Q_OBJECT

public:
    explicit MainWindow(QWidget* parent = nullptr);
    ~MainWindow() override;

protected:
    void closeEvent(QCloseEvent* event) override;

private:
    void chooseAgent();
    void chooseConfig();
    void chooseDataDirectory();

    void startAgent();
    void stopAgent();
    void forceKillAgent();

    void onRunFinished();
    void refreshUi();

    void appendDiagnostic(const QString& text);

    void setupStatusPanel();
    void refreshStatusPanel();

    void refreshTcpTable();

    void setupEventPanel();
    void drainRecordBatch();
    void refreshEventDetails();

    [[nodiscard]] QString processStateText() const;
    [[nodiscard]] QString exitResultText() const;

    Ui::MainWindow* ui;

    // 作为成员持有，窗口正常关闭前必须完成子进程回收。
    edge::console::AgentProcess agent_;

    edge::console::EventTableModel* eventModel_ = nullptr;

    QTimer refreshTimer_;

    bool dirty_ = true;
    bool hasRun_ = false;
    bool closing_ = false;

    QLabel* snapshotScopeLabel_ = nullptr;
    QLabel* snapshotIdentityLabel_ = nullptr;
    QLabel* snapshotRuntimeLabel_ = nullptr;

    QLabel* snapshotCountersLabel_ = nullptr;
    QLabel* snapshotOutboxLabel_ = nullptr;
    
    QLabel* snapshotDiskLabel_ = nullptr;
    QLabel* snapshotWarningsLabel_ = nullptr;
    QLabel* snapshotUploadLabel_ = nullptr;
    QLabel* snapshotRecordingLabel_ = nullptr;

    QLabel* tcpScopeLabel_ = nullptr;
    QLabel* tcpTotalsLabel_ = nullptr;
    QTableWidget* tcpTable_ = nullptr;

    QTableView* eventTable_ = nullptr;
    QPlainTextEdit* eventDetails_ = nullptr;
    QLabel* eventStatsLabel_ = nullptr;
};

#endif