#include "presentation/StatusText.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <optional>

namespace {

using namespace edge::console;

StatusSummary sampleSnapshot()
{
    StatusSummary snapshot;

    snapshot.agent_id = "status-text-test";
    snapshot.run_id = std::string(32, 'a');

    snapshot.sequence = 4;
    snapshot.checked_at_ms = 1700000000000LL;
    snapshot.uptime_ms = 0;

    snapshot.process_state = ProcessState::Running;
    snapshot.active_connections = 0;

    snapshot.unacked_warn_records = 1000;
    snapshot.disk_available_warn_bytes = 209715200;

    for (const char* name : {
            "valid_events",
            "stored_events",
            "duplicate_events",
            "identity_conflicts",
            "capacity_rejections",
            "storage_errors",
            "upload_acked"
        }) {
        snapshot.counters.emplace(name, 0);
    }

    return snapshot;
}

OutboxCounts sampleCounts()
{
    OutboxCounts counts;

    counts.pending = 5;
    counts.sending = 2;
    counts.retry = 3;
    counts.acked = 7;
    counts.dead = 4;

    counts.total = 21;
    counts.unacked = 14;
    counts.backlog = 10;

    return counts;
}

TEST(StatusTextTest, MissingSnapshotIsUnknown)
{
    const auto text = makeStatusText(std::nullopt, true);

    EXPECT_TRUE(text.scope.contains(QStringLiteral("等待")));
    EXPECT_EQ(text.runtime, QStringLiteral("未知"));
    EXPECT_EQ(text.counters, QStringLiteral("未知"));
    EXPECT_EQ(text.outbox, QStringLiteral("未知"));
    EXPECT_EQ(text.disk, QStringLiteral("未知"));
}

TEST(StatusTextTest, ZeroIsValidData)
{
    auto snapshot = sampleSnapshot();

    snapshot.outbox.state = SampleState::Valid;
    snapshot.outbox.checked_at_ms = snapshot.checked_at_ms;
    snapshot.outbox.value = OutboxCounts{};

    snapshot.disk_available_bytes.state = SampleState::Valid;
    snapshot.disk_available_bytes.checked_at_ms =
        snapshot.checked_at_ms;
    snapshot.disk_available_bytes.value = 0;

    snapshot.unacked_warning = false;
    snapshot.disk_warning = true;

    const auto text = makeStatusText(snapshot, true);

    EXPECT_TRUE(text.runtime.contains(QStringLiteral("连接数=0")));
    EXPECT_TRUE(text.counters.contains(QStringLiteral("入库=0")));
    EXPECT_TRUE(text.outbox.contains(QStringLiteral("有效")));
    EXPECT_TRUE(text.outbox.contains(QStringLiteral("总量=0")));
    EXPECT_TRUE(text.disk.contains(QStringLiteral("0 B")));

    EXPECT_TRUE(
        text.warnings.contains(QStringLiteral("未确认记录：正常"))
    );

    EXPECT_TRUE(
        text.warnings.contains(QStringLiteral("磁盘可用空间：告警"))
    );
}

TEST(StatusTextTest, StaleOutboxRetainsOldValue)
{
    auto snapshot = sampleSnapshot();

    snapshot.outbox.state = SampleState::Stale;
    snapshot.outbox.checked_at_ms = snapshot.checked_at_ms - 1000;
    snapshot.outbox.value = sampleCounts();

    const auto text = makeStatusText(snapshot, true);

    EXPECT_TRUE(text.outbox.contains(QStringLiteral("过期")));
    EXPECT_TRUE(text.outbox.contains(QStringLiteral("未确认=14")));

    EXPECT_TRUE(
        text.outbox.contains(QStringLiteral("积压（不含DEAD）=10"))
    );

    EXPECT_TRUE(
        text.warnings.contains(QStringLiteral("未确认记录：无法判定"))
    );
}

TEST(StatusTextTest, ErrorDoesNotInventZero)
{
    auto snapshot = sampleSnapshot();

    snapshot.outbox.state = SampleState::Error;
    snapshot.outbox.checked_at_ms = snapshot.checked_at_ms;
    snapshot.outbox.error = "database busy";

    const auto text = makeStatusText(snapshot, true);

    EXPECT_TRUE(text.outbox.contains(QStringLiteral("失败")));
    EXPECT_TRUE(text.outbox.contains(QStringLiteral("database busy")));
    EXPECT_FALSE(text.outbox.contains(QStringLiteral("总量=0")));
}

TEST(StatusTextTest, StaleDiskKeepsOldValue)
{
    auto snapshot = sampleSnapshot();

    snapshot.disk_available_bytes.state = SampleState::Stale;
    snapshot.disk_available_bytes.checked_at_ms =
        snapshot.checked_at_ms - 1000;
    snapshot.disk_available_bytes.value = 1048576;

    const auto text = makeStatusText(snapshot, true);

    EXPECT_TRUE(text.disk.contains(QStringLiteral("过期")));
    EXPECT_TRUE(text.disk.contains(QStringLiteral("1048576 B")));
}

TEST(StatusTextTest, WarningsNeedValidSamples)
{
    auto snapshot = sampleSnapshot();

    snapshot.outbox.state = SampleState::Stale;
    snapshot.outbox.value = sampleCounts();

    // 即使调用者给了旧的告警值，也不能显示成当前判断。
    snapshot.unacked_warning = false;

    const auto text = makeStatusText(snapshot, true);

    EXPECT_TRUE(
        text.warnings.contains(QStringLiteral("未确认记录：无法判定"))
    );

    EXPECT_TRUE(
        text.warnings.contains(QStringLiteral("磁盘可用空间：无法判定"))
    );
}

TEST(StatusTextTest, PreservesUint64Precision)
{
    auto snapshot = sampleSnapshot();

    const auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    snapshot.sequence = maximum;
    snapshot.uptime_ms = maximum;
    snapshot.active_connections = maximum;
    snapshot.counters["stored_events"] = maximum;

    snapshot.disk_available_bytes.state = SampleState::Valid;
    snapshot.disk_available_bytes.checked_at_ms =
        snapshot.checked_at_ms;
    snapshot.disk_available_bytes.value = maximum;

    const auto text = makeStatusText(snapshot, true);

    const QString expected =
        QStringLiteral("18446744073709551615");

    EXPECT_TRUE(text.identity.contains(expected));
    EXPECT_TRUE(text.runtime.contains(expected));
    EXPECT_TRUE(text.counters.contains(expected));
    EXPECT_TRUE(text.disk.contains(expected));
}

TEST(StatusTextTest, MissingCounterIsUnknown)
{
    auto snapshot = sampleSnapshot();

    snapshot.counters.erase("stored_events");

    const auto text = makeStatusText(snapshot, true);

    EXPECT_TRUE(text.counters.contains(QStringLiteral("入库=未知")));
}

TEST(StatusTextTest, MarksExitedSnapshotAsHistorical)
{
    auto snapshot = sampleSnapshot();

    snapshot.counters["stored_events"] = 12;

    const auto text = makeStatusText(snapshot, false);

    EXPECT_TRUE(text.scope.contains(QStringLiteral("进程已退出")));
    EXPECT_TRUE(text.scope.contains(QStringLiteral("未确认最终状态")));
    EXPECT_TRUE(text.counters.contains(QStringLiteral("入库=12")));
}

TEST(StatusTextTest, StoppedSnapshotDoesNotImplyOsExit)
{
    auto snapshot = sampleSnapshot();

    snapshot.process_state = ProcessState::Stopped;
    snapshot.exit_code = 0;

    const auto activeText = makeStatusText(snapshot, true);

    EXPECT_TRUE(
        activeText.scope.contains(QStringLiteral("等待进程退出"))
    );

    const auto exitedText = makeStatusText(snapshot, false);

    EXPECT_TRUE(
        exitedText.scope.contains(QStringLiteral("最后收到 stopped"))
    );
}

UploadObservation enabledUploadObservation()
{
    UploadObservation observation;

    observation.state = UploadState::Enabled;
    observation.initialized = true;

    return observation;
}

RecordingObservation activeRecordingObservation()
{
    RecordingObservation observation;

    observation.state = RecordingState::Active;
    observation.enabled = true;
    observation.operational = true;

    return observation;
}

TEST(StatusTextTest, RuntimeUnknownDiffersFromDisabled)
{
    auto snapshot = sampleSnapshot();

    const auto unknown = makeStatusText(snapshot, true);

    EXPECT_EQ(unknown.upload, QStringLiteral("未知"));
    EXPECT_EQ(unknown.recording, QStringLiteral("未知"));

    RecordingObservation recording;

    recording.state = RecordingState::Disabled;
    recording.enabled = false;
    recording.operational = false;

    snapshot.recording = recording;

    const auto disabled = makeStatusText(snapshot, true);

    EXPECT_TRUE(
        disabled.recording.contains(QStringLiteral("DISABLED"))
    );

    EXPECT_NE(disabled.recording, QStringLiteral("未知"));
}

TEST(StatusTextTest, EnabledUploadDoesNotClaimRequestInProgress)
{
    auto snapshot = sampleSnapshot();

    snapshot.upload = enabledUploadObservation();

    const auto text = makeStatusText(snapshot, true);

    EXPECT_TRUE(
        text.upload.contains(QStringLiteral("ENABLED"))
    );

    EXPECT_TRUE(
        text.upload.contains(QStringLiteral("已初始化=是"))
    );

    EXPECT_FALSE(
        text.upload.contains(QStringLiteral("正在上传"))
    );
}

TEST(StatusTextTest, StoppedUploadKeepsBlockedReason)
{
    auto snapshot = sampleSnapshot();

    auto upload = enabledUploadObservation();

    upload.state = UploadState::Blocked;
    upload.blocked = true;
    upload.blocked_http_status = 403;

    snapshot.upload = upload;

    const auto blocked = makeStatusText(snapshot, true);

    EXPECT_TRUE(
        blocked.upload.contains(QStringLiteral("BLOCKED"))
    );

    EXPECT_TRUE(
        blocked.upload.contains(QStringLiteral("阻塞HTTP状态=403"))
    );

    upload.state = UploadState::Stopped;
    upload.stop_requested = true;
    upload.finished = true;

    snapshot.upload = upload;

    const auto stopped = makeStatusText(snapshot, false);

    EXPECT_TRUE(
        stopped.upload.contains(QStringLiteral("STOPPED"))
    );

    EXPECT_TRUE(
        stopped.upload.contains(QStringLiteral("已结束=是"))
    );

    EXPECT_TRUE(
        stopped.upload.contains(QStringLiteral("阻塞标志=是"))
    );

    EXPECT_TRUE(
        stopped.upload.contains(QStringLiteral("阻塞HTTP状态=403"))
    );
}

TEST(StatusTextTest, FailedUploadRemainsVisibleAfterFinish)
{
    auto snapshot = sampleSnapshot();

    UploadObservation upload;

    upload.state = UploadState::Failed;
    upload.failed = true;
    upload.finished = true;

    snapshot.upload = upload;

    const auto text = makeStatusText(snapshot, true);

    EXPECT_TRUE(
        text.upload.contains(QStringLiteral("FAILED"))
    );

    EXPECT_TRUE(
        text.upload.contains(QStringLiteral("已结束=是"))
    );

    EXPECT_TRUE(
        text.upload.contains(QStringLiteral("失败=是"))
    );
}

TEST(StatusTextTest, RecordingZeroAndUnknownRemainDistinct)
{
    auto snapshot = sampleSnapshot();

    snapshot.recording = activeRecordingObservation();

    const auto unknownFiles = makeStatusText(snapshot, true);

    EXPECT_TRUE(
        unknownFiles.recording.contains(
            QStringLiteral("保留已关闭文件=未知")
        )
    );

    snapshot.recording->retained_closed_files = 0;

    const auto zeroFiles = makeStatusText(snapshot, true);

    EXPECT_TRUE(
        zeroFiles.recording.contains(
            QStringLiteral("保留已关闭文件=0")
        )
    );

    EXPECT_TRUE(
        zeroFiles.recording.contains(
            QStringLiteral("收到字节=0 B")
        )
    );

    EXPECT_TRUE(
        zeroFiles.recording.contains(
            QStringLiteral("活动文件=0")
        )
    );
}

TEST(StatusTextTest, DegradedRecordingKeepsPartialWriteAndError)
{
    auto snapshot = sampleSnapshot();

    auto recording = activeRecordingObservation();

    recording.state = RecordingState::Degraded;

    recording.connections_seen = 1;

    recording.bytes_received = 100;
    recording.bytes_written = 40;
    recording.bytes_skipped = 60;

    recording.opened_files = 1;
    recording.closed_files = 1;

    recording.session_errors = 1;
    recording.last_error_stage = RecordingErrorStage::Session;
    recording.last_error_code = 5;
    recording.last_error_connection = 1;
    recording.last_session_operation = RecordingOperation::Write;

    snapshot.recording = recording;

    const auto text = makeStatusText(snapshot, true);

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("DEGRADED"))
    );

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("写入字节=40 B"))
    );

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("跳过字节=60 B"))
    );

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("阶段=SESSION"))
    );

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("errno=5"))
    );

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("会话操作=WRITE"))
    );
}

TEST(StatusTextTest, RecordingFailureDoesNotChangeProcessState)
{
    auto snapshot = sampleSnapshot();

    snapshot.process_state = ProcessState::Running;

    auto recording = activeRecordingObservation();

    recording.state = RecordingState::Failed;
    recording.operational = false;

    recording.startup_errors = 1;
    recording.last_error_stage = RecordingErrorStage::Startup;
    recording.last_error_code = 13;

    snapshot.recording = recording;

    const auto text = makeStatusText(snapshot, true);

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("FAILED"))
    );

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("阶段=STARTUP"))
    );

    EXPECT_TRUE(
        text.runtime.contains(QStringLiteral("RUNNING"))
    );

    EXPECT_EQ(snapshot.process_state, ProcessState::Running);
}

TEST(StatusTextTest, StoppedRecordingRetainsHistoricalErrors)
{
    auto snapshot = sampleSnapshot();

    auto recording = activeRecordingObservation();

    recording.state = RecordingState::Stopped;
    recording.operational = false;
    recording.finished = true;

    recording.connections_seen = 2;
    recording.session_errors = 2;

    recording.last_error_stage = RecordingErrorStage::Session;
    recording.last_error_code = 5;
    recording.last_error_connection = 2;
    recording.last_session_operation = RecordingOperation::Write;

    snapshot.recording = recording;

    const auto text = makeStatusText(snapshot, false);

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("STOPPED"))
    );

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("SESSION=2"))
    );

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("errno=5"))
    );

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("会话操作=WRITE"))
    );
}

TEST(StatusTextTest, RecordingPreservesUint64AndSignedErrorCode)
{
    auto snapshot = sampleSnapshot();

    const auto maximum =
        std::numeric_limits<std::uint64_t>::max();

    auto recording = activeRecordingObservation();

    recording.state = RecordingState::Degraded;

    recording.connections_seen = maximum;

    recording.bytes_received = maximum;
    recording.bytes_written = maximum;
    recording.bytes_skipped = 0;

    recording.opened_files = 1;
    recording.closed_files = 1;

    // 保留与删除数量可涉及以前运行留下的文件。
    recording.retained_closed_files = maximum;
    recording.files_deleted = maximum;

    recording.session_errors = 1;
    recording.last_error_stage = RecordingErrorStage::Session;
    recording.last_error_code = -7;
    recording.last_error_connection = maximum;
    recording.last_session_operation = RecordingOperation::Write;

    snapshot.recording = recording;

    const auto text = makeStatusText(snapshot, true);

    const QString expected =
        QStringLiteral("18446744073709551615");

    EXPECT_TRUE(
        text.recording.contains(
            QStringLiteral("收到字节=")
            + expected
            + QStringLiteral(" B")
        )
    );

    EXPECT_TRUE(
        text.recording.contains(
            QStringLiteral("保留已关闭文件=")
            + expected
        )
    );

    EXPECT_TRUE(
        text.recording.contains(
            QStringLiteral("已删除文件=")
            + expected
        )
    );

    EXPECT_TRUE(
        text.recording.contains(QStringLiteral("errno=-7"))
    );
}

} // namespace