#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <filesystem>

#include "edge/config/AgentConfig.h"
#include "edge/config/ConfigValidator.h"

namespace {

using Code = edge::ConfigValidationCode;

void expectConfigError(
    const edge::AgentConfig& config,
    Code expected_code,
    const char* expected_field
)
{
    const auto error = edge::validateAgentConfig(config);

    ASSERT_TRUE(error.has_value());

    EXPECT_EQ(error->code, expected_code);
    EXPECT_EQ(error->field, expected_field);
    EXPECT_FALSE(error->message.empty());
}

TEST(ConfigValidator, DefaultConfigIsValid)
{
    const edge::AgentConfig config;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
}

TEST(ConfigValidator, RejectsUnsupportedVersion)
{
    const std::int32_t versions[] = {0, -1, 2};

    for (const auto version : versions) {
        SCOPED_TRACE(version);

        edge::AgentConfig config;
        config.version = version;

        expectConfigError(
            config,
            Code::UnsupportedVersion,
            "version"
        );
    }
}

TEST(ConfigValidator, RejectsEmptyAgentId)
{
    edge::AgentConfig config;
    config.agent.id.clear();

    expectConfigError(
        config,
        Code::EmptyAgentId,
        "agent.id"
    );
}

TEST(ConfigValidator, ChecksLogLevel)
{
    const char* valid_levels[] = {
        "trace", "debug", "info", "warn",
        "error", "critical", "off"
    };

    for (const char* level : valid_levels) {
        SCOPED_TRACE(level);

        edge::AgentConfig config;
        config.agent.log_level = level;

        EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
    }

    const char* invalid_levels[] = {
        "", "INFO", "verbose"
    };

    for (const char* level : invalid_levels) {
        SCOPED_TRACE(level);

        edge::AgentConfig config;
        config.agent.log_level = level;

        expectConfigError(
            config,
            Code::InvalidLogLevel,
            "agent.log_level"
        );
    }
}

TEST(ConfigValidator, ChecksBindAddress)
{
    const char* valid_addresses[] = {
        "127.0.0.1",
        "0.0.0.0",
        "192.168.1.10"
    };

    for (const char* address : valid_addresses) {
        SCOPED_TRACE(address);

        edge::AgentConfig config;
        config.tcp.bind = address;

        EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
    }

    const std::string invalid_addresses[] = {
        "",
        "localhost",
        "999.1.1.1",
        "127.0.0",
        "::1",
        std::string("127.0.0.1") + '\0' + "extra"
    };

    for (const auto& address : invalid_addresses) {
        SCOPED_TRACE(address);

        edge::AgentConfig config;
        config.tcp.bind = address;

        expectConfigError(
            config,
            Code::InvalidBindAddress,
            "tcp.bind"
        );
    }
}

TEST(ConfigValidator, ChecksPortRange)
{
    const std::int32_t valid_ports[] = {1, 65535};

    for (const auto port : valid_ports) {
        edge::AgentConfig config;
        config.tcp.port = port;

        EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
    }

    const std::int32_t invalid_ports[] = {-1, 0, 65536};

    for (const auto port : invalid_ports) {
        SCOPED_TRACE(port);

        edge::AgentConfig config;
        config.tcp.port = port;

        expectConfigError(config, Code::InvalidPort, "tcp.port");
    }
}

TEST(ConfigValidator, ChecksConnectionLimit)
{
    const std::int32_t valid_limits[] = {1, 8};

    for (const auto limit : valid_limits) {
        edge::AgentConfig config;
        config.tcp.max_connections = limit;

        EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
    }

    const std::int32_t invalid_limits[] = {-1, 0, 9};

    for (const auto limit : invalid_limits) {
        SCOPED_TRACE(limit);

        edge::AgentConfig config;
        config.tcp.max_connections = limit;

        expectConfigError(
            config,
            Code::InvalidConnectionLimit,
            "tcp.max_connections"
        );
    }
}

TEST(ConfigValidator, ChecksPayloadLimit)
{
    const std::int32_t valid_limits[] = {1, 8192};

    for (const auto limit : valid_limits) {
        edge::AgentConfig config;
        config.tcp.max_payload_bytes = limit;

        EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
    }

    const std::int32_t invalid_limits[] = {-1, 0, 8193};

    for (const auto limit : invalid_limits) {
        SCOPED_TRACE(limit);

        edge::AgentConfig config;
        config.tcp.max_payload_bytes = limit;

        expectConfigError(
            config,
            Code::InvalidPayloadLimit,
            "tcp.max_payload_bytes"
        );
    }
}

TEST(ConfigValidator, ChecksTimeouts)
{
    const std::int32_t invalid_times[] = {0, -1};

    for (const auto timeout : invalid_times) {
        SCOPED_TRACE(timeout);

        edge::AgentConfig config;
        config.tcp.partial_timeout_ms = timeout;

        expectConfigError(
            config,
            Code::InvalidPartialTimeout,
            "tcp.partial_timeout_ms"
        );

        // 恢复默认配置，再单独检查另一个超时字段。
        config = edge::AgentConfig{};
        config.tcp.silent_after_ms = timeout;

        expectConfigError(
            config,
            Code::InvalidSilentTimeout,
            "tcp.silent_after_ms"
        );
    }
}

TEST(ConfigValidator, RejectsInvalidStoragePath)
{
    const std::filesystem::path invalid_paths[] = {
        "",
        ".",
        "..",
        "/",
        "run/",
        std::filesystem::path(
            std::string("outbox.db") + '\0' + "extra"
        )
    };

    for (const auto& path : invalid_paths) {
        SCOPED_TRACE(path.string());

        edge::AgentConfig config;
        config.storage.path = path;

        expectConfigError(
            config,
            Code::InvalidStoragePath,
            "storage.path"
        );
    }
}

TEST(ConfigValidator, RejectsNonPositiveStorageCapacities)
{
    const std::int64_t invalid_limits[] = {0, -1};

    for (const auto limit : invalid_limits) {
        SCOPED_TRACE(limit);

        edge::AgentConfig config;
        config.storage.max_pending_records = limit;

        expectConfigError(
            config,
            Code::InvalidPendingLimit,
            "storage.max_pending_records"
        );

        // 恢复默认配置，再单独检查总容量。
        config = edge::AgentConfig{};
        config.storage.max_total_records = limit;

        expectConfigError(
            config,
            Code::InvalidTotalLimit,
            "storage.max_total_records"
        );
    }
}

TEST(ConfigValidator, ChecksStorageCapacityRelations)
{
    edge::AgentConfig config;

    config.storage.max_total_records = 100;
    config.storage.max_pending_records = 100;
    config.storage.acked_keep_max_records = 100;

    // 告警阈值不能超过新的待处理容量
    config.status.pending_warn_records = 100;

    // 两个独立上限都可以等于总上限。
    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());

    config.storage.max_pending_records = 101;

    expectConfigError(
        config,
        Code::InvalidCapacityRelation,
        "storage.max_pending_records"
    );

    config.storage.max_pending_records = 100;
    config.storage.acked_keep_max_records = 101;

    expectConfigError(
        config,
        Code::InvalidCapacityRelation,
        "storage.acked_keep_max_records"
    );
}

TEST(ConfigValidator, ChecksStorageRetention)
{
    const std::int32_t invalid_days[] = {0, -1};

    for (const auto days : invalid_days) {
        SCOPED_TRACE(days);

        edge::AgentConfig config;
        config.storage.acked_keep_days = days;

        expectConfigError(
            config,
            Code::InvalidRetentionDays,
            "storage.acked_keep_days"
        );
    }

    const std::int64_t invalid_limits[] = {0, -1};

    for (const auto limit : invalid_limits) {
        SCOPED_TRACE(limit);

        edge::AgentConfig config;
        config.storage.acked_keep_max_records = limit;

        expectConfigError(
            config,
            Code::InvalidRetentionLimit,
            "storage.acked_keep_max_records"
        );
    }
}

TEST(ConfigValidator, AcceptsStorageFilePaths)
{
    const std::filesystem::path valid_paths[] = {
        "outbox.db",
        "../run/agent/outbox.db",
        "/tmp/edge-relay/outbox.db",
        "run/数据/outbox.db"
    };

    for (const auto& path : valid_paths) {
        SCOPED_TRACE(path.string());

        edge::AgentConfig config;
        config.storage.path = path;

        // 这里只检查路径形式，不要求测试机器上已存在这些目录。
        EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
    }
}

TEST(ConfigValidator, AcceptsHttpUploadUrls)
{
    const char* urls[] = {
        "http://127.0.0.1:18080/v1/events/batch",
        "https://example.com/v1/events/batch",
        "http://localhost:18080/events?source=demo",
        "http://[::1]:18080/v1/events/batch"
    };

    for (const char* url : urls) {
        SCOPED_TRACE(url);

        edge::AgentConfig config;
        config.upload.url = url;

        EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
    }
}

TEST(ConfigValidator, RejectsInvalidUploadUrls)
{
    const std::string urls[] = {
        "",
        "127.0.0.1:18080/events",
        "ftp://127.0.0.1/events",
        "https://",
        "http://127.0.0.1:70000/events",
        "http://127.0.0.1/a b",
        "http://127.0.0.1/\n",
        std::string("http://127.0.0.1/events") + '\0' + "extra"
    };

    for (const auto& url : urls) {
        SCOPED_TRACE(url);

        edge::AgentConfig config;
        config.upload.url = url;

        expectConfigError(
            config,
            Code::InvalidUploadUrl,
            "upload.url"
        );
    }
}

TEST(ConfigValidator, ChecksUploadBatchSize)
{
    const std::int32_t valid_sizes[] = {1, 100};

    for (const auto size : valid_sizes) {
        edge::AgentConfig config;
        config.upload.batch_size = size;

        EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
    }

    const std::int32_t invalid_sizes[] = {-1, 0, 101};

    for (const auto size : invalid_sizes) {
        SCOPED_TRACE(size);

        edge::AgentConfig config;
        config.upload.batch_size = size;

        expectConfigError(
            config,
            Code::InvalidBatchSize,
            "upload.batch_size"
        );
    }
}

TEST(ConfigValidator, ChecksUploadFlushInterval)
{
    const std::int32_t invalid_intervals[] = {0, -1};

    for (const auto interval : invalid_intervals) {
        edge::AgentConfig config;
        config.upload.flush_interval_ms = interval;

        expectConfigError(
            config,
            Code::InvalidFlushInterval,
            "upload.flush_interval_ms"
        );
    }

    edge::AgentConfig config;
    config.upload.flush_interval_ms = 1;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
}

TEST(ConfigValidator, ChecksUploadTimeoutRanges)
{
    const std::int32_t invalid_times[] = {0, -1};

    for (const auto timeout : invalid_times) {
        edge::AgentConfig config;
        config.upload.connect_timeout_ms = timeout;

        expectConfigError(
            config,
            Code::InvalidConnectTimeout,
            "upload.connect_timeout_ms"
        );

        config = edge::AgentConfig{};
        config.upload.request_timeout_ms = timeout;

        expectConfigError(
            config,
            Code::InvalidRequestTimeout,
            "upload.request_timeout_ms"
        );
    }

    edge::AgentConfig config;
    config.upload.request_timeout_ms = 8000;

    expectConfigError(
        config,
        Code::InvalidRequestTimeout,
        "upload.request_timeout_ms"
    );

    config.upload.request_timeout_ms = 7999;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
}

TEST(ConfigValidator, ChecksUploadTimeoutRelation)
{
    edge::AgentConfig config;

    config.upload.connect_timeout_ms = 1000;
    config.upload.request_timeout_ms = 1000;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());

    config.upload.connect_timeout_ms = 1001;

    expectConfigError(
        config,
        Code::InvalidTimeoutRelation,
        "upload.connect_timeout_ms"
    );
}

TEST(ConfigValidator, ChecksRetryIntervals)
{
    const std::int32_t invalid_intervals[] = {0, -1};

    for (const auto interval : invalid_intervals) {
        edge::AgentConfig config;
        config.upload.retry_initial_ms = interval;

        expectConfigError(
            config,
            Code::InvalidRetryInitial,
            "upload.retry_initial_ms"
        );

        config = edge::AgentConfig{};
        config.upload.retry_max_ms = interval;

        expectConfigError(
            config,
            Code::InvalidRetryMaximum,
            "upload.retry_max_ms"
        );
    }

    edge::AgentConfig config;
    config.upload.retry_initial_ms = 1;
    config.upload.retry_max_ms = 1;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
}

TEST(ConfigValidator, ChecksRetryRelation)
{
    edge::AgentConfig config;

    config.upload.retry_initial_ms = 10000;
    config.upload.retry_max_ms = 10000;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());

    config.upload.retry_initial_ms = 10001;

    expectConfigError(
        config,
        Code::InvalidRetryRelation,
        "upload.retry_initial_ms"
    );
}

TEST(ConfigValidator, ChecksFailureWarnAttempts)
{
    const std::int32_t invalid_attempts[] = {0, -1};

    for (const auto attempts : invalid_attempts) {
        edge::AgentConfig config;
        config.upload.failure_warn_attempts = attempts;

        expectConfigError(
            config,
            Code::InvalidFailureWarnAttempts,
            "upload.failure_warn_attempts"
        );
    }

    edge::AgentConfig config;
    config.upload.failure_warn_attempts = 1;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
}

TEST(ConfigValidator, ChecksRecordingDirectory)
{
    const std::filesystem::path invalid_paths[] = {
        "",
        std::filesystem::path(
            std::string("raw") + '\0' + "extra"
        )
    };

    for (const auto& path : invalid_paths) {
        SCOPED_TRACE(path.string());

        edge::AgentConfig config;
        config.recording.directory = path;

        expectConfigError(
            config,
            Code::InvalidRecordingDirectory,
            "recording.directory"
        );
    }

    const std::filesystem::path valid_paths[] = {
        ".",
        "../run/agent/raw",
        "run/raw/",
        "/tmp/edge-relay/raw",
        "run/原始录制"
    };

    for (const auto& path : valid_paths) {
        SCOPED_TRACE(path.string());

        edge::AgentConfig config;
        config.recording.directory = path;

        EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
    }
}

TEST(ConfigValidator, ChecksRecordingFileSize)
{
    const std::int64_t invalid_sizes[] = {0, -1};

    for (const auto size : invalid_sizes) {
        edge::AgentConfig config;
        config.recording.max_file_bytes = size;

        // 即使 enabled 默认为 false，也检查参数是否合法。
        expectConfigError(
            config,
            Code::InvalidRecordingFileSize,
            "recording.max_file_bytes"
        );
    }

    edge::AgentConfig config;
    config.recording.max_file_bytes = 1;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
}

TEST(ConfigValidator, ChecksRecordingRetention)
{
    edge::AgentConfig config;
    config.recording.keep_closed_files = -1;

    expectConfigError(
        config,
        Code::InvalidRecordingRetention,
        "recording.keep_closed_files"
    );

    // 允许不保留已关闭文件。
    config.recording.keep_closed_files = 0;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
}

TEST(ConfigValidator, ChecksStatusInterval)
{
    const std::int32_t invalid_intervals[] = {0, -1};

    for (const auto interval : invalid_intervals) {
        edge::AgentConfig config;
        config.status.interval_ms = interval;

        expectConfigError(
            config,
            Code::InvalidStatusInterval,
            "status.interval_ms"
        );
    }

    edge::AgentConfig config;
    config.status.interval_ms = 1;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
}

TEST(ConfigValidator, ChecksStatusFilePaths)
{
    const std::filesystem::path invalid_paths[] = {
        "",
        ".",
        "..",
        "/",
        "run/",
        std::filesystem::path(
            std::string("status.json") + '\0' + "extra"
        )
    };

    for (const auto& path : invalid_paths) {
        SCOPED_TRACE(path.string());

        edge::AgentConfig config;
        config.status.file = path;

        expectConfigError(
            config,
            Code::InvalidStatusFile,
            "status.file"
        );
    }

    edge::AgentConfig config;
    config.status.file = "status.json";

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
}

TEST(ConfigValidator, ChecksPendingWarningThreshold)
{
    const std::int64_t invalid_thresholds[] = {0, -1};

    for (const auto threshold : invalid_thresholds) {
        edge::AgentConfig config;
        config.status.pending_warn_records = threshold;

        expectConfigError(
            config,
            Code::InvalidPendingWarnThreshold,
            "status.pending_warn_records"
        );
    }

    edge::AgentConfig config;

    // 等于容量上限允许，超过容量上限则无效。
    config.status.pending_warn_records = 10000;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());

    config.status.pending_warn_records = 10001;

    expectConfigError(
        config,
        Code::InvalidPendingWarnThreshold,
        "status.pending_warn_records"
    );
}

TEST(ConfigValidator, ChecksDiskWarningThreshold)
{
    const std::int64_t invalid_thresholds[] = {0, -1};

    for (const auto threshold : invalid_thresholds) {
        edge::AgentConfig config;
        config.status.disk_free_warn_mb = threshold;

        expectConfigError(
            config,
            Code::InvalidDiskWarnThreshold,
            "status.disk_free_warn_mb"
        );
    }

    edge::AgentConfig config;
    config.status.disk_free_warn_mb = 1;

    EXPECT_FALSE(edge::validateAgentConfig(config).has_value());
}

} // namespace