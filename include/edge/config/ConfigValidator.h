#pragma once

#include <optional>
#include <string>

#include "edge/config/AgentConfig.h"

namespace edge {

enum class ConfigValidationCode {
    UnsupportedVersion,
    EmptyAgentId,
    InvalidLogLevel,
    InvalidBindAddress,
    InvalidPort,
    InvalidConnectionLimit,
    InvalidPayloadLimit,
    InvalidPartialTimeout,
    InvalidSilentTimeout,

    InvalidStoragePath,
    InvalidPendingLimit,
    InvalidTotalLimit,
    InvalidCapacityRelation,
    InvalidRetentionDays,
    InvalidRetentionLimit,

    InvalidUploadUrl,
    InvalidBatchSize,
    InvalidFlushInterval,
    InvalidConnectTimeout,
    InvalidRequestTimeout,
    InvalidTimeoutRelation,
    InvalidRetryInitial,
    InvalidRetryMaximum,
    InvalidRetryRelation,
    InvalidFailureWarnAttempts,

    InvalidRecordingDirectory,
    InvalidRecordingFileSize,
    InvalidRecordingRetention,
    InvalidStatusInterval,
    InvalidStatusFile,
    InvalidPendingWarnThreshold,
    InvalidDiskWarnThreshold

};

struct ConfigValidationError {
    ConfigValidationCode code;
    std::string field;
    std::string message;
};

// 校验存储分组，便于统一入口组合调用。
std::optional<ConfigValidationError> validateStorageConfig(
    const StorageConfig& config
);

std::optional<ConfigValidationError> validateUploadConfig(
    const UploadConfig& config
);

std::optional<ConfigValidationError> validateRecordingConfig(
    const RecordingConfig& config
);

// 状态告警阈值需要与存储容量一起检查。
std::optional<ConfigValidationError> validateStatusConfig(
    const StatusConfig& config,
    const StorageConfig& storage
);

// 校验完整配置，返回第一个错误。
std::optional<ConfigValidationError> validateAgentConfig(
    const AgentConfig& config
);

} // namespace edge