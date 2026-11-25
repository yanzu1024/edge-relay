#include "edge/config/ConfigValidator.h"

#include <string_view>
#include <arpa/inet.h>
#include <curl/curl.h>
#include <memory>
#include <new>
#include <filesystem>

#include "edge/protocol/FrameFormat.h"
#include "edge/net/CurlRuntime.h"

namespace edge {

namespace {

// 检查路径文本本身，适用于目录和文件。
bool isValidPathText(const std::filesystem::path& path){
    return !path.empty()
        && path.native().find('\0') == std::string::npos;
}

// 文件路径还必须包含正常文件名。
// 目录路径可以是 "." 或以 "/" 结尾，因此单独处理。
bool isValidFilePath(const std::filesystem::path& path){
    if (!isValidPathText(path)) {
        return false;
    }

    const auto filename = path.filename();

    return !filename.empty()
        && filename != "."
        && filename != "..";
}

bool isSupportedLogLevel(std::string_view level){
    constexpr const char* levels[] = {
        "trace",
        "debug",
        "info",
        "warn",
        "error",
        "critical",
        "off"
    };

    for (const char* supported : levels) {
        if (level == supported) {
            return true;
        }
    }

    return false;
}

bool isValidIpv4Address(const std::string& text){
    // inet_pton 接收 C 风格字符串，因此提前拒绝内嵌空字符，
    // 防止只检查到字符串中途。
    if (text.empty()
        || text.find('\0') != std::string::npos) {
        return false;
    }

    in_addr address{};

    return ::inet_pton(
        AF_INET,
        text.c_str(),
        &address
    ) == 1;
}

bool isValidHttpUrl(const std::string& url){
    if (url.empty()) {
        return false;
    }

    // 拒绝空格、控制字符和内嵌空字符。
    // 地址中的空格应使用 %20 等 URL 编码。
    for (unsigned char character : url) {
        if (character <= 0x20U || character == 0x7FU) {
            return false;
        }
    }

    // 要求显式提供协议，避免依靠库猜测。
    const auto separator = url.find("://");

    if (separator == std::string::npos) {
        return false;
    }

    auto scheme = url.substr(0, separator);

    // URL 协议名称不区分 ASCII 大小写。
    for (char& character : scheme) {
        if (character >= 'A' && character <= 'Z') {
            character = static_cast<char>(
                character - 'A' + 'a'
            );
        }
    }

    if (scheme != "http" && scheme != "https") {
        return false;
    }

    // 必须在创建 URL 句柄之前完成全局初始化。
    // 初始化失败属于运行环境错误，异常继续向上传递。
    ensureCurlInitialized();

    // URL 句柄由当前作用域独占管理。
    std::unique_ptr<CURLU, decltype(&curl_url_cleanup)> handle(
        curl_url(),
        &curl_url_cleanup
    );

    if (!handle) {
        throw std::bad_alloc{};
    }

    // 进一步检查主机、端口和其他 URL 结构。
    const auto result = curl_url_set(
        handle.get(),
        CURLUPART_URL,
        url.c_str(),
        0
    );

    // 资源错误不能误报为用户 URL 配置错误。
    if (result == CURLUE_OUT_OF_MEMORY) {
        throw std::bad_alloc{};
    }

    return result == CURLUE_OK;
}

} // namespace

std::optional<ConfigValidationError> validateAgentConfig(
    const AgentConfig& config
)
{
    if (config.version != 1) {
        return ConfigValidationError{
            ConfigValidationCode::UnsupportedVersion,
            "version",
            "仅支持配置格式版本 1。"
        };
    }

    if (config.agent.id.empty()) {
        return ConfigValidationError{
            ConfigValidationCode::EmptyAgentId,
            "agent.id",
            "Agent 标识不能为空。"
        };
    }

    if (!isSupportedLogLevel(config.agent.log_level)) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidLogLevel,
            "agent.log_level",
            "日志级别必须为 trace、debug、info、warn、error、critical 或 off。"
        };
    }

    if (!isValidIpv4Address(config.tcp.bind)) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidBindAddress,
            "tcp.bind",
            "监听地址必须是合法的 IPv4 字面量。"
        };
    }

    if (config.tcp.port < 1 || config.tcp.port > 65535) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidPort,
            "tcp.port",
            "监听端口必须在 1–65535 范围内。"
        };
    }

    if (config.tcp.max_connections < 1
        || config.tcp.max_connections > kMaxTcpConnections) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidConnectionLimit,
            "tcp.max_connections",
            "首版最大连接数必须在 1–8 范围内。"
        };
    }

    if (config.tcp.max_payload_bytes < 1
        || config.tcp.max_payload_bytes
            > static_cast<std::int32_t>(kMaxFramePayloadBytes)) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidPayloadLimit,
            "tcp.max_payload_bytes",
            "载荷上限必须在 1–8192 字节范围内。"
        };
    }

    if (config.tcp.partial_timeout_ms <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidPartialTimeout,
            "tcp.partial_timeout_ms",
            "半帧超时必须大于 0 毫秒。"
        };
    }

    if (config.tcp.silent_after_ms <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidSilentTimeout,
            "tcp.silent_after_ms",
            "静默判定时间必须大于 0 毫秒。"
        };
    }

    if (const auto error = validateStorageConfig(config.storage)) {
        return error;
    }

    if (const auto error = validateUploadConfig(config.upload)){
        return error;
    }

    if (const auto error = validateRecordingConfig(config.recording)){
        return error;
    }

    return validateStatusConfig(config.status, config.storage);
}

std::optional<ConfigValidationError> validateStorageConfig(
    const StorageConfig& config
){
    if (!isValidFilePath(config.path)) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidStoragePath,
            "storage.path",
            "数据库路径必须包含有效文件名, 且不能包含空字符"
        };
    }

    if (config.max_pending_records <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidPendingLimit,
            "storage.max_pending_records",
            "待处理记录上限必须大于 0。"
        };
    }

    if (config.max_total_records <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidTotalLimit,
            "storage.max_total_records",
            "总记录上限必须大于 0。"
        };
    }

    if (config.max_pending_records > config.max_total_records) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidCapacityRelation,
            "storage.max_pending_records",
            "待处理记录上限不能超过总记录上限。"
        };
    }

    if (config.acked_keep_days <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidRetentionDays,
            "storage.acked_keep_days",
            "ACKED 保留天数必须大于 0。"
        };
    }

    if (config.acked_keep_max_records <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidRetentionLimit,
            "storage.acked_keep_max_records",
            "ACKED 保留记录上限必须大于 0。"
        };
    }

    if (config.acked_keep_max_records > config.max_total_records) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidCapacityRelation,
            "storage.acked_keep_max_records",
            "ACKED 保留记录上限不能超过总记录上限。"
        };
    }

    return std::nullopt;
}

std::optional<ConfigValidationError> validateUploadConfig(
    const UploadConfig& config
)
{
    if (!isValidHttpUrl(config.url)) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidUploadUrl,
            "upload.url",
            "上传地址必须是合法的 HTTP 或 HTTPS URL。"
        };
    }

    if (config.batch_size < 1 || config.batch_size > 100) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidBatchSize,
            "upload.batch_size",
            "上传批大小必须在 1–100 范围内。"
        };
    }

    if (config.flush_interval_ms <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidFlushInterval,
            "upload.flush_interval_ms",
            "最长聚合等待时间必须大于 0 毫秒。"
        };
    }

    if (config.connect_timeout_ms <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidConnectTimeout,
            "upload.connect_timeout_ms",
            "连接超时必须大于 0 毫秒。"
        };
    }

    if (config.request_timeout_ms <= 0
        || config.request_timeout_ms >= kShutdownBudgetMs) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidRequestTimeout,
            "upload.request_timeout_ms",
            "请求总超时必须大于 0 且小于 8000 毫秒。"
        };
    }

    if (config.connect_timeout_ms > config.request_timeout_ms) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidTimeoutRelation,
            "upload.connect_timeout_ms",
            "连接超时不能超过请求总超时。"
        };
    }

    if (config.retry_initial_ms <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidRetryInitial,
            "upload.retry_initial_ms",
            "初始重试间隔必须大于 0 毫秒。"
        };
    }

    if (config.retry_max_ms <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidRetryMaximum,
            "upload.retry_max_ms",
            "最大重试间隔必须大于 0 毫秒。"
        };
    }

    if (config.retry_initial_ms > config.retry_max_ms) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidRetryRelation,
            "upload.retry_initial_ms",
            "初始重试间隔不能超过最大重试间隔。"
        };
    }

    if (config.failure_warn_attempts <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidFailureWarnAttempts,
            "upload.failure_warn_attempts",
            "连续失败告警阈值必须大于 0。"
        };
    }

    return std::nullopt;
}

std::optional<ConfigValidationError> validateRecordingConfig(
    const RecordingConfig& config
)
{
    if (!isValidPathText(config.directory)) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidRecordingDirectory,
            "recording.directory",
            "录制目录不能为空，且不能包含空字符。"
        };
    }

    if (config.max_file_bytes <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidRecordingFileSize,
            "recording.max_file_bytes",
            "录制文件大小上限必须大于 0 字节。"
        };
    }

    if (config.keep_closed_files < 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidRecordingRetention,
            "recording.keep_closed_files",
            "已关闭录制文件的保留数量不能为负数。"
        };
    }

    return std::nullopt;
}

std::optional<ConfigValidationError> validateStatusConfig(
    const StatusConfig& config,
    const StorageConfig& storage
)
{
    if (config.interval_ms <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidStatusInterval,
            "status.interval_ms",
            "状态输出间隔必须大于 0 毫秒。"
        };
    }

    if (!isValidFilePath(config.file)) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidStatusFile,
            "status.file",
            "状态文件路径必须包含有效文件名，且不能包含空字符。"
        };
    }

    // 超过容量上限的告警阈值无法正常触发，应在启动时发现。
    if (config.pending_warn_records <= 0
        || config.pending_warn_records > storage.max_pending_records) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidPendingWarnThreshold,
            "status.pending_warn_records",
            "待处理告警阈值必须大于 0, 且不能超过待处理记录上限。"
        };
    }

    if (config.disk_free_warn_mb <= 0) {
        return ConfigValidationError{
            ConfigValidationCode::InvalidDiskWarnThreshold,
            "status.disk_free_warn_mb",
            "磁盘空间告警阈值必须大于 0"
        };
    }

    return std::nullopt;
}

} // namespace edge