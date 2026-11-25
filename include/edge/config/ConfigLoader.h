#pragma once

#include <filesystem>
#include <string>
#include <variant>
#include <cstdint>
#include <optional>

#include "edge/config/ConfigValidator.h"
#include "edge/config/ConfigYaml.h"

namespace edge {

enum class ConfigFileCode {
    InvalidPath,
    PathResolutionFailed,
    NotRegularFile,
    OpenFailed,
    ReadFailed,
    ConflictingPaths
};

struct ConfigFileError {
    ConfigFileCode code;

    // config_file 或具体配置项，例如 storage.path。
    std::string field;
    std::string message;
};

struct ConfigOverrides {
    // optional 表示是否提供覆盖，而不是参数值是否非零。
    std::optional<std::int32_t> port;
    std::optional<std::string> receiver_url;
    std::optional<std::filesystem::path> data_dir;
};

struct LoadedAgentConfig {
    // 配置文件实际目标的绝对路径，便于后续展示配置来源。
    std::filesystem::path source_file;

    // 其中的存储、录制和状态路径已经解析为绝对路径。
    AgentConfig config;
};

// 保留不同层次的错误类型，调用方可以分别处理。
using ConfigLoadResult = std::variant<
    LoadedAgentConfig,
    ConfigFileError,
    ConfigYamlError,
    ConfigValidationError
>;

ConfigLoadResult loadAgentConfig(
    const std::filesystem::path& config_file,
    const ConfigOverrides& overrides = {}
);

} // namespace edge