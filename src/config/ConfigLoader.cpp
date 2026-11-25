#include "edge/config/ConfigLoader.h"

#include <fstream>
#include <optional>
#include <string>
#include <system_error>
#include <utility>

namespace edge {

namespace {

namespace fs = std::filesystem;

// value 必须通过引用传入，解析结果才能写回 config 中的成员。
std::optional<ConfigFileError> resolveConfigPath(
    fs::path& value,
    const fs::path& base_directory,
    const char* field
)
{
    const auto combined = value.is_absolute()
        ? value
        : base_directory / value;

    std::error_code error;
    auto resolved = fs::weakly_canonical(combined, error);

    if (error) {
        return ConfigFileError{
            ConfigFileCode::PathResolutionFailed,
            field,
            "无法解析配置路径：" + error.message()
        };
    }

    if (!resolved.is_absolute()) {
        return ConfigFileError{
            ConfigFileCode::PathResolutionFailed,
            field,
            "配置路径未能转换为绝对路径。"
        };
    }

    // 必须写回，不能只计算 resolved 后直接返回。
    value = std::move(resolved);

    return std::nullopt;
}

} // namespace

ConfigLoadResult loadAgentConfig(
    const std::filesystem::path& config_file,
    const ConfigOverrides& overrides
)
{
    if (config_file.empty()
        || config_file.native().find('\0') != std::string::npos) {
        return ConfigFileError{
            ConfigFileCode::InvalidPath,
            "config_file",
            "配置文件路径不能为空，且不能包含空字符。"
        };
    }

    std::error_code error;

    // 获取实际配置文件的绝对路径，同时解析符号链接。
    const auto source_file = fs::canonical(config_file, error);

    if (error) {
        return ConfigFileError{
            ConfigFileCode::PathResolutionFailed,
            "config_file",
            "无法解析配置文件路径：" + error.message()
        };
    }

    const bool regular_file =
        fs::is_regular_file(source_file, error);

    if (error) {
        return ConfigFileError{
            ConfigFileCode::PathResolutionFailed,
            "config_file",
            "无法检查配置文件类型：" + error.message()
        };
    }

    if (!regular_file) {
        return ConfigFileError{
            ConfigFileCode::NotRegularFile,
            "config_file",
            "配置路径必须指向普通文件。"
        };
    }

    std::ifstream input(source_file, std::ios::binary);

    if (!input.is_open()) {
        return ConfigFileError{
            ConfigFileCode::OpenFailed,
            "config_file",
            "无法打开配置文件。"
        };
    }

    std::string text;
    char block[4096];

    while (input) {
        input.read(
            block,
            static_cast<std::streamsize>(sizeof(block))
        );

        text.append(
            block,
            static_cast<std::size_t>(input.gcount())
        );
    }

    if (input.bad() || !input.eof()) {
        return ConfigFileError{
            ConfigFileCode::ReadFailed,
            "config_file",
            "读取配置文件时发生错误。"
        };
    }

    auto parsed = parseAgentConfigYaml(text);

    if (const auto* yaml_error =
            std::get_if<ConfigYamlError>(&parsed)) {
        return *yaml_error;
    }

    auto config = std::get<AgentConfig>(std::move(parsed));

    // 第一阶段：应用 CLI 覆盖。
    if (overrides.port.has_value()) {
        config.tcp.port = *overrides.port;
    }

    if (overrides.receiver_url.has_value()) {
        config.upload.url = *overrides.receiver_url;
    }

    if (overrides.data_dir.has_value()) {
        const auto& directory = *overrides.data_dir;

        if (directory.empty()
            || directory.native().find('\0') != std::string::npos) {
            return ConfigFileError{
                ConfigFileCode::InvalidPath,
                "data_dir",
                "命令行数据目录不能为空，且不能包含空字符。"
            };
        }

        std::error_code path_error;

        // CLI 相对目录先以当前工作目录为基准转成绝对路径。
        // 此时仍可能包含 ../，后面还需要统一规范化。
        const auto absolute_directory =
            fs::absolute(directory, path_error);

        if (path_error) {
            return ConfigFileError{
                ConfigFileCode::PathResolutionFailed,
                "data_dir",
                "无法解析命令行数据目录：" + path_error.message()
            };
        }

        config.storage.path = absolute_directory / "outbox.db";
        config.recording.directory = absolute_directory / "raw";
        config.status.file = absolute_directory / "status.json";
    }

    // 第二阶段：检查覆盖后的原始配置值。
    // 空路径等错误必须在路径拼接之前发现。
    if (const auto validation_error = validateAgentConfig(config)) {
        return *validation_error;
    }

    // 第三阶段：统一解析所有输出路径。
    const auto base_directory = source_file.parent_path();

    struct PathEntry {
        fs::path* value;
        const char* field;
    };

    const PathEntry paths[] = {
        {&config.storage.path, "storage.path"},
        {&config.recording.directory, "recording.directory"},
        {&config.status.file, "status.file"}
    };

    for (const auto& entry : paths) {
        // 解引用成员指针，辅助函数通过引用修改实际配置成员。
        if (const auto path_error = resolveConfigPath(
                *entry.value,
                base_directory,
                entry.field)) {
            return *path_error;
        }
    }

    // 第四阶段：再次检查解析后的配置。
    if (const auto validation_error = validateAgentConfig(config)) {
        return *validation_error;
    }

    // 第五阶段：使用解析后的路径检查冲突。
    if (config.storage.path == source_file) {
        return ConfigFileError{
            ConfigFileCode::ConflictingPaths,
            "storage.path",
            "数据库文件不能与配置文件使用相同路径。"
        };
    }

    if (config.status.file == source_file
        || config.status.file == config.storage.path) {
        return ConfigFileError{
            ConfigFileCode::ConflictingPaths,
            "status.file",
            "状态文件不能与配置文件或数据库文件使用相同路径。"
        };
    }

    // 成功返回必须放在路径解析和冲突检查之后。
    return LoadedAgentConfig{
        source_file,
        std::move(config)
    };
}

} // namespace edge