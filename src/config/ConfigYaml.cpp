#include "edge/config/ConfigYaml.h"

#include <charconv>
#include <filesystem>
#include <initializer_list>
#include <set>
#include <stdexcept>
#include <system_error>
#include <type_traits>
#include <utility>

#include <yaml-cpp/yaml.h>

namespace edge {

namespace {

// 内部异常用于立即结束本次解析。
// 公开函数会捕获它，并转换成 ConfigYamlError。
class YamlReadError : public std::runtime_error {
public:
    YamlReadError(
        ConfigYamlCode error_code,
        std::string error_field,
        const char* message
    )
        : std::runtime_error(message),
          code(error_code),
          field(std::move(error_field))
    {
    }

    ConfigYamlCode code;
    std::string field;
};

[[noreturn]] void fail(
    ConfigYamlCode code,
    std::string field,
    const char* message
)
{
    throw YamlReadError(code, std::move(field), message);
}

std::string fieldPath(
    std::string_view group,
    std::string_view key
)
{
    if (group.empty()) {
        return std::string(key);
    }

    return std::string(group) + "." + std::string(key);
}

// 每个映射都检查字段白名单和重复键。
void checkMap(
    const YAML::Node& node,
    std::string_view group,
    std::initializer_list<std::string_view> allowed
)
{
    if (!node.IsMap()) {
        fail(
            ConfigYamlCode::ExpectedMap,
            std::string(group),
            "配置节点必须为映射。"
        );
    }

    std::set<std::string> seen;

    for (const auto& entry : node) {
        if (!entry.first.IsScalar()) {
            fail(
                ConfigYamlCode::InvalidKey,
                std::string(group),
                "配置键必须为标量名称。"
            );
        }

        const auto key = entry.first.Scalar();
        const auto field = fieldPath(group, key);

        if (!seen.insert(key).second) {
            fail(
                ConfigYamlCode::DuplicateField,
                field,
                "配置中存在重复字段。"
            );
        }

        bool known = false;

        for (const auto expected : allowed) {
            if (std::string_view(key) == expected) {
                known = true;
                break;
            }
        }

        if (!known) {
            fail(
                ConfigYamlCode::UnknownField,
                field,
                "配置中存在未知字段。"
            );
        }
    }
}

// 直接读取到目标整数类型，避免先转换后发生截断。
// 接受十进制整数及可选正负号，拒绝小数、科学记数法等形式。
template <typename T>
T readInteger(
    std::string_view text,
    const std::string& field
)
{
    static_assert(std::is_integral_v<T>);

    if (!text.empty() && text.front() == '+') {
        text.remove_prefix(1);

        if (text.empty()
            || text.front() == '+'
            || text.front() == '-') {
            fail(
                ConfigYamlCode::InvalidScalar,
                field,
                "字段必须为十进制整数。"
            );
        }
    }

    if (text.empty()) {
        fail(
            ConfigYamlCode::InvalidScalar,
            field,
            "字段必须为十进制整数。"
        );
    }

    T value{};

    const auto result = std::from_chars(
        text.data(),
        text.data() + text.size(),
        value,
        10
    );

    if (result.ec == std::errc::result_out_of_range) {
        fail(
            ConfigYamlCode::IntegerOutOfRange,
            field,
            "整数超过目标类型可表示的范围。"
        );
    }

    if (result.ec != std::errc{}
        || result.ptr != text.data() + text.size()) {
        fail(
            ConfigYamlCode::InvalidScalar,
            field,
            "字段必须为十进制整数。"
        );
    }

    return value;
}

// 缺省字段保留 output 的原值；显式 null 或集合节点会报错。
template <typename T>
void readField(
    const YAML::Node& map,
    std::string_view group,
    const char* key,
    T& output
)
{
    const auto node = map[key];

    if (!node.IsDefined()) {
        return;
    }

    const auto field = fieldPath(group, key);

    if (!node.IsScalar()) {
        fail(
            ConfigYamlCode::InvalidFieldType,
            field,
            "配置字段必须为非 null 的标量。"
        );
    }

    const auto text = node.Scalar();

    if constexpr (std::is_same_v<T, std::string>) {
        output = text;
    } else if constexpr (
        std::is_same_v<T, std::filesystem::path>
    ) {
        output = std::filesystem::path(text);
    } else if constexpr (std::is_same_v<T, bool>) {
        if (text == "true") {
            output = true;
        } else if (text == "false") {
            output = false;
        } else {
            fail(
                ConfigYamlCode::InvalidScalar,
                field,
                "布尔值必须使用小写 true 或 false。"
            );
        }
    } else {
        static_assert(std::is_integral_v<T>);
        output = readInteger<T>(text, field);
    }
}

// 分组缺省时保留默认值；出现时先检查映射结构，再读取成员。
template <typename Function>
void readGroup(
    const YAML::Node& root,
    const char* name,
    std::initializer_list<std::string_view> allowed,
    const Function& apply
)
{
    const auto node = root[name];

    if (!node.IsDefined()) {
        return;
    }

    checkMap(node, name, allowed);
    apply(node);
}

} // namespace

ConfigYamlResult parseAgentConfigYaml(std::string_view text)
{
    try {
        // LoadAll 使我们能够明确拒绝多文档配置。
        const auto documents = YAML::LoadAll(std::string(text));

        if (documents.size() != 1) {
            fail(
                ConfigYamlCode::InvalidYaml,
                "",
                "配置文本必须包含且只能包含一份 YAML 文档。"
            );
        }

        const YAML::Node root = documents.front();

        checkMap(root, "", {
            "version", "agent", "tcp", "storage",
            "upload", "recording", "status"
        });

        if (!root["version"].IsDefined()) {
            fail(
                ConfigYamlCode::MissingField,
                "version",
                "必须显式提供配置格式版本。"
            );
        }

        AgentConfig config;

        readField(root, "", "version", config.version);

        readGroup(root, "agent", {"id", "log_level"},
            [&](const YAML::Node& node) {
                readField(node, "agent", "id", config.agent.id);
                readField(node, "agent", "log_level", config.agent.log_level);
            }
        );

        readGroup(root, "tcp", {
            "bind", "port", "max_connections", "max_payload_bytes",
            "partial_timeout_ms", "silent_after_ms"
        }, [&](const YAML::Node& node) {
            readField(node, "tcp", "bind", config.tcp.bind);
            readField(node, "tcp", "port", config.tcp.port);
            readField(node, "tcp", "max_connections", config.tcp.max_connections);
            readField(node, "tcp", "max_payload_bytes", config.tcp.max_payload_bytes);
            readField(node, "tcp", "partial_timeout_ms", config.tcp.partial_timeout_ms);
            readField(node, "tcp", "silent_after_ms", config.tcp.silent_after_ms);
        });

        readGroup(root, "storage", {
            "path", "max_pending_records", "max_total_records",
            "acked_keep_days", "acked_keep_max_records"
        }, [&](const YAML::Node& node) {
            readField(node, "storage", "path", config.storage.path);
            readField(node, "storage", "max_pending_records", config.storage.max_pending_records);
            readField(node, "storage", "max_total_records", config.storage.max_total_records);
            readField(node, "storage", "acked_keep_days", config.storage.acked_keep_days);
            readField(node, "storage", "acked_keep_max_records", config.storage.acked_keep_max_records);
        });

        readGroup(root, "upload", {
            "url", "batch_size", "flush_interval_ms",
            "connect_timeout_ms", "request_timeout_ms",
            "retry_initial_ms", "retry_max_ms", "failure_warn_attempts"
        }, [&](const YAML::Node& node) {
            readField(node, "upload", "url", config.upload.url);
            readField(node, "upload", "batch_size", config.upload.batch_size);
            readField(node, "upload", "flush_interval_ms", config.upload.flush_interval_ms);
            readField(node, "upload", "connect_timeout_ms", config.upload.connect_timeout_ms);
            readField(node, "upload", "request_timeout_ms", config.upload.request_timeout_ms);
            readField(node, "upload", "retry_initial_ms", config.upload.retry_initial_ms);
            readField(node, "upload", "retry_max_ms", config.upload.retry_max_ms);
            readField(node, "upload", "failure_warn_attempts", config.upload.failure_warn_attempts);
        });

        readGroup(root, "recording", {
            "enabled", "directory", "max_file_bytes", "keep_closed_files"
        }, [&](const YAML::Node& node) {
            readField(node, "recording", "enabled", config.recording.enabled);
            readField(node, "recording", "directory", config.recording.directory);
            readField(node, "recording", "max_file_bytes", config.recording.max_file_bytes);
            readField(node, "recording", "keep_closed_files", config.recording.keep_closed_files);
        });

        readGroup(root, "status", {
            "interval_ms", "file", "pending_warn_records", "disk_free_warn_mb"
        }, [&](const YAML::Node& node) {
            readField(node, "status", "interval_ms", config.status.interval_ms);
            readField(node, "status", "file", config.status.file);
            readField(node, "status", "pending_warn_records", config.status.pending_warn_records);
            readField(node, "status", "disk_free_warn_mb", config.status.disk_free_warn_mb);
        });

        return config;
    } catch (const YamlReadError& error) {
        return ConfigYamlError{
            error.code,
            error.field,
            error.what()
        };
    } catch (const YAML::Exception& error) {
        return ConfigYamlError{
            ConfigYamlCode::InvalidYaml,
            "",
            error.what()
        };
    }
}

} // namespace edge