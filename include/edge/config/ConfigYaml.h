#pragma once

#include <string>
#include <string_view>
#include <variant>

#include "edge/config/AgentConfig.h"

namespace edge {

enum class ConfigYamlCode {
    InvalidYaml,
    ExpectedMap,
    InvalidKey,
    UnknownField,
    DuplicateField,
    MissingField,
    InvalidFieldType,
    InvalidScalar,
    IntegerOutOfRange
};

struct ConfigYamlError {
    ConfigYamlCode code;
    std::string field;
    std::string message;
};

using ConfigYamlResult =
    std::variant<AgentConfig, ConfigYamlError>;

// 解析单份 YAML 配置文本。
// 检查结构、字段和标量转换；业务取值由配置验证器检查。
// 返回的路径暂时保持文本中提供的相对或绝对形式。
ConfigYamlResult parseAgentConfigYaml(std::string_view text);

} // namespace edge