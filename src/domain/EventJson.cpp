#include "edge/domain/EventJson.h"

#include <cstdint>
#include <cmath>
#include <limits>
#include <optional>

#include <nlohmann/json.hpp>


namespace edge{

namespace{

using Json = nlohmann::json;

// TCP 事件载荷只允许这 8 个字段, 不包含 event_id, 字段白名单
// 使用 constexpr const char* 数组, 避免每次都创建临时的 std::vector<std::string>, 在编译期就能确定数组内容, 提高性能, 不占用堆内存
constexpr const char* kEventFields[] = {
    "schema_version",   // 事件数据格式版本
    "device_id",        // 设备 ID, 由设备端生成, 用于区分不同设备
    "session_id",       // 会话 ID, 由设备端生成, 用于区分不同会话
    "sequence",         // 事件序号, 由设备端生成, 用于区分同一会话内的不同事件
    "observed_at_ms",   // 观测时间, 由设备端生成, 用于记录事件发生的时间
    "metric",           // 测量指标, 由设备端生成, 用于区分不同的测量类型
    "value",            // 测量值, 由设备端生成, 用于记录测量结果
    "unit"              // 测量单位, 由设备端生成, 用于记录测量结果的单位
};

constexpr const char* kStringFields[] = {   // 仅检查类型, 不检查内容, 字段白名单
    "device_id",
    "session_id",
    "metric",
    "unit"
};

// 用于检查 JSON 对象中是否包含未知字段, 避免使用 std::set<std::string> 或 std::unordered_set<std::string>
// 这些容器会在运行时分配堆内存, 而且需要哈希计算, 性能较低
bool isKnownField(std::string_view name){
    for(const char* field : kEventFields){
        if(name == field){
            return true;
        }
    }

    return false;
}

// 严格读取整数, 避免把字符串、布尔值或浮点数转换成整数, nlohmann::json 库默认会把浮点数或bool转换成整数, 不符合严格读数的要求
// output 只有在读取成功时才会赋值
std::optional<EventJsonError> readInt64(
    const Json& value,
    const char* field,
    std::int64_t& output
){
    if(!value.is_number_integer()){
        return EventJsonError{
            EventJsonCode::InvalidFieldType,
            field,
            "字段必须为整数，不能使用字符串、布尔值、浮点数或 null"
        };
    }

    // 已确认为整数, 但 Json 库可能会把正整数保存为 uint64_t
    // 在转换成 int64_t 之前必须先检查范围, 避免发生错误转换
    if(value.is_number_unsigned()){
        const auto number = value.get<std::uint64_t>();

        // std::numeric_limits<std::int64_t>::max() 是 int64_t 类型能表示的最大整数, 也就是 2^63 - 1
        // 如果 number 超过这个值, 就不能安全地转换成 int64_t, 会发生整数溢出
        const auto max_value = static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max()
        );

        if(number > max_value){
            return EventJsonError{
                EventJsonCode::IntegerOutOfRange,
                field,
                "整数超过 int64_t 可表示的范围"
            };
        }

        // 安全地转换成 int64_t, 因为已经检查过范围
        output = static_cast<std::int64_t>(number);
    }else{  // 负整数或零, 可以安全地转换成 int64_t
        output = value.get<std::int64_t>();
    }

    return std::nullopt;    // 返回 std::nullopt 表示没有错误, 读取成功
}

}   // namespace


// 解析一个事件 JSON 对象
EventJsonResult parseSensorEventJson(std::string_view payload){
    if(payload.empty()){    // 空字符串不是合法的 JSON, 不能解析, 返回错误
        return EventJsonError{
            EventJsonCode::InvalidJson,
            "",
            "JSON 文本不能为空"
        };
    }

    // false 表示解析错误通过 discarded 结果报告
    // 默认保持严格的 Json 语法, 不起用注释等扩展
    const auto root = Json::parse(
        payload.begin(),
        payload.end(),
        nullptr,    // parser_callback_t, 不注册回调
        false       // 解析错误不抛出异常, 而是返回 discarded
    );

    if(root.is_discarded()){    // is_discarded() 检查解析是否失败, 失败时返回 true, 不会抛出异常, 安全的检测json是否解析成功
        return EventJsonError{
            EventJsonCode::InvalidJson,
            "",
            "JSON 语法、编码或数值表示不合法"
        };
    }

    if(!root.is_object()){  // 事件载荷必须是 JSON 对象, 不能是数组、字符串、数字或布尔值, 否则返回错误
        return EventJsonError{
            EventJsonCode::ExpectedObject,
            "",
            "事件载荷必须是一个 JSON 对象"
        };
    }

    // 所有字段都必须显示存在, 避免使用结构体默认值补全缺失字段
    for(const char* field : kEventFields){
        if(!root.contains(field)){
            return EventJsonError{
                EventJsonCode::MissingField,
                field,
                "缺少必需字段"
            };
        }
    }

    // 检查 JSON 对象中是否包含未知字段, 避免使用 std::set<std::string> 或 std::unordered_set<std::string>
    for(auto iterator = root.begin(); iterator != root.end(); ++iterator){
        if(!isKnownField(iterator.key())){
            return EventJsonError{
                EventJsonCode::UnknownField,
                iterator.key(),
                "事件中包含未知字段"
            };
        }
    }

    // 检查字符串字段的类型, 不能是整数、浮点数、布尔值或 null, 否则返回错误
    for(const char* field : kStringFields){
        if(!root.at(field).is_string()){
            return EventJsonError{
                EventJsonCode::InvalidFieldType,
                field,
                "字段必须为字符串"
            };
        }
    }

    SensorEvent event;

    // 先按 int64_t 读取, 在检查模型中 int32_t 字段的表示范围
    // 解析 schema_version, 该字段在模型中是 int32_t 类型, 需要检查范围
    std::int64_t schema_version = 0;

    if(const auto error = readInt64(    // 读取 schema_version 字段, 检查类型和范围
        root.at("schema_version"),
        "schema_version",
        schema_version
    )){
        return *error;  
    }

    // 再次检查 schema_version 是否在 int32_t 的表示范围内, 避免整数溢出
    if(schema_version < std::numeric_limits<std::int32_t>::min() ||
        schema_version > std::numeric_limits<std::int32_t>::max()
    ){
        return EventJsonError{
            EventJsonCode::IntegerOutOfRange,
            "schema_version",
            "版本字段超过 int32_t 可表示的范围"
        };
    }

    // 将 schema_version 转换为 int32_t 类型, 因为已经检查过范围, 不会发生溢出
    event.schema_version = static_cast<std::int32_t>(schema_version);

    // 解析 sequence 和 observed_at_ms, 这些字段在模型中是 int64_t 类型, 不需要再次检查范围
    if(const auto error = readInt64(
        root.at("sequence"),
        "sequence",
        event.sequence
    )){
        return *error;
    }

    if(const auto error = readInt64(
        root.at("observed_at_ms"),
        "observed_at_ms",
        event.observed_at_ms
    )){
        return *error;
    }

    // value 接受整数或浮点数, 拒绝字符串、布尔值和 null, 校验器会检查非有限值, 这里不做检查
    if(!root.at("value").is_number()){
        return EventJsonError{
            EventJsonCode::InvalidFieldType,
            "value",
            "测量值必须为数值"
        };
    }

    // 解析字符串字段, 这些字段在模型中是 std::string 类型, 不需要再次检查范围
    event.device_id = root.at("device_id").get<std::string>();
    event.session_id = root.at("session_id").get<std::string>();
    event.metric = root.at("metric").get<std::string>();
    event.unit = root.at("unit").get<std::string>();
    event.value = root.at("value").get<double>();   // value 既可以是整数也可以是浮点数, 解析为 double 类型

    return event;
}

// 将事件转换为协议载荷, 输出八个字段的 JSON 对象, 字段顺序与协议一致
// 该函数检查数值是否有限, 字符串是否为 UTF-8, 业务规则由验证器检查
EventJsonSerializeResult serializeSensorEventJson(
    const SensorEvent& event
){
    if (!std::isfinite(event.value)) {
        return EventJsonError{
            EventJsonCode::NonFiniteValue,
            "value",
            "测量值必须为有限数值。"
        };
    }

    // 正零与负零比较相等, 因此两者都进入 0.0 分支
    // 这里必须使用正的 0.0, 零值归一化
    const double normalized_value = (event.value == 0.0) ? 0.0 : event.value;

    Json root = Json::object();

    root["schema_version"] = event.schema_version;
    root["device_id"] = event.device_id;
    root["session_id"] = event.session_id;
    root["sequence"] = event.sequence;
    root["observed_at_ms"] = event.observed_at_ms;
    root["metric"] = event.metric;

    // 使用归一化后的值, 不能再写 event.value
    root["value"] = normalized_value;

    root["unit"] = event.unit;

    try {
        return root.dump();
    } catch (const Json::type_error&) {
        return EventJsonError{
            EventJsonCode::InvalidTextEncoding,
            "",
            "事件字符串包含非法 UTF-8 字节"
        };
    }
}

}   // namespace