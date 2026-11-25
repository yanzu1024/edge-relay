#pragma once


#include <string>
#include <string_view>
#include <variant>

#include "edge/domain/SensorEvent.h"


namespace edge{

enum class EventJsonCode{
    InvalidJson,
    ExpectedObject,
    MissingField,
    UnknownField,
    InvalidFieldType,
    IntegerOutOfRange,

    // 序列化时使用的错误类型
    NonFiniteValue,
    InvalidTextEncoding
};


struct EventJsonError{
    EventJsonCode code;

    // 整体 JSON 出错时, field 为空, 字段错误时填写字段名
    std::string field;      // 出错的字段名
    std::string message;    // 诊断说明, 便于调试, 不用于业务逻辑
};


// 返回值保存解析事件或解析错误中的一种
/*
    std::variant<T, U, V> 它与 std::optional<T>类似, 但它的返回值必然有一个值, TUV三个中的一个, 不会为空
    它算是一个类型安全的联合体, 它解决了 Union的类型安全问题, 并且支持类对象
    可以使用 std::holds_alternative<T>() 来检查当前 variant 中的数据类型
    取出 variant 中的值, 两种方式:
        使用 std::get<T>(variant) 直接取, 如果取出的类型不为 T, 会抛出异常 std::bad_variant_access
        使用 std::get_if<T>(&variant) 安全取, 返回一个指针, 类型对了就返回指针, 否则返回 nullptr
*/
using EventJsonResult = std::variant<SensorEvent, EventJsonError>;  // JSON解析, 返回事件或错误

using EventJsonSerializeResult = std::variant<std::string, EventJsonError>;  // JSON序列化, 返回字符串或错误

// 解析一个事件 JSON 对象
// 该函数检查结构、类型和整数表示范围, 业务规则由验证器检查
// payload 仅在调用期间被读取, 返回的事件拥有自己的字符串
EventJsonResult parseSensorEventJson(std::string_view payload);

// 将事件转换为协议载荷, 输出八个字段的 JSON 对象, 字段顺序与协议一致
// 该函数检查数值是否有限, 字符串是否为 UTF-8, 业务规则由验证器检查
EventJsonSerializeResult serializeSensorEventJson(const SensorEvent& event);

}   // namespace