#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "edge/domain/SensorEvent.h"

namespace edge{

// 业务校验错误类型, 调用方可以根据错误码进行分类处理
/*
    enum class 是 C++11 引入的强类型枚举(作用域枚举), 其枚举值不会隐式转换为整数, 也不会与其他枚举类型混用
    这有助于避免枚举值的命名冲突和类型安全问题, 默认情况下, 枚举值的底层类型是 int, 但可以显式指定其他整数类型, 只能是整型, 不能是浮点型或其他类型
        例如: enum class Color : std::uint8_t { Red, Green, Blue };
    枚举值有作用域, 需要使用 Color::Red 来访问, 而不是直接使用 Red
*/
enum class EventValidationCode{
    UnsupportedSchemaVersion,
    InvalidDeviceId,
    InvalidSessionId,
    InvalidSequence,
    InvalidReferenceTime,
    InvalidTimestamp,
    NonFiniteValue,
    UnsupportedMetric,
    UnitMismatch
};

struct EventValidationError{
    EventValidationCode code;

    // 出错字段和可读说明, 便于后续输出日志或诊断信息
    std::string field;
    std::string message;
};

/*
    校验事件, 返回遇到的第一个错误
    返回 std::nullopt 表示检查全部通过
    由调用方提供时间, 方便测试精确控制时间边界
*/
std::optional<EventValidationError> validateSensorEvent(
    const SensorEvent &event, 
    std::int64_t now_ms // 校验时的 Unix 毫秒时间
);

}