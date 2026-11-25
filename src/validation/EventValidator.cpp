#include "edge/validation/EventValidator.h"

#include <cmath>
#include <cstddef>
#include <string_view>


/*
    验证器
    它不会保存最新序号, 因此不会因为收到就序号而拒绝事件, 这样才能支持原场景重放
*/

namespace edge{

// 允许观测时间最多领先校验时间 5 min
constexpr std::int64_t kFutureToleranceMs = 5 * 60 * 1000;

// 使用明确的 ASCII 范围, 避免字符分类受系统区域设置影响
// 把字符限制在大小写字母、0-9 和 - _ 中
bool isAllowedDeviceCharacter(char character){
    return (character >= 'a' && character <= 'z') 
        || (character >= 'A' && character <= 'Z')
        || (character >= '0' && character <= '9')
        || character == '_'
        || character == '-';
}

// 校验设备 id 是否有效
// std::string_view 是一个轻量的 string 引用
bool isvalidDeviceId(std::string_view device_id){
    // 设备 id 长度为32, 所以不能超过 32
    if(device_id.empty() || device_id.size() > 32){
        return false;
    }

    for(char character : device_id){
        if(!isAllowedDeviceCharacter(character)){
            return false;
        }
    }

    return true;
}

// 
bool isLowercaseHexCharacter(char character){
    return (character >= '0' && character <= '9') 
        || (character >= 'a' && character <= 'f');
}

/*
    检查小写 UUID, 文本格式: 8-4-4-4-12
    只检查文本形式, 不额外限定 UUID 的版本
*/
bool isValidSessionId(std::string_view session_id){
    if(session_id.size() != 36){
        return false;
    }

    for(std::size_t index = 0; index < session_id.size(); ++index){
        // 确定分隔符 - 的位置
        const bool is_separator = 
            index == 8 
            || index == 13
            || index == 18
            || index == 23;
        
        if(is_separator){
            if(session_id[index] != '-'){
                return false;
            }
        }else if(!isLowercaseHexCharacter(session_id[index])){
            return false;
        }
    }

    return true;
}


/*
    std::optional<T> 是一个标准的模板类, 它专门用来表示一个可能存在也可能不存在的值
    它解决了函数返回值可能为空的问题, 这种返回类型的函数, 其返回值要么是 T 类型的值, 要么为空(nullopt or {})
    
    采用快速失败模式(Fail-Fast)
    只要发现不符合协议要求的数据, 立刻打包一个包含错误码、出错字段、错误原因的 EventValidationError 对象返回
    如果校验全部通过, 会返回 std::nullopt, 即空的 optional
*/
std::optional<EventValidationError> validateSensorEvent(
    const SensorEvent &event,
    std::int64_t now_ms
){
    if(event.schema_version != 1){
        return EventValidationError{
            EventValidationCode::UnsupportedSchemaVersion,
            "schema_version",
            "仅支持事件格式版本 1"
        };
    }

    if(!isvalidDeviceId(event.device_id)){
        return EventValidationError{
            EventValidationCode::InvalidDeviceId,
            "device_id",
            "设备 ID 必须为 1–32 个 ASCII 字母、数字、下划线或短横线"
        };
    }

    if(!isValidSessionId(event.session_id)){
        return EventValidationError{
            EventValidationCode::InvalidSessionId,
            "session_id",
            "会话 ID 必须符合小写 UUID 文本格式"
        };
    }

    // session_id 已经设置为了 int64_t, 类型本身限定了上限
    // 检查协议要求的下限
    if(event.sequence < 1){
        return EventValidationError{
            EventValidationCode::InvalidSequence,
            "sequence",
            "事件序号必须大于或等于 1"
        };
    }

    if(now_ms <= 0){
        return EventValidationError{
            EventValidationCode::InvalidReferenceTime,
            "now_ms",
            "校验参考时间必须为正的 Unix 毫秒时间"
        };
    }

    if(event.observed_at_ms <= 0){
        return EventValidationError{
            EventValidationCode::InvalidTimestamp,
            "observed_at_ms",
            "观测时间必须为正的 Unix 毫秒时间"
        };
    }

    /*
        历史时间允许重放, 只限制过度超前的时间
        两个时间都确认大于 0, 先比较再相减, 避免使用
        now_ms 加上容忍值时, 可能会发生整数溢出
    */
    if(event.observed_at_ms > now_ms && event.observed_at_ms - now_ms > kFutureToleranceMs){
        return EventValidationError{
            EventValidationCode::InvalidTimestamp,
            "observed_at_ms",
            "观测时间不能领先校验时间超过 5 分钟"
        };
    }

    // double 除了普通数值, 还能保存 NaN(Not a Number) 或 正负无穷
    if(!std::isfinite(event.value)){
        return EventValidationError{
            EventValidationCode::NonFiniteValue,
            "value",
            "测量值必须为有限数值"
        };
    }

    // std::string_view 只引用下方字符串常量, 不拥有或复制字符串(零拷贝)
    std::string_view expected_unit;

    if(event.metric == "temperature"){
        expected_unit = "degC";
    }else if(event.metric == "vibration"){
        expected_unit = "mm_s";
    }else if(event.metric == "displacement"){
        expected_unit = "mm";
    }else{
        return EventValidationError{
            EventValidationCode::UnsupportedMetric,
            "metric",
            "不支持该测项"
        };
    }

    if(std::string_view(event.unit) != expected_unit){
        return EventValidationError{
            EventValidationCode::UnitMismatch,
            "unit",
            "单位与测项不匹配"
        };
    }

    return std::nullopt;
}

}