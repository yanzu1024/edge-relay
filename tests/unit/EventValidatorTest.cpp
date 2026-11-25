#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>

#include "edge/domain/SensorEvent.h"
#include "edge/validation/EventValidator.h"

namespace {

// 固定校验时间, 使测试结果不依赖机器当前时间
constexpr std::int64_t kNowMs = 1792713601000;

using Code = edge::EventValidationCode;


/*
    Factory函数, 工厂函数, 辅助函数
    它是一个专门用来创建对象的函数, 这样就不需要手动 new 对象以及挨个设置其属性
    每次返回一个新的合法事件, 避免测试之间共享可变状态

*/
edge::SensorEvent makeValidEvent(){
    edge::SensorEvent event;

    event.schema_version = 1;
    event.device_id = "sim-001";
    event.session_id = "184ab3d6-d911-44d5-821f-0123456789ab";
    event.sequence = 1;
    event.observed_at_ms = 1792713600000;
    event.metric = "temperature";
    event.value = 23.5;
    event.unit = "degC";

    return event;
}


// 统一检查错误类型和字段, 避免每个失败场景重复这些断言
void expectValidationError(
    const edge::SensorEvent &event, 
    Code expected_code, 
    const char* expected_field,
    std::int64_t now_ms = kNowMs
){
    const auto error = edge::validateSensorEvent(event, now_ms);

    /*
        ASSERT_TRUE, EXPECT_EQ, EXPECT_FALSE 都是 google test C++ 单元测试框架中的断言宏(Assertion Macros)
        GTest 把断言分为两大系列:
            ASSERT_ 系列, 致命断言, 一旦失败, 会立即中止当前测试函数的后续执行, 它属于前置依赖检查
            EXPECT_ 系列, 非致命断言, 一旦失败, 会继续执行当前测试函数中的下一条断言, 方便在单次测试中收集更多的失败信息
    */

    // 必须先确认 optional 有值, 才能安全访问 error->成员
    ASSERT_TRUE(error.has_value());

    EXPECT_EQ(error->code, expected_code);
    EXPECT_EQ(error->field, expected_field);

    // 诊断说明应为非空, 但不把具体文案固定在测试中
    EXPECT_FALSE(error->message.empty());
}


/*
    标准 GTest 测试用例(函数)
    采用经典的数据驱动测试或循环遍历测试
    TEST宏, 看到这样全部大写的关键字, 基本都是宏
        第一个参数 EventValidator, 是测试套件名(Test Suite), 通常对应被测试的类或模块
        第二个参数 AcceptsSupportedMetricUnitPairs, 是具体的测试名称, 描述了该测试要进行的具体行为
    
*/
TEST(EventValidator, AcceptsSupportedMetricUnitPairs){
    struct MetricCase{  // 用来测试的数据
        const char *metric;
        const char *unit;
    };

    const MetricCase cases[] = {    // 同样是用来测试的数据
        {"temperature", "degC"},    // 温度/摄氏度
        {"vibration", "mm_s"},      // 振动/毫米每秒
        {"displacement", "mm"}      // 位移/毫米
    };

    for(const auto& item : cases){  // 核心测试逻辑: 循环与断言
        // 循环中的断言失败时, 补充当前正在检查的测项
        // 作用域追踪宏
        SCOPED_TRACE(item.metric);

        auto event = makeValidEvent();  // 使用工厂函数创建一个完全合法的基准事件
        event.metric = item.metric;
        event.unit = item.unit;

        // 
        const auto error = edge::validateSensorEvent(event, kNowMs);

        EXPECT_FALSE(error.has_value());
    }
}


TEST(EventValidator, ChecksDeviceIdRules){
    auto event = makeValidEvent();

    // 长度下界、上界和允许的字符组合
    event.device_id = "a";
    EXPECT_FALSE(edge::validateSensorEvent(event, kNowMs).has_value());

    event.device_id = std::string(32, 'a');
    EXPECT_FALSE(edge::validateSensorEvent(event, kNowMs).has_value());

    event.device_id = "AZaz09_-";
    EXPECT_FALSE(edge::validateSensorEvent(event, kNowMs).has_value());

    const std::string invalid_ids[] = {
        "",
        std::string(33, 'a'),
        "sim/001",
        "sim 001",
        "sim.001",
        "设备-001"
    };

    for(const auto& device_id : invalid_ids){
        SCOPED_TRACE(device_id);

        event.device_id = device_id;

        expectValidationError(
            event,
            Code::InvalidDeviceId,
            "device_id"
        );
    }
}


TEST(EventValidator, RejectsInvalidSessionId){
    const std::string invalid_ids[] = {
        "",
        "184ab3d6-d911-44d5-821f-0123456789a",
        "184AB3D6-d911-44d5-821f-0123456789ab",
        "184ab3d6_d911-44d5-821f-0123456789ab",
        "184ab3d6-d911-44d5-821f-0123456789ag"
    };

    for(const auto& session_id : invalid_ids){
        SCOPED_TRACE(session_id);

        auto event = makeValidEvent();
        event.session_id = session_id;

        expectValidationError(
            event,
            Code::InvalidSessionId,
            "session_id"
        );
    }
}


TEST(EventValidator, RejectsUnsupportedSchemaVersion){
    const std::int64_t invalid_versions[] = {0, -1, 2};

    for(const auto& version : invalid_versions){
        SCOPED_TRACE(version);

        auto event = makeValidEvent();
        event.schema_version = version;

        expectValidationError(
            event,
            Code::UnsupportedSchemaVersion,
            "schema_version"
        );
    }
}


TEST(EventValidator, ChecksSequenceRange){
    auto event = makeValidEvent();

    const std::int64_t invalid_sequences[] = {0, -1};

    for(const auto& sequence : invalid_sequences){
        SCOPED_TRACE(sequence);

        event.sequence = sequence;

        expectValidationError(
            event,
            Code::InvalidSequence,
            "sequence"
        );
    }

    // 协议允许 int64_t 能表示的最大正整数
    event.sequence = std::numeric_limits<std::int64_t>::max();

    EXPECT_FALSE(edge::validateSensorEvent(event, kNowMs).has_value());
}


TEST(EventValidator, RejectsNonPositiveTimes){
    const std::int64_t invalid_times[] = {0, -1};

    for(const auto& invalid_time : invalid_times){
        SCOPED_TRACE(invalid_time);

        auto event = makeValidEvent();

        // 事件本身合法, 单独检查调用方法传入的参考时间
        expectValidationError(
            event,
            Code::InvalidReferenceTime,
            "now_ms",
            invalid_time
        );

        // 参考时间合法, 单独检查事件中的观测时间
        event.observed_at_ms = invalid_time;

        expectValidationError(
            event,
            Code::InvalidTimestamp,
            "observed_at_ms"
        );
    }
}


TEST(EventValidator, AcceptsHistoricalTime){
    auto event = makeValidEvent();

    // 协议允许历史场景重放, 不设置历史时间的过期限制
    event.observed_at_ms = 1;
    
    EXPECT_FALSE(edge::validateSensorEvent(event, kNowMs).has_value());
}


TEST(EventValidator, ChecksFutureTimeBoundary){
    auto event = makeValidEvent();

    // 恰好领先 5Min 仍应通过
    event.observed_at_ms = kNowMs + 300000;

    EXPECT_FALSE(edge::validateSensorEvent(event, kNowMs).has_value());

    // 超过边界 1ms 应被拒绝
    event.observed_at_ms = kNowMs + 300001;

    expectValidationError(
        event,
        Code::InvalidTimestamp,
        "observed_at_ms"
    );
}


TEST(EventValidator, HandlesLargeTimestamps){
    auto event = makeValidEvent();

    const auto max_time = std::numeric_limits<std::int64_t>::max();
    event.observed_at_ms = max_time;

    // 接近整数上界时, 时间比较也必须保持正确
    EXPECT_FALSE(
        edge::validateSensorEvent(event, max_time - 100).has_value()
    );

    expectValidationError(
        event,
        Code::InvalidTimestamp,
        "observed_at_ms",
        max_time - 300001
    );
}


TEST(EventValidator, RejectsNonFiniteValues){
    const double invalid_values[] = {
        std::numeric_limits<double>::quiet_NaN(),
        std::numeric_limits<double>::infinity(),    // 正无穷
        -std::numeric_limits<double>::infinity()    // 负无穷
    };

    for(const auto value : invalid_values){
        SCOPED_TRACE(value);

        auto event = makeValidEvent();
        event.value = value;

        expectValidationError(
            event,
            Code::NonFiniteValue,
            "value"
        );
    }
}


TEST(EventValidator, RejectsUnsupportedMetric){
    const std::string invalid_metrics[] = {"", "humidity"};

    for(const auto& metric : invalid_metrics){
        SCOPED_TRACE(metric);
        
        auto event = makeValidEvent();
        event.metric = metric;

        expectValidationError(
            event,
            Code::UnsupportedMetric,
            "metric"
        );
    }
}


TEST(EventValidator, RejectsMismatchedUnit){
    const std::string invalid_units[] = {"", "mm", "DegC"};

    for(const auto& unit : invalid_units){
        SCOPED_TRACE(unit);

        auto event = makeValidEvent();
        event.unit = unit;

        expectValidationError(
            event,
            Code::UnitMismatch,
            "unit"
        );
    }
}

}   // namespace