#pragma once

#include <cstdint>
#include <string>


namespace edge{


/*
    一条时间只描述一个测项, 对应 TCP JSON 载荷中的八个字段
    这里只保存数据: 字段是否合法由后续验证器检查
*/
struct SensorEvent{

    /* 这里使用普通的结构体是便于后续使用 JSON 解析器填充数据, 验证器读取数据 */
    // 字段与协议中的八个字段一一对应
    std::int32_t schema_version = 1;    // 事件数据格式版本
    
    std::string device_id;  // 设备id
    std::string session_id; // 同一模拟会话内保持不变, 重放已有场景时保留原值, 会话id

    std::int64_t sequence = 0;
    std::int64_t observed_at_ms = 0;

    std::string metric;
    double value = 0.0;
    std::string unit;
};

}