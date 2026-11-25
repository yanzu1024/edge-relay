#pragma once

#include <string>

#include "edge/domain/SensorEvent.h"

namespace edge{
    // 传引用, 既不复制整个结构体, 也不允许函数修改事件
    std::string makeEventId(const SensorEvent &event);

}