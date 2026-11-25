#include "edge/domain/EventId.h"


namespace edge{

std::string makeEventId(const SensorEvent &event){
    // 生成规则沿用了已经确定的协议 device_id/session_id/sequence
    return event.device_id 
        + "/" 
        + event.session_id 
        + "/" 
        + std::to_string(event.sequence);
}

}