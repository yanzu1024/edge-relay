#include "edge/protocol/FrameEncoder.h"
#include "edge/protocol/FrameFormat.h"


namespace edge{

FrameEncodeResult encodeFrame(std::string_view payload){
    if(payload.empty()){    // 空载荷不允许, 因为协议不允许零字节载荷
        return FrameEncodeError{
            FrameEncodeCode::EmptyPayload,
            "帧载荷不能为空"
        };
    }

    if(payload.size() > kMaxFramePayloadBytes){ // 载荷超过协议限制, 不能再继续编码, 因为长度字段只有 4 字节, 最大表示 2^32-1, 但协议限制为 8192 字节
        return FrameEncodeError{
            FrameEncodeCode::PayloadTooLarge,
            "帧载荷超过协议限制的 " + std::to_string(kMaxFramePayloadBytes) + " 字节"
        };
    }

    // 已经确认长度不超过 8192 字节, 可以安全地计算帧总长度, 转换为 uint32_t 也不会溢出
    const auto length = static_cast<std::uint32_t>(payload.size());

    EncodedFrame frame;

    // 提前分配最终容量, 避免多次内存分配
    frame.reserve(kFrameHeaderSize + payload.size());

    frame.insert(
        frame.end(),
        kFrameMagic.begin(),
        kFrameMagic.end()
    );

    // 大端序, 先写高字节, 再写低字节, 逐字节编码, 不依赖本机 CPU 的字节序
    // 例如 length = 0x12345678, 则写入顺序为 0x12, 0x34, 0x56, 0x78
    frame.push_back(
        static_cast<std::uint8_t>((length >> 24U) & 0xFFU)
    );
    frame.push_back(
        static_cast<std::uint8_t>((length >> 16U) & 0xFFU)
    );
    frame.push_back(
        static_cast<std::uint8_t>((length >> 8U) & 0xFFU)
    );
    frame.push_back(
        static_cast<std::uint8_t>(length & 0xFFU)
    );

    // 按 string_view 的顺序追加载荷, 不需要再检查长度, 因为前面已经检查过了
    frame.insert(
        frame.end(),
        payload.begin(),
        payload.end()
    );

    return frame;
}

/*
最终帧布局如下:
    +--------+--------+--------+--------+--------+--------+--------+--------+------------------+
    | 0x45   | 0x54   | 0x4C   | 0x4D   | len>>24| len>>16| len>>8 | len    | payload ...      |
    | 'E'    | 'T'    | 'L'    | 'M'    |        |        |        |        |                  |
    +--------+--------+--------+--------+--------+--------+--------+--------+------------------+
    |<----------- 4 字节魔数 ----------->|<----------- 4 字节大端长度 --------->|<-- N 字节 -->|
    |<-------------------------------- 8 字节帧头 --------------------------------->|

*/


}   // namespace edge