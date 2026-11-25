#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>


namespace edge{

enum class FrameEncodeCode{
    EmptyPayload,       // 帧载荷为空
    PayloadTooLarge     // 帧载荷超过协议限制的 8192 字节
};


struct FrameEncodeError{
    FrameEncodeCode code;
    std::string message;    // 诊断说明, 便于调试, 不用于业务逻辑
};


// 完整帧是原始字节序列, 可以包含任意字节值
using EncodedFrame = std::vector<std::uint8_t>;  // 编码后的帧, 包含帧头和帧载荷

using FrameEncodeResult = std::variant<EncodedFrame, FrameEncodeError>;  // 编码结果, 成功返回帧, 失败返回错误


// 将已经序列化的事件 JSON 对象编码为协议帧, 包含帧头和帧载荷
// 函数会检查 payload 是否为空、是否超过协议限制, 调用方负责处理返回的错误
// payload 仅在调用期间被读取, 返回的帧拥有自己的字节序列
FrameEncodeResult encodeFrame(std::string_view payload);

}