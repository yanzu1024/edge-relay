#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

// 协议 v1 的帧格式定义, 包含帧头和载荷
namespace edge{

// ASCII "ETLM" 的十六进制表示, 即 0x45 0x54 0x4C 0x4D 分别对应 E T L M 的 ASCII 码, 用于识别帧起点
inline constexpr std::array<std::uint8_t, 4> kFrameMagic = {
    0x45, 0x54, 0x4C, 0x4D
};

// 8 字节帧头：4 字节魔数 + 4 字节大端长度字段
// inline 是 C++17 的新特性, 允许在头文件中定义常量, 避免重复定义
inline constexpr std::size_t kFrameHeaderSize = 8;

// 协议 v1 的载荷硬性上限, 8KB, 不能再增加, 因为长度字段只有 4 字节, 最大表示 2^32-1, 但协议限制为 8192 字节
inline constexpr std::size_t kMaxFramePayloadBytes = 8192;

}   // namespace edge