#pragma once

#include "edge/net/TcpConnections.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace edge {

inline constexpr std::size_t kTcpReadChunkBytes = 4096;
inline constexpr std::size_t kTcpReadCallsPerRound = 4;
inline constexpr std::size_t kTcpAcceptAttemptsPerRound = 4;

enum class TcpIoKind {
    Accepted,
    RejectedAtCapacity,
    Data,
    Closed,
    ClientError
};

struct TcpIoEvent {
    TcpIoKind kind;

    // RejectedAtCapacity 没有槽位，其余事件有槽位。
    std::optional<std::size_t> slot;

    // 仅 Data 事件携带字节；数据由事件自身持有。
    std::vector<std::uint8_t> bytes;

    // 仅 ClientError 使用。
    int error_code = 0;
};

struct TcpPollError {
    std::string operation;
    int error_code = 0;
    std::string message;
};

struct TcpPollBatch {
    std::vector<TcpIoEvent> events;

    // poll 被信号打断，交回调用方决定下一步。
    bool interrupted = false;

    // poll、监听或接入层面的错误。
    // 即使有错误，前面已经生成的 events 仍需要处理。
    std::optional<TcpPollError> error;
};

// 单线程调用，执行期间不能从其他地方关闭或修改这些 Socket。
//
// timeout_ms 必须非负；0 表示立即检查。
// 返回后必须按顺序消费本批 events，再调用下一轮。
//
// 同一槽位可能先 Closed，再 Accepted，表示槽位已被新连接复用。
// 本轮刚接入的连接，从下一轮开始读取。
[[nodiscard]] TcpPollBatch pollTcpOnce(
    int listening_fd,
    TcpConnections& connections,
    int timeout_ms = 100
);

} // namespace edge