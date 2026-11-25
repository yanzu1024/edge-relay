#pragma once

#include "JsonlLineFramer.h"
#include "JsonlMessageParser.h"
#include "StatusSummary.h"

#include <functional>
#include <optional>
#include <string_view>
#include <variant>

namespace edge::console {

struct JsonlReceiveFramingError {
    // 接收器只会交付 LineTooLong 或 IncompleteTail。
    JsonlFrameKind kind;
};

struct JsonlReceivedMessage {
    // 保留拥有自身数据的完整消息载荷。
    JsonlMessage message;

    // status、stopped 消息具有状态摘要。
    // ready、record 消息不设置此字段。
    std::optional<StatusSummary> status;
};

using JsonlReceiveResult = std::variant<
    JsonlReceivedMessage,
    JsonlReceiveFramingError,
    JsonlParseError,
    StatusDecodeError
>;

class JsonlReceiver {
public:
    using Sink = std::function<void(JsonlReceiveResult)>;

    JsonlReceiver() = default;

    JsonlReceiver(const JsonlReceiver&) = delete;
    JsonlReceiver& operator=(const JsonlReceiver&) = delete;

    // 同线程同步调用，不在内部积累消息历史。
    // 回调不能重入修改本接收器。
    // 回调异常向外传播，调用方应在回调内部处理展示错误。
    void feed(std::string_view bytes, const Sink& sink);

    // 在本次 stdout 字节流结束后调用。
    // 仅报告残留半行，不伪造 stopped 消息。
    void finish(const Sink& sink);

    // 新一轮 Agent 启动前调用。
    void reset();

private:
    void onFrame(JsonlFrame frame, const Sink& sink);

    JsonlLineFramer framer_;
    JsonlMessageParser parser_;
};

} // namespace edge::console