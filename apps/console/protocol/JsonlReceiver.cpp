#include "JsonlReceiver.h"

#include <stdexcept>
#include <utility>

namespace edge::console {

void JsonlReceiver::feed(
    std::string_view bytes,
    const Sink& sink
)
{
    if (!sink) {
        throw std::invalid_argument(
            "JsonlReceiver requires a result sink"
        );
    }

    framer_.feed(bytes, [this, &sink](JsonlFrame frame) {
        onFrame(std::move(frame), sink);
    });
}

void JsonlReceiver::finish(const Sink& sink)
{
    if (!sink) {
        throw std::invalid_argument(
            "JsonlReceiver requires a result sink"
        );
    }

    framer_.finish([this, &sink](JsonlFrame frame) {
        onFrame(std::move(frame), sink);
    });
}

void JsonlReceiver::reset()
{
    // 先让拆行器检查是否处于回调中。
    // 如果重入被拒绝，解析器也必须保持原状。
    framer_.reset();
    parser_.reset();
}

void JsonlReceiver::onFrame(
    JsonlFrame frame,
    const Sink& sink
)
{
    if (frame.kind != JsonlFrameKind::Line) {
        sink(JsonlReceiveFramingError{
            frame.kind
        });

        return;
    }

    // 解析器只拥有运行身份、序号等少量值状态。
    // 先在副本上解析，避免模型失败时污染正式状态。
    auto candidate = parser_;

    auto parsed = candidate.parse(frame.bytes);

    if (auto* error = std::get_if<JsonlParseError>(&parsed)) {
        sink(std::move(*error));
        return;
    }

    auto message = std::get<JsonlMessage>(std::move(parsed));

    std::optional<StatusSummary> status;

    if (message.kind == JsonlMessageKind::Status
        || message.kind == JsonlMessageKind::Stopped) {
        auto decoded = decodeStatusSummary(message);

        if (auto* error =
                std::get_if<StatusDecodeError>(&decoded)) {
            sink(std::move(*error));
            return;
        }

        status.emplace(
            std::get<StatusSummary>(std::move(decoded))
        );
    }

    // 先构造交付结果，再提交接收状态。
    JsonlReceiveResult result{
        JsonlReceivedMessage{
            std::move(message),
            std::move(status)
        }
    };

    parser_ = std::move(candidate);

    // 此时消息已通过接收层校验并提交。
    sink(std::move(result));
}

} // namespace edge::console