#include "edge/upload/BatchRequest.h"

#include <string_view>
#include <unordered_set>
#include <utility>

#include <nlohmann/json.hpp>

#include "edge/domain/EventId.h"
#include "edge/net/HttpJsonPost.h"

namespace edge {

namespace {

using Json = nlohmann::json;

// 只有剩余空间足够时才追加。
// target 的长度始终不超过请求体上限。
bool appendLimited(
    std::string& target,
    std::string_view text
)
{
    if (text.size() > kMaxHttpRequestBytes - target.size()) {
        return false;
    }

    target.append(text.data(), text.size());
    return true;
}

BatchRequestError requestTooLarge(std::size_t index)
{
    return BatchRequestError{
        BatchRequestErrorCode::RequestTooLarge,
        index,
        std::nullopt,
        "Encoded batch exceeds the HTTP request size limit."
    };
}

} // namespace

BatchRequestEncodeResult encodeBatchRequest(
    const std::vector<SensorEvent>& events
)
{
    if (events.empty() || events.size() > 100) {
        return BatchRequestError{
            BatchRequestErrorCode::InvalidBatchSize,
            std::nullopt,
            std::nullopt,
            "Batch must contain 1..100 events."
        };
    }

    BatchRequest request;
    request.body = "{\"protocol_version\":1,\"events\":[";
    request.event_ids.reserve(events.size());

    std::unordered_set<std::string> seen_ids;
    seen_ids.reserve(events.size());

    for (std::size_t index = 0; index < events.size(); ++index) {
        const auto& event = events[index];

        // 复用已有序列化器的有限数值、UTF-8 和零值处理。
        const auto serialized = serializeSensorEventJson(event);

        if (const auto* error =
                std::get_if<EventJsonError>(&serialized)) {
            return BatchRequestError{
                BatchRequestErrorCode::EventSerialization,
                index,
                *error,
                "Event serialization failed."
            };
        }

        const auto& payload = std::get<std::string>(serialized);

        auto event_id = makeEventId(event);

        if (!seen_ids.insert(event_id).second) {
            return BatchRequestError{
                BatchRequestErrorCode::DuplicateEventId,
                index,
                std::nullopt,
                "Batch contains a repeated event_id."
            };
        }

        // event_id 作为 JSON 字符串，必须正确转义。
        // 上面的事件序列化已检查其来源字符串的 UTF-8。
        const auto encoded_id = Json(event_id).dump();

        // payload 已由现有序列化器生成，是完整 JSON 对象。
        // 直接嵌入对象文本，避免把它编码成普通字符串。
        if ((index != 0 && !appendLimited(request.body, ","))
            || !appendLimited(request.body, "{\"event_id\":")
            || !appendLimited(request.body, encoded_id)
            || !appendLimited(request.body, ",\"event\":")
            || !appendLimited(request.body, payload)
            || !appendLimited(request.body, "}")) {
            return requestTooLarge(index);
        }

        request.event_ids.push_back(std::move(event_id));
    }

    if (!appendLimited(request.body, "]}")) {
        return requestTooLarge(events.size() - 1);
    }

    return request;
}

} // namespace edge