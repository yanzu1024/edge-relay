#include "edge/upload/BatchUploader.h"

#include <stdexcept>
#include <utility>

#include "edge/config/ConfigValidator.h"

namespace edge {

BatchUploadResult uploadBatch(
    const UploadConfig& config,
    const std::vector<SensorEvent>& events
)
{
    if (validateUploadConfig(config)) {
        throw std::invalid_argument{
            "Invalid upload configuration."
        };
    }

    if (events.size() > static_cast<std::size_t>(config.batch_size)) {
        return BatchRequestError{
            BatchRequestErrorCode::InvalidBatchSize,
            std::nullopt,
            std::nullopt,
            "Batch exceeds upload.batch_size."
        };
    }

    auto encoded = encodeBatchRequest(events);

    if (auto* error =
            std::get_if<BatchRequestError>(&encoded)) {
        return std::move(*error);
    }

    const auto& request = std::get<BatchRequest>(encoded);

    auto posted = postJson(
        config,
        request.body
    );

    if (auto* error =
            std::get_if<HttpRequestError>(&posted)) {
        return std::move(*error);
    }

    auto& http_response = std::get<HttpResponse>(posted);

    // 首版协议只接受 HTTP 200 作为业务响应入口。
    if (http_response.status_code != 200) {
        return BatchHttpError{
            http_response.status_code,
            std::move(http_response.body)
        };
    }

    auto parsed = parseBatchResponse(
        http_response.body,
        request.event_ids
    );

    if (auto* error =
            std::get_if<BatchResponseError>(&parsed)) {
        return std::move(*error);
    }

    return std::move(std::get<BatchResponse>(parsed));
}

} // namespace edge