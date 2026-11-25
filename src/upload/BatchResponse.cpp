#include "edge/upload/BatchResponse.h"

#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <nlohmann/json.hpp>

#include "edge/net/HttpJsonPost.h"

namespace edge {

namespace {

using Json = nlohmann::json;

class StrictJsonError final : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

bool hasTwoFields(
    const Json& value,
    const char* first,
    const char* second
)
{
    return value.is_object()
        && value.size() == 2
        && value.contains(first)
        && value.contains(second);
}

Json parseStrictJson(std::string_view body)
{
    // 每个尚未结束的对象，各自保存已经出现的键。
    std::vector<std::unordered_set<std::string>> object_keys;

    const auto callback = [&object_keys](
        int depth,
        Json::parse_event_t event,
        Json& value
    ) -> bool {
        if (depth > 16) {
            throw StrictJsonError{
                "JSON nesting exceeds the supported limit."
            };
        }

        if (event == Json::parse_event_t::object_start) {
            object_keys.emplace_back();
        }
        else if (event == Json::parse_event_t::key) {
            const auto& key =
                value.get_ref<const std::string&>();

            if (!object_keys.back().insert(key).second) {
                throw StrictJsonError{
                    "Duplicate JSON object key."
                };
            }
        }
        else if (event == Json::parse_event_t::object_end) {
            object_keys.pop_back();
        }

        // 不丢弃字段；完整保留后再检查协议结构。
        return true;
    };

    return Json::parse(
        body.begin(),
        body.end(),
        callback
    );
}

} // namespace

BatchResponseParseResult parseBatchResponse(
    std::string_view body,
    const std::vector<std::string>& requested_ids
)
{
    if (requested_ids.empty() || requested_ids.size() > 100) {
        throw std::invalid_argument{
            "Requested ID count must be 1..100."
        };
    }

    // ID -> 请求中的位置。
    std::unordered_map<std::string, std::size_t> positions;
    positions.reserve(requested_ids.size());

    for (std::size_t index = 0;
         index < requested_ids.size();
         ++index) {
        const auto& id = requested_ids[index];

        if (id.empty()
            || !positions.emplace(id, index).second) {
            throw std::invalid_argument{
                "Requested IDs must be non-empty and unique."
            };
        }
    }

    if (body.size() > kMaxHttpResponseBytes) {
        return BatchResponseError{
            "Response body exceeds the size limit."
        };
    }

    // JSON 转义的 \u0000 由解析器处理；
    // 原始文本中的零字节不是合法 JSON 文本内容。
    if (body.find('\0') != std::string_view::npos) {
        return BatchResponseError{
            "Response body contains a raw null byte."
        };
    }

    Json document;

    try {
        document = parseStrictJson(body);
    }
    catch (const StrictJsonError& error) {
        return BatchResponseError{
            error.what()
        };
    }
    catch (const Json::exception& error) {
        return BatchResponseError{
            std::string{"Invalid response JSON: "}
            + error.what()
        };
    }

    if (!hasTwoFields(
            document,
            "protocol_version",
            "results")) {
        return BatchResponseError{
            "Response must contain only protocol_version and results."
        };
    }

    const auto& version = document.at("protocol_version");

    if (!version.is_number_integer() || version != 1) {
        return BatchResponseError{
            "protocol_version must be integer 1."
        };
    }

    const auto& results = document.at("results");

    if (!results.is_array()
        || results.size() != requested_ids.size()) {
        return BatchResponseError{
            "Result count must match the request."
        };
    }

    // 未赋值表示该请求 ID 尚未得到结果。
    std::vector<std::optional<BatchEventStatus>> statuses(
        requested_ids.size()
    );

    for (const auto& item : results) {
        if (!hasTwoFields(item, "event_id", "status")) {
            return BatchResponseError{
                "Each result must contain only event_id and status."
            };
        }

        const auto& id_value = item.at("event_id");
        const auto& status_value = item.at("status");

        if (!id_value.is_string() || !status_value.is_string()) {
            return BatchResponseError{
                "event_id and status must be strings."
            };
        }

        const auto& id =
            id_value.get_ref<const std::string&>();

        const auto position = positions.find(id);

        if (position == positions.end()) {
            return BatchResponseError{
                "Response contains an unknown event_id."
            };
        }

        auto& slot = statuses[position->second];

        if (slot.has_value()) {
            return BatchResponseError{
                "Response contains a repeated event_id."
            };
        }

        const auto& status =
            status_value.get_ref<const std::string&>();

        if (status == "accepted") {
            slot = BatchEventStatus::Accepted;
        }
        else if (status == "duplicate") {
            slot = BatchEventStatus::Duplicate;
        }
        else if (status == "conflict") {
            slot = BatchEventStatus::Conflict;
        }
        else {
            return BatchResponseError{
                "Response contains an unsupported status."
            };
        }
    }

    BatchResponse response;
    response.results.reserve(requested_ids.size());

    for (std::size_t index = 0;
         index < requested_ids.size();
         ++index) {
        if (!statuses[index].has_value()) {
            return BatchResponseError{
                "Response is missing a requested event_id."
            };
        }

        response.results.push_back(BatchEventResult{
            requested_ids[index],
            *statuses[index]
        });
    }

    return response;
}

} // namespace edge