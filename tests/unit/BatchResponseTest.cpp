#include "edge/upload/BatchResponse.h"
#include "edge/net/HttpJsonPost.h"

#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace {

using Json = nlohmann::json;

Json validDocument()
{
    return Json{
        {"protocol_version", 1},
        {"results", Json::array({
            Json{
                {"event_id", "a"},
                {"status", "accepted"}
            }
        })}
    };
}

void expectRejected(
    const std::string& body,
    const std::vector<std::string>& ids = {"a"}
)
{
    const auto result = edge::parseBatchResponse(body, ids);

    EXPECT_TRUE(
        std::holds_alternative<edge::BatchResponseError>(result)
    );
}

TEST(BatchResponseTest, AcceptsAllStatusesAndRestoresRequestOrder)
{
    const std::string body = R"({
        "protocol_version": 1,
        "results": [
            {"event_id": "c", "status": "conflict"},
            {"event_id": "a", "status": "accepted"},
            {"event_id": "b", "status": "duplicate"}
        ]
    })";

    const auto result = edge::parseBatchResponse(
        body,
        {"a", "b", "c"}
    );

    const auto* response =
        std::get_if<edge::BatchResponse>(&result);

    ASSERT_NE(response, nullptr);
    ASSERT_EQ(response->results.size(), 3U);

    EXPECT_EQ(response->results[0].event_id, "a");
    EXPECT_EQ(
        response->results[0].status,
        edge::BatchEventStatus::Accepted
    );

    EXPECT_EQ(response->results[1].event_id, "b");
    EXPECT_EQ(
        response->results[1].status,
        edge::BatchEventStatus::Duplicate
    );

    EXPECT_EQ(response->results[2].event_id, "c");
    EXPECT_EQ(
        response->results[2].status,
        edge::BatchEventStatus::Conflict
    );
}

TEST(BatchResponseTest, RejectsInvalidJsonText)
{
    expectRejected("");
    expectRejected("{");
    expectRejected(validDocument().dump() + " trailing");

    auto body = validDocument().dump();
    body.push_back('\0');
    expectRejected(body);

    body = "{\"protocol_version\":1,\"results\":\"";
    body.push_back(static_cast<char>(0xFF));
    body += "\"}";
    expectRejected(body);
}

TEST(BatchResponseTest, RejectsInvalidEnvelope)
{
    expectRejected("null");
    expectRejected("[]");
    expectRejected("{}");

    auto document = validDocument();
    document.erase("results");
    expectRejected(document.dump());

    document = validDocument();
    document["extra"] = 1;
    expectRejected(document.dump());

    document = validDocument();
    document["results"] = Json::object();
    expectRejected(document.dump());
}

TEST(BatchResponseTest, RequiresIntegerProtocolVersionOne)
{
    const std::vector<Json> invalid_versions = {
        true,
        "1",
        1.0,
        0,
        2,
        nullptr
    };

    for (const auto& version : invalid_versions) {
        auto document = validDocument();
        document["protocol_version"] = version;

        SCOPED_TRACE(document.dump());
        expectRejected(document.dump());
    }
}

TEST(BatchResponseTest, RejectsInvalidResultFieldsAndStatuses)
{
    const std::vector<Json> invalid_items = {
        nullptr,
        Json{{"event_id", "a"}},
        Json{{"status", "accepted"}},
        Json{
            {"event_id", "a"},
            {"status", "accepted"},
            {"extra", 1}
        },
        Json{{"event_id", 1}, {"status", "accepted"}},
        Json{{"event_id", "a"}, {"status", true}},
        Json{{"event_id", "a"}, {"status", "unknown"}}
    };

    for (const auto& item : invalid_items) {
        auto document = validDocument();
        document["results"][0] = item;

        SCOPED_TRACE(document.dump());
        expectRejected(document.dump());
    }
}

TEST(BatchResponseTest, RejectsMissingExtraRepeatedAndUnknownIds)
{
    // 缺少 b。
    expectRejected(validDocument().dump(), {"a", "b"});

    auto document = validDocument();

    // 多出 b。
    document["results"].push_back(
        Json{{"event_id", "b"}, {"status", "accepted"}}
    );
    expectRejected(document.dump(), {"a"});

    // 两条结果都是 a，不能把它们当成 a 和 b 的结果。
    document["results"][1]["event_id"] = "a";
    expectRejected(document.dump(), {"a", "b"});

    // 数量正确，但第二条是未请求的 c。
    document["results"][1]["event_id"] = "c";
    expectRejected(document.dump(), {"a", "b"});
}

TEST(BatchResponseTest, RejectsDuplicateJsonKeys)
{
    expectRejected(R"({
        "protocol_version": 1,
        "protocol_version": 1,
        "results": [
            {"event_id": "a", "status": "accepted"}
        ]
    })");

    expectRejected(R"({
        "protocol_version": 1,
        "results": [
            {
                "event_id": "a",
                "status": "accepted",
                "status": "duplicate"
            }
        ]
    })");
}

TEST(BatchResponseTest, RejectsOversizedResponse)
{
    const std::string body(
        edge::kMaxHttpResponseBytes + 1,
        ' '
    );

    const auto result = edge::parseBatchResponse(body, {"a"});

    const auto* error =
        std::get_if<edge::BatchResponseError>(&result);

    ASSERT_NE(error, nullptr);
    EXPECT_NE(error->message.find("size limit"), std::string::npos);
}

TEST(BatchResponseTest, RejectsExcessiveNesting)
{
    const std::string body =
        std::string(32, '[')
        + "0"
        + std::string(32, ']');

    const auto result = edge::parseBatchResponse(body, {"a"});

    const auto* error =
        std::get_if<edge::BatchResponseError>(&result);

    ASSERT_NE(error, nullptr);
    EXPECT_NE(error->message.find("nesting"), std::string::npos);
}

TEST(BatchResponseTest, RejectsInvalidRequestedIdLists)
{
    const auto body = validDocument().dump();

    EXPECT_THROW(
        edge::parseBatchResponse(body, {}),
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::parseBatchResponse(body, {""}),
        std::invalid_argument
    );

    EXPECT_THROW(
        edge::parseBatchResponse(body, {"a", "a"}),
        std::invalid_argument
    );

    const std::vector<std::string> too_many(101, "a");

    EXPECT_THROW(
        edge::parseBatchResponse(body, too_many),
        std::invalid_argument
    );
}

} // namespace