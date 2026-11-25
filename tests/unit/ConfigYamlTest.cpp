#include <gtest/gtest.h>

#include <filesystem>
#include <variant>

#include "edge/config/ConfigValidator.h"
#include "edge/config/ConfigYaml.h"

namespace {

using Code = edge::ConfigYamlCode;

void expectYamlError(
    const char* text,
    Code expected_code,
    const char* expected_field
)
{
    const auto result = edge::parseAgentConfigYaml(text);
    const auto* error = std::get_if<edge::ConfigYamlError>(&result);

    ASSERT_NE(error, nullptr);

    EXPECT_EQ(error->code, expected_code);
    EXPECT_EQ(error->field, expected_field);
    EXPECT_FALSE(error->message.empty());
}

TEST(ConfigYaml, UsesDefaultsForOmittedFields)
{
    const auto result = edge::parseAgentConfigYaml("version: 1\n");
    const auto* config = std::get_if<edge::AgentConfig>(&result);

    ASSERT_NE(config, nullptr);

    EXPECT_EQ(config->agent.id, "demo-agent");
    EXPECT_EQ(config->tcp.port, 19090);
    EXPECT_EQ(config->storage.max_pending_records, 10000);
    EXPECT_EQ(config->upload.batch_size, 20);
    EXPECT_FALSE(config->recording.enabled);
    EXPECT_EQ(config->status.interval_ms, 1000);

    EXPECT_FALSE(edge::validateAgentConfig(*config).has_value());
}

TEST(ConfigYaml, ReadsConfiguredGroups)
{
    const auto result = edge::parseAgentConfigYaml(R"(
version: 1
agent:
  id: yaml-agent
  log_level: debug
tcp:
  port: "19100"
  max_connections: 4
storage:
  path: data/outbox.db
upload:
  batch_size: 10
  retry_initial_ms: 200
recording:
  enabled: true
  directory: data/raw
status:
  interval_ms: 500
  file: data/status.json
)");

    const auto* config = std::get_if<edge::AgentConfig>(&result);

    ASSERT_NE(config, nullptr);

    EXPECT_EQ(config->agent.id, "yaml-agent");
    EXPECT_EQ(config->agent.log_level, "debug");
    EXPECT_EQ(config->tcp.port, 19100);
    EXPECT_EQ(config->tcp.max_connections, 4);
    EXPECT_EQ(
        config->storage.path,
        std::filesystem::path("data/outbox.db")
    );
    EXPECT_EQ(config->upload.batch_size, 10);
    EXPECT_EQ(config->upload.retry_initial_ms, 200);
    EXPECT_TRUE(config->recording.enabled);
    EXPECT_EQ(
        config->recording.directory,
        std::filesystem::path("data/raw")
    );
    EXPECT_EQ(config->status.interval_ms, 500);
    EXPECT_EQ(
        config->status.file,
        std::filesystem::path("data/status.json")
    );

    EXPECT_FALSE(edge::validateAgentConfig(*config).has_value());
}

TEST(ConfigYaml, RejectsBadStructure)
{
    struct Case {
        const char* text;
        Code code;
        const char* field;
    };

    const Case cases[] = {
        {"", Code::InvalidYaml, ""},
        {"version: [", Code::InvalidYaml, ""},
        {"[]", Code::ExpectedMap, ""},
        {"{}", Code::MissingField, "version"},
        {
            "version: 1\n---\nversion: 1\n",
            Code::InvalidYaml,
            ""
        },
        {
            "version: 1\ntcp: []\n",
            Code::ExpectedMap,
            "tcp"
        },
        {
            "version: 1\nextra: true\n",
            Code::UnknownField,
            "extra"
        },
        {
            "version: 1\ntcp:\n  porrt: 19090\n",
            Code::UnknownField,
            "tcp.porrt"
        },
        {
            "version: 1\nversion: 1\n",
            Code::DuplicateField,
            "version"
        },
        {
            "version: 1\ntcp:\n  port: 19090\n  port: 19100\n",
            Code::DuplicateField,
            "tcp.port"
        }
    };

    for (const auto& item : cases) {
        SCOPED_TRACE(item.text);

        expectYamlError(item.text, item.code, item.field);
    }
}

TEST(ConfigYaml, RejectsBadScalarValues)
{
    struct Case {
        const char* text;
        Code code;
        const char* field;
    };

    const Case cases[] = {
        {
            "version: 1.0\n",
            Code::InvalidScalar,
            "version"
        },
        {
            "version: 1\ntcp:\n  port: null\n",
            Code::InvalidFieldType,
            "tcp.port"
        },
        {
            "version: 1\ntcp:\n  port: 2147483648\n",
            Code::IntegerOutOfRange,
            "tcp.port"
        },
        {
            "version: 1\nstorage:\n  max_pending_records: 9223372036854775808\n",
            Code::IntegerOutOfRange,
            "storage.max_pending_records"
        },
        {
            "version: 1\nrecording:\n  enabled: yes\n",
            Code::InvalidScalar,
            "recording.enabled"
        },
        {
            "version: 1\nagent:\n  id: [a, b]\n",
            Code::InvalidFieldType,
            "agent.id"
        }
    };

    for (const auto& item : cases) {
        SCOPED_TRACE(item.text);

        expectYamlError(item.text, item.code, item.field);
    }
}

TEST(ConfigYaml, LeavesBusinessValidationToValidator)
{
    const auto result = edge::parseAgentConfigYaml(
        "version: 1\ntcp:\n  port: 0\n"
    );

    const auto* config = std::get_if<edge::AgentConfig>(&result);

    ASSERT_NE(config, nullptr);
    EXPECT_EQ(config->tcp.port, 0);

    // 0 可以被整数类型保存，但不符合端口的业务约束。
    const auto error = edge::validateAgentConfig(*config);

    ASSERT_TRUE(error.has_value());
    EXPECT_EQ(error->code, edge::ConfigValidationCode::InvalidPort);
    EXPECT_EQ(error->field, "tcp.port");
}

} // namespace