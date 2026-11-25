#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <variant>
#include <vector>
#include <cstdint>

#include <yaml-cpp/yaml.h>

#include "edge/config/ConfigLoader.h"
#include "edge/app/EventStreamProcessor.h"
#include "edge/protocol/FrameEncoder.h"
#include "edge/protocol/FrameFormat.h"

namespace {

namespace fs = std::filesystem;

class ConfigLoaderTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        const auto pattern = (
            fs::temp_directory_path() / "edge-relay-config-XXXXXX"
        ).string();

        // mkdtemp 需要可修改、以空字符结尾的字符数组。
        std::vector<char> writable(pattern.begin(), pattern.end());
        writable.push_back('\0');

        char* created = ::mkdtemp(writable.data());

        ASSERT_NE(created, nullptr);

        root_ = fs::canonical(created);
        fs::create_directories(root_ / "config");
    }

    void TearDown() override
    {
        if (!root_.empty()) {
            std::error_code error;
            fs::remove_all(root_, error);

            EXPECT_FALSE(error) << error.message();
        }
    }

    fs::path writeConfig(std::string_view text)
    {
        const auto path = root_ / "config" / "demo.yml";

        std::ofstream output(path, std::ios::binary);

        if (!output.is_open()) {
            throw std::runtime_error("Cannot create test config");
        }

        output.write(
            text.data(),
            static_cast<std::streamsize>(text.size())
        );

        output.close();

        if (!output) {
            throw std::runtime_error("Cannot write test config");
        }

        return path;
    }

    void expectFileError(
        const fs::path& path,
        edge::ConfigFileCode expected_code,
        const char* expected_field = "config_file"
    )
    {
        const auto result = edge::loadAgentConfig(path);

        const auto* error =
            std::get_if<edge::ConfigFileError>(&result);

        ASSERT_NE(error, nullptr);

        EXPECT_EQ(error->code, expected_code);
        EXPECT_EQ(error->field, expected_field);
        EXPECT_FALSE(error->message.empty());
    }

    fs::path root_;
};

TEST_F(ConfigLoaderTest, LoadsDefaultsAndResolvesPaths)
{
    const auto path = writeConfig("version: 1\n");
    const auto result = edge::loadAgentConfig(path);

    const auto* loaded =
        std::get_if<edge::LoadedAgentConfig>(&result);

    ASSERT_NE(loaded, nullptr);

    EXPECT_EQ(loaded->source_file, fs::canonical(path));

    EXPECT_EQ(
        loaded->config.storage.path,
        root_ / "run" / "agent" / "outbox.db"
    );
    EXPECT_EQ(
        loaded->config.recording.directory,
        root_ / "run" / "agent" / "raw"
    );
    EXPECT_EQ(
        loaded->config.status.file,
        root_ / "run" / "agent" / "status.json"
    );

    // 加载配置只解析路径，不创建运行目录。
    EXPECT_FALSE(fs::exists(root_ / "run"));
}

TEST_F(ConfigLoaderTest, ResolvesConfiguredPaths)
{
    const auto absolute_directory = root_ / "absolute raw";

    // 用 YAML 库输出文本，自动处理路径中的空格等字符。
    YAML::Node yaml;
    yaml["version"] = 1;
    yaml["storage"]["path"] = "data/../data/outbox.db";
    yaml["recording"]["directory"] = absolute_directory.string();
    yaml["status"]["file"] = "state/./status.json";

    YAML::Emitter emitter;
    emitter << yaml;

    ASSERT_TRUE(emitter.good());

    const auto path = writeConfig(emitter.c_str());
    const auto result = edge::loadAgentConfig(path);

    const auto* loaded =
        std::get_if<edge::LoadedAgentConfig>(&result);

    ASSERT_NE(loaded, nullptr);

    EXPECT_EQ(
        loaded->config.storage.path,
        root_ / "config" / "data" / "outbox.db"
    );
    EXPECT_EQ(
        loaded->config.recording.directory,
        absolute_directory
    );
    EXPECT_EQ(
        loaded->config.status.file,
        root_ / "config" / "state" / "status.json"
    );
}

TEST_F(ConfigLoaderTest, AcceptsRelativeConfigFilePath)
{
    const auto path = writeConfig("version: 1\n");

    // 保持工作目录不变，通过相对路径找到配置文件。
    const auto relative = fs::relative(path, fs::current_path());

    const auto result = edge::loadAgentConfig(relative);

    const auto* loaded =
        std::get_if<edge::LoadedAgentConfig>(&result);

    ASSERT_NE(loaded, nullptr);

    EXPECT_EQ(loaded->source_file, fs::canonical(path));
    EXPECT_EQ(
        loaded->config.storage.path,
        root_ / "run" / "agent" / "outbox.db"
    );
}

TEST_F(ConfigLoaderTest, RejectsInvalidFiles)
{
    expectFileError(
        fs::path{},
        edge::ConfigFileCode::InvalidPath
    );

    expectFileError(
        fs::path(std::string("demo.yml") + '\0' + "extra"),
        edge::ConfigFileCode::InvalidPath
    );

    expectFileError(
        root_ / "missing.yml",
        edge::ConfigFileCode::PathResolutionFailed
    );

    expectFileError(
        root_ / "config",
        edge::ConfigFileCode::NotRegularFile
    );
}

TEST_F(ConfigLoaderTest, PropagatesParsingAndValidationErrors)
{
    auto path = writeConfig("version: [");
    auto result = edge::loadAgentConfig(path);

    const auto* yaml_error =
        std::get_if<edge::ConfigYamlError>(&result);

    ASSERT_NE(yaml_error, nullptr);
    EXPECT_EQ(yaml_error->code, edge::ConfigYamlCode::InvalidYaml);

    path = writeConfig("version: 1\ntcp:\n  port: 0\n");
    result = edge::loadAgentConfig(path);

    const auto* validation_error =
        std::get_if<edge::ConfigValidationError>(&result);

    ASSERT_NE(validation_error, nullptr);
    EXPECT_EQ(
        validation_error->code,
        edge::ConfigValidationCode::InvalidPort
    );
    EXPECT_EQ(validation_error->field, "tcp.port");
}

TEST_F(ConfigLoaderTest, RejectsConflictingOutputPaths)
{
    struct Case {
        const char* text;
        const char* field;
    };

    const Case cases[] = {
        {
            "version: 1\n"
            "status:\n"
            "  file: ../run/agent/./outbox.db\n",
            "status.file"
        },
        {
            "version: 1\n"
            "status:\n"
            "  file: demo.yml\n",
            "status.file"
        },
        {
            "version: 1\n"
            "storage:\n"
            "  path: demo.yml\n",
            "storage.path"
        }
    };

    for (const auto& item : cases) {
        SCOPED_TRACE(item.text);

        const auto path = writeConfig(item.text);

        expectFileError(
            path,
            edge::ConfigFileCode::ConflictingPaths,
            item.field
        );
    }
}

TEST_F(ConfigLoaderTest, UsesConfigSymlinkTargetDirectory)
{
    const auto target = writeConfig("version: 1\n");
    const auto link = root_ / "entry.yml";

    fs::create_symlink(target, link);

    const auto result = edge::loadAgentConfig(link);

    const auto* loaded =
        std::get_if<edge::LoadedAgentConfig>(&result);

    ASSERT_NE(loaded, nullptr);

    EXPECT_EQ(loaded->source_file, fs::canonical(target));

    // 基准是实际目标所在的 config 目录，而非链接所在目录。
    EXPECT_EQ(
        loaded->config.storage.path,
        root_ / "run" / "agent" / "outbox.db"
    );
}

TEST_F(ConfigLoaderTest, AppliesOverridesBeforeValidation)
{
    // 这些值类型正确，但原始业务取值不合法。
    const auto path = writeConfig(
        "version: 1\n"
        "agent:\n"
        "  id: file-agent\n"
        "tcp:\n"
        "  port: 0\n"
        "upload:\n"
        "  url: ftp://localhost/events\n"
        "storage:\n"
        "  path: ''\n"
        "recording:\n"
        "  directory: ''\n"
        "status:\n"
        "  file: ''\n"
    );

    edge::ConfigOverrides overrides;

    overrides.port = 19100;
    overrides.receiver_url = "http://localhost:18081/events";
    overrides.data_dir = root_ / "chosen";

    const auto result = edge::loadAgentConfig(path, overrides);

    const auto* loaded =
        std::get_if<edge::LoadedAgentConfig>(&result);

    ASSERT_NE(loaded, nullptr);

    EXPECT_EQ(loaded->config.agent.id, "file-agent");
    EXPECT_EQ(loaded->config.tcp.port, 19100);
    EXPECT_EQ(
        loaded->config.upload.url,
        "http://localhost:18081/events"
    );
    EXPECT_EQ(
        loaded->config.storage.path,
        root_ / "chosen" / "outbox.db"
    );
    EXPECT_EQ(
        loaded->config.recording.directory,
        root_ / "chosen" / "raw"
    );
    EXPECT_EQ(
        loaded->config.status.file,
        root_ / "chosen" / "status.json"
    );
}

TEST_F(ConfigLoaderTest, ResolvesRelativeDataDirectoryFromWorkingDirectory)
{
    const auto path = writeConfig("version: 1\n");
    const auto desired_directory = root_ / "cli data";

    edge::ConfigOverrides overrides;

    overrides.data_dir = fs::relative(
        desired_directory,
        fs::current_path()
    );

    ASSERT_TRUE(overrides.data_dir->is_relative());

    const auto result = edge::loadAgentConfig(path, overrides);

    const auto* loaded =
        std::get_if<edge::LoadedAgentConfig>(&result);

    ASSERT_NE(loaded, nullptr);

    EXPECT_EQ(
        loaded->config.storage.path,
        desired_directory / "outbox.db"
    );
    EXPECT_EQ(
        loaded->config.recording.directory,
        desired_directory / "raw"
    );
    EXPECT_EQ(
        loaded->config.status.file,
        desired_directory / "status.json"
    );
}

TEST_F(ConfigLoaderTest, RejectsInvalidOverrideValues)
{
    const auto path = writeConfig("version: 1\n");

    edge::ConfigOverrides overrides;
    overrides.port = 0;

    const auto bad_port = edge::loadAgentConfig(path, overrides);
    const auto* port_error =
        std::get_if<edge::ConfigValidationError>(&bad_port);

    ASSERT_NE(port_error, nullptr);
    EXPECT_EQ(port_error->field, "tcp.port");

    overrides = edge::ConfigOverrides{};
    overrides.receiver_url = "ftp://localhost/events";

    const auto bad_url = edge::loadAgentConfig(path, overrides);
    const auto* url_error =
        std::get_if<edge::ConfigValidationError>(&bad_url);

    ASSERT_NE(url_error, nullptr);
    EXPECT_EQ(url_error->field, "upload.url");

    overrides = edge::ConfigOverrides{};
    overrides.data_dir = fs::path{};

    const auto bad_directory = edge::loadAgentConfig(path, overrides);
    const auto* directory_error =
        std::get_if<edge::ConfigFileError>(&bad_directory);

    ASSERT_NE(directory_error, nullptr);
    EXPECT_EQ(directory_error->field, "data_dir");
}

TEST_F(ConfigLoaderTest, PassesPayloadLimitToEventProcessor)
{
    const std::string payload = R"({
        "schema_version": 1,
        "device_id": "sim-001",
        "session_id": "184ab3d6-d911-44d5-821f-0123456789ab",
        "sequence": 1,
        "observed_at_ms": 1792713600000,
        "metric": "temperature",
        "value": 23.5,
        "unit": "degC"
    })";

    ASSERT_LT(payload.size(), edge::kMaxFramePayloadBytes);

    // 配置上限恰好等于合法事件载荷的字节数。
    const auto path = writeConfig(
        "version: 1\n"
        "tcp:\n"
        "  max_payload_bytes: "
        + std::to_string(payload.size())
        + "\n"
        "  partial_timeout_ms: 250\n"
    );

    const auto loaded_result = edge::loadAgentConfig(path);

    const auto* loaded =
        std::get_if<edge::LoadedAgentConfig>(&loaded_result);

    ASSERT_NE(loaded, nullptr);

    edge::EventStreamProcessor processor(loaded->config.tcp);

    // 超限载荷仍小于协议硬上限，编码器可以封帧，
    // 但接收端应按照配置上限拒绝它。
    auto input = std::get<edge::EncodedFrame>(
        edge::encodeFrame(
            std::string(payload.size() + 1, 'x')
        )
    );

    const auto valid = std::get<edge::EncodedFrame>(
        edge::encodeFrame(payload)
    );

    input.insert(input.end(), valid.begin(), valid.end());

    constexpr std::int64_t now_ms = 1792713601000;

    const auto results = processor.feed(
        input.data(),
        input.size(),
        now_ms
    );

    ASSERT_EQ(results.size(), 1U);

    EXPECT_TRUE(
        std::holds_alternative<edge::SensorEvent>(results[0])
    );

    const auto stats = processor.stats();

    EXPECT_EQ(stats.framing.invalid_lengths, 1U);
    EXPECT_EQ(stats.framing.completed_frames, 1U);
    EXPECT_EQ(stats.valid_events, 1U);

    // 超限载荷在帧层已经被拒绝，不应进入 JSON 解析。
    EXPECT_EQ(stats.json_errors, 0U);
    EXPECT_EQ(stats.validation_errors, 0U);
}

} // namespace