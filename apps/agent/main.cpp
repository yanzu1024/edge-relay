#include <charconv>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>
#include <system_error>

#include "edge/Version.h"
#include "edge/config/ConfigLoader.h"
#include "RunAgent.h"

namespace {

void printHelp()
{
    std::cout
        << "Usage:\n"
        << "  edge-agent --help\n"
        << "  edge-agent --version\n"
        << "  edge-agent --config PATH --check-config [overrides]\n"
        << "  edge-agent --config PATH --run [overrides] [--jsonl] [--events-jsonl]\n"
        << "  edge-agent --config PATH --drain-only [overrides] [--jsonl] [--events-jsonl]\n"
        << "  edge-agent --config PATH --export-dead PATH [overrides]\n"
        << "\n"
        << "Options:\n"
        << "  --help              Show this help message.\n"
        << "  --version           Show the current version.\n"
        << "  --config PATH       Read configuration from PATH.\n"
        << "  --check-config      Check configuration and print effective settings.\n"
        << "  --run               Start TCP reception.\n"
        << "  --drain-only        Upload existing backlog without TCP reception.\n"
        << "  --export-dead PATH  Export DEAD records without starting the Agent.\n"
        << "  --port PORT         Override the TCP listening port.\n"
        << "  --receiver-url URL  Override the HTTP receiver URL.\n"
        << "  --data-dir DIR      Override database, recording and status paths.\n"
        << "  --jsonl             Use JSONL runtime output.\n"
        << "  --events-jsonl      Include recent records; implies --jsonl.\n";
}

int argumentError(const std::string& message)
{
    std::cerr << message << '\n';
    std::cerr << "Use --help for usage.\n";
    return 2;
}

template <typename Error>
int configurationError(const Error& error)
{
    std::cerr << "Configuration error";

    if (!error.field.empty()) {
        std::cerr << " [" << error.field << ']';
    }

    std::cerr << ": " << error.message << '\n';

    return 2;
}

// 这里只检查参数能否完整表示为 int32_t。
// 端口的 1–65535 业务范围由配置验证器检查。
std::optional<std::int32_t> parsePort(const std::string& text)
{
    if (text.empty()) {
        return std::nullopt;
    }

    std::int32_t port{};

    const auto result = std::from_chars(
        text.data(),
        text.data() + text.size(),
        port,
        10
    );

    if (result.ec != std::errc{}
        || result.ptr != text.data() + text.size()) {
        return std::nullopt;
    }

    return port;
}

int executeConfiguration(
    const std::filesystem::path& path,
    const edge::ConfigOverrides& overrides,
    bool run_mode,
    const edge::AgentRunOptions& run_options,
    const std::optional<std::filesystem::path>& export_path
)
{
    const auto result = edge::loadAgentConfig(path, overrides);

    if (const auto* loaded =
            std::get_if<edge::LoadedAgentConfig>(&result)) {
        const auto& config = loaded->config;

        if (export_path.has_value()) {
            return edge::exportDead(
                config,
                *export_path
            );
        }

        if (run_mode) {
            return edge::runAgent(config, run_options);
        }

        std::cout
            << "Configuration OK\n"
            << "source_file: " << loaded->source_file.string() << '\n'
            << "agent_id: " << config.agent.id << '\n'
            << "tcp_bind: " << config.tcp.bind << '\n'
            << "tcp_port: " << config.tcp.port << '\n'
            << "receiver_url: " << config.upload.url << '\n'
            << "storage_path: " << config.storage.path.string() << '\n'
            << "recording_directory: "
            << config.recording.directory.string() << '\n'
            << "status_file: " << config.status.file.string() << '\n';

        return 0;
    }

    if (const auto* error =
            std::get_if<edge::ConfigFileError>(&result)) {
        return configurationError(*error);
    }

    if (const auto* error =
            std::get_if<edge::ConfigYamlError>(&result)) {
        return configurationError(*error);
    }

    return configurationError(
        std::get<edge::ConfigValidationError>(result)
    );
}

int runCli(int argc, char* argv[])
{
    if (argc == 1) {
        std::cout << "EdgeRelay Agent\n";
        std::cout
            << "Device telemetry collection and forwarding service.\n";
        return 0;
    }

    bool has_config = false;
    bool check_config = false;
    bool run_mode = false;
    bool drain_only = false;
    bool jsonl = false;
    bool events_jsonl = false;

    std::filesystem::path config_file;
    edge::ConfigOverrides overrides;

    std::optional<std::filesystem::path> export_dead;

    for (int index = 1; index < argc; ++index) {
        const std::string argument = argv[index];

        if (argument == "--help" || argument == "--version") {
            if (argc != 2) {
                return argumentError(
                    "Unsupported argument combination."
                );
            }

            if (argument == "--help") {
                printHelp();
            } else {
                std::cout
                    << "Current version: " << edge::version() << '\n';
            }

            return 0;
        }

        if (argument == "--check-config") {
            if (check_config) {
                return argumentError(
                    "Option repeated: --check-config."
                );
            }

            check_config = true;
            continue;
        }

        if (argument == "--run") {
            if (run_mode) {
                return argumentError(
                    "Option repeated: --run."
                );
            }

            run_mode = true;
            continue;
        }

        if (argument == "--drain-only") {
            if (drain_only) {
                return argumentError(
                    "Option repeated: --drain-only."
                );
            }

            drain_only = true;
            continue;
        }

        if (argument == "--jsonl") {
            if (jsonl) {
                return argumentError(
                    "Option repeated: --jsonl."
                );
            }

            jsonl = true;
            continue;
        }

        if (argument == "--events-jsonl") {
            if (events_jsonl) {
                return argumentError(
                    "Option repeated: --events-jsonl."
                );
            }

            events_jsonl = true;
            continue;
        }

        const bool needs_value =
            argument == "--config"
            || argument == "--port"
            || argument == "--receiver-url"
            || argument == "--data-dir"
            || argument == "--export-dead";

        if (!needs_value) {
            return argumentError("Unknown argument: " + argument);
        }

        bool repeated = false;

        if (argument == "--config") {
            repeated = has_config;
        } else if (argument == "--port") {
            repeated = overrides.port.has_value();
        } else if (argument == "--receiver-url") {
            repeated = overrides.receiver_url.has_value();
        } else if(argument == "--data-dir"){
            repeated = overrides.data_dir.has_value();
        }else{
            repeated = export_dead.has_value();
        }

        if (repeated) {
            return argumentError(
                "Option repeated: " + argument + "."
            );
        }

        if (index + 1 >= argc) {
            return argumentError(
                "Missing value for " + argument + "."
            );
        }

        const std::string value = argv[index + 1];

        if (value.rfind("--", 0) == 0) {
            return argumentError(
                "Missing value for " + argument + "."
            );
        }

        ++index;

        if (argument == "--config") {
            config_file = value;
            has_config = true;
        } else if (argument == "--port") {
            const auto port = parsePort(value);

            if (!port.has_value()) {
                return argumentError(
                    "Invalid integer for --port."
                );
            }

            overrides.port = *port;
        } else if (argument == "--receiver-url") {
            overrides.receiver_url = value;
        } else if(argument == "--data-dir"){
            overrides.data_dir = std::filesystem::path(value);
        }else{
            export_dead = 
                std::filesystem::path(value);
        }
    }

    if (export_dead.has_value()) {
        if (export_dead->empty()) {
            return argumentError(
                "--export-dead requires a nonempty output path."
            );
        }

        if (check_config || run_mode || drain_only) {
            return argumentError(
                "--export-dead cannot be combined with --check-config, --run or --drain-only."
            );
        }

        if (jsonl || events_jsonl) {
            return argumentError(
                "--export-dead does not support --jsonl or --events-jsonl."
            );
        }
    }

    if (check_config && run_mode) {
        return argumentError(
            "--check-config and --run are mutually exclusive."
        );
    }

    if (drain_only && (check_config || run_mode)) {
        return argumentError(
            "--drain-only cannot be combined with --run or --check-config."
        );
    }

    if ((jsonl || events_jsonl) && !run_mode && !drain_only) {
        return argumentError(
            "--jsonl and --events-jsonl require --run or --drain-only."
        );
    }

    if (!has_config) {
        if (export_dead.has_value()) {
            return argumentError(
                "--export-dead requires --config PATH."
            );
        }

        if (drain_only) {
            return argumentError(
                "--drain-only requires --config PATH."
            );
        }

        return argumentError(
            run_mode
                ? "--run requires --config PATH."
                : "--check-config requires --config PATH."
        );
    }

    if (
        !check_config
        && !run_mode
        && !drain_only
        && !export_dead.has_value()
    ) {
        return argumentError(
            "--config requires --check-config, --run, --drain-only or --export-dead."
        );
    }

    return executeConfiguration(
        config_file,
        overrides,
        run_mode || drain_only,
        edge::AgentRunOptions{
            jsonl || events_jsonl,
            events_jsonl,
            drain_only
        },
        export_dead
    );
}

} // namespace

int main(int argc, char* argv[])
{
    try {
        return runCli(argc, argv);
    } catch (const std::exception& error) {
        std::cerr << "Startup error: " << error.what() << '\n';
        return 2;
    }
}