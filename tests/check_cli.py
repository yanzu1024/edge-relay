#!/usr/bin/env python3
"""Check EdgeRelay CLI behavior using only the Python standard library."""

import argparse
from pathlib import Path
import subprocess
import sys
import tempfile


INTRO = (
    "EdgeRelay Agent\n"
    "Device telemetry collection and forwarding service.\n"
)

HELP = (
    "Usage:\n"
    "  edge-agent --help\n"
    "  edge-agent --version\n"
    "  edge-agent --config PATH --check-config [overrides]\n"
    "  edge-agent --config PATH --run [overrides] [--jsonl] [--events-jsonl]\n"
    "  edge-agent --config PATH --drain-only [overrides] [--jsonl] [--events-jsonl]\n"
    "  edge-agent --config PATH --export-dead PATH [overrides]\n"
    "\n"
    "Options:\n"
    "  --help              Show this help message.\n"
    "  --version           Show the current version.\n"
    "  --config PATH       Read configuration from PATH.\n"
    "  --check-config      Check configuration and print effective settings.\n"
    "  --run               Start TCP reception.\n"
    "  --drain-only        Upload existing backlog without TCP reception.\n"
    "  --export-dead PATH  Export DEAD records without starting the Agent.\n"
    "  --port PORT         Override the TCP listening port.\n"
    "  --receiver-url URL  Override the HTTP receiver URL.\n"
    "  --data-dir DIR      Override database, recording and status paths.\n"
    "  --jsonl             Use JSONL runtime output.\n"
    "  --events-jsonl      Include recent records; implies --jsonl.\n"
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable", type=Path)
    parser.add_argument("expected_version")
    arguments = parser.parse_args()

    executable = arguments.executable.resolve()

    if not executable.is_file():
        print(
            f"FAIL: executable does not exist: {executable}",
            file=sys.stderr,
        )
        return 4

    def check_case(
        name,
        options,
        expected_code,
        stdout="",
        stderr="",
        stderr_contains=None,
    ):
        try:
            result = subprocess.run(
                [str(executable), *options],
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=5,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired, UnicodeError) as error:
            print(f"FAIL {name}: {error}", file=sys.stderr)
            return False

        differences = []

        if result.returncode != expected_code:
            differences.append(
                f"  returncode: expected {expected_code!r}, "
                f"got {result.returncode!r}"
            )

        if result.stdout != stdout:
            differences.append(
                f"  stdout: expected {stdout!r}, "
                f"got {result.stdout!r}"
            )

        if stderr_contains is None:
            if result.stderr != stderr:
                differences.append(
                    f"  stderr: expected {stderr!r}, "
                    f"got {result.stderr!r}"
                )
        elif stderr_contains not in result.stderr:
            # 文件系统和 YAML 库的完整错误文案可能不同，
            # 这类场景检查稳定的字段或错误前缀。
            differences.append(
                f"  stderr: expected to contain {stderr_contains!r}, "
                f"got {result.stderr!r}"
            )

        if differences:
            print(f"FAIL {name}", file=sys.stderr)
            print("\n".join(differences), file=sys.stderr)
            return False

        print(f"PASS {name}")
        return True

    # 路径中加入空格，检查程序是否正确接收完整参数。
    with tempfile.TemporaryDirectory(prefix="edge-relay cli ") as directory:
        root = Path(directory).resolve()
        config_dir = root / "config"
        config_dir.mkdir()

        valid_file = config_dir / "demo config.yml"
        valid_file.write_text(
            "version: 1\n"
            "agent:\n"
            "  id: cli-agent\n"
            "tcp:\n"
            "  port: 19100\n",
            encoding="utf-8",
        )

        invalid_yaml = config_dir / "invalid.yml"
        invalid_yaml.write_text("version: [\n", encoding="utf-8")

        invalid_value = config_dir / "invalid-port.yml"
        invalid_value.write_text(
            "version: 1\n"
            "tcp:\n"
            "  port: 0\n",
            encoding="utf-8",
        )

        missing_file = config_dir / "missing.yml"

        def config_output(
            source=valid_file,
            port=19100,
            receiver_url="http://127.0.0.1:18080/v1/events/batch",
            data_dir=None,
        ):
            directory = (
                root / "run" / "agent"
                if data_dir is None
                else Path(data_dir).resolve()
            )

            return (
                "Configuration OK\n"
                f"source_file: {Path(source).resolve()}\n"
                "agent_id: cli-agent\n"
                "tcp_bind: 127.0.0.1\n"
                f"tcp_port: {port}\n"
                f"receiver_url: {receiver_url}\n"
                f"storage_path: {directory / 'outbox.db'}\n"
                f"recording_directory: {directory / 'raw'}\n"
                f"status_file: {directory / 'status.json'}\n"
            )

        expected_config_output = config_output()

        # 列表中的函数调用按顺序执行，记录每个场景的检查结果。
        checks = [
            check_case(
                "no_arguments", [], 0,
                stdout=INTRO,
            ),
            check_case(
                "help", ["--help"], 0,
                stdout=HELP,
            ),
            check_case(
                "version", ["--version"], 0,
                stdout=f"Current version: {arguments.expected_version}\n",
            ),
            check_case(
                "unknown_argument", ["--unknown"], 2,
                stderr="Unknown argument: --unknown\nUse --help for usage.\n",
            ),
            check_case(
                "unsupported_combination", ["--help", "extra"], 2,
                stderr=(
                    "Unsupported argument combination.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "check_config",
                ["--config", str(valid_file), "--check-config"],
                0,
                stdout=expected_config_output,
            ),
            check_case(
                "check_config_reordered",
                ["--check-config", "--config", str(valid_file)],
                0,
                stdout=expected_config_output,
            ),
            check_case(
                "missing_config_value", ["--config"], 2,
                stderr="Missing value for --config.\nUse --help for usage.\n",
            ),
            check_case(
                "missing_config_option", ["--check-config"], 2,
                stderr=(
                    "--check-config requires --config PATH.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "missing_check_mode", ["--config", str(valid_file)], 2,
                stderr=(
                    "--config requires --check-config, --run, --drain-only or --export-dead.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "duplicate_config",
                [
                    "--config", str(valid_file),
                    "--config", str(valid_file),
                    "--check-config",
                ],
                2,
                stderr="Option repeated: --config.\nUse --help for usage.\n",
            ),
            check_case(
                "duplicate_check_mode",
                [
                    "--check-config", "--check-config",
                    "--config", str(valid_file),
                ],
                2,
                stderr=(
                    "Option repeated: --check-config.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "invalid_yaml",
                ["--config", str(invalid_yaml), "--check-config"],
                2,
                stderr_contains="Configuration error:",
            ),
            check_case(
                "invalid_config_value",
                ["--config", str(invalid_value), "--check-config"],
                2,
                stderr_contains="[tcp.port]",
            ),
            check_case(
                "missing_config_file",
                ["--config", str(missing_file), "--check-config"],
                2,
                stderr_contains="[config_file]",
            ),
        ]

        custom_dir = root / "custom data"
        relative_dir = Path("cli-relative-" + root.name)
        receiver = "http://localhost:18081/v1/events/batch"

        repair_file = config_dir / "repair.yml"
        repair_file.write_text(
            "version: 1\n"
            "agent:\n"
            "  id: cli-agent\n"
            "tcp:\n"
            "  port: 0\n"
            "upload:\n"
            "  url: ftp://localhost/events\n"
            "storage:\n"
            "  path: ''\n"
            "recording:\n"
            "  directory: ''\n"
            "status:\n"
            "  file: ''\n",
            encoding="utf-8",
        )

        common = ["--config", str(valid_file), "--check-config"]

        checks.extend([
            check_case(
                "override_port",
                common + ["--port", "19101"],
                0,
                stdout=config_output(port=19101),
            ),
            check_case(
                "override_receiver_url",
                common + ["--receiver-url", receiver],
                0,
                stdout=config_output(receiver_url=receiver),
            ),
            check_case(
                "override_data_dir",
                common + ["--data-dir", str(custom_dir)],
                0,
                stdout=config_output(data_dir=custom_dir),
            ),
            check_case(
                "override_all",
                [
                    "--port", "19102",
                    "--data-dir", str(custom_dir),
                    "--check-config",
                    "--receiver-url", receiver,
                    "--config", str(valid_file),
                ],
                0,
                stdout=config_output(
                    port=19102,
                    receiver_url=receiver,
                    data_dir=custom_dir,
                ),
            ),
            check_case(
                "override_relative_data_dir",
                common + ["--data-dir", str(relative_dir)],
                0,
                stdout=config_output(data_dir=relative_dir),
            ),
            check_case(
                "invalid_port_text",
                common + ["--port", "abc"],
                2,
                stderr="Invalid integer for --port.\nUse --help for usage.\n",
            ),
            check_case(
                "invalid_port_range",
                common + ["--port", "0"],
                2,
                stderr_contains="[tcp.port]",
            ),
            check_case(
                "invalid_receiver_url",
                common + ["--receiver-url", "ftp://localhost/events"],
                2,
                stderr_contains="[upload.url]",
            ),
            check_case(
                "empty_data_dir",
                common + ["--data-dir", ""],
                2,
                stderr_contains="[data_dir]",
            ),
            check_case(
                "duplicate_port",
                common + ["--port", "19101", "--port", "19102"],
                2,
                stderr="Option repeated: --port.\nUse --help for usage.\n",
            ),
            check_case(
                "missing_receiver_url_value",
                common + ["--receiver-url"],
                2,
                stderr=(
                    "Missing value for --receiver-url.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "repairs_file_values",
                [
                    "--config", str(repair_file),
                    "--check-config",
                    "--port", "19102",
                    "--receiver-url", receiver,
                    "--data-dir", str(custom_dir),
                ],
                0,
                stdout=config_output(
                    source=repair_file,
                    port=19102,
                    receiver_url=receiver,
                    data_dir=custom_dir,
                ),
            ),
        ])

        checks.extend([
            check_case(
                "run_requires_config",
                ["--run"],
                2,
                stderr=(
                    "--run requires --config PATH.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "duplicate_run",
                ["--config", str(valid_file), "--run", "--run"],
                2,
                stderr=(
                    "Option repeated: --run.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "mutually_exclusive_modes",
                [
                    "--config", str(valid_file),
                    "--run", "--check-config",
                ],
                2,
                stderr=(
                    "--check-config and --run are mutually exclusive.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "run_invalid_yaml",
                ["--run", "--config", str(invalid_yaml)],
                2,
                stderr_contains="Configuration error:",
            ),
            check_case(
                "run_invalid_config",
                ["--run", "--config", str(invalid_value)],
                2,
                stderr_contains="[tcp.port]",
            ),
            check_case(
                "run_missing_file",
                ["--run", "--config", str(missing_file)],
                2,
                stderr_contains="[config_file]",
            ),
            check_case(
                "jsonl_requires_run",
                ["--jsonl"],
                2,
                stderr=(
                    "--jsonl and --events-jsonl require --run or --drain-only.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "events_jsonl_requires_run",
                [
                    "--config", str(valid_file),
                    "--check-config", "--events-jsonl",
                ],
                2,
                stderr=(
                    "--jsonl and --events-jsonl require --run or --drain-only.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "duplicate_jsonl",
                [
                    "--config", str(valid_file),
                    "--run", "--jsonl", "--jsonl",
                ],
                2,
                stderr=(
                    "Option repeated: --jsonl.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "duplicate_events_jsonl",
                [
                    "--config", str(valid_file),
                    "--run",
                    "--events-jsonl", "--events-jsonl",
                ],
                2,
                stderr=(
                    "Option repeated: --events-jsonl.\n"
                    "Use --help for usage.\n"
                ),
            ),
        ])

        checks.extend([
            check_case(
                "drain_requires_config",
                ["--drain-only"],
                2,
                stderr=(
                    "--drain-only requires --config PATH.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "duplicate_drain",
                [
                    "--config", str(valid_file),
                    "--drain-only", "--drain-only",
                ],
                2,
                stderr=(
                    "Option repeated: --drain-only.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "drain_conflicts_with_run",
                [
                    "--config", str(valid_file),
                    "--drain-only", "--run",
                ],
                2,
                stderr=(
                    "--drain-only cannot be combined with --run or --check-config.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "drain_conflicts_with_check",
                [
                    "--config", str(valid_file),
                    "--check-config", "--drain-only",
                ],
                2,
                stderr=(
                    "--drain-only cannot be combined with --run or --check-config.\n"
                    "Use --help for usage.\n"
                ),
            ),
            check_case(
                "drain_invalid_yaml",
                [
                    "--config", str(invalid_yaml),
                    "--drain-only",
                ],
                2,
                stderr_contains="Configuration error:",
            ),
            check_case(
                "drain_invalid_config",
                [
                    "--config", str(invalid_value),
                    "--drain-only",
                ],
                2,
                stderr_contains="[tcp.port]",
            ),
            check_case(
                "drain_missing_file",
                [
                    "--config", str(missing_file),
                    "--drain-only",
                ],
                2,
                stderr_contains="[config_file]",
            ),
        ])

    passed = sum(checks)
    print(f"CLI scenarios: {passed}/{len(checks)} passed")

    return 0 if passed == len(checks) else 4


if __name__ == "__main__":
    sys.exit(main())