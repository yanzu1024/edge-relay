#!/usr/bin/env python3
"""Check empty drain-only exit while the configured TCP port is occupied."""

import argparse
import json
from pathlib import Path
import socket
import sqlite3
from contextlib import closing
import subprocess
import sys
import tempfile


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable", type=Path)
    arguments = parser.parse_args()

    executable = arguments.executable.resolve()

    require(
        executable.is_file(),
        f"Executable does not exist: {executable}",
    )

    with tempfile.TemporaryDirectory(
        prefix="edge drain empty "
    ) as directory:
        root = Path(directory).resolve()

        database = root / "outbox.db"
        status_file = root / "status.json"
        raw_directory = root / "raw"
        config_file = root / "config.yml"

        def quoted(path):
            return json.dumps(str(path), ensure_ascii=False)

        with socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM,
        ) as occupied:
            occupied.bind(("127.0.0.1", 0))
            occupied.listen(1)

            port = occupied.getsockname()[1]

            # recording 故意配置为 true，
            # 检查 drain-only 在运行时关闭录制。
            config_file.write_text(
                f"""version: 1

agent:
  id: drain-empty-test

tcp:
  bind: 127.0.0.1
  port: {port}

storage:
  path: {quoted(database)}

upload:
  url: http://127.0.0.1:18080/v1/events/batch

recording:
  enabled: true
  directory: {quoted(raw_directory)}

status:
  file: {quoted(status_file)}
""",
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    str(executable),
                    "--config", str(config_file),
                    "--drain-only",
                    "--jsonl",
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=10,
                check=False,
            )

            require(
                result.returncode == 0,
                (
                    f"Unexpected exit code: {result.returncode}\n"
                    f"stdout:\n{result.stdout}\n"
                    f"stderr:\n{result.stderr}"
                ),
            )

        require(
            status_file.is_file(),
            "Final status file is missing.",
        )

        status = json.loads(
            status_file.read_text(encoding="utf-8")
        )

        require(
            status["agent_id"] == "drain-empty-test",
            "Unexpected Agent identity.",
        )

        require(
            status["process_state"] == "STOPPED",
            "Agent did not publish STOPPED.",
        )

        require(
            status["active_connections"] == 0,
            "Drain-only reported active TCP connections.",
        )

        require(
            status["tcp_activity"]["accepted_connections"] == 0,
            "Drain-only accepted a TCP connection.",
        )

        require(
            status["upload"]["initialized"] is True
            and status["upload"]["finished"] is True
            and status["upload"]["failed"] is False,
            "Upload worker did not initialize and finish normally.",
        )

        require(
            status["recording"]["enabled"] is False,
            "Recording was not disabled in drain-only mode.",
        )

        require(
            status["outbox"]["state"] == "VALID",
            "Final Outbox sample is not valid.",
        )

        counts = status["outbox"]["value"]

        require(
            counts["total"] == 0
            and counts["backlog"] == 0
            and counts["unacked"] == 0,
            "Empty Outbox counts are incorrect.",
        )

        lines = result.stdout.splitlines()

        require(
            bool(lines),
            "JSONL output is empty.",
        )

        for line in lines:
            require(
                isinstance(json.loads(line), dict),
                "Runtime output contains a non-object JSON value.",
            )

        with closing(sqlite3.connect(
            database.as_uri() + "?mode=ro",
            uri=True,
        )) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM outbox"
            ).fetchone()[0]

        require(
            count == 0,
            "Empty drain created an unexpected event.",
        )

    print("PASS: drain-empty-contract")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)