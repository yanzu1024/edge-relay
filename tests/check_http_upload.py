#!/usr/bin/env python3

from contextlib import closing
import json
import os
from pathlib import Path
import re
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def test_environment():
    environment = os.environ.copy()
    environment["PATH"] = "/usr/bin:/bin"

    for name in (
        "LD_LIBRARY_PATH",
        "LD_PRELOAD",
        "LIBRARY_PATH",
        "CMAKE_PREFIX_PATH",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ):
        environment.pop(name, None)

    environment["NO_PROXY"] = "*"
    environment["no_proxy"] = "*"
    environment["PYTHONUNBUFFERED"] = "1"

    return environment


class Receiver:
    def __init__(self, script, database_path, mode="normal"):
        self.script = script
        self.database_path = database_path
        self.mode = mode

        self.log_path = database_path.parent / f"{mode}.log"
        self.log_file = None
        self.process = None
        self.url = None

    def read_log(self):
        if not self.log_path.exists():
            return ""

        return self.log_path.read_text(
            encoding="utf-8",
            errors="replace",
        )

    def stop(self):
        try:
            if self.process is not None:
                if self.process.poll() is None:
                    self.process.send_signal(signal.SIGINT)

                    try:
                        self.process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        self.process.kill()
                        self.process.wait(timeout=3)
        finally:
            if self.log_file is not None:
                self.log_file.close()
                self.log_file = None

    def __enter__(self):
        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.log_file = self.log_path.open(
            "w",
            encoding="utf-8",
        )

        try:
            self.process = subprocess.Popen(
                [
                    sys.executable,
                    str(self.script),
                    "--bind", "127.0.0.1",
                    "--port", "0",
                    "--db", str(self.database_path),
                    "--mode", self.mode,
                    "--delay-ms", "4000",
                ],
                stdout=self.log_file,
                stderr=subprocess.STDOUT,
                env=test_environment(),
            )

            deadline = time.monotonic() + 5.0

            while time.monotonic() < deadline:
                text = self.read_log()

                if self.process.poll() is not None:
                    raise AssertionError(
                        f"Receiver exited during startup:\n{text}"
                    )

                match = re.search(
                    r"Listening: "
                    r"(http://127\.0\.0\.1:\d+/v1/events/batch)",
                    text,
                )

                if match:
                    self.url = match.group(1)
                    return self

                time.sleep(0.05)

            raise AssertionError(
                f"Receiver startup timed out:\n{self.read_log()}"
            )

        except BaseException:
            self.stop()
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        return False


def run_tool(executable, url, *arguments, expected_code=0):
    environment = test_environment()
    environment["EDGE_TEST_UPLOAD_URL"] = url

    completed = subprocess.run(
        [str(executable), *arguments],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=12,
        env=environment,
    )

    output = completed.stdout + completed.stderr

    require(
        completed.returncode == expected_code,
        (
            f"Unexpected exit code: {completed.returncode}, "
            f"expected {expected_code}\n{output}"
        ),
    )

    return output


def check_batch(batch_tool, url, session, statuses, option=None):
    arguments = [session]

    if option is not None:
        arguments.append(option)

    output = run_tool(
        batch_tool,
        url,
        *arguments,
        expected_code=1 if "conflict" in statuses else 0,
    )

    lines = output.splitlines()

    require("response_valid: yes" in lines, output)

    prefix = f"device-001/{session}/"

    actual = [
        line
        for line in lines
        if line.startswith(prefix)
    ]

    expected = [
        f"{prefix}{index}: {status}"
        for index, status in enumerate(statuses, start=1)
    ]

    require(actual == expected, output)

    conflicts = statuses.count("conflict")
    confirmable = len(statuses) - conflicts

    require(f"confirmable: {confirmable}" in lines, output)
    require(f"conflicts: {conflicts}" in lines, output)


def stored_records(database_path):
    with closing(sqlite3.connect(database_path)) as database:
        rows = database.execute(
            """
            SELECT event_id, payload_json
            FROM received_events
            """
        ).fetchall()

    return {
        event_id: json.loads(payload)
        for event_id, payload in rows
    }


def case_normal(root, batch_tool, receiver_script):
    database = root / "normal" / "receiver.db"
    session = "auto-normal"

    with Receiver(receiver_script, database) as receiver:
        check_batch(
            batch_tool, receiver.url, session,
            ["accepted", "accepted"],
        )

        check_batch(
            batch_tool, receiver.url, session,
            ["duplicate", "duplicate"],
        )

        check_batch(
            batch_tool, receiver.url, session,
            ["conflict", "duplicate"],
            "--conflict-first",
        )

        check_batch(
            batch_tool, receiver.url, session,
            ["duplicate", "duplicate"],
        )

        records = stored_records(database)

        require(len(records) == 2, "Unexpected stored record count")
        require(
            records[f"device-001/{session}/1"]["value"] == 25.5,
            "Conflict overwrote the original event",
        )


def case_encoding(root, batch_tool, receiver_script):
    database = root / "encoding" / "receiver.db"

    with Receiver(receiver_script, database) as receiver:
        output = run_tool(
            batch_tool,
            receiver.url,
            "auto-invalid",
            "--invalid-value",
            expected_code=1,
        )

        require("category: encoding" in output, output)
        require("event_field: value" in output, output)

        output = run_tool(
            batch_tool,
            receiver.url,
            "auto-limit",
            "--batch-limit-one",
            expected_code=1,
        )

        require("category: encoding" in output, output)
        require("Batch exceeds upload.batch_size." in output, output)

        require(
            not stored_records(database),
            "Encoding failures unexpectedly stored events",
        )

        require(
            "POST /v1/events/batch" not in receiver.read_log(),
            "Encoding failures unexpectedly sent HTTP requests",
        )


def case_failure(
    root,
    batch_tool,
    receiver_script,
    mode,
    expected_text,
    saved,
):
    database = root / mode / "receiver.db"
    session = f"auto-{mode}"

    with Receiver(receiver_script, database, mode) as receiver:
        output = run_tool(
            batch_tool,
            receiver.url,
            session,
            expected_code=1,
        )

        require(expected_text in output, output)

        expected_count = 2 if saved else 0

        require(
            len(stored_records(database)) == expected_count,
            f"Unexpected stored count for mode {mode}",
        )

    # 重启接收端，复用同一数据库和同一事件身份。
    with Receiver(receiver_script, database) as receiver:
        expected_status = "duplicate" if saved else "accepted"

        check_batch(
            batch_tool,
            receiver.url,
            session,
            [expected_status, expected_status],
        )

        require(
            len(stored_records(database)) == 2,
            f"Unexpected record count after recovery: {mode}",
        )


def case_response_limit(
    root,
    batch_tool,
    http_tool,
    receiver_script,
):
    database = root / "response-limit" / "receiver.db"
    session = "auto-response-limit"

    with Receiver(receiver_script, database) as receiver:
        events = []

        for sequence in (1, 2):
            event = {
                "schema_version": 1,
                "device_id": "device-001",
                "session_id": session,
                "sequence": sequence,
                "observed_at_ms": 1794441600000,
                "metric": "temperature",
                "value": 25.5,
                "unit": "degC",
            }

            events.append({
                "event_id": f"device-001/{session}/{sequence}",
                "event": event,
            })

        request_path = database.parent / "request.json"

        request_path.write_text(
            json.dumps({
                "protocol_version": 1,
                "events": events,
            }),
            encoding="utf-8",
        )

        output = run_tool(
            http_tool,
            receiver.url,
            str(request_path),
            "--tiny-response",
            expected_code=1,
        )

        require("error: response_too_large" in output, output)

        require(
            len(stored_records(database)) == 2,
            "Receiver did not commit before response was rejected",
        )

        check_batch(
            batch_tool,
            receiver.url,
            session,
            ["duplicate", "duplicate"],
        )


def case_connection_failure(batch_tool):
    # 占住端口但不监听，避免选择端口后被其他进程抢占。
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]

        output = run_tool(
            batch_tool,
            f"http://127.0.0.1:{port}/v1/events/batch",
            "auto-no-listener",
            expected_code=1,
        )

        require("category: request" in output, output)
        require("reason: transport" in output, output)


def run_case(name, function, *arguments):
    function(*arguments)
    print(f"PASS {name}", flush=True)


def main():
    if len(sys.argv) != 4:
        print(
            "Usage: check_http_upload.py "
            "BATCH_TOOL HTTP_TOOL RECEIVER_SCRIPT",
            file=sys.stderr,
        )
        return 2

    batch_tool = Path(sys.argv[1]).resolve()
    http_tool = Path(sys.argv[2]).resolve()
    receiver_script = Path(sys.argv[3]).resolve()

    for path in (batch_tool, http_tool, receiver_script):
        require(path.is_file(), f"File not found: {path}")

    with tempfile.TemporaryDirectory(
        prefix="edge-http-test-",
    ) as temporary:
        root = Path(temporary)

        try:
            run_case(
                "normal_duplicate_conflict",
                case_normal,
                root, batch_tool, receiver_script,
            )

            run_case(
                "encoding_rejections",
                case_encoding,
                root, batch_tool, receiver_script,
            )

            failures = [
                (
                    "http-503",
                    "category: http\nhttp_status: 503",
                    False,
                ),
                (
                    "http-401",
                    "category: http\nhttp_status: 401",
                    False,
                ),
                (
                    "http-409",
                    "category: http\nhttp_status: 409",
                    False,
                ),
                (
                    "http-302",
                    "category: http\nhttp_status: 302",
                    False,
                ),
                (
                    "http-429",
                    "category: http\nhttp_status: 429",
                    False,
                ),
                (
                    "invalid-json",
                    "category: protocol",
                    True,
                ),
                (
                    "drop-after-commit",
                    "category: request\nreason: transport",
                    True,
                ),
                (
                    "delay",
                    "category: request\nreason: timeout",
                    True,
                ),
            ]

            for mode, expected_text, saved in failures:
                run_case(
                    f"failure_{mode}",
                    case_failure,
                    root,
                    batch_tool,
                    receiver_script,
                    mode,
                    expected_text,
                    saved,
                )

            run_case(
                "response_limit_recovery",
                case_response_limit,
                root, batch_tool, http_tool, receiver_script,
            )

            run_case(
                "connection_failure",
                case_connection_failure,
                batch_tool,
            )

        except Exception as error:
            print(f"FAIL: {error}", file=sys.stderr)

            for log_path in sorted(root.rglob("*.log")):
                print(
                    f"\nReceiver log: {log_path.relative_to(root)}",
                    file=sys.stderr,
                )
                print(
                    log_path.read_text(
                        encoding="utf-8",
                        errors="replace",
                    ),
                    file=sys.stderr,
                )

            return 1

    print("All 12 HTTP integration scenarios passed", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())