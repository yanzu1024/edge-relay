#!/usr/bin/env python3
"""Check TCP reception, Outbox persistence and process lifecycle."""

import argparse
from contextlib import closing
import json
import os
from pathlib import Path
import selectors
import signal
import socket
import sqlite3
import struct
import subprocess
import tempfile
import time


SESSION = "12345678-1234-1234-1234-123456789abc"


def expect(condition, message):
    if not condition:
        raise AssertionError(message)


def unused_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def event_id(sequence):
    return f"runtime_sensor/{SESSION}/{sequence}"


def event(sequence):
    return {
        "schema_version": 1,
        "device_id": "runtime_sensor",
        "session_id": SESSION,
        "sequence": sequence,
        "observed_at_ms": 1700000000000,
        "metric": "temperature",
        "value": 23.5,
        "unit": "degC",
    }


def frame(payload):
    if isinstance(payload, dict):
        payload = json.dumps(payload, separators=(",", ":"))

    encoded = payload.encode("utf-8")
    return b"ETLM" + struct.pack("!I", len(encoded)) + encoded


def wait_for(process, needle, captured, occurrences=1):
    deadline = time.monotonic() + 5

    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)

        while captured.count(needle) < occurrences:
            remaining = deadline - time.monotonic()

            if remaining <= 0:
                raise RuntimeError(f"timed out waiting for {needle!r}")

            if not selector.select(remaining):
                continue

            chunk = os.read(process.stdout.fileno(), 4096)

            if not chunk:
                raise RuntimeError(
                    f"stdout closed before receiving {needle!r}"
                )

            captured.extend(chunk)


def stats(
    completed=0,
    valid=0,
    json_errors=0,
    validation_errors=0,
    stored=0,
    duplicates=0,
    conflicts=0,
    capacity=0,
    recovered=0,
    upload_cycles=0,
    upload_claimed=0,
    upload_acked=0,
    upload_retry=0,
    upload_dead=0,
    upload_request_failures=0,
    upload_http_failures=0,
    upload_protocol_failures=0,
    upload_errors=0,
    upload_warnings=0,
    upload_blocked=0,
    upload_blocked_http_status=0,
):
    return (
        "Stopped\n"
        f"completed_frames: {completed}\n"
        f"valid_events: {valid}\n"
        f"json_errors: {json_errors}\n"
        f"validation_errors: {validation_errors}\n"
        "partial_timeouts: 0\n"
        f"stored_events: {stored}\n"
        f"duplicate_events: {duplicates}\n"
        f"identity_conflicts: {conflicts}\n"
        f"capacity_rejections: {capacity}\n"
        "serialization_errors: 0\n"
        "storage_errors: 0\n"
        "storage_skipped: 0\n"
        "acked_deleted: 0\n"
        f"recovered_sending: {recovered}\n"
        f"upload_cycles: {upload_cycles}\n"
        f"upload_claimed: {upload_claimed}\n"
        f"upload_acked: {upload_acked}\n"
        f"upload_retry: {upload_retry}\n"
        f"upload_dead: {upload_dead}\n"
        f"upload_request_failures: {upload_request_failures}\n"
        f"upload_http_failures: {upload_http_failures}\n"
        f"upload_protocol_failures: {upload_protocol_failures}\n"
        f"upload_errors: {upload_errors}\n"
        f"upload_warnings: {upload_warnings}\n"
        f"upload_blocked: {upload_blocked}\n"
        f"upload_blocked_http_status: "
        f"{upload_blocked_http_status}\n"
    ).encode()


def check_result(result, stdout, stderr=b"", code=0):
    actual_code, actual_stdout, actual_stderr = result

    expect(
        actual_code == code
        and actual_stdout == stdout
        and actual_stderr == stderr,
        f"code={actual_code}, stdout={actual_stdout!r}, "
        f"stderr={actual_stderr!r}",
    )


def read_rows(data_dir):
    path = data_dir / "outbox.db"

    expect(path.is_file(), f"database missing: {path}")

    # Connection 的上下文管理器不负责 close，因此使用 closing。
    with closing(sqlite3.connect(path, timeout=1)) as database:
        return database.execute(
            "SELECT event_id, payload_json, state, attempt_count, "
            "created_at_ms, updated_at_ms "
            "FROM outbox ORDER BY event_id"
        ).fetchall()


class Agent:
    def __init__(
            self, 
            executable, 
            config, 
            data_dir, 
            run_first=False,
            extra_options=(),
    ):
        self.port = unused_port()
        self.captured = bytearray()
        self.result = None

        options = (
            [
                "--run", "--port", str(self.port),
                "--config", str(config),
            ]
            if run_first
            else [
                "--config", str(config),
                "--run", "--port", str(self.port),
            ]
        )

        self.process = subprocess.Popen(
            [
                str(executable),
                *options,
                "--data-dir", str(data_dir),
                *extra_options,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )

        self.ready = (
            f"Listening on 127.0.0.1:{self.port}\n"
        ).encode()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if self.result is None:
                if self.process.poll() is None:
                    self.process.kill()

                tail, stderr = self.process.communicate(timeout=5)
                self.captured.extend(tail)

                if exc_type is not None:
                    print(
                        f"  child stdout={bytes(self.captured)!r}, "
                        f"stderr={stderr!r}"
                    )
        finally:
            self.process.stdout.close()
            self.process.stderr.close()

    def wait_ready(self):
        wait_for(self.process, self.ready, self.captured)

    def wait_line(self, line, occurrences=1):
        wait_for(
            self.process,
            line.encode(),
            self.captured,
            occurrences,
        )

    def send(self, wire):
        with socket.create_connection(
            ("127.0.0.1", self.port),
            timeout=3,
        ) as client:
            # 保留原有的分片发送检查。
            client.sendall(wire[:3])
            client.sendall(wire[3:])

    def finish(self, signum=signal.SIGTERM):
        if self.result is None:
            if self.process.poll() is None:
                self.process.send_signal(signum)

            tail, stderr = self.process.communicate(timeout=5)
            self.captured.extend(tail)

            self.result = (
                self.process.returncode,
                bytes(self.captured),
                stderr,
            )

        return self.result


def write_config(path, small_capacity=False):
    text = (
        "version: 1\n"
        "tcp:\n"
        "  bind: 127.0.0.1\n"
        "  port: 0\n"
    )

    if small_capacity:
        text += (
            "storage:\n"
            "  max_pending_records: 1\n"
            "  max_total_records: 2\n"
            "  acked_keep_days: 7\n"
            "  acked_keep_max_records: 1\n"
        )

    # 原有场景专门验证接收、存储和进程生命周期。
    # 使用合法的长聚合间隔，将上传行为隔离到专门测试。
    text += (
        "upload:\n"
        "  batch_size: 100\n"
        "  flush_interval_ms: 60000\n"
    )

    if small_capacity:
        text += (
            "status:\n"
            "  pending_warn_records: 1\n"
        )

    path.write_text(text, encoding="utf-8")


def check_config_case(executable, config, root):
    data_dir = root / "config-check"

    result = subprocess.run(
        [
            str(executable),
            "--config", str(config),
            "--check-config",
            "--port", str(unused_port()),
            "--data-dir", str(data_dir),
        ],
        capture_output=True,
        timeout=5,
        check=False,
    )

    expect(
        result.returncode == 0
        and result.stdout.startswith(b"Configuration OK\n")
        and result.stderr == b"",
        f"code={result.returncode}, stdout={result.stdout!r}, "
        f"stderr={result.stderr!r}",
    )

    expect(
        not data_dir.exists(),
        "--check-config created the data directory",
    )


def signal_case(executable, config, root, signum, send_events):
    data_dir = root / signal.Signals(signum).name

    with Agent(
        executable,
        config,
        data_dir,
        run_first=(signum == signal.SIGINT),
    ) as agent:
        agent.wait_ready()

        event_output = b""

        if send_events:
            agent.send(
                frame(event(1))
                + frame("{")
                + frame(event(0))
                + frame(event(2))
            )

            agent.wait_line(f"Event stored: {event_id(2)}\n")

            event_output = (
                f"Event stored: {event_id(1)}\n"
                f"Event stored: {event_id(2)}\n"
            ).encode()

        result = agent.finish(signum)

        expected_stats = (
            stats(
                completed=4,
                valid=2,
                json_errors=1,
                validation_errors=1,
                stored=2,
            )
            if send_events
            else stats()
        )

        expected_stderr = (
            b"Event rejected: json\n"
            b"Event rejected: validation\n"
            if send_events
            else b""
        )

        check_result(
            result,
            agent.ready + event_output + expected_stats,
            expected_stderr,
        )

    rows = read_rows(data_dir)

    expect(
        len(rows) == (2 if send_events else 0),
        f"unexpected database rows: {rows!r}",
    )

    if send_events:
        expect(
            [json.loads(row[1]) for row in rows]
            == [event(1), event(2)],
            f"unexpected stored payloads: {rows!r}",
        )


def occupied_port_case(executable, config, root):
    data_dir = root / "occupied-port"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as blocker:
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)

        result = subprocess.run(
            [
                str(executable),
                "--config", str(config),
                "--run",
                "--port", str(blocker.getsockname()[1]),
                "--data-dir", str(data_dir),
            ],
            capture_output=True,
            timeout=5,
            check=False,
        )

    expect(
        result.returncode == 1
        and result.stdout == b""
        and b"Runtime error [bind]:" in result.stderr,
        f"code={result.returncode}, stdout={result.stdout!r}, "
        f"stderr={result.stderr!r}",
    )

    expect(
        not data_dir.exists(),
        "occupied port created storage files",
    )


def duplicate_conflict_capacity_case(executable, root):
    config = root / "capacity.yml"
    write_config(config, small_capacity=True)

    data_dir = root / "capacity"
    changed = event(1)
    changed["value"] = 99.0

    with Agent(executable, config, data_dir) as agent:
        agent.wait_ready()

        agent.send(
            frame(event(1))
            + frame(event(1))
            + frame(changed)
            + frame(event(2))
            + frame(event(1))
        )

        agent.wait_line(
            f"Event duplicate: {event_id(1)}\n",
            occurrences=2,
        )

        # Agent 仍在运行时读取，确认 stored 输出对应已提交数据。
        rows = read_rows(data_dir)

        expect(
            len(rows) == 1
            and json.loads(rows[0][1]) == event(1),
            f"unexpected database rows: {rows!r}",
        )

        expected_stdout = agent.ready + (
            f"Event stored: {event_id(1)}\n"
            f"Event duplicate: {event_id(1)}\n"
            f"Event duplicate: {event_id(1)}\n"
        ).encode() + stats(
            completed=5,
            valid=5,
            stored=1,
            duplicates=2,
            conflicts=1,
            capacity=1,
        )

        expected_stderr = (
            f"Event rejected: identity_conflict {event_id(1)}\n"
            f"Event rejected: storage_capacity {event_id(2)} "
            "kind=pending pending=1 total=1 limit=1\n"
        ).encode()

        check_result(
            agent.finish(),
            expected_stdout,
            expected_stderr,
        )


def restart_case(executable, config, root):
    data_dir = root / "restart"

    with Agent(executable, config, data_dir) as first:
        first.wait_ready()
        first.send(frame(event(1)))
        first.wait_line(f"Event stored: {event_id(1)}\n")

        before = read_rows(data_dir)
        expect(len(before) == 1, f"unexpected rows: {before!r}")

        check_result(
            first.finish(signal.SIGKILL),
            first.ready + f"Event stored: {event_id(1)}\n".encode(),
            code=-signal.SIGKILL,
        )

    with Agent(executable, config, data_dir) as second:
        second.wait_ready()
        second.send(frame(event(1)) + frame(event(2)))
        second.wait_line(f"Event stored: {event_id(2)}\n")

        check_result(
            second.finish(),
            second.ready + (
                f"Event duplicate: {event_id(1)}\n"
                f"Event stored: {event_id(2)}\n"
            ).encode() + stats(
                completed=2,
                valid=2,
                stored=1,
                duplicates=1,
            ),
        )

    after = read_rows(data_dir)

    expect(
        len(after) == 2 and after[0] == before[0],
        f"restart changed existing record: before={before!r}, "
        f"after={after!r}",
    )


def corrupt_database_case(executable, config, root):
    data_dir = root / "corrupt"
    data_dir.mkdir()

    path = data_dir / "outbox.db"
    original = b"not a SQLite database"
    path.write_bytes(original)

    result = subprocess.run(
        [
            str(executable),
            "--config", str(config),
            "--run",
            "--port", str(unused_port()),
            "--data-dir", str(data_dir),
        ],
        capture_output=True,
        timeout=5,
        check=False,
    )

    expect(
        result.returncode == 1
        and result.stdout == b""
        and b"Runtime error [storage]:" in result.stderr,
        f"code={result.returncode}, stdout={result.stdout!r}, "
        f"stderr={result.stderr!r}",
    )

    expect(path.read_bytes() == original, "corrupt file was overwritten")


def same_database_case(executable, config, root):
    data_dir = root / "single-owner"

    with Agent(executable, config, data_dir) as owner:
        owner.wait_ready()

        result = subprocess.run(
            [
                str(executable),
                "--config", str(config),
                "--run",
                "--port", str(unused_port()),
                "--data-dir", str(data_dir),
            ],
            capture_output=True,
            timeout=5,
            check=False,
        )

        expect(
            result.returncode == 1
            and result.stdout == b""
            and b"Outbox lock" in result.stderr,
            f"code={result.returncode}, stdout={result.stdout!r}, "
            f"stderr={result.stderr!r}",
        )

        # 第二个实例失败后，第一个仍正常工作。
        owner.send(frame(event(1)))
        owner.wait_line(f"Event stored: {event_id(1)}\n")

        check_result(
            owner.finish(),
            owner.ready
            + f"Event stored: {event_id(1)}\n".encode()
            + stats(completed=1, valid=1, stored=1),
        )


def recovery_case(executable, config, root):
    data_dir = root / "sending-recovery"

    # 先通过真实 TCP 链路产生五条已提交记录。
    with Agent(executable, config, data_dir) as seed:
        seed.wait_ready()

        seed.send(
            b"".join(frame(event(sequence)) for sequence in range(1, 6))
        )

        seed.wait_line(f"Event stored: {event_id(5)}\n")

        expected_events = "".join(
            f"Event stored: {event_id(sequence)}\n"
            for sequence in range(1, 6)
        ).encode()

        check_result(
            seed.finish(),
            seed.ready
            + expected_events
            + stats(completed=5, valid=5, stored=5),
        )

    # Agent 停止后构造遗留状态, 独立核对下一次启动恢复
    now_ms = int(time.time() * 1000)

    with closing(sqlite3.connect(data_dir / "outbox.db")) as database:
        database.execute(
            "UPDATE outbox SET state = 'SENDING', "
            "attempt_count = 3, next_attempt_at_ms = ?, "
            "updated_at_ms = ?, last_error = 'interrupted attempt' "
            "WHERE event_id = ?",
            (now_ms + 60000, now_ms - 10000, event_id(1)),
        )

        database.execute(
            "UPDATE outbox SET state = 'RETRY', "
            "attempt_count = 7, next_attempt_at_ms = ? "
            "WHERE event_id = ?",
            (now_ms + 120000, event_id(2)),
        )

        database.execute(
            "UPDATE outbox SET state = 'ACKED', acked_at_ms = ? "
            "WHERE event_id = ?",
            (now_ms, event_id(3)),
        )

        database.execute(
            "UPDATE outbox SET state = 'DEAD' WHERE event_id = ?",
            (event_id(4),),
        )

        database.commit()
        database.row_factory = sqlite3.Row

        before = {
            row["event_id"]: dict(row)
            for row in database.execute("SELECT * FROM outbox")
        }

    with Agent(executable, config, data_dir) as agent:
        agent.wait_ready()

        # Listening 输出前，启动恢复必须已经提交。
        with closing(sqlite3.connect(data_dir / "outbox.db")) as database:
            database.row_factory = sqlite3.Row

            after = {
                row["event_id"]: dict(row)
                for row in database.execute("SELECT * FROM outbox")
            }

        expect(
            after.keys() == before.keys(),
            "startup recovery changed event identities",
        )

        recovered = after[event_id(1)]
        original = before[event_id(1)]

        expect(recovered["state"] == "RETRY", f"bad recovery: {recovered!r}")

        for field in (
            "event_id",
            "payload_json",
            "attempt_count",
            "created_at_ms",
        ):
            expect(
                recovered[field] == original[field],
                f"recovery changed {field}: {recovered!r}",
            )

        expect(
            recovered["next_attempt_at_ms"]
            == recovered["updated_at_ms"]
            and recovered["updated_at_ms"] > 0
            and recovered["updated_at_ms"] != original["updated_at_ms"],
            f"bad recovery timestamps: {recovered!r}",
        )

        expect(
            recovered["last_error"] != original["last_error"],
            f"recovery reason was not recorded: {recovered!r}",
        )

        for sequence in range(2, 6):
            expect(
                after[event_id(sequence)] == before[event_id(sequence)],
                f"startup changed unrelated event {sequence}",
            )

        check_result(
            agent.finish(),
            agent.ready + stats(recovered=1),
        )


def run_case(name, action):
    try:
        action()
        print(f"PASS {name}")
        return True
    except Exception as error:
        print(f"FAIL {name}: {error}")
        return False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable", type=Path)
    arguments = parser.parse_args()

    executable = arguments.executable.resolve()

    with tempfile.TemporaryDirectory(prefix="edge runtime ") as directory:
        root = Path(directory)
        config = root / "demo.yml"
        write_config(config)

        checks = [
            run_case(
                "check_config_no_storage",
                lambda: check_config_case(executable, config, root),
            ),
            run_case(
                "stop_SIGINT",
                lambda: signal_case(
                    executable, config, root, signal.SIGINT, False
                ),
            ),
            run_case(
                "stop_SIGTERM",
                lambda: signal_case(
                    executable, config, root, signal.SIGTERM, True
                ),
            ),
            run_case(
                "occupied_port",
                lambda: occupied_port_case(executable, config, root),
            ),
            run_case(
                "duplicate_conflict_capacity",
                lambda: duplicate_conflict_capacity_case(executable, root),
            ),
            run_case(
                "restart_after_SIGKILL",
                lambda: restart_case(executable, config, root),
            ),
            run_case(
                "corrupt_database",
                lambda: corrupt_database_case(executable, config, root),
            ),
            run_case(
                "same_database_in_use",
                lambda: same_database_case(executable, config, root),
            ),
            run_case(
                "startup_sending_recovery",
                lambda: recovery_case(executable, config, root),
            ),
        ]

    passed = sum(checks)
    print(f"Runtime scenarios: {passed}/{len(checks)} passed")

    return 0 if passed == len(checks) else 4


if __name__ == "__main__":
    raise SystemExit(main())