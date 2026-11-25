#!/usr/bin/env python3
"""Check drain-only backlog, retry deadlines, DEAD retention and restart."""

import argparse
from contextlib import closing
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time

from check_run import event


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def read_json(path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def rows(database, table="outbox"):
    require(
        table in ("outbox", "received_events"),
        "Invalid table.",
    )

    with closing(sqlite3.connect(
        database.as_uri() + "?mode=ro",
        uri=True,
        timeout=2,
    )) as connection:
        connection.row_factory = sqlite3.Row

        return {
            row["event_id"]: dict(row)
            for row in connection.execute(
                f"SELECT * FROM {table}"
            )
        }


def identifier(value):
    return (
        f"{value['device_id']}/"
        f"{value['session_id']}/"
        f"{value['sequence']}"
    )


def insert_fixture(database, values, states, due=None):
    """只向本测试的独立数据库注入状态夹具。"""
    now = time.time_ns() // 1000000

    with closing(sqlite3.connect(
        database,
        timeout=2,
    )) as connection:
        for value, state in zip(values, states):
            connection.execute(
                """
                INSERT INTO outbox (
                    event_id,
                    payload_json,
                    state,
                    attempt_count,
                    next_attempt_at_ms,
                    created_at_ms,
                    updated_at_ms,
                    acked_at_ms,
                    last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    identifier(value),
                    json.dumps(value, separators=(",", ":")),
                    state,
                    1 if state != "PENDING" else 0,
                    due
                    if state == "RETRY" and due is not None
                    else 0,
                    now,
                    now,
                    now if state == "ACKED" else None,
                    "retained rejection"
                    if state == "DEAD"
                    else "",
                ),
            )

        connection.commit()


class Harness:
    def __init__(self, executable, receiver, root):
        self.executable = executable
        self.receiver = receiver
        self.root = root

        self.database = root / "outbox.db"
        self.status_file = root / "status.json"
        self.config_file = root / "config.yml"
        self.receiver_db = root / "receiver.db"

        self.children = []
        self.logs = {}

    def spawn(self, label, command):
        out_path = self.root / f"{label}.stdout.log"
        err_path = self.root / f"{label}.stderr.log"

        with (
            out_path.open("wb") as output,
            err_path.open("wb") as error,
        ):
            process = subprocess.Popen(
                command,
                stdout=output,
                stderr=error,
            )

        self.children.append(process)
        self.logs[process.pid] = (out_path, err_path)

        return process

    def details(self, process):
        return "\n".join(
            f"{path.name}:\n"
            + path.read_text(
                encoding="utf-8",
                errors="replace",
            )[-5000:]
            for path in self.logs[process.pid]
        )

    def wait_for(
        self,
        process,
        predicate,
        label,
        timeout=5,
    ):
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(
                    f"Process exited while waiting for {label}.\n"
                    + self.details(process)
                )

            value = predicate()

            if value:
                return value

            time.sleep(0.05)

        raise RuntimeError(
            f"Timed out waiting for {label}.\n"
            + self.details(process)
        )

    def start_receiver(self):
        process = self.spawn(
            "receiver",
            [
                sys.executable,
                str(self.receiver),
                "--bind", "127.0.0.1",
                "--port", "0",
                "--db", str(self.receiver_db),
                "--mode", "normal",
            ],
        )

        def address():
            text = self.logs[process.pid][0].read_text(
                encoding="utf-8"
            )

            for line in text.splitlines():
                if line.startswith("Listening: "):
                    return line.removeprefix(
                        "Listening: "
                    ).strip()

            return None

        return self.wait_for(
            process,
            address,
            "receiver startup",
        )

    def configure(self, tcp_port, receiver_url):
        def quoted(value):
            return json.dumps(
                str(value),
                ensure_ascii=False,
            )

        self.config_file.write_text(
            f"""version: 1

agent:
  id: drain-backlog-test

tcp:
  bind: 127.0.0.1
  port: {tcp_port}

storage:
  path: {quoted(self.database)}

upload:
  url: {quoted(receiver_url)}
  flush_interval_ms: 100
  connect_timeout_ms: 500
  request_timeout_ms: 1500
  retry_initial_ms: 200
  retry_max_ms: 1000

recording:
  enabled: true
  directory: {quoted(self.root / "raw")}

status:
  interval_ms: 100
  file: {quoted(self.status_file)}
""",
            encoding="utf-8",
        )

    def start(self, label, receiver_url=None):
        previous = read_json(self.status_file) or {}

        command = [
            str(self.executable),
            "--config", str(self.config_file),
            "--drain-only",
            "--jsonl",
        ]

        if receiver_url is not None:
            command += [
                "--receiver-url",
                receiver_url,
            ]

        process = self.spawn(label, command)

        return process, previous.get("run_id")

    def running(
        self,
        run,
        predicate=lambda status: True,
    ):
        process, previous = run

        def ready():
            status = read_json(self.status_file)

            if (
                status
                and status.get("run_id") != previous
                and status.get("process_state") == "RUNNING"
                and predicate(status)
            ):
                return status

            return None

        return self.wait_for(
            process,
            ready,
            "fresh RUNNING status",
        )

    def finish(self, run, timeout=20):
        process, previous = run

        process.wait(timeout=timeout)

        require(
            process.returncode == 0,
            self.details(process),
        )

        status = read_json(self.status_file)

        require(
            status
            and status.get("run_id") not in (None, previous),
            "Final status did not belong to this run.",
        )

        require(
            status["process_state"] == "STOPPED",
            "Expected STOPPED.",
        )

        require(
            status["outbox"]["state"] == "VALID",
            "Invalid Outbox sample.",
        )

        require(
            status["upload"]["finished"] is True
            and status["upload"]["failed"] is False,
            "Upload worker failed.",
        )

        require(
            status["active_connections"] == 0
            and status["tcp_activity"]["accepted_connections"] == 0,
            "Drain-only used TCP reception.",
        )

        require(
            status["recording"]["enabled"] is False,
            "Recording enabled.",
        )

        lines = self.logs[process.pid][0].read_text(
            encoding="utf-8"
        ).splitlines()

        require(
            lines
            and all(
                isinstance(json.loads(line), dict)
                for line in lines
            ),
            "Invalid JSONL output.",
        )

        label = (
            self.logs[process.pid][0]
            .name.removesuffix(".stdout.log")
        )

        (self.root / f"{label}.final-status.json").write_text(
            json.dumps(
                status,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        return status

    def received(self, expected):
        actual = {
            key: json.loads(row["payload_json"])
            for key, row in rows(
                self.receiver_db,
                "received_events",
            ).items()
        }

        require(
            actual == expected,
            "Receiver IDs or full payloads differ.",
        )

    def close(self):
        # 只回收本测试自己创建的进程。
        for process in reversed(self.children):
            if process.poll() is None:
                process.terminate()

                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)


def completed(status, acked, dead, uploaded):
    value = status["outbox"]["value"]

    expected = {
        "pending": 0,
        "sending": 0,
        "retry": 0,
        "acked": acked,
        "dead": dead,
        "total": acked + dead,
        "unacked": dead,
        "backlog": 0,
    }

    require(
        all(
            value.get(key) == item
            for key, item in expected.items()
        ),
        f"Unexpected Outbox counts: {value}",
    )

    require(
        status["counters"]["upload_acked"] == uploaded,
        "Per-run ACK counter differs.",
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable", type=Path)
    parser.add_argument("receiver", type=Path)
    args = parser.parse_args()

    executable = args.executable.resolve(strict=True)
    receiver = args.receiver.resolve(strict=True)

    # 保留证据目录，失败时也可查看数据库、配置和各轮日志。
    root = Path(tempfile.mkdtemp(
        prefix="edge-drain-backlog-"
    )).resolve()

    harness = Harness(executable, receiver, root)

    print(f"Evidence: {root}", flush=True)

    try:
        with socket.socket() as tcp, socket.socket() as offline:
            # 始终占住采集端口，证明各轮不创建 TCP 监听。
            tcp.bind(("127.0.0.1", 0))
            tcp.listen(1)

            # 只绑定而不 listen，使 HTTP 连接失败。
            # 保持 socket 存在，防止端口被其他服务抢占。
            offline.bind(("127.0.0.1", 0))

            offline_url = (
                "http://127.0.0.1:"
                f"{offline.getsockname()[1]}"
                "/v1/events/batch"
            )

            receiver_url = harness.start_receiver()

            harness.configure(
                tcp.getsockname()[1],
                receiver_url,
            )

            # 先让真实 Agent 创建正确标识和版本的数据库。
            completed(
                harness.finish(
                    harness.start("initialize")
                ),
                0,
                0,
                0,
            )

            values = [
                event(sequence)
                for sequence in range(1, 8)
            ]

            insert_fixture(
                harness.database,
                values[:5],
                [
                    "PENDING",
                    "SENDING",
                    "RETRY",
                    "ACKED",
                    "DEAD",
                ],
            )

            before = rows(harness.database)

            status = harness.finish(
                harness.start("mixed")
            )

            completed(status, 4, 1, 3)

            require(
                status["counters"]["recovered_sending"] == 1,
                "SENDING recovery count differs.",
            )

            after = rows(harness.database)

            for value in values[3:5]:
                key = identifier(value)

                require(
                    after[key] == before[key],
                    "Existing ACKED/DEAD row changed.",
                )

            expected = {
                identifier(value): value
                for value in values[:3]
            }

            harness.received(expected)

            print(
                "PASS: mixed backlog, SENDING recovery "
                "and full payloads"
            )

            # 只有 ACKED/DEAD 时，不应再次上传或继续等待。
            completed(
                harness.finish(
                    harness.start("no-backlog")
                ),
                4,
                1,
                0,
            )

            harness.received(expected)

            print(
                "PASS: existing DEAD does not block completion"
            )

            # 单独留下一个尚未到期的 RETRY。
            due = time.time_ns() // 1000000 + 3000

            insert_fixture(
                harness.database,
                [values[5]],
                ["RETRY"],
                due,
            )

            run = harness.start("future-retry")

            harness.running(
                run,
                lambda status: (
                    status["outbox"].get("value") or {}
                ).get("retry") == 1,
            )

            time.sleep(0.25)

            require(
                run[0].poll() is None,
                "Exited before RETRY became eligible.",
            )

            require(
                time.time_ns() // 1000000 < due - 100,
                "Test missed the pre-deadline observation window.",
            )

            harness.received(expected)

            completed(
                harness.finish(run),
                5,
                1,
                1,
            )

            expected[identifier(values[5])] = values[5]
            harness.received(expected)

            print(
                "PASS: future RETRY waits and is eventually uploaded"
            )

            # 网络失败时不允许提前退出或删除未确认记录。
            insert_fixture(
                harness.database,
                [values[6]],
                ["PENDING"],
            )

            run = harness.start(
                "offline-stop",
                offline_url,
            )

            harness.running(
                run,
                lambda status: (
                    status["counters"]["upload_request_failures"] >= 1
                ),
            )

            require(
                run[0].poll() is None,
                "Exited during a retryable failure.",
            )

            started = time.monotonic()
            run[0].terminate()

            stopped = harness.finish(
                run,
                timeout=8,
            )

            elapsed = time.monotonic() - started

            require(
                elapsed < 8,
                "Normal stop exceeded the 8-second budget.",
            )

            retained = rows(harness.database)[
                identifier(values[6])
            ]

            require(
                retained["state"] in ("PENDING", "RETRY")
                and json.loads(
                    retained["payload_json"]
                ) == values[6],
                "Unacknowledged event was lost or misclassified.",
            )

            require(
                stopped["outbox"]["value"]["backlog"] == 1
                and stopped["outbox"]["value"]["unacked"] == 2,
                "Stopped state did not retain backlog and DEAD.",
            )

            harness.received(expected)

            print(
                "PASS: offline retries and normal stop "
                f"({elapsed:.3f}s)"
            )

            # 回到正常接收端，处理上轮保留的事件。
            completed(
                harness.finish(
                    harness.start("restart")
                ),
                6,
                1,
                1,
            )

            expected[identifier(values[6])] = values[6]
            harness.received(expected)

            require(
                rows(harness.database)[
                    identifier(values[4])
                ] == before[identifier(values[4])],
                "DEAD row changed during stop/restart.",
            )

            print(
                "PASS: restart uploads retained event "
                "and preserves DEAD"
            )

        print("PASS: drain-backlog-contract")
        return 0

    finally:
        harness.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)