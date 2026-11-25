#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import shlex
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
CURRENT = ROOT / "run" / "m8-current-session.txt"

EVENT_COUNT = 30


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def write_json(path, value):
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def current_session():
    require(
        CURRENT.is_file(),
        "请先执行 prepare。",
    )

    session = Path(
        CURRENT.read_text(encoding="utf-8").strip()
    )

    require(session.is_dir(), "验收目录不存在。")

    return session


def read_status(session):
    path = session / "agent" / "status.json"

    try:
        return read_json(path)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def prepare():
    run_directory = ROOT / "run"
    run_directory.mkdir(parents=True, exist_ok=True)

    session = Path(tempfile.mkdtemp(
        prefix="m8-acceptance-",
        dir=run_directory,
    ))

    agent = session / "agent"
    evidence = session / "evidence"

    agent.mkdir()
    evidence.mkdir()

    def quoted(path):
        # JSON 字符串的引号形式也可用于 YAML。
        return json.dumps(
            str(path),
            ensure_ascii=False,
        )

    configuration = f"""version: 1

agent:
  id: m8-acceptance-agent
  log_level: info

tcp:
  bind: 127.0.0.1
  port: 19090
  max_payload_bytes: 4096
  partial_timeout_ms: 3000

storage:
  path: {quoted(agent / "outbox.db")}

upload:
  url: http://127.0.0.1:18080/v1/events/batch

recording:
  enabled: true
  directory: {quoted(agent / "raw")}
  max_file_bytes: 4194304
  keep_closed_files: 8

status:
  file: {quoted(agent / "status.json")}
"""

    config_path = session / "config.yml"
    config_path.write_text(
        configuration,
        encoding="utf-8",
    )

    CURRENT.write_text(
        str(session),
        encoding="utf-8",
    )

    print(f"验收目录：{session}")
    print(f"GUI 配置文件：{config_path}")
    print("GUI 数据目录：清空并留空")
    print(f"接收端数据库：{session / 'receiver.db'}")


def receiver(session):
    command = [
        sys.executable,
        str(ROOT / "scripts" / "mock_http_receiver.py"),
        "--bind", "127.0.0.1",
        "--port", "18080",
        "--db", str(session / "receiver.db"),
        "--mode", "normal",
    ]

    print(shlex.join(command), flush=True)

    subprocess.run(
        command,
        cwd=ROOT,
        check=True,
    )


def wait_ready(session):
    deadline = time.monotonic() + 15

    while time.monotonic() < deadline:
        status = read_status(session)

        if (
            status is not None
            and status.get("process_state") == "RUNNING"
            and status.get("upload", {}).get("initialized") is True
            and status.get("recording", {}).get("operational") is True
        ):
            return status

        time.sleep(0.05)

    raise RuntimeError(
        "未等到 Agent 就绪：请确认 GUI 已选择本轮配置、"
        "数据目录留空，并且已成功启动。"
    )


def send(session):
    evidence = session / "evidence"
    manifest_path = evidence / "manifest.json"

    require(
        not manifest_path.exists(),
        "本轮已有发送记录。需要重新验收时执行 prepare，"
        "使用新的会话目录。",
    )

    status = wait_ready(session)

    # 复用工程已经验收过的事件样例和帧编码。
    sys.path.insert(0, str(ROOT / "tests"))
    from check_run import event, frame

    events = [
        event(sequence)
        for sequence in range(1, EVENT_COUNT + 1)
    ]

    frames = [frame(value) for value in events]
    frames.append(frame(events[0]))

    wire = b"".join(frames)

    require(
        len(wire) < 4194304,
        "本轮输入超过单个录制文件上限。",
    )

    write_json(evidence / "events.json", events)
    (evidence / "wire.bin").write_bytes(wire)

    manifest = {
        "run_id": status["run_id"],
        "unique": EVENT_COUNT,
        "frames": len(frames),
        "bytes": len(wire),
        "complete": False,
    }

    # 先记录发送尝试，避免中途失败后重复发送而混淆计数。
    write_json(manifest_path, manifest)

    with socket.create_connection(
        ("127.0.0.1", 19090),
        timeout=5,
    ) as client:
        for packet in frames:
            client.sendall(packet)
            time.sleep(0.03)

    manifest["complete"] = True
    write_json(manifest_path, manifest)

    print(
        f"已发送：唯一事件={EVENT_COUNT}，"
        f"重复事件=1，帧数={len(frames)}，"
        f"字节数={len(wire)}"
    )

    print(f"本轮 run_id：{manifest['run_id']}")


def compare_fields(actual, expected, name, errors):
    for key, expected_value in expected.items():
        actual_value = actual.get(key)

        if (
            type(actual_value) is not type(expected_value)
            or actual_value != expected_value
        ):
            errors.append(
                f"{name}.{key}: "
                f"期望 {expected_value!r}，实际 {actual_value!r}"
            )


def status_errors(status, manifest, final):
    if status is None:
        return ["尚未取得 status.json"]

    errors = []

    compare_fields(
        status,
        {
            "run_id": manifest["run_id"],
            "process_state": "STOPPED" if final else "RUNNING",
            "active_connections": 0,
            "display_dropped": 0,
        },
        "status",
        errors,
    )

    compare_fields(
        status.get("counters", {}),
        {
            "completed_frames": manifest["frames"],
            "valid_events": manifest["frames"],
            "stored_events": manifest["unique"],
            "duplicate_events": 1,
            "identity_conflicts": 0,
            "capacity_rejections": 0,
            "json_errors": 0,
            "validation_errors": 0,
            "serialization_errors": 0,
            "storage_errors": 0,
            "storage_skipped": 0,
            "upload_acked": manifest["unique"],
            "upload_request_failures": 0,
            "upload_http_failures": 0,
            "upload_protocol_failures": 0,
            "upload_errors": 0,
            "upload_warnings": 0,
        },
        "counters",
        errors,
    )

    outbox = status.get("outbox", {})

    compare_fields(
        outbox,
        {"state": "VALID"},
        "outbox",
        errors,
    )

    value = outbox.get("value")
    if not isinstance(value, dict):
        value = {}

    compare_fields(
        value,
        {
            "pending": 0,
            "sending": 0,
            "retry": 0,
            "acked": manifest["unique"],
            "dead": 0,
            "total": manifest["unique"],
            "unacked": 0,
            "backlog": 0,
        },
        "outbox.value",
        errors,
    )

    compare_fields(
        status.get("upload", {}),
        {
            "initialized": True,
            "finished": final,
            "failed": False,
            "blocked": False,
        },
        "upload",
        errors,
    )

    recording = status.get("recording", {})

    compare_fields(
        recording,
        {
            "state": "STOPPED" if final else "ACTIVE",
            "enabled": True,
            "operational": not final,
            "finished": final,
            "bytes_received": manifest["bytes"],
            "bytes_written": manifest["bytes"],
            "bytes_skipped": 0,
            "active_sessions": 0,
            "active_files": 0,
            "startup_errors": 0,
            "event_errors": 0,
            "session_errors": 0,
            "retention_errors": 0,
        },
        "recording",
        errors,
    )

    opened = recording.get("opened_files")
    closed = recording.get("closed_files")

    if (
        type(opened) is not int
        or opened < 1
        or opened != closed
    ):
        errors.append(
            "录制文件未正确关闭："
            f"opened={opened!r}, closed={closed!r}"
        )

    compare_fields(
        status.get("tcp_activity", {}),
        {
            "state": "VALID",
            "accepted_connections": 1,
            "closed_connections": 1,
            "rejected_connections": 0,
            "client_errors": 0,
            "bytes_received": manifest["bytes"],
            "connected_count": 0,
            "waiting_count": 0,
            "active_count": 0,
            "silent_count": 0,
        },
        "tcp_activity",
        errors,
    )

    return errors


def verify_storage(session):
    events = read_json(
        session / "evidence" / "events.json"
    )

    expected = {
        (
            f'{value["device_id"]}/'
            f'{value["session_id"]}/'
            f'{value["sequence"]}'
        ): value
        for value in events
    }

    database = session / "receiver.db"

    require(
        database.is_file(),
        "本轮接收端数据库不存在，请核对 receiver 命令。",
    )

    # 只读连接，核对接收端真正提交的事件及载荷。
    with sqlite3.connect(
        database.as_uri() + "?mode=ro",
        uri=True,
        timeout=2,
    ) as connection:
        rows = connection.execute(
            "SELECT event_id, payload_json FROM received_events"
        ).fetchall()

    received = {
        identifier: json.loads(payload)
        for identifier, payload in rows
    }

    require(
        received == expected,
        "接收端的事件身份或载荷与发送数据不一致。",
    )

    wire = (
        session / "evidence" / "wire.bin"
    ).read_bytes()

    raw_directory = session / "agent" / "raw"

    # 不依赖录制文件命名规则，直接比较实际字节。
    matches = []

    for path in raw_directory.rglob("*"):
        if (
            path.is_file()
            and path.stat().st_size == len(wire)
            and path.read_bytes() == wire
        ):
            matches.append(str(path))

    require(
        len(matches) == 1,
        "未找到唯一的、与输入逐字节一致的原始录制文件。",
    )

    return matches[0]


def check(session, final):
    manifest = read_json(
        session / "evidence" / "manifest.json"
    )

    require(
        manifest.get("complete") is True,
        "本轮发送未完整完成，请使用新会话重新验收。",
    )

    deadline = time.monotonic() + 30
    errors = []

    while time.monotonic() < deadline:
        status = read_status(session)

        errors = status_errors(
            status,
            manifest,
            final,
        )

        if not errors:
            raw_file = verify_storage(session)

            phase = "final" if final else "live"

            print(f"PASS: {phase}")
            print(f"run_id: {manifest['run_id']}")
            print(
                f"入库/接收端事件={manifest['unique']}，"
                f"重复事件=1，"
                f"录制字节={manifest['bytes']}"
            )
            print(f"原始录制：{raw_file}")

            if final:
                write_json(
                    session / "evidence" / "final-status.json",
                    status,
                )

            return

        time.sleep(0.1)

    raise RuntimeError(
        "验收条件未满足：\n" + "\n".join(errors)
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "action",
        choices=(
            "prepare",
            "receiver",
            "send",
            "check",
            "verify",
        ),
    )

    args = parser.parse_args()

    if args.action == "prepare":
        prepare()
        return

    session = current_session()

    if args.action == "receiver":
        receiver(session)
    elif args.action == "send":
        send(session)
    elif args.action == "check":
        check(session, final=False)
    else:
        check(session, final=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n已停止")
        sys.exit(130)
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)