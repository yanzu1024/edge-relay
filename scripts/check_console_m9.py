#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
import socket
import sys
import tempfile
import time

import check_console_m8 as m8


ROOT = Path(__file__).resolve().parents[1]
CURRENT = ROOT / "run" / "m9-current-session.txt"

EVENT_COUNT = 1000


def current_session():
    m8.require(
        CURRENT.is_file(),
        "请先执行 M9 的 prepare。",
    )

    session = Path(
        CURRENT.read_text(encoding="utf-8").strip()
    )

    m8.require(
        session.is_dir(),
        "M9 验收目录不存在。",
    )

    return session


def prepare():
    run_directory = ROOT / "run"
    run_directory.mkdir(parents=True, exist_ok=True)

    session = Path(tempfile.mkdtemp(
        prefix="m9-acceptance-",
        dir=run_directory,
    ))

    agent = session / "agent"
    evidence = session / "evidence"

    agent.mkdir()
    evidence.mkdir()

    def quoted(path):
        return json.dumps(
            str(path),
            ensure_ascii=False,
        )

    configuration = f"""version: 1

agent:
  id: m9-acceptance-agent
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

    print(f"M9 验收目录：{session}")
    print(f"GUI 配置文件：{config_path}")
    print("GUI 数据目录：清空并留空")
    print(f"接收端数据库：{session / 'receiver.db'}")
    print(f"唯一事件数量：{EVENT_COUNT}")


def send(session):
    evidence = session / "evidence"
    manifest_path = evidence / "manifest.json"

    m8.require(
        not manifest_path.exists(),
        "本轮已有发送记录；重新验收请执行 M9 prepare。",
    )

    status = m8.wait_ready(session)

    m8.require(
        status.get("agent_id") == "m9-acceptance-agent",
        "Agent 标识不匹配，请选择本轮 M9 config.yml。",
    )

    sys.path.insert(0, str(ROOT / "tests"))
    from check_run import event, frame

    events = [
        event(sequence)
        for sequence in range(1, EVENT_COUNT + 1)
    ]

    frames = [frame(value) for value in events]

    # 追加完全相同的第一条事件，验证本地去重。
    frames.append(frame(events[0]))

    wire = b"".join(frames)

    m8.require(
        len(wire) < 4194304,
        "输入超过本轮单个录制文件上限。",
    )

    m8.write_json(
        evidence / "events.json",
        events,
    )

    (evidence / "wire.bin").write_bytes(wire)

    manifest = {
        "run_id": status["run_id"],
        "unique": EVENT_COUNT,
        "frames": len(frames),
        "bytes": len(wire),
        "complete": False,
    }

    m8.write_json(manifest_path, manifest)

    with socket.create_connection(
        ("127.0.0.1", 19090),
        timeout=5,
    ) as client:
        for index, packet in enumerate(frames, start=1):
            client.sendall(packet)

            if index % 100 == 0:
                print(
                    f"已发送 {index}/{len(frames)} 帧",
                    flush=True,
                )

            # 保持正常展示负载，便于核对完整接收数量。
            time.sleep(0.03)

    manifest["complete"] = True
    m8.write_json(manifest_path, manifest)

    print(
        f"发送完成：唯一事件={EVENT_COUNT}，"
        f"重复事件=1，"
        f"总帧数={len(frames)}，"
        f"总字节数={len(wire)}"
    )

    print(f"本轮 run_id：{manifest['run_id']}")


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
        m8.receiver(session)
    elif args.action == "send":
        send(session)
    elif args.action == "check":
        m8.check(session, final=False)
    else:
        m8.check(session, final=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n已停止")
        sys.exit(130)
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)