#!/usr/bin/env python3
"""M9.2：GUI 驱动的三设备、30 分钟持续运行验收。"""

import argparse
from contextlib import ExitStack, closing
import json
import os
from pathlib import Path
import socket
import sqlite3
import statistics
import sys
import tempfile
import time
import uuid

import check_console_m8 as m8


ROOT = Path(__file__).resolve().parents[1]
CURRENT = ROOT / "run" / "m9-soak-current-session.txt"

SECONDS = 1800
RATE = 5
DEVICES = 3

PER_DEVICE = SECONDS * RATE
TOTAL = PER_DEVICE * DEVICES

SAMPLE_SECONDS = 5
RAW_LIMIT = 16 * 1024 * 1024

AGENT_ID = "m9-soak-agent"


def current_session():
    m8.require(CURRENT.is_file(), "请先执行 prepare。")

    session = Path(
        CURRENT.read_text(encoding="utf-8").strip()
    )

    m8.require(session.is_dir(), "验收目录不存在。")
    return session


def prepare():
    directory = ROOT / "run"
    directory.mkdir(parents=True, exist_ok=True)

    session = Path(tempfile.mkdtemp(
        prefix="m9-soak-",
        dir=directory,
    ))

    agent = session / "agent"
    agent.mkdir()
    (session / "evidence").mkdir()

    def quoted(path):
        return json.dumps(str(path), ensure_ascii=False)

    config = f"""version: 1

agent:
  id: {AGENT_ID}
  log_level: info

tcp:
  bind: 127.0.0.1
  port: 19090
  max_connections: 8
  max_payload_bytes: 4096
  partial_timeout_ms: 3000

storage:
  path: {quoted(agent / "outbox.db")}
  max_pending_records: 10000
  max_total_records: 50000
  acked_keep_days: 7
  acked_keep_max_records: 40000

upload:
  url: http://127.0.0.1:18080/v1/events/batch

recording:
  enabled: true
  directory: {quoted(agent / "raw")}
  max_file_bytes: {RAW_LIMIT}
  keep_closed_files: 8

status:
  interval_ms: 1000
  file: {quoted(agent / "status.json")}
"""

    (session / "config.yml").write_text(
        config,
        encoding="utf-8",
    )

    CURRENT.write_text(
        str(session),
        encoding="utf-8",
    )

    print(f"会话：{session}")
    print(f"GUI 配置：{session / 'config.yml'}")
    print("GUI 数据目录：清空并留空")

    print(
        f"负载：{DEVICES} 设备 × "
        f"{RATE} 条/秒 × {SECONDS} 秒 = {TOTAL} 条"
    )


def process_metrics(pid):
    """通过 /proc 采样，同时保留启动时间以识别 PID 复用。"""
    directory = Path("/proc") / str(pid)

    before = (
        (directory / "stat")
        .read_text()
        .rsplit(")", 1)[1]
        .split()
    )

    fields = {}

    for line in (directory / "status").read_text().splitlines():
        key, value = line.split(":", 1)
        fields[key] = value.strip()

    result = {
        "pid": pid,
        "start_ticks": int(before[19]),
        "ppid": int(fields["PPid"]),
        "rss_kib": int(fields["VmRSS"].split()[0]),
        "threads": int(fields["Threads"]),
        "fds": len(list((directory / "fd").iterdir())),
        "cpu_seconds": (
            int(before[11]) + int(before[12])
        ) / os.sysconf("SC_CLK_TCK"),
    }

    after = (
        (directory / "stat")
        .read_text()
        .rsplit(")", 1)[1]
        .split()
    )

    m8.require(
        before[19] == after[19],
        "采样时 PID 被复用。",
    )

    return result


def find_processes(session):
    """只匹配使用本轮配置的 Agent，不操作其他进程。"""
    expected = str(session / "config.yml")
    candidates = []

    for directory in Path("/proc").iterdir():
        if not directory.name.isdecimal():
            continue

        try:
            executable = (directory / "exe").resolve()

            if executable.name != "edge-agent":
                continue

            arguments = (
                (directory / "cmdline")
                .read_bytes()
                .split(b"\0")
            )

            arguments = [
                os.fsdecode(value)
                for value in arguments
            ]

            if any(
                value == expected
                or value == "--config=" + expected
                for value in arguments
            ):
                candidates.append(int(directory.name))

        except (OSError, RuntimeError):
            continue

    m8.require(
        len(candidates) == 1,
        "未找到唯一的本轮 Agent；"
        "请在 GUI 中使用本轮绝对配置路径。",
    )

    agent = process_metrics(candidates[0])
    parent = agent["ppid"]

    parent_executable = (
        Path("/proc") / str(parent) / "exe"
    ).resolve()

    m8.require(
        parent_executable.name == "edge-console",
        "本轮应由 edge-console 启动 Agent。",
    )

    return {
        "agent": agent,
        "console": process_metrics(parent),
    }


def sample(session, manifest, started):
    status = m8.read_status(session)

    m8.require(
        isinstance(status, dict),
        "无法读取状态快照。",
    )

    m8.require(
        status.get("run_id") == manifest["run_id"],
        "运行身份改变。",
    )

    m8.require(
        status.get("process_state") == "RUNNING",
        "Agent 已停止运行。",
    )

    age = abs(
        time.time() * 1000 - status["checked_at_ms"]
    )

    m8.require(
        age <= 15000,
        "状态快照超过 15 秒未更新或系统时钟变化。",
    )

    metrics = {}

    for name, identity in manifest["processes"].items():
        value = process_metrics(identity["pid"])

        m8.require(
            value["start_ticks"] == identity["start_ticks"],
            f"{name} 已退出或 PID 被复用。",
        )

        metrics[name] = value

    sizes = {}

    databases = (
        ("outbox", session / "agent" / "outbox.db"),
        ("receiver", session / "receiver.db"),
    )

    for name, database in databases:
        sizes[name] = {}

        for suffix in ("", "-wal", "-shm"):
            path = Path(str(database) + suffix)

            sizes[name][suffix or "main"] = (
                path.stat().st_size
                if path.exists()
                else 0
            )

    return {
        "elapsed_s": time.monotonic() - started,
        "status": status,
        "processes": metrics,
        "database_bytes": sizes,
    }


def send(session):
    evidence = session / "evidence"
    manifest_path = evidence / "manifest.json"

    m8.require(
        not manifest_path.exists(),
        "本轮已发送或曾中断，请重新 prepare。",
    )

    status = m8.wait_ready(session)

    m8.require(
        status.get("agent_id") == AGENT_ID,
        "请使用本轮 config.yml。",
    )

    m8.require(
        status["counters"]["valid_events"] == 0,
        "本轮已有输入，请使用新会话。",
    )

    sys.path.insert(0, str(ROOT / "tests"))
    from check_run import event, frame

    events = []
    packets = []

    session_id = str(uuid.uuid4())
    observed = time.time_ns() // 1000000

    for device in range(1, DEVICES + 1):
        group = []

        for sequence in range(1, PER_DEVICE + 1):
            value = event(sequence)

            value.update(
                device_id=f"soak_sensor_{device}",
                session_id=session_id,
                observed_at_ms=(
                    observed + (sequence - 1) * 200
                ),
                value=(
                    20.0 + device + (sequence % 16) / 4
                ),
            )

            events.append(value)
            group.append(frame(value))

        wire = b"".join(group)

        m8.require(
            len(wire) < RAW_LIMIT,
            "单设备输入超过录制上限。",
        )

        (evidence / f"wire-{device}.bin").write_bytes(wire)
        packets.append(group)

    m8.write_json(evidence / "events.json", events)

    manifest = {
        "run_id": status["run_id"],
        "unique": TOTAL,
        "frames": TOTAL,
        "bytes": sum(
            len(packet)
            for group in packets
            for packet in group
        ),
        "processes": find_processes(session),
        "complete": False,
        "sent": [0] * DEVICES,
        "elapsed_s": 0,
        "max_lateness_s": 0,
    }

    # 已创建 manifest 的轮次不允许直接重复发送。
    with manifest_path.open("x", encoding="utf-8") as output:
        json.dump(manifest, output, indent=2)

    started = None

    try:
        with ExitStack() as stack:
            clients = [
                stack.enter_context(
                    socket.create_connection(
                        ("127.0.0.1", 19090),
                        timeout=5,
                    )
                )
                for _ in range(DEVICES)
            ]

            log = stack.enter_context(
                (evidence / "samples.jsonl").open(
                    "x",
                    encoding="utf-8",
                )
            )

            started = time.monotonic()
            next_sample = 0.0

            for index in range(PER_DEVICE):
                # 使用绝对单调时间安排发送，避免 sleep 累积漂移。
                target = started + index / RATE

                time.sleep(max(
                    0,
                    target - time.monotonic(),
                ))

                lateness = time.monotonic() - target

                manifest["max_lateness_s"] = max(
                    manifest["max_lateness_s"],
                    lateness,
                )

                m8.require(
                    lateness < 2,
                    "发送落后超过 2 秒；"
                    "本轮中断，不突发补发。",
                )

                for device, client in enumerate(clients):
                    client.sendall(packets[device][index])
                    manifest["sent"][device] += 1

                elapsed = time.monotonic() - started

                if elapsed >= next_sample:
                    row = sample(session, manifest, started)

                    log.write(
                        json.dumps(row, ensure_ascii=False)
                        + "\n"
                    )
                    log.flush()

                    next_sample = (
                        row["elapsed_s"] + SAMPLE_SECONDS
                    )

                if (index + 1) % (60 * RATE) == 0:
                    print(
                        f"已发送 {sum(manifest['sent'])}/{TOTAL} 条，"
                        f"已运行 {elapsed:.1f} 秒",
                        flush=True,
                    )

            # 最后一条发送后仍保持连接，覆盖完整 1800 秒。
            time.sleep(max(
                0,
                started + SECONDS - time.monotonic(),
            ))

            log.write(
                json.dumps(
                    sample(session, manifest, started)
                ) + "\n"
            )

        manifest["complete"] = True

    finally:
        if started is not None:
            manifest["elapsed_s"] = (
                time.monotonic() - started
            )

        m8.write_json(manifest_path, manifest)

    print(
        "发送完成。保持 GUI 和接收端运行，"
        "接着执行 check。"
    )


def status_errors(status, manifest, final):
    if not isinstance(status, dict):
        return ["尚无状态快照"]

    errors = []

    def compare(actual, expected, name):
        m8.compare_fields(
            actual or {},
            expected,
            name,
            errors,
        )

    compare(
        status,
        {
            "agent_id": AGENT_ID,
            "run_id": manifest["run_id"],
            "process_state": (
                "STOPPED" if final else "RUNNING"
            ),
            "active_connections": 0,
            "display_dropped": 0,
        },
        "status",
    )

    counters = {
        key: 0
        for key in (
            "duplicate_events",
            "identity_conflicts",
            "capacity_rejections",
            "json_errors",
            "validation_errors",
            "partial_timeouts",
            "serialization_errors",
            "storage_errors",
            "storage_skipped",
            "acked_deleted",
            "recovered_sending",
            "upload_retry",
            "upload_dead",
            "upload_request_failures",
            "upload_http_failures",
            "upload_protocol_failures",
            "upload_errors",
            "upload_warnings",
        )
    }

    counters.update(
        completed_frames=TOTAL,
        valid_events=TOTAL,
        stored_events=TOTAL,
        upload_acked=TOTAL,
    )

    compare(
        status.get("counters"),
        counters,
        "counters",
    )

    compare(
        status.get("outbox"),
        {"state": "VALID"},
        "outbox",
    )

    compare(
        (status.get("outbox") or {}).get("value"),
        {
            "pending": 0,
            "sending": 0,
            "retry": 0,
            "dead": 0,
            "acked": TOTAL,
            "total": TOTAL,
            "unacked": 0,
            "backlog": 0,
        },
        "outbox.value",
    )

    compare(
        status.get("upload"),
        {
            "initialized": True,
            "finished": final,
            "failed": False,
            "blocked": False,
        },
        "upload",
    )

    compare(
        status.get("recording"),
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
            "connections_seen": DEVICES,
            "opened_files": DEVICES,
            "closed_files": DEVICES,
            "files_deleted": 0,
            "startup_errors": 0,
            "event_errors": 0,
            "session_errors": 0,
            "retention_errors": 0,
        },
        "recording",
    )

    compare(
        status.get("tcp_activity"),
        {
            "state": "VALID",
            "accepted_connections": DEVICES,
            "closed_connections": DEVICES,
            "rejected_connections": 0,
            "client_errors": 0,
            "bytes_received": manifest["bytes"],
            "connected_count": 0,
            "waiting_count": 0,
            "active_count": 0,
            "silent_count": 0,
        },
        "tcp_activity",
    )

    compare(
        status.get("monitoring"),
        {
            "outbox_query_errors": 0,
            "disk_query_errors": 0,
            "output_errors": 0,
        },
        "monitoring",
    )

    return errors


def verify_data(session):
    evidence = session / "evidence"

    events = m8.read_json(evidence / "events.json")

    expected = {
        (
            f"{value['device_id']}/"
            f"{value['session_id']}/"
            f"{value['sequence']}"
        ): value
        for value in events
    }

    m8.require(
        len(events) == len(expected) == TOTAL,
        "预期事件证据不完整。",
    )

    databases = (
        (session / "receiver.db", "received_events"),
        (session / "agent" / "outbox.db", "outbox"),
    )

    for database, table in databases:
        m8.require(
            database.is_file(),
            f"数据库缺失：{database}",
        )

        with closing(sqlite3.connect(
            database.as_uri() + "?mode=ro",
            uri=True,
            timeout=2,
        )) as connection:
            # table 只来自上面的固定列表。
            rows = connection.execute(
                f"SELECT event_id, payload_json FROM {table}"
            ).fetchall()

            actual = {
                key: json.loads(payload)
                for key, payload in rows
            }

            m8.require(
                len(rows) == TOTAL and actual == expected,
                f"{table} 的完整 ID/载荷不一致。",
            )

            if table == "outbox":
                states = dict(connection.execute(
                    "SELECT state, COUNT(*) "
                    "FROM outbox GROUP BY state"
                ))

                m8.require(
                    states == {"ACKED": TOTAL},
                    "Outbox 未全部 ACKED。",
                )

    raw_files = [
        path
        for path in (session / "agent" / "raw").rglob("*")
        if path.is_file()
    ]

    used = set()

    for device in range(1, DEVICES + 1):
        wire = (
            evidence / f"wire-{device}.bin"
        ).read_bytes()

        matches = [
            path
            for path in raw_files
            if path.stat().st_size == len(wire)
            and path.read_bytes() == wire
        ]

        m8.require(
            len(matches) == 1 and matches[0] not in used,
            f"设备 {device} 未找到唯一匹配的完整录制。",
        )

        used.add(matches[0])

    return [str(path) for path in sorted(used)]


def resource_report(session, manifest):
    sample_path = (
        session / "evidence" / "samples.jsonl"
    )

    rows = [
        json.loads(line)
        for line in sample_path.read_text(
            encoding="utf-8"
        ).splitlines()
    ]

    m8.require(
        len(rows) >= SECONDS // (SAMPLE_SECONDS + 1),
        "资源采样数量不足。",
    )

    times = [row["elapsed_s"] for row in rows]

    m8.require(
        times[0] < 5 and times[-1] >= SECONDS,
        "采样未覆盖完整 30 分钟。",
    )

    gaps = [
        later - earlier
        for earlier, later in zip(times, times[1:])
    ]

    m8.require(
        all(0 < gap <= 15 for gap in gaps),
        "资源采样存在异常间隔。",
    )

    for row in rows:
        status = row["status"]

        m8.require(
            status["run_id"] == manifest["run_id"]
            and status["process_state"] == "RUNNING",
            "采样运行身份不一致。",
        )

        # 排除刚建立连接时的快照延迟。
        if 10 <= row["elapsed_s"] < SECONDS - 2:
            m8.require(
                status["active_connections"] == DEVICES,
                "持续运行期间未保持三连接。",
            )

        for name, identity in manifest["processes"].items():
            actual = row["processes"][name]

            m8.require(
                (
                    actual["pid"],
                    actual["start_ticks"],
                ) == (
                    identity["pid"],
                    identity["start_ticks"],
                ),
                "进程身份改变。",
            )

    report = {
        "sample_count": len(rows),
        "duration_s": manifest["elapsed_s"],
        "max_sample_gap_s": max(gaps),
        "processes": {},
    }

    for name in ("agent", "console"):
        values = [
            row["processes"][name]
            for row in rows
        ]

        rss = [value["rss_kib"] for value in values]
        quarter = max(1, len(values) // 4)

        report["processes"][name] = {
            "rss_first_quarter_median_kib": (
                statistics.median(rss[:quarter])
            ),
            "rss_last_quarter_median_kib": (
                statistics.median(rss[-quarter:])
            ),
            "rss_peak_kib": max(rss),
            "fd_min": min(value["fds"] for value in values),
            "fd_max": max(value["fds"] for value in values),
            "fd_first": values[0]["fds"],
            "fd_last": values[-1]["fds"],
            "threads_min": min(
                value["threads"] for value in values
            ),
            "threads_max": max(
                value["threads"] for value in values
            ),
            # 单核占用口径，100% 表示占满一个逻辑核。
            "average_cpu_percent_one_core": 100 * (
                values[-1]["cpu_seconds"]
                - values[0]["cpu_seconds"]
            ) / (times[-1] - times[0]),
        }

    report["unacked_peak"] = max(
        row["status"]["outbox"]["value"]["unacked"]
        for row in rows
        if row["status"]["outbox"]["state"] == "VALID"
    )

    report["unacked_first"] = (
        rows[0]["status"]["outbox"]["value"]["unacked"]
    )

    report["unacked_last_sample"] = (
        rows[-1]["status"]["outbox"]["value"]["unacked"]
    )

    report["database_bytes_first"] = (
        rows[0]["database_bytes"]
    )

    report["database_bytes_last"] = (
        rows[-1]["database_bytes"]
    )

    return report


def check(session, final):
    evidence = session / "evidence"

    manifest = m8.read_json(
        evidence / "manifest.json"
    )

    m8.require(
        manifest["complete"] is True
        and manifest["sent"] == [PER_DEVICE] * DEVICES
        and manifest["elapsed_s"] >= SECONDS,
        "本轮发送中断或时长不足。",
    )

    deadline = time.monotonic() + 60

    while True:
        status = m8.read_status(session)

        errors = status_errors(
            status,
            manifest,
            final,
        )

        if not errors:
            break

        m8.require(
            time.monotonic() < deadline,
            "\n".join(errors),
        )

        time.sleep(0.2)

    raw_files = verify_data(session)
    report = resource_report(session, manifest)

    phase = "final" if final else "live"

    report.update(
        phase=phase,
        events=TOTAL,
        raw_files=raw_files,
    )

    if final:
        m8.write_json(
            evidence / "final-status.json",
            status,
        )

    m8.write_json(
        evidence / f"report-{phase}.json",
        report,
    )

    print(f"PASS: soak-{phase}")

    print(json.dumps(
        report,
        ensure_ascii=False,
        indent=2,
    ))

    print(
        "资源数值用于趋势复核；"
        "本结果不等于证明无内存泄漏。"
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

    action = parser.parse_args().action

    if action == "prepare":
        prepare()
        return

    session = current_session()

    if action == "receiver":
        m8.receiver(session)
    elif action == "send":
        send(session)
    else:
        check(session, final=action == "verify")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(
            "\n本轮中断；请保留证据，"
            "使用新会话重新验收。",
            file=sys.stderr,
        )
        sys.exit(130)
    except Exception as error:
        print(
            f"FAIL: {error}",
            file=sys.stderr,
        )
        sys.exit(1)