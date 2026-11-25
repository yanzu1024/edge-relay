#!/usr/bin/env python3

from contextlib import closing
import json
from pathlib import Path
import socket
import sqlite3
import sys
import tempfile
import urllib.request

from check_http_upload import (
    Receiver,
    require,
    run_tool,
    stored_records,
)


def event_items(session):
    items = []

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

        items.append({
            "event_id": f"device-001/{session}/{sequence}",
            "event": event,
        })

    return items


def preload(url, items):
    request = urllib.request.Request(
        url,
        data=json.dumps({
            "protocol_version": 1,
            "events": items,
        }).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    # 测试准备请求也直接访问本地接收端。
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({})
    )

    with opener.open(request, timeout=5) as response:
        require(response.status == 200, "Preload failed")
        body = json.load(response)

    require(
        len(body["results"]) == len(items),
        "Unexpected preload response",
    )

    require(
        all(
            item["status"] == "accepted"
            for item in body["results"]
        ),
        f"Unexpected preload results: {body}",
    )


def outbox_rows(database_path):
    with closing(sqlite3.connect(database_path)) as database:
        rows = database.execute(
            """
            SELECT
                event_id,
                payload_json,
                state,
                attempt_count,
                next_attempt_at_ms,
                created_at_ms,
                updated_at_ms,
                acked_at_ms,
                last_error
            FROM outbox
            """
        ).fetchall()

    result = {}

    for row in rows:
        result[row[0]] = {
            "payload": json.loads(row[1]),
            "state": row[2],
            "attempt_count": row[3],
            "next_attempt_at_ms": row[4],
            "created_at_ms": row[5],
            "updated_at_ms": row[6],
            "acked_at_ms": row[7],
            "last_error": row[8],
        }

    return result


def check_rows(
    database_path,
    session,
    states,
    attempt_count,
    updated_at_ms,
    next_attempt_at_ms,
):
    rows = outbox_rows(database_path)

    expected_payloads = {
        item["event_id"]: item["event"]
        for item in event_items(session)
    }

    require(
        set(rows) == set(expected_payloads),
        f"Unexpected Outbox identities: {rows}",
    )

    for sequence, state in enumerate(states, start=1):
        event_id = f"device-001/{session}/{sequence}"
        row = rows[event_id]

        require(row["payload"] == expected_payloads[event_id], row)
        require(row["state"] == state, row)
        require(row["attempt_count"] == attempt_count, row)
        require(row["created_at_ms"] == 1000, row)
        require(row["updated_at_ms"] == updated_at_ms, row)

        require(
            row["next_attempt_at_ms"] == next_attempt_at_ms,
            row,
        )

        expected_acked = updated_at_ms if state == "ACKED" else None

        require(row["acked_at_ms"] == expected_acked, row)

    return rows


def run_cycle(
    probe,
    url,
    database_path,
    session,
    now_ms,
    expected_counts,
    expected_failure="none",
):
    output = run_tool(
        probe,
        url,
        str(database_path),
        session,
        str(now_ms),
    )

    report = {}

    for line in output.splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            report[key] = value

    names = (
        "claimed_records",
        "acked_records",
        "retry_records",
        "dead_records",
    )

    for name, expected in zip(names, expected_counts):
        require(report.get(name) == str(expected), output)

    require(
        report.get("failure") == expected_failure,
        output,
    )

    return report


def post_count(receiver):
    return receiver.read_log().count(
        '"POST /v1/events/batch '
    )


def case_normal(root, probe, receiver_script):
    directory = root / "normal"
    server_db = directory / "server.db"
    outbox = directory / "outbox.db"
    second_outbox = directory / "second-outbox.db"

    session = "outbox-normal"

    with Receiver(receiver_script, server_db) as receiver:
        run_cycle(
            probe, receiver.url, outbox, session, 2000,
            (2, 2, 0, 0),
        )

        check_rows(
            outbox, session,
            ["ACKED", "ACKED"],
            1, 2000, 0,
        )

        require(len(stored_records(server_db)) == 2, "Missing events")

        before = outbox_rows(outbox)
        requests_before = post_count(receiver)

        # 已确认记录不能再次领取，也不能被重复入队重置。
        run_cycle(
            probe, receiver.url, outbox, session, 2200,
            (0, 0, 0, 0),
        )

        require(outbox_rows(outbox) == before, "Idle changed records")
        require(
            post_count(receiver) == requests_before,
            "Idle cycle sent an HTTP request",
        )

        # 新建一个本地 Outbox，服务端已经有相同事件。
        # 这次服务端返回 duplicate，本地也应进入 ACKED。
        run_cycle(
            probe, receiver.url, second_outbox, session, 2300,
            (2, 2, 0, 0),
        )

        check_rows(
            second_outbox, session,
            ["ACKED", "ACKED"],
            1, 2300, 0,
        )

        require(
            len(stored_records(server_db)) == 2,
            "Duplicate upload created extra server records",
        )


def case_conflict(root, probe, receiver_script):
    directory = root / "conflict"
    server_db = directory / "server.db"
    outbox = directory / "outbox.db"

    session = "outbox-conflict"

    with Receiver(receiver_script, server_db) as receiver:
        items = event_items(session)
        items[0]["event"]["value"] = 99.0

        # 只预置第一条，并使内容与本地事件不同。
        preload(receiver.url, items[:1])

        run_cycle(
            probe, receiver.url, outbox, session, 2000,
            (2, 1, 0, 1),
        )

        rows = check_rows(
            outbox, session,
            ["DEAD", "ACKED"],
            1, 2000, 0,
        )

        first_id = f"device-001/{session}/1"

        require(
            rows[first_id]["last_error"]
            == "Server reported event identity conflict",
            rows,
        )

        server_records = stored_records(server_db)

        require(server_records[first_id]["value"] == 99.0, server_records)
        require(len(server_records) == 2, server_records)

        before = outbox_rows(outbox)
        requests_before = post_count(receiver)

        run_cycle(
            probe, receiver.url, outbox, session, 3000,
            (0, 0, 0, 0),
        )

        require(outbox_rows(outbox) == before, "Terminal state changed")
        require(
            post_count(receiver) == requests_before,
            "Terminal records were uploaded again",
        )


def case_http_due_time(root, probe, receiver_script):
    directory = root / "http-503"
    server_db = directory / "server.db"
    outbox = directory / "outbox.db"

    session = "outbox-http"

    with Receiver(receiver_script, server_db, "http-503") as receiver:
        report = run_cycle(
            probe, receiver.url, outbox, session, 2000,
            (2, 0, 2, 0),
            "http",
        )

        require(report.get("http_status") == "503", report)

        check_rows(
            outbox, session,
            ["RETRY", "RETRY"],
            1, 2000, 2500,
        )

        require(not stored_records(server_db), "503 stored events")

        before = outbox_rows(outbox)
        requests_before = post_count(receiver)

        # 距离到期时间还差 1ms。
        run_cycle(
            probe, receiver.url, outbox, session, 2499,
            (0, 0, 0, 0),
        )

        require(outbox_rows(outbox) == before, "Early cycle changed records")
        require(
            post_count(receiver) == requests_before,
            "Retry was sent before its deadline",
        )

    with Receiver(receiver_script, server_db) as receiver:
        # 精确到期，重新领取并成功确认。
        run_cycle(
            probe, receiver.url, outbox, session, 2500,
            (2, 2, 0, 0),
        )

        check_rows(
            outbox, session,
            ["ACKED", "ACKED"],
            2, 2500, 2500,
        )

        require(len(stored_records(server_db)) == 2, "Missing recovered events")


def case_saved_failure(
    root,
    probe,
    receiver_script,
    mode,
    failure,
    request_reason,
    summary,
):
    directory = root / mode
    server_db = directory / "server.db"
    outbox = directory / "outbox.db"

    session = f"outbox-{mode}"

    with Receiver(receiver_script, server_db, mode) as receiver:
        report = run_cycle(
            probe, receiver.url, outbox, session, 2000,
            (2, 0, 2, 0),
            failure,
        )

        if request_reason is not None:
            require(
                report.get("request_reason") == request_reason,
                report,
            )

        rows = check_rows(
            outbox, session,
            ["RETRY", "RETRY"],
            1, 2000, 2500,
        )

        require(
            all(row["last_error"] == summary for row in rows.values()),
            rows,
        )

        saved_before = stored_records(server_db)

        require(
            len(saved_before) == 2,
            f"Receiver did not commit in mode {mode}",
        )

    with Receiver(receiver_script, server_db) as receiver:
        run_cycle(
            probe, receiver.url, outbox, session, 2500,
            (2, 2, 0, 0),
        )

        rows = check_rows(
            outbox, session,
            ["ACKED", "ACKED"],
            2, 2500, 2500,
        )

        require(
            all(not row["last_error"] for row in rows.values()),
            rows,
        )

        require(
            stored_records(server_db) == saved_before,
            "Resend changed identities or event content",
        )


def case_connection_failure(root, probe, receiver_script):
    directory = root / "connection"
    server_db = directory / "server.db"
    outbox = directory / "outbox.db"

    session = "outbox-connection"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserved:
        reserved.bind(("127.0.0.1", 0))
        port = reserved.getsockname()[1]

        report = run_cycle(
            probe,
            f"http://127.0.0.1:{port}/v1/events/batch",
            outbox,
            session,
            2000,
            (2, 0, 2, 0),
            "request",
        )

        require(report.get("request_reason") == "transport", report)

    check_rows(
        outbox, session,
        ["RETRY", "RETRY"],
        1, 2000, 2500,
    )

    with Receiver(receiver_script, server_db) as receiver:
        run_cycle(
            probe, receiver.url, outbox, session, 2500,
            (2, 2, 0, 0),
        )

        check_rows(
            outbox, session,
            ["ACKED", "ACKED"],
            2, 2500, 2500,
        )


def run_case(name, function, *arguments):
    function(*arguments)
    print(f"PASS {name}", flush=True)


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: check_outbox_upload.py PROBE RECEIVER_SCRIPT",
            file=sys.stderr,
        )
        return 2

    probe = Path(sys.argv[1]).resolve()
    receiver_script = Path(sys.argv[2]).resolve()

    require(probe.is_file(), f"File not found: {probe}")
    require(receiver_script.is_file(), f"File not found: {receiver_script}")

    with tempfile.TemporaryDirectory(
        prefix="edge-outbox-upload-",
    ) as temporary:
        root = Path(temporary)

        try:
            run_case(
                "normal_duplicate_idle",
                case_normal,
                root, probe, receiver_script,
            )

            run_case(
                "mixed_conflict_idle",
                case_conflict,
                root, probe, receiver_script,
            )

            run_case(
                "http_503_due_time",
                case_http_due_time,
                root, probe, receiver_script,
            )

            failures = [
                (
                    "invalid-json",
                    "protocol",
                    None,
                    "Invalid batch response",
                ),
                (
                    "drop-after-commit",
                    "request",
                    "transport",
                    "HTTP transport failure",
                ),
                (
                    "delay",
                    "request",
                    "timeout",
                    "HTTP request timed out",
                ),
                (
                    "oversized-body",
                    "request",
                    "response_too_large",
                    "HTTP response exceeded size limit",
                ),
            ]

            for mode, failure, reason, summary in failures:
                run_case(
                    f"saved_failure_{mode}",
                    case_saved_failure,
                    root,
                    probe,
                    receiver_script,
                    mode,
                    failure,
                    reason,
                    summary,
                )

            run_case(
                "connection_failure_recovery",
                case_connection_failure,
                root, probe, receiver_script,
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

    print("All 8 Outbox integration scenarios passed", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())