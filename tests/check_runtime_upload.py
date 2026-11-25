#!/usr/bin/env python3

import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import time
import urllib.request

from check_http_upload import (
    Receiver,
    stored_records,
    test_environment,
)

from check_outbox_upload import outbox_rows

from check_run import (
    Agent,
    check_config_case,
    check_result,
    event,
    event_id,
    expect,
    frame,
    stats,
)


def write_upload_config(
    path,
    url,
    batch_size=2,
    flush_interval_ms=100,
    request_timeout_ms=1000,
    failure_warn_attempts=2,
    retry_initial_ms=100,
    retry_max_ms=200,
):
    path.write_text(
        "version: 1\n"
        "tcp:\n"
        "  bind: 127.0.0.1\n"
        "  port: 0\n"
        "upload:\n"
        f"  url: {url}\n"
        f"  batch_size: {batch_size}\n"
        f"  flush_interval_ms: {flush_interval_ms}\n"
        "  connect_timeout_ms: 500\n"
        f"  request_timeout_ms: {request_timeout_ms}\n"
        f"  retry_initial_ms: {retry_initial_ms}\n"
        f"  retry_max_ms: {retry_max_ms}\n"
        f"  failure_warn_attempts: {failure_warn_attempts}\n",
        encoding="utf-8",
    )


def cycle_line(
    claimed=2,
    acked=2,
    retry=0,
    dead=0,
    failure="none",
    http_status=None,
    curl_code=None,
):
    text = (
        f"Upload cycle: claimed={claimed}"
        f" acked={acked}"
        f" retry={retry}"
        f" dead={dead}"
        f" failure={failure}"
    )

    if curl_code is not None:
        text += f" curl_code={curl_code}"
    elif http_status is not None:
        text += f" http_status={http_status}"

    return (text + "\n").encode()


def stored_output(count):
    return "".join(
        f"Event stored: {event_id(sequence)}\n"
        for sequence in range(1, count + 1)
    ).encode()


def expected_output(
    agent,
    event_count=0,
    cycles=b"",
    **counters,
):
    return (
        agent.ready
        + stored_output(event_count)
        + cycles
        + stats(**counters)
    )


def expected_server_events(count=2):
    return {
        event_id(sequence): event(sequence)
        for sequence in range(1, count + 1)
    }


def check_local(data_dir, states, attempt_count):
    rows = outbox_rows(data_dir / "outbox.db")

    expected_ids = {
        event_id(sequence)
        for sequence in range(1, len(states) + 1)
    }

    expect(
        set(rows) == expected_ids,
        f"Unexpected local identities: {rows!r}",
    )

    for sequence, state in enumerate(states, start=1):
        row = rows[event_id(sequence)]

        expect(
            row["payload"] == event(sequence),
            f"Payload changed: {row!r}",
        )

        expect(
            row["state"] == state,
            f"Unexpected state: {row!r}",
        )

        expect(
            row["attempt_count"] == attempt_count,
            f"Unexpected attempt count: {row!r}",
        )

        if state == "ACKED":
            expect(
                row["acked_at_ms"] is not None
                and row["acked_at_ms"] > 0
                and row["acked_at_ms"] == row["updated_at_ms"]
                and row["last_error"] == "",
                f"Invalid acknowledgement: {row!r}",
            )
        else:
            expect(
                row["acked_at_ms"] is None,
                f"Unconfirmed record has acked_at_ms: {row!r}",
            )

    return rows


def check_preserved(before, after):
    expect(
        before.keys() == after.keys(),
        "Recovery changed event identities",
    )

    for identity in before:
        for field in ("payload", "created_at_ms"):
            expect(
                before[identity][field] == after[identity][field],
                f"Recovery changed {field}: {identity}",
            )


def post_count(receiver):
    return receiver.read_log().count(
        '"POST /v1/events/batch '
    )


def preload_conflict(url):
    conflicting = event(1)
    conflicting["value"] = 99.0

    request = urllib.request.Request(
        url,
        data=json.dumps({
            "protocol_version": 1,
            "events": [{
                "event_id": event_id(1),
                "event": conflicting,
            }],
        }).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({})
    )

    with opener.open(request, timeout=5) as response:
        expect(response.status == 200, "Conflict preload failed")
        body = json.load(response)

    expect(
        body == {
            "protocol_version": 1,
            "results": [{
                "event_id": event_id(1),
                "status": "accepted",
            }],
        },
        f"Unexpected preload response: {body!r}",
    )


def wait_for_inflight(data_dir, server_db):
    deadline = time.monotonic() + 2.0
    last_rows = {}

    while time.monotonic() < deadline:
        last_rows = outbox_rows(data_dir / "outbox.db")
        received = stored_records(server_db)

        if (
            set(last_rows) == set(expected_server_events())
            and all(
                row["state"] == "SENDING"
                and row["attempt_count"] == 1
                for row in last_rows.values()
            )
            and received == expected_server_events()
        ):
            return last_rows

        time.sleep(0.02)

    raise AssertionError(
        "Did not observe the first committed SENDING batch "
        f"while the receiver had saved it: {last_rows!r}"
    )


def resume_to_acked(
    executable,
    receiver_script,
    config,
    server_db,
    data_dir,
    before,
    recovered=0,
):
    previous_attempts = {
        row["attempt_count"]
        for row in before.values()
    }

    expect(
        len(previous_attempts) == 1,
        f"Unexpected recovery fixture: {before!r}",
    )

    next_attempt = next(iter(previous_attempts)) + 1

    with Receiver(receiver_script, server_db) as receiver:
        write_upload_config(config, receiver.url)

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()

            success = cycle_line()
            agent.wait_line(success.decode())

            after = check_local(
                data_dir,
                ["ACKED", "ACKED"],
                next_attempt,
            )

            check_preserved(before, after)

            expect(
                stored_records(server_db) == expected_server_events(),
                "Recovery changed server identities or payloads",
            )

            check_result(
                agent.finish(),
                expected_output(
                    agent,
                    cycles=success,
                    recovered=recovered,
                    upload_cycles=1,
                    upload_claimed=2,
                    upload_acked=2,
                ),
            )


def successful_upload(
    executable,
    receiver_script,
    root,
    batch_size,
    event_count,
    flush_interval_ms,
    check_configuration=False,
    minimum_elapsed=0.0,
):
    server_db = root / "receiver.db"

    with Receiver(receiver_script, server_db) as receiver:
        config = root / "demo.yml"

        write_upload_config(
            config,
            receiver.url,
            batch_size=batch_size,
            flush_interval_ms=flush_interval_ms,
        )

        if check_configuration:
            check_config_case(executable, config, root)

            expect(
                post_count(receiver) == 0,
                "--check-config sent an upload request",
            )

        data_dir = root / "agent"

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()

            started = time.monotonic()

            agent.send(
                b"".join(
                    frame(event(sequence))
                    for sequence in range(1, event_count + 1)
                )
            )

            success = cycle_line(
                claimed=event_count,
                acked=event_count,
            )

            agent.wait_line(success.decode())

            elapsed = time.monotonic() - started

            expect(
                elapsed >= minimum_elapsed,
                f"Partial batch uploaded too early: {elapsed:.3f}s",
            )

            check_local(
                data_dir,
                ["ACKED"] * event_count,
                1,
            )

            expect(
                stored_records(server_db)
                == expected_server_events(event_count),
                "Unexpected server records",
            )

            check_result(
                agent.finish(),
                expected_output(
                    agent,
                    event_count=event_count,
                    cycles=success,
                    completed=event_count,
                    valid=event_count,
                    stored=event_count,
                    upload_cycles=1,
                    upload_claimed=event_count,
                    upload_acked=event_count,
                ),
            )


def case_full_batch(executable, receiver_script, root):
    successful_upload(
        executable,
        receiver_script,
        root,
        batch_size=2,
        event_count=2,
        flush_interval_ms=100,
        check_configuration=True,
    )


def case_partial_batch(executable, receiver_script, root):
    successful_upload(
        executable,
        receiver_script,
        root,
        batch_size=20,
        event_count=1,
        flush_interval_ms=1000,
        minimum_elapsed=0.9,
    )


def case_retry_and_warning(
    executable,
    receiver_script,
    root,
    mode="http-503",
    status_code=503,
):
    server_db = root / "receiver.db"
    data_dir = root / "agent"
    config = root / "demo.yml"

    failure = cycle_line(
        acked=0,
        retry=2,
        failure="http",
        http_status=status_code,
    )

    with Receiver(
        receiver_script,
        server_db,
        mode,
    ) as receiver:
        write_upload_config(config, receiver.url)

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()

            agent.send(
                frame(event(1))
                + frame(event(2))
            )

            agent.wait_line(
                failure.decode(),
                occurrences=2,
            )

            result = agent.finish()

            actual_cycles = [
                line
                for line in result[1].splitlines(keepends=True)
                if line.startswith(b"Upload cycle:")
            ]

            cycle_count = len(actual_cycles)

            expect(
                cycle_count >= 2
                and all(
                    line == failure
                    for line in actual_cycles
                ),
                f"Unexpected failure cycles: {actual_cycles!r}",
            )

            check_result(
                result,
                expected_output(
                    agent,
                    event_count=2,
                    cycles=failure * cycle_count,
                    completed=2,
                    valid=2,
                    stored=2,
                    upload_cycles=cycle_count,
                    upload_claimed=2 * cycle_count,
                    upload_retry=2 * cycle_count,
                    upload_http_failures=cycle_count,
                    upload_warnings=1,
                ),
                b"Upload warning: consecutive_failed_cycles=2\n",
            )

            before = check_local(
                data_dir,
                ["RETRY", "RETRY"],
                cycle_count,
            )

            for row in before.values():
                expect(
                    row["last_error"]
                    == f"HTTP status {status_code}"
                    and row["next_attempt_at_ms"]
                        - row["updated_at_ms"] == 200,
                    f"Unexpected retry schedule: {row!r}",
                )

            expect(
                post_count(receiver) == cycle_count,
                "HTTP request count and cycle count differ",
            )

            expect(
                not stored_records(server_db),
                f"{status_code} mode unexpectedly saved events",
            )

    resume_to_acked(
        executable,
        receiver_script,
        config,
        server_db,
        data_dir,
        before,
    )


def case_conflict(executable, receiver_script, root):
    server_db = root / "receiver.db"
    data_dir = root / "agent"
    config = root / "demo.yml"

    with Receiver(receiver_script, server_db) as receiver:
        write_upload_config(config, receiver.url)
        preload_conflict(receiver.url)

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()
            agent.send(frame(event(1)) + frame(event(2)))

            completed = cycle_line(acked=1, dead=1)
            agent.wait_line(completed.decode())

            rows = check_local(
                data_dir,
                ["DEAD", "ACKED"],
                1,
            )

            expect(
                rows[event_id(1)]["last_error"]
                == "Server reported event identity conflict",
                f"Missing conflict reason: {rows!r}",
            )

            expected_received = expected_server_events()
            expected_received[event_id(1)]["value"] = 99.0

            expect(
                stored_records(server_db) == expected_received,
                "Conflict overwrote the server event",
            )

            check_result(
                agent.finish(),
                expected_output(
                    agent,
                    event_count=2,
                    cycles=completed,
                    completed=2,
                    valid=2,
                    stored=2,
                    upload_cycles=1,
                    upload_claimed=2,
                    upload_acked=1,
                    upload_dead=1,
                ),
                b"Upload conflict: records=1\n",
            )

    before = outbox_rows(data_dir / "outbox.db")

    # 重启后 ACKED 和 DEAD 都不能重新进入发送队列。
    with Receiver(receiver_script, server_db) as receiver:
        write_upload_config(config, receiver.url)

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()

            check_result(
                agent.finish(),
                expected_output(agent),
            )

        expect(
            outbox_rows(data_dir / "outbox.db") == before,
            "Restart changed terminal records",
        )

        expect(
            post_count(receiver) == 0,
            "Restart uploaded terminal records",
        )


def case_stop_before_flush(executable, receiver_script, root):
    server_db = root / "receiver.db"
    data_dir = root / "agent"

    with Receiver(receiver_script, server_db) as receiver:
        config = root / "demo.yml"

        write_upload_config(
            config,
            receiver.url,
            batch_size=20,
            flush_interval_ms=60000,
        )

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()
            agent.send(frame(event(1)))
            agent.wait_line(f"Event stored: {event_id(1)}\n")

            before = check_local(data_dir, ["PENDING"], 0)

            check_result(
                agent.finish(),
                expected_output(
                    agent,
                    event_count=1,
                    completed=1,
                    valid=1,
                    stored=1,
                ),
            )

        expect(
            outbox_rows(data_dir / "outbox.db") == before,
            "Stop changed an unsent event",
        )

        expect(
            post_count(receiver) == 0
            and not stored_records(server_db),
            "Stop started an extra upload",
        )


def case_stop_during_upload(executable, receiver_script, root):
    server_db = root / "receiver.db"
    data_dir = root / "agent"
    config = root / "demo.yml"

    with Receiver(
        receiver_script,
        server_db,
        "delay",
    ) as receiver:
        write_upload_config(
            config,
            receiver.url,
            request_timeout_ms=3000,
        )

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()
            agent.send(frame(event(1)) + frame(event(2)))
            agent.wait_line(f"Event stored: {event_id(2)}\n")

            wait_for_inflight(data_dir, server_db)

            # 接收端延迟 4 秒，客户端总超时为 3 秒。
            # 停止请求到达后，当前请求完成处理，不再发起下一轮。
            timeout_cycle = cycle_line(
                acked=0,
                retry=2,
                failure="request",
                curl_code=28,
            )

            check_result(
                agent.finish(signal.SIGTERM),
                expected_output(
                    agent,
                    event_count=2,
                    cycles=timeout_cycle,
                    completed=2,
                    valid=2,
                    stored=2,
                    upload_cycles=1,
                    upload_claimed=2,
                    upload_retry=2,
                    upload_request_failures=1,
                ),
            )

            before = check_local(
                data_dir,
                ["RETRY", "RETRY"],
                1,
            )

    resume_to_acked(
        executable,
        receiver_script,
        config,
        server_db,
        data_dir,
        before,
    )


def case_kill_during_upload(executable, receiver_script, root):
    server_db = root / "receiver.db"
    data_dir = root / "agent"
    config = root / "demo.yml"

    with Receiver(
        receiver_script,
        server_db,
        "delay",
    ) as receiver:
        write_upload_config(
            config,
            receiver.url,
            request_timeout_ms=3000,
        )

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()
            agent.send(frame(event(1)) + frame(event(2)))
            agent.wait_line(f"Event stored: {event_id(2)}\n")

            wait_for_inflight(data_dir, server_db)

            # 强制终止时不应有 Stopped 或完整上传结果输出。
            check_result(
                agent.finish(signal.SIGKILL),
                agent.ready + stored_output(2),
                code=-signal.SIGKILL,
            )

            before = check_local(
                data_dir,
                ["SENDING", "SENDING"],
                1,
            )

    # 新实例必须恢复两条 SENDING，再以原身份重发。
    resume_to_acked(
        executable,
        receiver_script,
        config,
        server_db,
        data_dir,
        before,
        recovered=2,
    )


def case_receive_while_upload_inflight(
    executable,
    receiver_script,
    root,
):
    server_db = root / "receiver.db"
    data_dir = root / "agent"
    config = root / "demo.yml"

    with Receiver(
        receiver_script,
        server_db,
        "delay",
    ) as receiver:
        write_upload_config(
            config,
            receiver.url,
            batch_size=2,
            flush_interval_ms=60000,
            request_timeout_ms=6000,
        )

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()

            started = time.monotonic()

            agent.send(
                frame(event(1))
                + frame(event(2))
            )

            agent.wait_line(
                f"Event stored: {event_id(2)}\n"
            )

            # 第一批已领取，服务端已保存，
            # 但仍处于延迟响应阶段。
            wait_for_inflight(data_dir, server_db)

            agent.send(frame(event(3)))

            agent.wait_line(
                f"Event stored: {event_id(3)}\n"
            )

            before_stop = outbox_rows(
                data_dir / "outbox.db"
            )

            elapsed = time.monotonic() - started

            # 接收端 delay 模式默认延迟 4 秒。
            # 在更早的窗口内核对第三条记录已提交，
            # 避免把 HTTP 完成后才入库误认为并发。
            expect(
                elapsed < 3.0,
                "Third event was not committed inside "
                "the receiver response-delay window: "
                f"{elapsed:.3f}s",
            )

            expect(
                {
                    identity: (
                        row["state"],
                        row["attempt_count"],
                    )
                    for identity, row in before_stop.items()
                }
                == {
                    event_id(1): ("SENDING", 1),
                    event_id(2): ("SENDING", 1),
                    event_id(3): ("PENDING", 0),
                },
                "Did not observe the new committed event "
                "while the first batch was still SENDING: "
                f"{before_stop!r}",
            )

            expect(
                {
                    identity: row["payload"]
                    for identity, row in before_stop.items()
                }
                == expected_server_events(3),
                "Unexpected local payloads",
            )

            expect(
                b"Upload cycle:" not in bytes(agent.captured),
                "The first upload cycle had already been reported",
            )

            # 停止后完成当前请求，但第三条不足一批，
            # 不允许在退出阶段补发。
            success = cycle_line()

            check_result(
                agent.finish(),
                expected_output(
                    agent,
                    event_count=3,
                    cycles=success,
                    completed=3,
                    valid=3,
                    stored=3,
                    upload_cycles=1,
                    upload_claimed=2,
                    upload_acked=2,
                ),
            )

        after_stop = outbox_rows(
            data_dir / "outbox.db"
        )

        expect(
            {
                identity: (
                    row["state"],
                    row["attempt_count"],
                )
                for identity, row in after_stop.items()
            }
            == {
                event_id(1): ("ACKED", 1),
                event_id(2): ("ACKED", 1),
                event_id(3): ("PENDING", 0),
            },
            f"Unexpected final local state: {after_stop!r}",
        )

        expect(
            {
                identity: row["payload"]
                for identity, row in after_stop.items()
            }
            == expected_server_events(3),
            "Stop changed local payloads",
        )

        expect(
            stored_records(server_db)
            == expected_server_events(2),
            "The unsent third event reached the receiver",
        )

        expect(
            post_count(receiver) == 1,
            "Stop started an extra HTTP request",
        )


def case_stop_during_retry_backoff(
    executable,
    receiver_script,
    root,
):
    server_db = root / "receiver.db"
    data_dir = root / "agent"
    config = root / "demo.yml"

    failure = cycle_line(
        acked=0,
        retry=2,
        failure="http",
        http_status=503,
    )

    with Receiver(
        receiver_script,
        server_db,
        "http-503",
    ) as receiver:
        write_upload_config(
            config,
            receiver.url,
            retry_initial_ms=5000,
            retry_max_ms=10000,
        )

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()

            agent.send(
                frame(event(1))
                + frame(event(2))
            )

            # 第一轮失败已经完成回写并输出。
            agent.wait_line(failure.decode())

            before_stop = check_local(
                data_dir,
                ["RETRY", "RETRY"],
                1,
            )

            for row in before_stop.values():
                expect(
                    row["last_error"] == "HTTP status 503"
                    and row["next_attempt_at_ms"]
                        - row["updated_at_ms"] == 5000,
                    f"Unexpected retry schedule: {row!r}",
                )

            # 正在等待五秒退避时请求停止，
            # 不应等完整个退避时间。
            started = time.monotonic()

            result = agent.finish()

            elapsed = time.monotonic() - started

            check_result(
                result,
                expected_output(
                    agent,
                    event_count=2,
                    cycles=failure,
                    completed=2,
                    valid=2,
                    stored=2,
                    upload_cycles=1,
                    upload_claimed=2,
                    upload_retry=2,
                    upload_http_failures=1,
                ),
            )

            expect(
                elapsed < 2.0,
                "Stopping during retry backoff was too slow: "
                f"{elapsed:.3f}s",
            )

        expect(
            outbox_rows(data_dir / "outbox.db")
            == before_stop,
            "Stop changed records waiting for retry",
        )

        expect(
            post_count(receiver) == 1,
            "Stop started another upload attempt",
        )

        expect(
            not stored_records(server_db),
            "503 receiver unexpectedly saved events",
        )


def case_upload_blocked(
    executable,
    receiver_script,
    root,
    mode,
    status_code,
):
    server_db = root / "receiver.db"
    data_dir = root / "agent"
    config = root / "demo.yml"

    failure = cycle_line(
        claimed=1,
        acked=0,
        retry=1,
        failure="http",
        http_status=status_code,
    )

    with Receiver(
        receiver_script,
        server_db,
        mode,
    ) as receiver:
        write_upload_config(
            config,
            receiver.url,
            batch_size=1,
        )

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()

            agent.send(frame(event(1)))
            agent.wait_line(failure.decode())

            # 上传已经失败，继续发送新事件。
            # batch_size=1：如果阻塞未生效，新事件会立即触发请求。
            agent.send(
                frame(event(2))
                + frame(event(3))
            )

            agent.wait_line(
                f"Event stored: {event_id(3)}\n"
            )

            before_stop = outbox_rows(
                data_dir / "outbox.db"
            )

            expect(
                {
                    identity: (
                        row["state"],
                        row["attempt_count"],
                    )
                    for identity, row in before_stop.items()
                }
                == {
                    event_id(1): ("RETRY", 1),
                    event_id(2): ("PENDING", 0),
                    event_id(3): ("PENDING", 0),
                },
                f"Unexpected blocked state: {before_stop!r}",
            )

            expect(
                {
                    identity: row["payload"]
                    for identity, row in before_stop.items()
                }
                == expected_server_events(3),
                "Blocked upload changed event payloads",
            )

            failed_row = before_stop[event_id(1)]

            expect(
                failed_row["last_error"]
                == f"HTTP status {status_code}"
                and failed_row["next_attempt_at_ms"]
                    - failed_row["updated_at_ms"] == 100,
                f"Invalid retry writeback: {failed_row!r}",
            )

            # 观察窗口超过已有的 100ms 初始退避。
            # 即使有新事件、旧记录也已到期，仍不能继续上传。
            deadline = time.monotonic() + 0.5

            while time.monotonic() < deadline:
                expect(
                    agent.process.poll() is None,
                    "Blocked Agent unexpectedly exited",
                )

                expect(
                    post_count(receiver) == 1,
                    "Blocked Agent sent another HTTP request",
                )

                expect(
                    outbox_rows(data_dir / "outbox.db")
                    == before_stop,
                    "Blocked Agent changed queued records",
                )

                time.sleep(0.02)

            new_event_output = (
                f"Event stored: {event_id(2)}\n"
                f"Event stored: {event_id(3)}\n"
            ).encode()

            check_result(
                agent.finish(),
                agent.ready
                + stored_output(1)
                + failure
                + new_event_output
                + stats(
                    completed=3,
                    valid=3,
                    stored=3,
                    upload_cycles=1,
                    upload_claimed=1,
                    upload_retry=1,
                    upload_http_failures=1,
                    upload_blocked=1,
                    upload_blocked_http_status=status_code,
                ),
                (
                    f"Upload blocked: http_status={status_code} "
                    "recovery=restart\n"
                ).encode(),
            )

        expect(
            outbox_rows(data_dir / "outbox.db")
            == before_stop,
            "Stop changed blocked records",
        )

        expect(
            post_count(receiver) == 1,
            "Stop started an extra request",
        )

        expect(
            not stored_records(server_db),
            "Failure mode unexpectedly stored events",
        )

    # 修正接收端和配置后重启 Agent。
    # 不手动修改 Outbox 状态、身份或领取次数。
    with Receiver(receiver_script, server_db) as receiver:
        write_upload_config(
            config,
            receiver.url,
            batch_size=1,
        )

        with Agent(executable, config, data_dir) as agent:
            agent.wait_ready()

            success = cycle_line(
                claimed=1,
                acked=1,
            )

            agent.wait_line(
                success.decode(),
                occurrences=3,
            )

            after = outbox_rows(data_dir / "outbox.db")

            check_preserved(before_stop, after)

            expect(
                {
                    identity: (
                        row["state"],
                        row["attempt_count"],
                    )
                    for identity, row in after.items()
                }
                == {
                    event_id(1): ("ACKED", 2),
                    event_id(2): ("ACKED", 1),
                    event_id(3): ("ACKED", 1),
                },
                f"Invalid restart recovery: {after!r}",
            )

            expect(
                stored_records(server_db)
                == expected_server_events(3),
                "Restart recovery changed server events",
            )

            check_result(
                agent.finish(),
                expected_output(
                    agent,
                    cycles=success * 3,
                    upload_cycles=3,
                    upload_claimed=3,
                    upload_acked=3,
                ),
            )

        expect(
            post_count(receiver) == 3,
            "Unexpected request count after restart",
        )


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: check_runtime_upload.py "
            "AGENT RECEIVER_SCRIPT",
            file=sys.stderr,
        )
        return 2

    executable = Path(sys.argv[1]).resolve()
    receiver_script = Path(sys.argv[2]).resolve()

    expect(executable.is_file(), f"File not found: {executable}")
    expect(receiver_script.is_file(), f"File not found: {receiver_script}")

    environment = test_environment()
    os.environ.clear()
    os.environ.update(environment)

    cases = [
        ("runtime_tcp_http_acked", case_full_batch),
        ("partial_batch_flush", case_partial_batch),
        ("retry_warning_and_restart_recovery", case_retry_and_warning),
        ("mixed_conflict_dead", case_conflict),
        ("stop_before_flush", case_stop_before_flush),
        ("stop_during_upload", case_stop_during_upload),
        ("kill_during_upload_recovery", case_kill_during_upload),
        (
            "receive_while_upload_inflight",
            case_receive_while_upload_inflight,
        ),
        (
            "stop_during_retry_backoff",
            case_stop_during_retry_backoff,
        ),
        (
            "upload_blocked_http_401",
            lambda exe, script, case_root: case_upload_blocked(
                exe, script, case_root, "http-401", 401
            ),
        ),
        (
            "upload_blocked_http_409",
            lambda exe, script, case_root: case_upload_blocked(
                exe, script, case_root, "http-409", 409
            ),
        ),
        (
            "upload_blocked_http_302",
            lambda exe, script, case_root: case_upload_blocked(
                exe, script, case_root, "http-302", 302
            ),
        ),
        (
            "retry_http_429",
            lambda exe, script, case_root: case_retry_and_warning(
                exe,
                script,
                case_root,
                mode="http-429",
                status_code=429,
            ),
        ),
    ]

    with tempfile.TemporaryDirectory(
        prefix="edge-runtime-upload-",
    ) as temporary:
        root = Path(temporary)
        case_name = "setup"

        try:
            for case_name, action in cases:
                action(
                    executable,
                    receiver_script,
                    root / case_name,
                )

                print(f"PASS {case_name}", flush=True)

        except Exception as error:
            print(
                f"FAIL {case_name}: {error}",
                file=sys.stderr,
            )

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

    print("All 13 runtime upload scenarios passed", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())