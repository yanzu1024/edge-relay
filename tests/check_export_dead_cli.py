#!/usr/bin/env python3
"""Check the locked, offline edge-agent --export-dead command."""

import argparse
from contextlib import closing
import fcntl
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


def read_rows(database):
    with closing(sqlite3.connect(
        database.as_uri() + "?mode=ro",
        uri=True,
    )) as connection:
        connection.row_factory = sqlite3.Row

        return [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM outbox "
                "ORDER BY created_at_ms, event_id"
            )
        ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("executable", type=Path)

    executable = (
        parser.parse_args()
        .executable.resolve(strict=True)
    )

    def run(options, code=0):
        result = subprocess.run(
            [str(executable), *map(str, options)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=10,
        )

        require(
            result.returncode == code,
            (
                f"Exit={result.returncode}, expected={code}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            ),
        )

        return result

    with (
        tempfile.TemporaryDirectory(
            prefix="edge export cli "
        ) as name,
        socket.socket() as occupied,
    ):
        root = Path(name).resolve()

        database = root / "outbox.db"
        config = root / "config.yml"
        status = root / "status.json"
        output = root / "dead output.jsonl"

        occupied.bind(("127.0.0.1", 0))
        occupied.listen(1)

        def configuration(path):
            return f"""version: 1

agent:
  id: export-dead-cli-test

tcp:
  bind: 127.0.0.1
  port: {occupied.getsockname()[1]}

storage:
  path: {json.dumps(str(path))}

recording:
  enabled: true
  directory: {json.dumps(str(root / "raw"))}

status:
  file: {json.dumps(str(status))}
"""

        config.write_text(
            configuration(database),
            encoding="utf-8",
        )

        common = ["--config", config]

        # 通过已验收的空 drain 创建真实版本的数据库。
        run(common + ["--drain-only", "--jsonl"])

        saved_status = status.read_bytes()
        now = time.time_ns() // 1000000

        states = [
            "DEAD",
            "ACKED",
            "RETRY",
            "SENDING",
            "DEAD",
        ]

        with closing(sqlite3.connect(database)) as connection:
            for sequence, state in enumerate(states, 1):
                value = event(sequence)

                key = (
                    f"{value['device_id']}/"
                    f"{value['session_id']}/"
                    f"{sequence}"
                )

                connection.execute(
                    """
                    INSERT INTO outbox (
                        event_id, payload_json, state,
                        attempt_count, next_attempt_at_ms,
                        created_at_ms, updated_at_ms,
                        acked_at_ms, last_error
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        key,
                        json.dumps(value),
                        state,
                        2,
                        now + 60000
                        if state == "RETRY"
                        else 0,
                        now,
                        now,
                        now if state == "ACKED" else None,
                        "拒收原因\n第二行"
                        if state == "DEAD"
                        else "",
                    ),
                )

            connection.commit()

        before = read_rows(database)

        # 持有与 Agent 相同的 flock，检查导出是否遵守锁。
        locked_output = root / "locked.jsonl"

        with Path(
            str(database) + ".lock"
        ).open("r+b") as lock:
            fcntl.flock(
                lock,
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )

            run(
                common + [
                    "--export-dead",
                    locked_output,
                ],
                code=2,
            )

            require(
                not locked_output.exists(),
                "Published output while locked.",
            )

        print(
            "PASS: export respects the Outbox instance lock"
        )

        result = run(
            common + ["--export-dead", output]
        )

        require(
            "Exported DEAD records: 2" in result.stdout,
            "Wrong export count.",
        )

        exported = [
            json.loads(line)
            for line in output.read_text(
                encoding="utf-8"
            ).splitlines()
        ]

        expected = [
            {"export_version": 1, **row}
            for row in before
            if row["state"] == "DEAD"
        ]

        require(
            exported == expected,
            "Exported IDs, payloads or diagnostics differ.",
        )

        require(
            read_rows(database) == before,
            "Export changed Outbox records.",
        )

        require(
            status.read_bytes() == saved_status,
            "Export started runtime status output.",
        )

        print(
            "PASS: complete DEAD export "
            "without runtime or state changes"
        )

        saved_output = output.read_bytes()

        run(
            common + ["--export-dead", output],
            code=2,
        )

        require(
            output.read_bytes() == saved_output,
            "Existing output overwritten.",
        )

        run(
            common + ["--export-dead", database],
            code=2,
        )

        require(
            read_rows(database) == before,
            "Source database changed.",
        )

        print(
            "PASS: existing output and database protected"
        )

        invalid = root / "invalid.jsonl"

        run(
            ["--export-dead", invalid],
            code=2,
        )

        run(
            common + ["--export-dead"],
            code=2,
        )

        run(
            common + ["--export-dead", ""],
            code=2,
        )

        run(
            common + [
                "--export-dead", invalid,
                "--export-dead", invalid,
            ],
            code=2,
        )

        for option in (
            "--run",
            "--drain-only",
            "--check-config",
            "--jsonl",
            "--events-jsonl",
        ):
            run(
                common + [
                    "--export-dead",
                    invalid,
                    option,
                ],
                code=2,
            )

        require(
            not invalid.exists(),
            "Invalid command created output.",
        )

        print(
            "PASS: missing values, duplicates "
            "and mode conflicts rejected"
        )

        missing = root / "missing.db"
        missing_config = root / "missing-config.yml"

        missing_config.write_text(
            configuration(missing),
            encoding="utf-8",
        )

        run(
            [
                "--config", missing_config,
                "--export-dead", invalid,
            ],
            code=2,
        )

        require(
            not missing.exists()
            and not invalid.exists(),
            "Created a missing database or output.",
        )

        print(
            "PASS: missing database is not created"
        )

        # 只修改本测试夹具，验证无 DEAD 的导出。
        with closing(sqlite3.connect(database)) as connection:
            connection.execute(
                "DELETE FROM outbox WHERE state = 'DEAD'"
            )
            connection.commit()

        remaining = read_rows(database)
        empty = root / "empty.jsonl"

        run(
            common + ["--export-dead", empty]
        )

        require(
            empty.read_bytes() == b"",
            "Empty DEAD set did not produce an empty file.",
        )

        require(
            read_rows(database) == remaining,
            "Empty export changed other states.",
        )

        require(
            status.read_bytes() == saved_status,
            "Offline export updated runtime status.",
        )

        print(
            "PASS: empty DEAD set leaves other states unchanged"
        )

    print("PASS: export-dead-cli-contract")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)