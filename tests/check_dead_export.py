#!/usr/bin/env python3
"""Check the DEAD export command using temporary SQLite databases."""

import argparse
from contextlib import closing
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def read_rows(database):
    with closing(sqlite3.connect(
        database.as_uri() + "?mode=ro",
        uri=True,
    )) as connection:
        return connection.execute(
            "SELECT * FROM outbox ORDER BY event_id"
        ).fetchall()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("exporter", type=Path)
    arguments = parser.parse_args()

    exporter = arguments.exporter.resolve()

    require(exporter.is_file(), "Exporter script does not exist.")

    def run(options, expected_code=0):
        result = subprocess.run(
            [sys.executable, str(exporter), *map(str, options)],
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=5,
            check=False,
        )

        require(
            result.returncode == expected_code,
            (
                f"Unexpected exit code: {result.returncode}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            ),
        )

        return result

    run(["--help"])
    print("PASS: help")

    with tempfile.TemporaryDirectory(
        prefix="edge dead export "
    ) as name:
        root = Path(name).resolve()
        database = root / "outbox.db"
        destination = root / "exported records.jsonl"

        columns = (
            "event_id",
            "payload_json",
            "state",
            "attempt_count",
            "next_attempt_at_ms",
            "created_at_ms",
            "updated_at_ms",
            "acked_at_ms",
            "last_error",
        )

        rows = [
            (
                "sensor/session/1",
                '{"sequence":1,"value":23.5}',
                "DEAD",
                2,
                0,
                1700000000000,
                1700000000100,
                None,
                "拒收原因：测试\n第二行",
            ),
            (
                "sensor/session/2",
                '{"sequence":2,"value":24.5}',
                "ACKED",
                1,
                0,
                1700000000001,
                1700000000101,
                1700000000101,
                "",
            ),
            (
                "sensor/session/3",
                '{"sequence":3,"value":25.5}',
                "PENDING",
                0,
                0,
                1700000000002,
                1700000000002,
                None,
                "",
            ),
            (
                "sensor/session/4",
                '{"sequence":4,"value":26.5}',
                "DEAD",
                3,
                0,
                1700000000003,
                1700000000103,
                None,
                "deterministic rejection",
            ),
        ]

        # 只建立导出命令所依赖的表结构。
        with closing(sqlite3.connect(database)) as connection:
            connection.execute("""
                CREATE TABLE outbox (
                    event_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempt_count INTEGER NOT NULL,
                    next_attempt_at_ms INTEGER NOT NULL,
                    created_at_ms INTEGER NOT NULL,
                    updated_at_ms INTEGER NOT NULL,
                    acked_at_ms INTEGER,
                    last_error TEXT NOT NULL
                )
            """)

            connection.executemany(
                "INSERT INTO outbox VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )
            connection.commit()

        before = read_rows(database)

        run(["--db", database, "--output", destination])

        exported = [
            json.loads(line)
            for line in destination.read_text(
                encoding="utf-8"
            ).splitlines()
        ]

        expected = [
            {
                "export_version": 1,
                **dict(zip(columns, row)),
            }
            for row in rows
            if row[2] == "DEAD"
        ]

        require(
            exported == expected,
            "Exported IDs, payloads or diagnostic fields differ.",
        )
        print("PASS: DEAD filtering and complete field preservation")

        require(
            read_rows(database) == before,
            "Export modified the source database.",
        )
        print("PASS: source records unchanged")

        previous_output = destination.read_bytes()

        run(
            ["--db", database, "--output", destination],
            expected_code=1,
        )

        require(
            destination.read_bytes() == previous_output,
            "Existing output was overwritten.",
        )
        print("PASS: existing output protected")

        run(
            ["--db", database, "--output", database],
            expected_code=1,
        )

        require(
            read_rows(database) == before,
            "Database was modified when used as output.",
        )
        print("PASS: database path protected")

        missing = root / "missing.db"
        missing_output = root / "missing.jsonl"

        run(
            ["--db", missing, "--output", missing_output],
            expected_code=1,
        )

        require(
            not missing.exists() and not missing_output.exists(),
            "Missing input caused files to be created.",
        )
        print("PASS: missing database rejected")

        wrong_database = root / "wrong-schema.db"
        wrong_output = root / "wrong-schema.jsonl"

        with closing(sqlite3.connect(wrong_database)) as connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
            connection.commit()

        run(
            ["--db", wrong_database, "--output", wrong_output],
            expected_code=1,
        )

        require(
            not wrong_output.exists(),
            "Schema failure published an output file.",
        )
        print("PASS: incompatible schema rejected")

        # 这里只修改测试夹具，验证无 DEAD 时的行为。
        with closing(sqlite3.connect(database)) as connection:
            connection.execute(
                "DELETE FROM outbox WHERE state = 'DEAD'"
            )
            connection.commit()

        empty_output = root / "empty.jsonl"

        run(["--db", database, "--output", empty_output])

        require(
            empty_output.is_file()
            and empty_output.read_bytes() == b"",
            "Empty DEAD set did not produce an empty file.",
        )
        print("PASS: empty DEAD set")

    print("PASS: dead-export-contract")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)