#!/usr/bin/env python3
"""Export DEAD Outbox records as JSONL without changing their state."""

import argparse
from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile


SELECT_DEAD = """
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
WHERE state = 'DEAD'
ORDER BY created_at_ms, event_id
"""


def export_dead(database, destination):
    database = database.expanduser().resolve(strict=True)
    destination = destination.expanduser().resolve()

    if not database.is_file():
        raise ValueError("Database must be an existing file.")

    # 同时保护数据库及其可能使用的辅助文件。
    protected = {
        database,
        Path(str(database) + "-wal"),
        Path(str(database) + "-shm"),
        Path(str(database) + "-journal"),
    }

    if destination in protected:
        raise ValueError(
            "Output must not replace the database or its sidecar files."
        )

    if destination.exists():
        raise FileExistsError(
            f"Output already exists: {destination}"
        )

    destination.parent.mkdir(parents=True, exist_ok=True)

    temporary = None
    count = 0

    try:
        with closing(sqlite3.connect(
            database.as_uri() + "?mode=ro",
            uri=True,
            timeout=2,
        )) as connection:
            connection.row_factory = sqlite3.Row

            # 连接只读，并在同一个事务快照中完成查询。
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")

            cursor = connection.execute(SELECT_DEAD)

            descriptor, name = tempfile.mkstemp(
                prefix=".dead-export-",
                suffix=".tmp",
                dir=destination.parent,
            )
            temporary = Path(name)

            with os.fdopen(
                descriptor,
                "w",
                encoding="utf-8",
                newline="\n",
            ) as output:
                for row in cursor:
                    record = {
                        "export_version": 1,
                        **dict(row),
                    }

                    # payload_json 保留数据库中的原始文本。
                    output.write(json.dumps(
                        record,
                        ensure_ascii=False,
                        allow_nan=False,
                    ) + "\n")

                    count += 1

                output.flush()
                os.fsync(output.fileno())

            connection.execute("ROLLBACK")

        # 临时文件与目标处于同一目录。
        # link 原子发布结果；目标已存在时失败，不覆盖已有文件。
        os.link(temporary, destination)

        return count

    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument(
        "--db",
        required=True,
        type=Path,
        help="Existing Outbox database.",
    )

    parser.add_argument(
        "--output",
        required=True,
        type=Path,
        help="New JSONL output file; existing files are not overwritten.",
    )

    arguments = parser.parse_args()

    count = export_dead(
        arguments.db,
        arguments.output,
    )

    print(f"Exported DEAD records: {count}")
    print(f"Output: {arguments.output.expanduser().resolve()}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, sqlite3.Error, ValueError, UnicodeError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)