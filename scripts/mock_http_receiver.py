#!/usr/bin/env python3

import argparse
import json
import math
from pathlib import Path
import socket
import sqlite3
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


MAX_REQUEST_BYTES = 2 * 1024 * 1024
MAX_RESPONSE_BYTES = 1024 * 1024

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

EVENT_FIELDS = {
    "schema_version",
    "device_id",
    "session_id",
    "sequence",
    "observed_at_ms",
    "metric",
    "value",
    "unit",
}


class HttpError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def require(condition, message):
    if not condition:
        raise HttpError(400, message)


def require_keys(value, expected, name):
    require(type(value) is dict, f"{name} must be an object")
    require(set(value) == expected, f"{name} has invalid fields")


def unique_object(pairs):
    """拒绝重复 JSON 键，避免解析时静默覆盖。"""
    result = {}

    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value

    return result


def reject_constant(text):
    # Python 默认允许 NaN/Infinity，本协议明确拒绝。
    raise ValueError(f"invalid JSON number: {text}")


def encode_json(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def decode_json(raw):
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique_object,
            parse_constant=reject_constant,
        )

        # 同时拒绝 JSON 转义产生的孤立代理字符等非法文本。
        encode_json(value)
        return value

    except (ValueError, UnicodeError, RecursionError) as error:
        raise HttpError(400, f"invalid JSON: {error}") from error


def validate_event(event):
    require_keys(event, EVENT_FIELDS, "event")

    require(
        type(event["schema_version"]) is int
        and event["schema_version"] == 1,
        "schema_version must be integer 1",
    )

    for name in ("device_id", "session_id", "metric", "unit"):
        require(
            type(event[name]) is str,
            f"{name} must be a string",
        )

    for name in ("sequence", "observed_at_ms"):
        value = event[name]

        # bool 是 Python int 的子类，因此使用 type(...) is int。
        require(
            type(value) is int
            and INT64_MIN <= value <= INT64_MAX,
            f"{name} must be an int64",
        )

    value = event["value"]

    require(
        type(value) in (int, float),
        "value must be a number",
    )

    try:
        normalized_value = float(value)
    except (OverflowError, ValueError) as error:
        raise HttpError(400, "value is outside double range") from error

    require(math.isfinite(normalized_value), "value must be finite")

    normalized = dict(event)

    # 与 C++ double 对齐，并统一正负零。
    normalized["value"] = (
        0.0 if normalized_value == 0.0 else normalized_value
    )

    return normalized


def validate_batch(document):
    require_keys(
        document,
        {"protocol_version", "events"},
        "request",
    )

    require(
        type(document["protocol_version"]) is int
        and document["protocol_version"] == 1,
        "protocol_version must be integer 1",
    )

    events = document["events"]

    require(type(events) is list, "events must be an array")
    require(1 <= len(events) <= 100, "events size must be 1..100")

    records = []
    seen_ids = set()

    for item in events:
        require_keys(item, {"event_id", "event"}, "events item")

        event_id = item["event_id"]
        require(type(event_id) is str, "event_id must be a string")

        event = validate_event(item["event"])

        expected_id = (
            f'{event["device_id"]}/'
            f'{event["session_id"]}/'
            f'{event["sequence"]}'
        )

        require(event_id == expected_id, "event_id does not match event")
        require(event_id not in seen_ids, "duplicate event_id in request")

        seen_ids.add(event_id)
        records.append((event_id, event))

    return records


def store_batch(database, records):
    results = []

    # with 负责提交或回滚；BEGIN IMMEDIATE 明确事务起点。
    with database:
        database.execute("BEGIN IMMEDIATE")

        for event_id, event in records:
            row = database.execute(
                """
                SELECT payload_json
                FROM received_events
                WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()

            if row is None:
                database.execute(
                    """
                    INSERT INTO received_events(event_id, payload_json)
                    VALUES (?, ?)
                    """,
                    (event_id, encode_json(event).decode("utf-8")),
                )
                status = "accepted"

            elif json.loads(row[0]) == event:
                # 按字段语义比较，忽略 JSON 键顺序和空白。
                status = "duplicate"

            else:
                # 冲突时保留原记录，禁止覆盖。
                status = "conflict"

            results.append({
                "event_id": event_id,
                "status": status,
            })

        response = encode_json({
            "protocol_version": 1,
            "results": results,
        })

        if len(response) > MAX_RESPONSE_BYTES:
            # 此异常仍在事务中，整批新增记录会回滚。
            raise HttpError(413, "response would exceed size limit")

    # 离开 with 且提交成功后，才把确认交给 HTTP 层。
    return response


class ReceiverServer(HTTPServer):
    def __init__(self, address, database, mode, delay_ms):
        super().__init__(address, ReceiverHandler)
        self.database = database
        self.mode = mode
        self.delay_ms = delay_ms


class ReceiverHandler(BaseHTTPRequestHandler):
    # 每个响应后关闭连接，简化模拟服务的连接管理。
    protocol_version = "HTTP/1.0"

    def setup(self):
        super().setup()
        self.connection.settimeout(5.0)

    def send_body(self, status, body):
        self.close_connection = True

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()

        self.wfile.write(body)
        self.wfile.flush()

    def read_records(self):
        if self.path != "/v1/events/batch":
            raise HttpError(404, "unknown endpoint")

        if self.headers.get("Transfer-Encoding") is not None:
            raise HttpError(400, "Transfer-Encoding is not supported")

        content_type = self.headers.get("Content-Type", "")
        media_type = content_type.split(";", 1)[0].strip().lower()

        if media_type != "application/json":
            raise HttpError(415, "Content-Type must be application/json")

        lengths = self.headers.get_all("Content-Length", [])

        if not lengths:
            raise HttpError(411, "Content-Length is required")

        require(len(lengths) == 1, "multiple Content-Length headers")

        length_text = lengths[0].strip()

        require(
            length_text.isascii()
            and length_text.isdecimal(),
            "invalid Content-Length",
        )

        # 先比较文本长度，避免对超长整数文本调用 int。
        if len(length_text) > 10:
            raise HttpError(413, "request body is too large")

        length = int(length_text)

        if length > MAX_REQUEST_BYTES:
            raise HttpError(413, "request body is too large")

        require(length > 0, "request body is empty")

        raw = self.rfile.read(length)
        require(len(raw) == length, "incomplete request body")

        return validate_batch(decode_json(raw))

    def process_post(self):
        records = self.read_records()

        forced_status = {
            "http-401": 401,
            "http-409": 409,
            "http-302": 302,
            "http-429": 429,
        }.get(self.server.mode)

        if forced_status is not None:
            # 这些模式只返回状态，不保存本次事件。
            self.send_body(
                forced_status,
                encode_json({
                    "error": f"simulated HTTP {forced_status}",
                }),
            )
            return

        if self.server.mode == "http-503":
            self.send_body(503, encode_json({
                "error": "simulated service unavailable",
            }))
            return

        response = store_batch(
            self.server.database,
            records,
        )

        # 以下故障都发生在存储提交之后。
        if self.server.mode == "drop-after-commit":
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            return

        if self.server.mode == "delay":
            time.sleep(self.server.delay_ms / 1000.0)

        if self.server.mode == "invalid-json":
            response = b'{"protocol_version":'

        elif self.server.mode == "oversized-body":
            response = b" " * (MAX_RESPONSE_BYTES + 1)

        self.send_body(200, response)

    def do_POST(self):
        try:
            try:
                self.process_post()

            except HttpError as error:
                self.send_body(error.status, encode_json({
                    "error": str(error),
                }))

            except sqlite3.Error as error:
                print(f"Storage error: {error}", file=sys.stderr, flush=True)
                self.send_body(500, encode_json({
                    "error": "receiver storage failure",
                }))

        except OSError as error:
            # 客户端超时或主动断开后，写响应可能失败。
            self.close_connection = True
            print(
                f"Connection ended: {error}",
                file=sys.stderr,
                flush=True,
            )


def main():
    parser = argparse.ArgumentParser(
        description="EdgeRelay mock HTTP receiver",
    )

    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--db", default="run/mock-http/receiver.db")
    parser.add_argument(
        "--mode",
        choices=(
            "normal",
            "http-401",
            "http-409",
            "http-302",
            "http-429",
            "http-503",
            "invalid-json",
            "drop-after-commit",
            "delay",
            "oversized-body",
        ),
        default="normal",
    )
    parser.add_argument("--delay-ms", type=int, default=4000)

    args = parser.parse_args()

    # 0 表示由操作系统分配可用端口, 供自动测试使用
    if not 0 <= args.port <= 65535:
        parser.error("--port must be 0..65535")

    if not 0 <= args.delay_ms <= 60000:
        parser.error("--delay-ms must be 0..60000")

    database_path = Path(args.db).resolve()
    database_path.parent.mkdir(parents=True, exist_ok=True)

    # 单线程服务：连接仅由运行 serve_forever 的线程使用。
    with sqlite3.connect(database_path, timeout=0.25) as database:
        database.execute("PRAGMA synchronous = FULL")
        database.execute(
            """
            CREATE TABLE IF NOT EXISTS received_events (
                event_id TEXT PRIMARY KEY NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )

    database = sqlite3.connect(database_path, timeout=0.25)

    try:
        database.execute("PRAGMA synchronous = FULL")

        with ReceiverServer(
            (args.bind, args.port),
            database,
            args.mode,
            args.delay_ms,
        ) as server:
            print(
                f"Listening: http://{args.bind}:{server.server_address[1]}/v1/events/batch\n"
                f"Database: {database_path}\n"
                f"Mode: {args.mode}",
                flush=True,
            )

            try:
                server.serve_forever()
            except KeyboardInterrupt:
                print("\nStopped", flush=True)

    finally:
        database.close()


if __name__ == "__main__":
    main()