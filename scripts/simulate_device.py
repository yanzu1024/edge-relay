#!/usr/bin/env python3
"""Generate simulated telemetry using the EdgeRelay ETLM protocol."""

import argparse
import json
import re
import socket
import struct
import sys
import time
import uuid


MAGIC = b"ETLM"
HEADER_SIZE = 8
MAX_PAYLOAD_BYTES = 8192


def positive_int(text):
    value = int(text)

    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")

    return value


def nonnegative_int(text):
    value = int(text)

    if value < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")

    return value


def parse_arguments():
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=positive_int, default=19090)

    parser.add_argument(
        "--mode",
        choices=[
            "normal",
            "split",
            "batch",
            "mixed",
            "timeout",
            "isolation",
            "resync",
        ],
        default="normal",
    )

    parser.add_argument("--device-id", default="sim_sensor")

    parser.add_argument(
        "--count",
        type=positive_int,
        default=3,
        help="event count for normal, split and batch; maximum 1000",
    )

    parser.add_argument(
        "--interval-ms",
        type=nonnegative_int,
        default=100,
        help="interval between events in normal and split modes",
    )

    parser.add_argument(
        "--chunk-size",
        type=positive_int,
        default=7,
        help="bytes per send in split mode",
    )

    parser.add_argument(
        "--chunk-gap-ms",
        type=nonnegative_int,
        default=20,
        help="interval between chunks in split mode",
    )

    parser.add_argument(
        "--wait-ms",
        type=positive_int,
        default=4000,
        help="partial-frame waiting time in timeout mode",
    )

    args = parser.parse_args()

    if args.port > 65535:
        parser.error("--port must be between 1 and 65535")

    if args.count > 1000:
        parser.error("--count must not exceed 1000")

    if re.fullmatch(r"[A-Za-z0-9_-]{1,32}", args.device_id) is None:
        parser.error(
            "--device-id must contain 1-32 ASCII letters, "
            "digits, underscores or hyphens"
        )

    if args.mode == "isolation" and len(args.device_id) > 30:
        parser.error(
            "--device-id must not exceed 30 characters "
            "in isolation mode"
        )

    return args


def pause_ms(milliseconds):
    if milliseconds > 0:
        time.sleep(milliseconds / 1000.0)


def make_event(device_id, session_id, sequence):
    # 同一连接复用 session_id，sequence 在会话内递增。
    # event_id 由接收端生成，不放入八字段载荷。
    return {
        "schema_version": 1,
        "device_id": device_id,
        "session_id": session_id,
        "sequence": sequence,
        "observed_at_ms": time.time_ns() // 1_000_000,
        "metric": "temperature",
        "value": round(23.0 + sequence * 0.1, 3),
        "unit": "degC",
    }


def encode_frame(payload):
    if isinstance(payload, dict):
        payload = json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )

    body = payload.encode("utf-8")

    # 长度必须按 UTF-8 字节数计算，不能使用字符数量。
    if not 1 <= len(body) <= MAX_PAYLOAD_BYTES:
        raise ValueError("payload size must be between 1 and 8192 bytes")

    # !I 表示网络字节序的 uint32。
    return MAGIC + struct.pack("!I", len(body)) + body


def event_frame(device_id, session_id, sequence):
    return encode_frame(
        make_event(device_id, session_id, sequence)
    )


def connect(args):
    # 超时作用于连接和后续阻塞 Socket 操作。
    return socket.create_connection(
        (args.host, args.port),
        timeout=5,
    )


def announce(device_id, session_id):
    print(
        f"Device: {device_id}, session: {session_id}",
        flush=True,
    )


def finish(connection):
    # 告知服务端本连接不会再发送数据。
    connection.shutdown(socket.SHUT_WR)

    # 当前服务端不发送业务响应，等待它关闭连接即可。
    # 这不是应用层 ACK，验证结果仍以服务端输出为准。
    if connection.recv(1) != b"":
        raise RuntimeError("unexpected data returned by server")


def send_regular(args):
    session_id = str(uuid.uuid4())
    announce(args.device_id, session_id)

    with connect(args) as connection:
        if args.mode == "batch":
            wire = b"".join(
                event_frame(args.device_id, session_id, sequence)
                for sequence in range(1, args.count + 1)
            )

            connection.sendall(wire)

        else:
            for sequence in range(1, args.count + 1):
                wire = event_frame(
                    args.device_id,
                    session_id,
                    sequence,
                )

                if args.mode == "split":
                    for offset in range(0, len(wire), args.chunk_size):
                        connection.sendall(
                            wire[offset:offset + args.chunk_size]
                        )

                        if offset + args.chunk_size < len(wire):
                            pause_ms(args.chunk_gap_ms)
                else:
                    connection.sendall(wire)

                if sequence < args.count:
                    pause_ms(args.interval_ms)

        finish(connection)


def send_mixed(args):
    session_id = str(uuid.uuid4())
    announce(args.device_id, session_id)

    wire = (
        event_frame(args.device_id, session_id, 1)
        + encode_frame("{")
        # JSON 结构正确，但 sequence=0 不符合业务规则。
        + event_frame(args.device_id, session_id, 0)
        + event_frame(args.device_id, session_id, 2)
    )

    with connect(args) as connection:
        connection.sendall(wire)
        finish(connection)


def send_timeout(args):
    session_id = str(uuid.uuid4())
    announce(args.device_id, session_id)

    late_frame = event_frame(args.device_id, session_id, 1)

    # 帧头完整、载荷未完成，使解码器进入部分帧计时状态。
    cut = HEADER_SIZE + 4

    with connect(args) as connection:
        connection.sendall(late_frame[:cut])

        print(
            f"Partial frame sent; waiting {args.wait_ms} ms",
            flush=True,
        )

        pause_ms(args.wait_ms)

        # 先发送旧帧迟到的剩余部分，再发送一个新的完整帧。
        # 超时足够长时，旧帧不应再被补齐。
        connection.sendall(
            late_frame[cut:]
            + event_frame(args.device_id, session_id, 2)
        )

        finish(connection)


def send_isolation(args):
    first_device = args.device_id + "_a"
    second_device = args.device_id + "_b"

    first_session = str(uuid.uuid4())
    second_session = str(uuid.uuid4())

    announce(first_device, first_session)
    announce(second_device, second_session)

    first_frame = event_frame(first_device, first_session, 1)
    second_frame = event_frame(second_device, second_session, 1)

    cut = len(first_frame) // 2

    with connect(args) as first, connect(args) as second:
        # A 暂留半帧。
        first.sendall(first_frame[:cut])

        # B 独立发送完整帧，并完成本连接。
        second.sendall(second_frame)
        finish(second)

        # 最后补齐 A。两个连接应分别得到合法事件。
        first.sendall(first_frame[cut:])
        finish(first)


def send_resync(args):
    session_id = str(uuid.uuid4())
    announce(args.device_id, session_id)

    # 故意绕过 encode_frame，构造非法帧头。
    zero_length = MAGIC + struct.pack("!I", 0)
    oversized = MAGIC + struct.pack("!I", MAX_PAYLOAD_BYTES + 1)

    wire = (
        b"\x00noise\xff"
        + zero_length
        + oversized
        + event_frame(args.device_id, session_id, 1)
    )

    with connect(args) as connection:
        connection.sendall(wire)
        finish(connection)


def main():
    args = parse_arguments()

    print(
        f"Target: {args.host}:{args.port}, mode: {args.mode}",
        flush=True,
    )

    try:
        if args.mode in ("normal", "split", "batch"):
            send_regular(args)
            expected = (args.count, 0, 0, 0)

        elif args.mode == "mixed":
            send_mixed(args)
            expected = (2, 1, 1, 0)

        elif args.mode == "timeout":
            send_timeout(args)
            expected = (1, 0, 0, 1)

        elif args.mode == "isolation":
            send_isolation(args)
            expected = (2, 0, 0, 0)

        else:
            send_resync(args)
            expected = (1, 0, 0, 0)

    except KeyboardInterrupt:
        print("Simulator interrupted.", file=sys.stderr)
        return 130

    except (OSError, ValueError, RuntimeError) as error:
        print(f"Simulation failed: {error}", file=sys.stderr)
        return 1

    valid, json_errors, validation_errors, timeouts = expected
    completed = valid + json_errors + validation_errors

    print("Sending finished. Check the agent output.")
    print(
        "Expected delta: "
        f"completed_frames={completed}, "
        f"valid_events={valid}, "
        f"json_errors={json_errors}, "
        f"validation_errors={validation_errors}, "
        f"partial_timeouts={timeouts}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())