# EdgeRelay 协议概览

本页记录现有实现的主要接口与观察口径。详细非法输入、字段约束和容错分支以验证器、编解码器及其测试为准。

## 1. TCP 输入帧

| 字段 | 长度 | 编码 |
| --- | ---: | --- |
| Magic | 4 字节 | ASCII `ETLM` |
| Payload length | 4 字节 | 无符号 32 位、大端序；只计算载荷字节数 |
| Payload | 指定长度 | UTF-8 JSON |

帧总长度为 `8 + payload_length`。长度按 UTF-8 字节计算，不能按字符数计算。TCP 一次接收可以包含半帧、多帧或混合边界，不能用 send/recv 次数判断帧数。

工程载荷硬上限为 8192 字节；具体运行还受 `tcp.max_payload_bytes` 限制。示例配置使用 4096 字节。

编码示例见 `tests/check_run.py` 的 `frame()`；实现入口包括 `FrameEncoder`、`StreamDecoder` 和 `TcpEventProcessor`。

## 2. 事件载荷

以下示例取自现有验收夹具的字段形式：

```json
{
  "schema_version": 1,
  "device_id": "runtime_sensor",
  "session_id": "12345678-1234-1234-1234-123456789abc",
  "sequence": 1,
  "observed_at_ms": 1700000000000,
  "metric": "temperature",
  "value": 23.5,
  "unit": "degC"
}
```

| 字段 | 含义 |
| --- | --- |
| schema_version | 事件格式版本，当前为 1 |
| device_id | 设备身份 |
| session_id | 输入会话身份，重放同一事件时保留 |
| sequence | 会话内事件序号，整数 |
| observed_at_ms | 事件观察时间，毫秒时间戳 |
| metric | 测项 |
| value | 有限数值 |
| unit | 与测项匹配的单位 |

事件身份为 `device_id/session_id/sequence`，不要把 JSONL 展示消息的 sequence 当成这里的事件序号。

同一事件 ID、相同规范化内容按重复处理；同一 ID、不同内容按身份冲突处理，不能覆盖已存事件。具体字段合法性由 `EventJson`、`EventValidator` 及相关测试定义。

## 3. HTTP 批量转发

示例接收地址：`POST /v1/events/batch`；Content-Type 为 `application/json`。

请求形式：

```json
{
  "protocol_version": 1,
  "events": [
    {
      "event_id": "runtime_sensor/12345678-1234-1234-1234-123456789abc/1",
      "event": {
        "schema_version": 1,
        "device_id": "runtime_sensor",
        "session_id": "12345678-1234-1234-1234-123456789abc",
        "sequence": 1,
        "observed_at_ms": 1700000000000,
        "metric": "temperature",
        "value": 23.5,
        "unit": "degC"
      }
    }
  ]
}
```

示例正常回执：

```json
{
  "protocol_version": 1,
  "results": [
    {
      "event_id": "runtime_sensor/12345678-1234-1234-123456789abc/1",
      "status": "accepted"
    }
  ]
}
```

现有模拟接收端 normal 模式返回 `accepted`、`duplicate` 或 `conflict`。它在数据库事务提交成功后返回确认，并保留发生冲突时的旧内容。

Agent 按事件核对回执；HTTP 200 本身不代表整批事件已确认。完整回执规则由 `BatchResponse`、上传策略与 HTTP 集成测试定义。

## 4. Outbox 状态与计数

| 状态 | 含义 |
| --- | --- |
| PENDING | 等待上传 |
| SENDING | 已领取、正在处理 |
| RETRY | 等待下次尝试 |
| ACKED | 已确认 |
| DEAD | 不再自动补传的记录，保留原因供导出 |

`unacked = total - acked`，包含 DEAD。

`backlog = pending + sending + retry = unacked - dead`，不包含 DEAD。

状态快照中的 backlog 按上述定义派生。

## 5. Agent 与 GUI 的 JSONL 观察通道

Agent 的 stdout 用于 JSONL，stderr 用于诊断。当前协议版本为 1，消息类型包括 ready、status、record、stopped。

| 消息 | 作用 |
| --- | --- |
| ready | 本轮进程完成初始化后的就绪声明 |
| status | 业务累计计数、数据库采样、上传及录制状态等 |
| record | 输入事件及其处置结果的展示消息 |
| stopped | Agent 的停止声明与最终观察 |

每条 JSONL 消息以 LF 结束，单行上限为 65536 字节，包含 LF。消息 sequence 为严格递增的 uint64，允许间隔；run_id 用于区分不同运行。

`--jsonl` 启用结构化运行输出；`--events-jsonl` 隐含启用 JSONL 并增加事件展示。展示通道有队列及限流，展示丢弃计数不能当成业务丢失数。

操作系统报告进程 started 与协议 ready 分开；Agent stopped 声明与实际退出结果也分开。缺失值与有效 0、UNKNOWN/VALID/STALE/ERROR 均保留区别。

完整编码定义见 `include/edge/status/JsonlProtocol.h`；GUI 解析见 `apps/console/protocol/`。本页不替代这些完整消息结构及验证测试。
