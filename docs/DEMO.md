# 完整链路演示

该演示使用固定的 30 条唯一事件和 1 条重放事件，完成输入、入库、上传、GUI 展示、录制及最终核对。无需真实设备或外部后端。

开始前按 [README](../README.md) 和 [运行说明](RUNNING.md#gui-构建) 完成 CLI、GUI 构建。以下使用 `build/core`、`build/gui`；其他构建方式替换为实际程序路径。

## 1. 创建独立会话

正常停止此前占用本项目 19090、18080 端口的进程。所有终端进入同一个项目根目录，每轮只执行一次：

```bash
/usr/bin/python3 scripts/check_console_m8.py prepare
```

脚本输出本轮配置文件绝对路径，并更新 `run/m8-current-session.txt`。旧会话目录保留。

## 2. 启动接收端

终端 A：

```bash
/usr/bin/python3 scripts/check_console_m8.py receiver
```

保持运行，接收端数据库应属于本轮会话。

## 3. 启动控制台

```bash
env -u LD_LIBRARY_PATH -u LD_PRELOAD PATH=/usr/bin:/bin \
  ./build/gui/edge-console
```

| 输入项 | 选择 |
| --- | --- |
| Agent | 本次构建的 `build/core/edge-agent` |
| 配置 | 本轮 prepare 输出的绝对路径 |
| 数据目录 | 清空并留空 |

本演示不使用 `config/release.yml`，因为验收脚本需要自己的会话配置与录制路径。点击启动并等待就绪。

## 4. 发送和核对

终端 B：

```bash
/usr/bin/python3 scripts/check_console_m8.py send
/usr/bin/python3 scripts/check_console_m8.py check
```

预期 `PASS: live`。在 GUI 点击正常停止，等待子进程正常结束，然后执行：

```bash
/usr/bin/python3 scripts/check_console_m8.py verify
```

预期 `PASS: final`。之后关闭窗口并停止接收端。

| 项目 | 本轮预期 |
| --- | ---: |
| 有效输入帧 | 31 |
| 唯一入库事件 | 30 |
| 重复事件 | 1 |
| 上传 ACK | 30 |
| 最终 Outbox ACKED / total | 30 / 30 |
| 最终 unacked / backlog | 0 / 0 |
| GUI 完整接收时的历史行 | 31 |
| Agent 展示丢弃、GUI 协议错误和事件缓存丢弃 | 0 |

脚本比较接收端完整 ID/载荷与原始录制字节；GUI 内部计数需在界面观察。

## 5. 证据

相关进程正常停止后，保留本轮会话目录，包括配置、两个数据库、状态文件、原始录制及 `evidence/` 中的输入和 manifest。构建产物及运行会话不提交到 Git。

发送失败或中断后，使用新的 prepare 会话重试，接收端和 GUI 同时切换。不要在同一发送 manifest 上重复发送。

## 专项验收入口

| 入口 | 场景 |
| --- | --- |
| `scripts/check_console_m9.py` | 1000 条唯一事件一致性 |
| `scripts/check_console_m9_soak.py` | 三设备、每设备 5 条/秒、30 分钟持续运行 |
| `drain_backlog_contract` | 积压补传、RETRY 等待、停止和恢复 |
| `export_dead_cli_contract` | 离线导出和单实例锁 |

脚本保留既有文件名。日常展示使用短场景，专项验收按对应目标单独执行。
