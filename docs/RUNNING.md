# 运行说明

本文适用于项目根目录中的源码。依赖要求和核心 CLI 构建命令见 [README](../README.md)。以下示例使用 `build/core/edge-agent`；若采用其他构建目录，替换为该目录中的实际程序路径。

## 配置

`config/release.yml` 是本地运行示例，默认监听 `127.0.0.1:19090`，转发到 `http://127.0.0.1:18080/v1/events/batch`。

配置中的相对路径以配置文件所在目录解析：

| 内容 | 默认示例位置 |
| --- | --- |
| Outbox | `run/release-agent/outbox.db` |
| 原始录制 | `run/release-agent/raw/` |
| 状态快照 | `run/release-agent/status.json` |

```bash
./build/core/edge-agent --config config/release.yml --check-config
```

`--port`、`--receiver-url`、`--data-dir` 可以覆盖配置。`--data-dir` 同时覆盖数据库、录制和状态路径；后续补传、导出必须使用相同覆盖。

## 启动、停止和状态

```bash
./build/core/edge-agent --config config/release.yml --run
```

使用 Ctrl+C 或 SIGTERM 正常停止。状态文件保留最后的快照；查看快照时同时检查采样时间和状态有效性。

```bash
./build/core/edge-agent --config config/release.yml --run --jsonl
./build/core/edge-agent --config config/release.yml --run --events-jsonl
```

`--jsonl` 启用 stdout 结构化运行输出；`--events-jsonl` 隐含启用 JSONL 并增加事件展示。stderr 为诊断通道。

## 仅补传

停止使用同一数据库的 Agent，并保持 HTTP 接收端运行：

```bash
./build/core/edge-agent --config config/release.yml --drain-only --jsonl
```

该模式不监听 TCP，不录制原始流。它处理 PENDING、遗留 SENDING 和 RETRY，包含尚未到期的重试等待；backlog 清零后正常退出。网络失败时继续重试，正常停止会保留未确认消息。

`backlog = PENDING + SENDING + RETRY`，不包含 DEAD。`unacked = total - ACKED`，包含 DEAD。因此正常结束并不表示历史 DEAD 记录消失。

## 离线导出

```bash
mkdir -p run/exports
./build/core/edge-agent --config config/release.yml \
  --export-dead "$PWD/run/exports/dead.jsonl"
```

该模式持有 Agent 的单实例锁，只读既有数据库，不启动采集、上传或状态流程。数据库不存在时失败；输出已存在时拒绝覆盖。无 DEAD 时生成空文件并正常退出。

输出字段包括 `export_version`、`event_id`、原始 `payload_json`、状态、尝试次数、时间字段和 `last_error`。

辅助脚本 `scripts/export_dead.py` 不持有 Agent 单实例锁；正式离线管理优先使用 Agent 命令。

## GUI 构建

GUI 需要额外安装 Qt6 Core、Widgets。以下按已验证的 Qt 6.11.0 安装布局举例，Qt 安装位置不同应调整路径。

```bash
(
  set -euo pipefail
  export PATH=/usr/bin:/bin
  unset LD_LIBRARY_PATH LD_PRELOAD LIBRARY_PATH CMAKE_PREFIX_PATH

  qt_cmake="$HOME/Qt/Tools/CMake/bin/cmake"
  qt_ctest="$HOME/Qt/Tools/CMake/bin/ctest"
  qt6_dir="$HOME/Qt/6.11.0/gcc_64/lib/cmake/Qt6"

  "$qt_cmake" -S . -B build/gui \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_COMPILER=/usr/bin/g++ \
    -DPython3_EXECUTABLE=/usr/bin/python3 \
    -DQt6_DIR="$qt6_dir" \
    -DEDGE_BUILD_GUI=ON \
    -DBUILD_TESTING=ON

  "$qt_cmake" --build build/gui \
    --target edge-console edge-console-tests edge-console-process-tests \
    --parallel 2

  "$qt_ctest" --test-dir build/gui -L '^console$' \
    --output-on-failure --no-tests=error
)
```

启动窗口：

```bash
env -u LD_LIBRARY_PATH -u LD_PRELOAD PATH=/usr/bin:/bin \
  ./build/gui/edge-console
```

选择 `build/core/edge-agent` 和配置文件。配置已经包含独立数据路径时，数据目录输入框留空。等窗口确认就绪后再发送事件，正常停止时等待子进程回收。

GUI 保留最近 200 条历史消息。历史淘汰、事件缓存丢弃、Agent 展示丢弃和业务处理计数分开统计。控制台仅管理其自己创建的子进程。

## 维护脚本

现有脚本保留原文件名，避免破坏调用和验收入口：

| 入口 | 用途 |
| --- | --- |
| `scripts/build_and_test.sh --core-only` | 使用系统工具链构建核心并测试 |
| `scripts/build_and_test.sh` | 核心和已配置 Qt Creator GUI 构建目录的回归 |
| `scripts/check_clean_build_m9.sh` | 新目录中的核心和 GUI 干净构建 |
| `scripts/check_release_m9.py` | 源码打包、外部目录解压构建及结果收集 |

这些维护入口与 README 的标准 CMake 命令使用不同的构建目录，运行程序时应选择对应产物。新解压源码没有旧 Qt Creator 缓存时，干净构建入口需要显式设置 `EDGE_QT6_DIR`。

## Agent 退出码

| 值 | 当前实现含义 |
| --- | --- |
| 0 | 正常操作或运行结束；正常停止允许保留待补传消息 |
| 1 | 采集、补传或运行输出初始化等运行流程失败 |
| 2 | 参数、配置错误；离线导出失败，包括数据库、目标文件和实例锁错误 |

Python 辅助程序各自定义退出码。验收还应核对最终状态和数据库内容。
