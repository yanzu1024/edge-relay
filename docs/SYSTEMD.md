# systemd 示例

**当前仅提供模板，尚未进行实际服务部署验收。**

模板：[systemd/edge-agent.service.example](systemd/edge-agent.service.example)。

| 路径或身份 | 模板假定 |
| --- | --- |
| `/opt/edge-relay/bin/edge-agent` | 适用于部署环境的可执行文件 |
| `/opt/edge-relay/config/release.yml` | 实际配置，服务用户可读 |
| `/opt/edge-relay/run/` | 服务用户可创建数据库、锁、状态及录制文件 |
| `edgerelay` 用户及组 | 事先创建并配置权限 |
| 动态库及 HTTP 接收端 | 由部署环境准备 |

示例配置的相对路径以配置文件目录解析，数据位于 `/opt/edge-relay/run/release-agent/`。

模板使用 `--run --jsonl`，stdout/stderr 交给 journal，SIGTERM 请求正常停止，停止超时示例为 12 秒。异常退出时按模板尝试重启。

不要与 GUI、手工 Agent 或补传进程同时使用同一数据库。实例锁会拒绝同库并发管理。

实际部署需另行验证用户权限、依赖加载、配置、服务启停、故障恢复和开机启动，并保留结果。模板本身不代表这些验证已经完成。
