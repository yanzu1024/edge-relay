#pragma once

#include "edge/status/StatusSnapshot.h"

#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>

namespace edge {

using StatusDiskQuery = std::function<
    std::uint64_t(const std::filesystem::path&)
>;

// 必须在拥有 store 及其数据库连接的线程中调用。
//
// 使用 snapshot.checked_at_ms 作为本轮采样开始时间。
// 每次重新取得两个资源的结果，不保留上次成功值冒充本次结果。
// 两项查询独立：其中一项失败，不阻止另一项采样。
//
// disk_query 为空时使用真实文件系统查询；
// 非空时用于注入可验证的磁盘结果或故障。
//
// 不修改业务计数，不创建目录，不输出日志。
void sampleStatusResources(
    StatusSnapshot& snapshot,
    OutboxStore& store,
    const std::filesystem::path& outbox_path,
    StatusDiskQuery disk_query = {}
);

// 两个路径必须是配置加载器已经解析的绝对文件路径。
// 防止状态文件覆盖 Outbox 数据库及其锁/日志文件。
//
// 在目标目录排他创建临时文件，完整写入并检查 close，
// 最后 rename 到目标位置。
// rename 前失败时保留原目标，并清理本次临时文件。
//
// 本函数不提供物理断电后的持久性保证。
// 失败通过异常交给运行层处理。
void writeStatusFileAtomically(
    const std::filesystem::path& status_path,
    const std::filesystem::path& outbox_path,
    const std::string& contents
);

} // namespace edge