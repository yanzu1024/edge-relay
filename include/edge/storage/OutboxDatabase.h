#pragma once

#include "edge/storage/SqliteDatabase.h"

#include <filesystem>

namespace edge {

// 使用配置加载器已经规范化的绝对路径。
// 创建父目录，打开数据库，初始化或校验 Outbox 结构。
// 成功后返回拥有连接的对象。
//
// 此函数有文件系统副作用，只能用于运行初始化，
// 不能放进 --check-config 或配置加载流程。
SqliteDatabase openOutboxDatabase(
    const std::filesystem::path& absolute_path
);

} // namespace edge