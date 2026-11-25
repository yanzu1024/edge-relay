#pragma once

#include <cstdint>
#include <filesystem>

namespace edge {

// 调用方必须在整个导出期间持有 StorageInstanceLock。
// 只读既有数据库，不覆盖输出文件。
std::uint64_t exportDeadFile(
    const std::filesystem::path& database_path,
    const std::filesystem::path& output_path
);

} // namespace edge