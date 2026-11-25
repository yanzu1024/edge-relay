#pragma once

#include <cstdint>

namespace edge {

class SqliteDatabase;

// 项目约定的识别码，十六进制字节对应 ERLY。
// 这是本项目内部约定，不代表全球注册的唯一编号。
inline constexpr std::int32_t kOutboxApplicationId = 0x45524C59;

inline constexpr std::int32_t kOutboxSchemaVersion = 1;

// 初始化或验证主数据库的 Outbox 结构。
// 使用自己的短事务，因此不能在外部事务中调用。
// 不创建目录，不配置上传，不修改现有事件数据。
void initializeOutboxSchema(SqliteDatabase& database);

} // namespace edge