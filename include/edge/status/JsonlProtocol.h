#pragma once

#include "edge/domain/SensorEvent.h"
#include "edge/status/StatusSnapshot.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <variant>

namespace edge {

inline constexpr std::int32_t kJsonlProtocolVersion = 1;

// 包含末尾换行。
inline constexpr std::size_t kMaxJsonlLineBytes = 64 * 1024;

// 后续展示发送层使用，不作用于业务入库或上传。
inline constexpr std::uint32_t kMaxJsonlRecordsPerSecond = 100;

struct JsonlMessageMeta {
    std::string run_id;
    std::uint64_t sequence = 0;
    std::int64_t emitted_at_ms = 0;
};

struct JsonlReadyData {
    std::string agent_id;
    std::string tcp_bind;
    std::int32_t tcp_port = 0;

    bool events_jsonl = false;
};

enum class JsonlRecordDisposition {
    Unknown,
    Stored,
    Duplicate,
    IdentityConflict,
    CapacityRejected,
    StorageSkipped,
    StorageError
};

struct JsonlRecordData {
    // 必须来自现有业务验证层。
    SensorEvent event;

    // 必须根据真实处理结果设置，不能在提交前标记 Stored。
    JsonlRecordDisposition disposition =
        JsonlRecordDisposition::Unknown;
};

enum class JsonlEncodeCode {
    InvalidMetadata,
    InvalidPayload,
    InvalidTextEncoding,
    LineTooLarge
};

struct JsonlEncodeError {
    JsonlEncodeCode code;
    std::string message;
};

// 成功字符串已经包含末尾换行。
// 失败只返回错误，不返回半截消息。
using JsonlEncodeResult = std::variant<
    std::string,
    JsonlEncodeError
>;

// 以下函数均不读取时钟、不生成身份、不写文件或控制台。
// 不修改调用方提供的序号。
JsonlEncodeResult encodeJsonlReady(
    const JsonlMessageMeta& meta,
    const JsonlReadyData& ready
);

JsonlEncodeResult encodeJsonlStatus(
    const JsonlMessageMeta& meta,
    const StatusSnapshot& snapshot,
    const StatusConfig& config
);

JsonlEncodeResult encodeJsonlRecord(
    const JsonlMessageMeta& meta,
    const JsonlRecordData& record
);

// 当前 runAgent 的运行退出码为 0 或 1。
// exit_code=0 对应 STOPPED，exit_code=1 对应 FAILED。
// 最终快照必须表明连接已关闭、上传线程已结束。
JsonlEncodeResult encodeJsonlStopped(
    const JsonlMessageMeta& meta,
    int exit_code,
    const StatusSnapshot& snapshot,
    const StatusConfig& config
);

} // namespace edge