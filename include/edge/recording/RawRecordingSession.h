#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <string>

namespace edge {

enum class RawRecordingOperation {
    None,
    Input,
    Metadata,
    Open,
    Write,
    Close,
    Notify,
    Limit
};

struct RawRecordingSessionStats {
    // 成功 write 返回的字节数，包括故障发生前的部分写入。
    std::uint64_t bytes_written = 0;

    std::uint64_t opened_files = 0;

    // 正常关闭流程中，close 检查成功的文件数。
    // 故障后的兜底关闭不计入。
    std::uint64_t closed_files = 0;

    bool failed = false;
    bool finished = false;

    RawRecordingOperation error_operation =
        RawRecordingOperation::None;

    int error_code = 0;
};

struct RawClosedSegment {
    std::filesystem::path path;

    std::uint64_t connection_id = 0;
    std::uint64_t part_index = 0;
    std::uint64_t bytes = 0;
};

// 回调发生时文件已经关闭，且 close 检查成功。
// 如需保留信息，回调必须自行复制，不得保存参数引用。
// 回调不得重入本对象的 append / close。
using RawClosedSegmentHandler =
    std::function<void(const RawClosedSegment&)>;

// 纯文件名构造，不执行文件系统操作。
// run_id 必须为 32 位小写十六进制字符串。
// connection_id、part_index 必须为正数。
std::string makeRawRecordingFileName(
    const std::string& run_id,
    std::uint64_t connection_id,
    std::uint64_t part_index
);

// 单个 TCP 连接的原始录制会话，单线程使用。
// 不拥有 TCP 连接，也不解析字节内容。
//
// directory 必须是已存在的绝对目录；
// 构造函数不创建目录、不打开录制文件。
// 第一次非空 append 时才排他创建文件。
//
// 上层应显式 close 并读取最终统计。
// 析构只用于兜底关闭；回调及其引用对象必须活得更久。
class RawRecordingSession {
public:
    RawRecordingSession(
        std::filesystem::path directory,
        std::string run_id,
        std::uint64_t connection_id,
        std::uint64_t max_file_bytes,
        RawClosedSegmentHandler on_closed = {}
    );

    ~RawRecordingSession() noexcept;

    RawRecordingSession(const RawRecordingSession&) = delete;

    RawRecordingSession& operator=(
        const RawRecordingSession&
    ) = delete;

    RawRecordingSession(RawRecordingSession&&) = delete;

    RawRecordingSession& operator=(
        RawRecordingSession&&
    ) = delete;

    // size=0 时不创建文件。
    // I/O 或回调失败返回 false，并锁定 failed 状态。
    // 已失败或已关闭的会话不能重新开始。
    bool append(
        const std::uint8_t* data,
        std::size_t size
    ) noexcept;

    // 可重复调用，不重复关闭或重复通知。
    bool close() noexcept;

    const RawRecordingSessionStats& stats() const noexcept;

    // 没有打开文件时返回空路径。
    const std::filesystem::path& activePath() const noexcept;

private:
    bool openNext() noexcept;
    bool closeCurrent() noexcept;

    void fail(
        RawRecordingOperation operation,
        int error_code
    ) noexcept;

    std::filesystem::path directory_;
    std::string run_id_;

    std::uint64_t connection_id_ = 0;
    std::uint64_t max_file_bytes_ = 0;

    RawClosedSegmentHandler on_closed_;

    int fd_ = -1;
    std::filesystem::path active_path_;

    std::uint64_t part_index_ = 0;
    std::uint64_t current_bytes_ = 0;

    RawRecordingSessionStats stats_;
};

} // namespace edge