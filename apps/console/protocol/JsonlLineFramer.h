#pragma once

#include <array>
#include <cstddef>
#include <functional>
#include <string>
#include <string_view>

namespace edge::console {

enum class JsonlFrameKind {
    Line,
    LineTooLong,
    IncompleteTail
};

struct JsonlFrame {
    JsonlFrameKind kind;

    // 仅 Line 携带内容，不包含末尾 LF。
    // 使用拥有自身数据的字符串，允许接收方移动或保存。
    std::string bytes;
};

class JsonlLineFramer {
public:
    // 协议 v1 的单行上限，包含末尾 LF。
    static constexpr std::size_t kMaxLineBytes = 65536;
    static constexpr std::size_t kMaxPayloadBytes =
        kMaxLineBytes - 1;

    using Sink = std::function<void(JsonlFrame)>;

    // 同步交付结果，不在内部保存历史消息。
    // sink 不应抛异常；异常会向调用方传播，
    // 当前输入片段的剩余部分不保证已经消费。
    void feed(std::string_view bytes, const Sink& sink);

    // 输入流结束后调用；重复调用无额外输出。
    // finish 后再次 feed 必须先 reset。
    void finish(const Sink& sink);

    // 新运行开始前清除所有拆行状态。
    // feed / finish 的回调期间禁止重入修改本对象。
    void reset();

    [[nodiscard]]
    std::size_t bufferedBytes() const noexcept;

private:
    // 固定大小缓冲，超长输入不能让它继续增长。
    std::array<char, kMaxPayloadBytes> pending_{};

    std::size_t used_ = 0;
    bool dropping_ = false;
    bool finished_ = false;
    bool processing_ = false;
};

} // namespace edge::console
