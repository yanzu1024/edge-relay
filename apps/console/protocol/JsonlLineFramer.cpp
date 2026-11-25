#include "JsonlLineFramer.h"

#include <stdexcept>
#include <utility>

namespace edge::console {

namespace {

// 防止回调中再次 feed、finish 或 reset，破坏当前输入状态。
class ProcessingGuard {
public:
    explicit ProcessingGuard(bool& processing)
        : processing_(processing)
    {
        if (processing_) {
            throw std::logic_error(
                "JsonlLineFramer does not allow reentrant modification"
            );
        }

        processing_ = true;
    }

    ~ProcessingGuard()
    {
        processing_ = false;
    }

    ProcessingGuard(const ProcessingGuard&) = delete;
    ProcessingGuard& operator=(const ProcessingGuard&) = delete;

private:
    bool& processing_;
};

} // namespace

void JsonlLineFramer::feed(
    std::string_view bytes,
    const Sink& sink
)
{
    ProcessingGuard guard(processing_);

    if (finished_) {
        throw std::logic_error(
            "Reset JsonlLineFramer before feeding a new stream"
        );
    }

    if (!sink) {
        throw std::invalid_argument(
            "JsonlLineFramer requires a result sink"
        );
    }

    for (const char byte : bytes) {
        if (dropping_) {
            // 超长行的剩余内容不再进入缓冲。
            if (byte == '\n') {
                dropping_ = false;
            }

            continue;
        }

        if (byte == '\n') {
            JsonlFrame frame{
                JsonlFrameKind::Line,
                std::string(pending_.data(), used_)
            };

            // 在回调前完成当前行的状态更新。
            used_ = 0;
            sink(std::move(frame));
            continue;
        }

        if (used_ == kMaxPayloadBytes) {
            used_ = 0;
            dropping_ = true;

            sink(JsonlFrame{
                JsonlFrameKind::LineTooLong,
                {}
            });

            continue;
        }

        pending_[used_] = byte;
        ++used_;
    }
}

void JsonlLineFramer::finish(const Sink& sink)
{
    ProcessingGuard guard(processing_);

    if (finished_) {
        return;
    }

    if (!sink) {
        throw std::invalid_argument(
            "JsonlLineFramer requires a result sink"
        );
    }

    // 已报告过的超长行不再重复报告不完整尾部。
    const bool incomplete_tail = !dropping_ && used_ != 0;

    used_ = 0;
    dropping_ = false;
    finished_ = true;

    if (incomplete_tail) {
        sink(JsonlFrame{
            JsonlFrameKind::IncompleteTail,
            {}
        });
    }
}

void JsonlLineFramer::reset()
{
    ProcessingGuard guard(processing_);

    used_ = 0;
    dropping_ = false;
    finished_ = false;
}

std::size_t JsonlLineFramer::bufferedBytes() const noexcept
{
    return used_;
}

} // namespace edge::console
