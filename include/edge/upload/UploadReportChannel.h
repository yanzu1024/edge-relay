#pragma once

#include "edge/upload/OutboxUploader.h"

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>

namespace edge {

// 单个上传线程生产报告，主线程消费。
//
// 本对象不拥有线程。
// 销毁前必须结束所有正在访问它的线程。
class UploadReportChannel {
public:
    using Clock = std::chrono::steady_clock;

    UploadReportChannel() = default;

    UploadReportChannel(const UploadReportChannel&) = delete;

    UploadReportChannel& operator=(
        const UploadReportChannel&
    ) = delete;

    UploadReportChannel(UploadReportChannel&&) = delete;

    UploadReportChannel& operator=(
        UploadReportChannel&&
    ) = delete;

    // 槽位满时等待。
    //
    // 返回 true：已复制并接收报告。
    // 返回 false：通道已经关闭，没有接收本次报告。
    //
    // 调用者不能把 false 当成报告已成功交付。
    bool publish(const UploadCycleResult& report);

    // 不等待；没有报告时返回 nullopt。
    std::optional<UploadCycleResult> tryTake();

    // 等待报告、通道关闭或截止时间。
    // 关闭后仍允许取走已经接收的报告。
    std::optional<UploadCycleResult> waitTakeUntil(
        Clock::time_point deadline
    );

    // 禁止继续接收报告，并唤醒所有等待者。
    // 不删除已经接收的报告；允许重复调用。
    void close();

private:
    std::mutex mutex_;
    std::condition_variable cv_;

    std::optional<UploadCycleResult> slot_;
    bool closed_ = false;
};

} // namespace edge