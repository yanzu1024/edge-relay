#include "edge/upload/UploadReportChannel.h"

#include <utility>

namespace edge {

bool UploadReportChannel::publish(
    const UploadCycleResult& report
)
{
    std::unique_lock<std::mutex> lock(mutex_);

    cv_.wait(lock, [this] {
        return closed_ || !slot_.has_value();
    });

    if (closed_) {
        return false;
    }

    // 复制报告，避免保存工作线程局部对象的引用。
    slot_.emplace(report);

    lock.unlock();

    cv_.notify_all();

    return true;
}

std::optional<UploadCycleResult>
UploadReportChannel::tryTake()
{
    std::unique_lock<std::mutex> lock(mutex_);

    if (!slot_.has_value()) {
        return std::nullopt;
    }

    auto result = std::move(slot_);

    slot_.reset();

    lock.unlock();

    // 生产者可能正在等待空槽位。
    cv_.notify_all();

    return result;
}

std::optional<UploadCycleResult>
UploadReportChannel::waitTakeUntil(
    Clock::time_point deadline
)
{
    std::unique_lock<std::mutex> lock(mutex_);

    cv_.wait_until(
        lock,
        deadline,
        [this] {
            return closed_ || slot_.has_value();
        }
    );

    if (!slot_.has_value()) {
        return std::nullopt;
    }

    auto result = std::move(slot_);

    slot_.reset();

    lock.unlock();

    cv_.notify_all();

    return result;
}

void UploadReportChannel::close()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);

        closed_ = true;
    }

    cv_.notify_all();
}

} // namespace edge