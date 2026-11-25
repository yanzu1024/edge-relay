#include "edge/upload/UploadTrigger.h"

#include <stdexcept>

#include "edge/config/AgentConfig.h"

namespace edge {

UploadTrigger::UploadTrigger(const UploadConfig& config)
{
    if (config.batch_size <= 0
        || config.batch_size > 100
        || config.flush_interval_ms <= 0) {
        throw std::invalid_argument(
            "Upload trigger requires batch size 1..100 "
            "and a positive flush interval"
        );
    }

    batch_size_ =
        static_cast<std::size_t>(config.batch_size);

    flush_interval_ = std::chrono::duration_cast<
        Clock::duration
    >(
        std::chrono::milliseconds(config.flush_interval_ms)
    );
}

bool UploadTrigger::shouldUpload(
    std::size_t ready_count,
    Clock::time_point now
)
{
    if (ready_count == 0) {
        deadline_.reset();
        return false;
    }

    if (ready_count >= batch_size_) {
        return true;
    }

    if (!deadline_.has_value()) {
        const auto maximum = Clock::time_point::max();

        // 避免构造截止时间时发生整数回绕。
        if (now > maximum - flush_interval_) {
            deadline_ = maximum;
        }
        else {
            deadline_ = now + flush_interval_;
        }
    }

    return now >= *deadline_;
}

void UploadTrigger::onCycleFinished() noexcept
{
    deadline_.reset();
}

} // namespace edge