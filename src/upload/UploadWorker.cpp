#include "edge/upload/UploadWorker.h"

#include <stdexcept>
#include <utility>

namespace edge {

UploadWorker::UploadWorker(Task task)
    : task_(std::move(task))
{
    if (!task_) {
        throw std::invalid_argument(
            "Upload worker requires a task"
        );
    }
}

UploadWorker::~UploadWorker() noexcept
{
    // 协作式停止。
    // 任务必须等待可唤醒的条件变量，或定期检查停止状态。
    //
    // owner 必须比工作线程活得久，
    // 且不能在工作线程内部析构 owner。
    requestStop();
    join();
}

void UploadWorker::start()
{
    if (start_attempted_) {
        throw std::logic_error(
            "Upload worker can only start once"
        );
    }

    start_attempted_ = true;

    try {
        thread_ = std::thread(
            &UploadWorker::threadMain,
            this
        );
    }
    catch (...) {
        // 线程创建失败，不存在需要 join 的工作线程。
        std::lock_guard<std::mutex> lock(mutex_);

        failure_ = std::current_exception();
        finished_ = true;

        throw;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    cv_.wait(lock, [this] {
        return ready_ || finished_;
    });

    if (ready_) {
        // ready_ 表示曾经完成初始化。
        // 此后发生的运行错误，通过 status() 获取。
        return;
    }

    const auto failure = failure_;

    lock.unlock();

    // 不在持有共享状态锁时 join。
    join();

    std::rethrow_exception(failure);
}

void UploadWorker::requestStop()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);

        stop_requested_ = true;
    }

    cv_.notify_all();
}

void UploadWorker::join()
{
    if (thread_.joinable()) {
        thread_.join();
    }
}

UploadWorkerStatus UploadWorker::status() const
{
    std::lock_guard<std::mutex> lock(mutex_);

    return {
        ready_,
        finished_,
        failure_
    };
}

void UploadWorker::Context::signalReady()
{
    {
        std::lock_guard<std::mutex> lock(owner_.mutex_);

        if (owner_.ready_) {
            throw std::logic_error(
                "Upload worker signalled ready twice"
            );
        }

        owner_.ready_ = true;
    }

    owner_.cv_.notify_all();
}

bool UploadWorker::Context::stopRequested() const
{
    std::lock_guard<std::mutex> lock(owner_.mutex_);

    return owner_.stop_requested_;
}

void UploadWorker::Context::waitForStop()
{
    std::unique_lock<std::mutex> lock(owner_.mutex_);

    owner_.cv_.wait(lock, [this] {
        return owner_.stop_requested_;
    });
}

void UploadWorker::threadMain() noexcept
{
    std::exception_ptr failure;

    try {
        Context context(*this);

        // 不持有 mutex_ 执行任务。
        // 将来的数据库与 HTTP 操作也在任务内部执行。
        task_(context);

        bool was_ready;
        bool was_stopping;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            was_ready = ready_;
            was_stopping = stop_requested_;
        }

        if (!was_ready) {
            throw std::runtime_error(
                "Upload worker returned before signalling ready"
            );
        }

        if (!was_stopping) {
            throw std::runtime_error(
                "Upload worker exited without a stop request"
            );
        }
    }
    catch (...) {
        // 同时捕获非 std::exception 类型。
        // 异常不能越过 std::thread 的入口。
        failure = std::current_exception();
    }

    // 任务函数已经返回或完成异常展开。
    // 在任务内部创建的局部资源，此时已经销毁。
    {
        std::lock_guard<std::mutex> lock(mutex_);

        failure_ = failure;
        finished_ = true;
    }

    cv_.notify_all();
}

void UploadWorker::notifyWork()
{
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 已停止或已结束的线程不再接收新工作。
        if (stop_requested_ || finished_) {
            return;
        }

        work_pending_ = true;
    }

    cv_.notify_all();
}

UploadWorker::WaitResult
UploadWorker::Context::waitForWork()
{
    return wait(std::nullopt);
}

UploadWorker::WaitResult
UploadWorker::Context::waitUntil(
    Clock::time_point deadline
)
{
    return wait(deadline);
}

UploadWorker::WaitResult
UploadWorker::Context::wait(
    std::optional<Clock::time_point> deadline
)
{
    std::unique_lock<std::mutex> lock(owner_.mutex_);

    const auto awakened = [this] {
        return owner_.stop_requested_
            || owner_.work_pending_;
    };

    if (deadline.has_value()) {
        owner_.cv_.wait_until(
            lock,
            *deadline,
            awakened
        );
    }
    else {
        owner_.cv_.wait(
            lock,
            awakened
        );
    }

    // 停止优先，避免停止时又开始处理新工作。
    if (owner_.stop_requested_) {
        return WaitResult::StopRequested;
    }

    if (owner_.work_pending_) {
        // 消费一次合并后的工作通知。
        owner_.work_pending_ = false;

        return WaitResult::Notified;
    }

    // 带谓词的无限等待不会在谓词为假时返回。
    // 因此只有有限截止时间到期，才会进入这里。
    return WaitResult::Deadline;
}

} // namespace edge