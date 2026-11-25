#pragma once

#include <chrono>
#include <condition_variable>
#include <exception>
#include <functional>
#include <mutex>
#include <optional>
#include <thread>

namespace edge {

struct UploadWorkerStatus {
    // 曾经完成初始化，不代表此刻仍在运行。
    bool ready = false;

    // 任务函数及其局部资源已经结束。
    bool finished = false;

    std::exception_ptr failure;
};

class UploadWorker {
public:
    using Clock = std::chrono::steady_clock;

    enum class WaitResult {
        Notified,
        Deadline,
        StopRequested
    };

    // 只交给工作线程使用。
    // 不允许保存到任务生命周期之外。
    class Context {
    public:
        Context(const Context&) = delete;
        Context& operator=(const Context&) = delete;

        void signalReady();

        bool stopRequested() const;

        // 保留原接口：只等待停止，不消费工作通知。
        void waitForStop();

        // 等待工作通知或停止，不设置截止时间。
        WaitResult waitForWork();

        // 等待工作通知、停止或指定截止时间。
        WaitResult waitUntil(Clock::time_point deadline);

    private:
        friend class UploadWorker;

        explicit Context(UploadWorker& owner)
            : owner_(owner)
        {
        }

        WaitResult wait(
            std::optional<Clock::time_point> deadline
        );

        UploadWorker& owner_;
    };

    using Task = std::function<void(Context&)>;

    explicit UploadWorker(Task task);

    ~UploadWorker() noexcept;

    UploadWorker(const UploadWorker&) = delete;
    UploadWorker& operator=(const UploadWorker&) = delete;

    UploadWorker(UploadWorker&&) = delete;
    UploadWorker& operator=(UploadWorker&&) = delete;

    // 同一个 owner 线程调用 start、join 和析构。
    // 只能启动一次。
    void start();

    // 设置持久停止状态，再唤醒等待。
    void requestStop();

    // 记录“数据库可能有新工作”，再唤醒等待。
    //
    // 多次通知允许合并，不代表事件数量。
    // 正式接入时，应在新事件提交成功后调用。
    void notifyWork();

    // 只等待退出，不隐含停止请求。
    void join();

    UploadWorkerStatus status() const;

private:
    void threadMain() noexcept;

    Task task_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;

    // 仅由 owner 线程访问。
    bool start_attempted_ = false;

    // 以下共享状态均由 mutex_ 保护。
    bool stop_requested_ = false;
    bool work_pending_ = false;
    bool ready_ = false;
    bool finished_ = false;

    std::exception_ptr failure_;

    std::thread thread_;
};

} // namespace edge