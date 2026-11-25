#pragma once

#include <condition_variable>
#include <cstddef>
#include <exception>
#include <mutex>
#include <thread>

namespace edge {

// 同一次 Agent 运行的两个 OutboxStore 共用。
//
// 不拥有数据库、不创建线程。
// 等待者按入队顺序进入，避免当前线程释放后反复抢先。
// 同线程允许嵌套，用于 Store 内部调用其他查询方法。
//
// 所有使用者结束后才能销毁本对象。
class OutboxAccessGate {
public:
    OutboxAccessGate() = default;

    OutboxAccessGate(const OutboxAccessGate&) = delete;

    OutboxAccessGate& operator=(
        const OutboxAccessGate&
    ) = delete;

    void lock()
    {
        std::unique_lock<std::mutex> lock(mutex_);

        const auto current = std::this_thread::get_id();

        if (depth_ != 0 && owner_ == current) {
            ++depth_;
            return;
        }

        // 等待节点由等待线程的栈持有，
        // 排队本身不需要额外分配内存。
        Waiter waiter;

        if (tail_ != nullptr) {
            tail_->next = &waiter;
        } else {
            head_ = &waiter;
        }

        tail_ = &waiter;

        try {
            cv_.wait(lock, [&] {
                return depth_ == 0 && head_ == &waiter;
            });
        }
        catch (...) {
            removeWaiter(&waiter);

            lock.unlock();
            cv_.notify_all();

            throw;
        }

        head_ = waiter.next;

        if (head_ == nullptr) {
            tail_ = nullptr;
        }

        owner_ = current;
        depth_ = 1;
    }

    void unlock() noexcept
    {
        bool notify = false;

        {
            std::lock_guard<std::mutex> lock(mutex_);

            if (depth_ == 0
                || owner_ != std::this_thread::get_id()) {
                std::terminate();
            }

            --depth_;

            if (depth_ == 0) {
                owner_ = std::thread::id{};
                notify = true;
            }
        }

        if (notify) {
            cv_.notify_all();
        }
    }

private:
    struct Waiter {
        Waiter* next = nullptr;
    };

    // 调用时持有 mutex_。
    void removeWaiter(Waiter* target) noexcept
    {
        Waiter* previous = nullptr;
        Waiter* current = head_;

        while (current != nullptr) {
            if (current == target) {
                if (previous != nullptr) {
                    previous->next = current->next;
                } else {
                    head_ = current->next;
                }

                if (tail_ == current) {
                    tail_ = previous;
                }

                return;
            }

            previous = current;
            current = current->next;
        }
    }

    std::mutex mutex_;
    std::condition_variable cv_;

    std::thread::id owner_{};
    std::size_t depth_ = 0;

    Waiter* head_ = nullptr;
    Waiter* tail_ = nullptr;
};

} // namespace edge