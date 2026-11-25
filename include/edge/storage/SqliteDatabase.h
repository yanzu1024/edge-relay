#pragma once

#include <filesystem>
#include <stdexcept>
#include <string>

// 这里只需要声明指针类型，不必向所有包含者引入 sqlite3.h。
struct sqlite3;

namespace edge {

// 本小节只保存 SQLite 返回码和已经复制的错误信息。
// 不在这里扩展整个项目的错误处理体系。
class SqliteError : public std::runtime_error {
public:
    SqliteError(int code, const std::string& message);

    int code() const noexcept;

private:
    int code_;
};

class SqliteDatabase {
public:
    // 打开已有数据库；不存在时尝试创建数据库文件。
    // 父目录必须已经存在。
    // ":memory:" 可用于建立独立的内存数据库。
    explicit SqliteDatabase(const std::filesystem::path& path);

    ~SqliteDatabase() noexcept;

    // 一个连接只能由一个对象拥有。
    SqliteDatabase(const SqliteDatabase&) = delete;
    SqliteDatabase& operator=(const SqliteDatabase&) = delete;

    // 移动后，源对象变为空状态，可以安全析构。
    SqliteDatabase(SqliteDatabase&& other) noexcept;
    SqliteDatabase& operator=(SqliteDatabase&& other) noexcept;

    // 借用接口：不得对返回值调用 sqlite3_close / sqlite3_close_v2。
    // 对象关闭或销毁后，不得再通过此前借用的句柄操作连接。
    sqlite3* get() const noexcept;

    bool isOpen() const noexcept;

    // 使本对象立即变为空状态。
    // 如果还有未释放语句，SQLite 内部资源的回收会延后。
    // 本操作不代表提交事务。
    void close() noexcept;

private:
    sqlite3* db_ = nullptr;
};

} // namespace edge