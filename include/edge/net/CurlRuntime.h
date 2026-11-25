#pragma once

namespace edge {

// 确保 libcurl 全局环境已初始化。
// 成功后重复调用不会再次初始化。
//
// 初始化失败时抛出 std::runtime_error。
//
// 当前在单线程配置校验阶段首次调用。
// 后续引入工作线程时，仍须在线程创建前完成首次调用。
void ensureCurlInitialized();

} // namespace edge