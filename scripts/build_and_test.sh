#!/usr/bin/env bash

# 任一步骤失败立即停止。
set -euo pipefail

# 默认同时验收 CLI 和控制台。
run_gui=1

if (( $# > 1 )); then
    printf 'Usage: %s [--core-only]\n' "$0" >&2
    exit 2
fi

case "${1:-}" in
    "")
        ;;
    --core-only)
        run_gui=0
        ;;
    --help|-h)
        printf 'Usage: %s [--core-only]\n' "$0"
        exit 0
        ;;
    *)
        printf 'Unknown option: %s\n' "$1" >&2
        exit 2
        ;;
esac

# 根据脚本位置定位工程，不依赖终端当前目录。
project_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

core_build_dir="${project_dir}/build-core-system"

# 沿用当前 Qt Creator 已配置成功的 GUI 构建目录。
gui_build_dir="${project_dir}/build/Desktop_Qt_6_11_0-Debug"

# GUI 使用 Qt 安装附带的 CMake 和 CTest。
qt_cmake="${HOME}/Qt/Tools/CMake/bin/cmake"
qt_ctest="${HOME}/Qt/Tools/CMake/bin/ctest"

# 清除可能来自 Anaconda 的工具及动态库搜索环境。
# 这些修改仅影响本脚本及其子进程。
export PATH=/usr/bin:/bin
unset LD_LIBRARY_PATH LD_PRELOAD LIBRARY_PATH CMAKE_PREFIX_PATH

# 在开始耗时的 CLI 回归前，先检查 GUI 验收所需条件。
if (( run_gui )); then
    for tool in "$qt_cmake" "$qt_ctest"; do
        if [[ ! -x "$tool" ]]; then
            printf 'Tool not found or not executable: %s\n' "$tool" >&2
            exit 1
        fi
    done

    if [[ ! -f "${gui_build_dir}/CMakeCache.txt" ]]; then
        printf 'GUI build directory is not configured: %s\n' \
            "$gui_build_dir" >&2
        printf 'Configure it with the Qt6 Kit in Qt Creator first.\n' >&2
        exit 1
    fi
fi

# 第一阶段：保持原有系统工具链和 GUI OFF 的验收基线。
printf '\n=== CLI: configure, build and test ===\n'

/usr/bin/cmake \
    -S "$project_dir" \
    -B "$core_build_dir" \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_COMPILER=/usr/bin/g++ \
    -DPython3_EXECUTABLE=/usr/bin/python3 \
    -DEDGE_BUILD_GUI=OFF \
    -DBUILD_TESTING=ON

/usr/bin/cmake \
    --build "$core_build_dir" \
    --parallel 4

/usr/bin/ctest \
    --test-dir "$core_build_dir" \
    --output-on-failure \
    --no-tests=error

# 第二阶段：使用现有 Qt Creator 构建缓存，保留 Kit 工具链配置。
if (( run_gui )); then
    printf '\n=== Console: configure, build and test ===\n'

    "$qt_cmake" \
        -S "$project_dir" \
        -B "$gui_build_dir" \
        -DPython3_EXECUTABLE=/usr/bin/python3 \
        -DEDGE_BUILD_GUI=ON \
        -DBUILD_TESTING=ON

    # 显式构建测试程序，确保 GoogleTest 完成测试发现。
    "$qt_cmake" \
        --build "$gui_build_dir" \
        --target \
            edge-console \
            edge-console-tests \
            edge-console-process-tests \
        --parallel 2

    # 仅运行控制台测试；CLI 已在第一阶段完整验收。
    "$qt_ctest" \
        --test-dir "$gui_build_dir" \
        -L '^console$' \
        --output-on-failure \
        --no-tests=error
fi

printf '\nAll requested build and test steps passed.\n'
