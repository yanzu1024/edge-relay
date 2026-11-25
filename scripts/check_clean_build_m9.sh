#!/usr/bin/env bash

set -Eeuo pipefail

project_dir="$(
    cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd
)"

qt_cmake="${HOME}/Qt/Tools/CMake/bin/cmake"
qt_ctest="${HOME}/Qt/Tools/CMake/bin/ctest"

old_gui_cache="${project_dir}/build/Desktop_Qt_6_11_0-Debug/CMakeCache.txt"
jobs="${EDGE_CLEAN_JOBS:-2}"

# 只影响本脚本及其子进程，避免继承 Anaconda 工具链。
export PATH=/usr/bin:/bin

unset LD_LIBRARY_PATH LD_PRELOAD LIBRARY_PATH CMAKE_PREFIX_PATH
unset CPATH CPLUS_INCLUDE_PATH C_INCLUDE_PATH PKG_CONFIG_PATH
unset CC CXX CFLAGS CXXFLAGS LDFLAGS CMAKE_TOOLCHAIN_FILE

if [[ ! "$jobs" =~ ^[1-9][0-9]*$ ]]; then
    printf 'EDGE_CLEAN_JOBS must be a positive integer.\n' >&2
    exit 2
fi

for tool in \
    /usr/bin/cmake \
    /usr/bin/ctest \
    /usr/bin/g++ \
    /usr/bin/make \
    /usr/bin/python3 \
    "$qt_cmake" \
    "$qt_ctest"
do
    if [[ ! -x "$tool" ]]; then
        printf 'Tool not found: %s\n' "$tool" >&2
        exit 1
    fi
done

# 仅读取已安装 Qt 的位置，不复制旧构建缓存。
qt6_dir="${EDGE_QT6_DIR:-}"

if [[ -z "$qt6_dir" ]]; then
    qt6_dir="$(
        /usr/bin/python3 - "$old_gui_cache" <<'PY'
from pathlib import Path
import sys

cache = Path(sys.argv[1])

if not cache.is_file():
    raise SystemExit(
        "GUI cache missing; set EDGE_QT6_DIR "
        "to the installed Qt6 directory."
    )

for line in cache.read_text(encoding="utf-8").splitlines():
    if line.startswith("Qt6_DIR:"):
        value = line.partition("=")[2].strip()

        if value and not value.endswith("-NOTFOUND"):
            print(value)
            break
else:
    raise SystemExit(
        "Qt6_DIR missing; set EDGE_QT6_DIR explicitly."
    )
PY
    )"
fi

if [[ ! -f "${qt6_dir}/Qt6Config.cmake" ]]; then
    printf 'Qt6Config.cmake not found in: %s\n' \
        "$qt6_dir" >&2
    exit 1
fi

# 每次创建新目录，不覆盖已通过验收的构建目录。
clean_dir="$(
    mktemp -d "${project_dir}/build-m9-clean-XXXXXXXX"
)"

mkdir -p "${clean_dir}/logs" "${project_dir}/run"

printf '%s\n' "$clean_dir" \
    > "${project_dir}/run/m9-clean-current-session.txt"

# 失败时保留完整目录和已有日志。
trap 'result=$?; printf "\nFAILED. Evidence retained: %s\n" "$clean_dir" >&2; exit "$result"' ERR

run_logged() {
    local name="$1"
    shift

    "$@" 2>&1 | tee "${clean_dir}/logs/${name}.log"
}

{
    date --iso-8601=seconds

    /usr/bin/g++ --version
    /usr/bin/cmake --version
    "$qt_cmake" --version
    /usr/bin/python3 --version

    printf 'Project: %s\nQt6_DIR: %s\nJobs: %s\n' \
        "$project_dir" "$qt6_dir" "$jobs"
} > "${clean_dir}/logs/toolchain.txt"

common=(
    -S "$project_dir"
    -G "Unix Makefiles"
    -DCMAKE_BUILD_TYPE=Debug
    -DCMAKE_CXX_COMPILER=/usr/bin/g++
    -DCMAKE_MAKE_PROGRAM=/usr/bin/make
    -DPython3_EXECUTABLE=/usr/bin/python3
    -DBUILD_TESTING=ON
    -DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF
    -DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF
)

printf '\nCLI clean build: %s\n' "${clean_dir}/core"

run_logged core-configure \
    /usr/bin/cmake \
    "${common[@]}" \
    -B "${clean_dir}/core" \
    -DEDGE_BUILD_GUI=OFF

run_logged core-build \
    /usr/bin/cmake \
    --build "${clean_dir}/core" \
    --parallel "$jobs"

# 串行运行，避免不同集成场景互相干扰。
run_logged core-tests \
    /usr/bin/ctest \
    --test-dir "${clean_dir}/core" \
    --parallel 1 \
    --output-on-failure \
    --no-tests=error \
    --output-junit "${clean_dir}/logs/core-tests.xml"

printf '\nConsole clean build: %s\n' "${clean_dir}/gui"

run_logged gui-configure \
    "$qt_cmake" \
    "${common[@]}" \
    -B "${clean_dir}/gui" \
    -DEDGE_BUILD_GUI=ON \
    "-DQt6_DIR=${qt6_dir}"

run_logged gui-build \
    "$qt_cmake" \
    --build "${clean_dir}/gui" \
    --parallel "$jobs"

# 核心测试已在上面的独立 CLI 构建中运行。
run_logged gui-tests \
    "$qt_ctest" \
    --test-dir "${clean_dir}/gui" \
    --parallel 1 \
    -L '^console$' \
    --output-on-failure \
    --no-tests=error \
    --output-junit "${clean_dir}/logs/gui-tests.xml"

# 生成独立的界面启动检查配置及自动检查结果。
/usr/bin/python3 - \
    "$clean_dir" \
    "$project_dir" \
    "$qt6_dir" <<'PY'
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

directory = Path(sys.argv[1])
smoke = directory / "smoke"
smoke.mkdir()

def quoted(path):
    return json.dumps(str(path), ensure_ascii=False)

config = f"""version: 1

agent:
  id: m9-clean-smoke-agent

tcp:
  bind: 127.0.0.1
  port: 19090

storage:
  path: {quoted(smoke / "outbox.db")}

upload:
  url: http://127.0.0.1:18080/v1/events/batch

recording:
  enabled: false

status:
  file: {quoted(smoke / "status.json")}
"""

(smoke / "config.yml").write_text(
    config,
    encoding="utf-8",
)

result = {
    "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    "source_directory": sys.argv[2],
    "build_directory": str(directory),
    "qt6_directory": sys.argv[3],
    "build_type": "Debug",
    "core_tests": "PASS",
    "console_tests": "PASS",
    "manual_gui_smoke": "PENDING",
}

(directory / "result.json").write_text(
    json.dumps(result, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)
PY

printf '\nPASS: clean-core-and-console\n'
printf 'Evidence: %s\n' "$clean_dir"
printf 'Console: %s/gui/edge-console\n' "$clean_dir"
printf 'Agent: %s/core/edge-agent\n' "$clean_dir"
printf 'Smoke config: %s/smoke/config.yml\n' "$clean_dir"
printf 'Manual GUI smoke check remains pending.\n'