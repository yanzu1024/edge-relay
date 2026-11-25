#!/usr/bin/env python3
"""Package source, unpack outside the project, and run clean-build checks."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = "SOURCE_MANIFEST.json"

ENTRIES = (
    "CMakeLists.txt",
    "apps",
    "include",
    "src",
    "scripts",
    "tests",
    "config",
)

OPTIONAL = (
    "README.md",
    "docs",
    "cmake",
    "CMakePresets.json",
)

CONTRACTS = {
    "cli_contract",
    "dead_export_contract",
    "drain_empty_contract",
    "drain_backlog_contract",
    "export_dead_cli_contract",
}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def write_json(path, value):
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )


def digest(path):
    value = hashlib.sha256()

    with path.open("rb") as source:
        for block in iter(
            lambda: source.read(1024 * 1024),
            b"",
        ):
            value.update(block)

    return value.hexdigest()


def qt_directory():
    value = os.environ.get("EDGE_QT6_DIR", "")

    if not value:
        cache = (
            ROOT
            / "build"
            / "Desktop_Qt_6_11_0-Debug"
            / "CMakeCache.txt"
        )

        require(
            cache.is_file(),
            "Qt cache missing; set EDGE_QT6_DIR explicitly.",
        )

        for line in cache.read_text(
            encoding="utf-8"
        ).splitlines():
            if line.startswith("Qt6_DIR:"):
                value = line.partition("=")[2].strip()
                break

    require(
        value and not value.endswith("-NOTFOUND"),
        "Qt6_DIR is unavailable.",
    )

    path = Path(value).resolve(strict=True)

    require(
        (path / "Qt6Config.cmake").is_file(),
        "Qt6Config.cmake is missing.",
    )

    require(
        not path.is_relative_to(ROOT),
        "Qt must be installed outside the original project.",
    )

    return path


def snapshot(destination):
    destination.mkdir(parents=True)

    ignore = shutil.ignore_patterns(
        "__pycache__",
        "*.pyc",
        "*.pyo",
        ".git",
        ".DS_Store",
    )

    # 只复制明确的源码入口，不遍历根目录中的 build 和 run。
    for name in ENTRIES + OPTIONAL:
        source = ROOT / name

        if not source.exists():
            require(
                name in OPTIONAL,
                f"Required source entry missing: {name}",
            )
            continue

        target = destination / name

        if source.is_symlink():
            target.symlink_to(
                os.readlink(source),
                target_is_directory=source.is_dir(),
            )
        elif source.is_dir():
            shutil.copytree(
                source,
                target,
                symlinks=True,
                ignore=ignore,
            )
        else:
            shutil.copy2(source, target)

    # 允许内部相对链接，拒绝依赖原目录的链接。
    for path in destination.rglob("*"):
        if path.is_symlink():
            require(
                not Path(os.readlink(path)).is_absolute(),
                f"Absolute source link: {path}",
            )

            require(
                path.resolve(strict=True).is_relative_to(
                    destination
                ),
                f"External source link: {path}",
            )

    hashes = {
        path.relative_to(destination).as_posix(): digest(path)
        for path in sorted(destination.rglob("*"))
        if path.is_file()
        and path != destination / MANIFEST
    }

    write_json(destination / MANIFEST, hashes)

    return hashes


def verify_files(directory, hashes):
    for name, expected in hashes.items():
        require(
            digest(directory / name) == expected,
            f"Source content changed: {name}",
        )


def test_report(path):
    cases = list(
        ET.parse(path).getroot().iter("testcase")
    )

    require(
        cases,
        f"No test cases recorded in {path}",
    )

    require(
        not any(
            case.find("failure") is not None
            or case.find("error") is not None
            for case in cases
        ),
        f"Failed test cases recorded in {path}",
    )

    return {
        "total": len(cases),
        "skipped": [
            case.get("name")
            for case in cases
            if case.find("skipped") is not None
        ],
        "passed_names": [
            case.get("name")
            for case in cases
            if case.find("skipped") is None
        ],
    }


def main():
    require(
        sys.version_info >= (3, 12),
        "Use /usr/bin/python3 from Ubuntu 24.04 "
        "(Python 3.12+).",
    )

    for name in (
        "scripts/check_clean_build_m9.sh",
        "apps/agent/ExportDead.cpp",
        "tests/check_drain_backlog.py",
        "tests/check_export_dead_cli.py",
    ):
        require(
            (ROOT / name).is_file(),
            f"Complete the previous subsection first: {name}",
        )

    qt6_dir = qt_directory()

    run = ROOT / "run"
    run.mkdir(exist_ok=True)

    release = Path(tempfile.mkdtemp(
        prefix="m9-release-",
        dir=run,
    )).resolve()

    (run / "m9-release-current-session.txt").write_text(
        str(release) + "\n",
        encoding="utf-8",
    )

    result_path = release / "result.json"

    result = {
        "automated_checks": "PENDING",
        "manual_gui_smoke": "PENDING",
    }

    write_json(result_path, result)

    print(
        f"Release directory: {release}",
        flush=True,
    )

    try:
        staged = release / "staging" / "edge-relay"
        hashes = snapshot(staged)

        archive = release / "EdgeRelay-source.tar.gz"

        with tarfile.open(archive, "w:gz") as output:
            output.add(
                staged,
                arcname="edge-relay",
            )

        checksum = digest(archive)

        (
            release / "EdgeRelay-source.tar.gz.sha256"
        ).write_text(
            f"{checksum}  {archive.name}\n",
            encoding="utf-8",
        )

        result.update(
            archive=str(archive),
            archive_sha256=checksum,
            source_files=len(hashes),
        )

        write_json(result_path, result)

        # 在原工程目录之外解压和构建。
        sandbox = Path(tempfile.mkdtemp(
            prefix="edge-relay-release-build-"
        )).resolve()

        require(
            not sandbox.is_relative_to(ROOT),
            "Build sandbox must be outside the original project.",
        )

        result["sandbox"] = str(sandbox)
        write_json(result_path, result)

        with tarfile.open(archive, "r:gz") as source:
            source.extractall(
                sandbox,
                filter="data",
            )

        unpacked = sandbox / "edge-relay"
        verify_files(unpacked, hashes)

        environment = os.environ.copy()
        environment["PATH"] = "/usr/bin:/bin"
        environment["EDGE_QT6_DIR"] = str(qt6_dir)

        for name in (
            "LD_LIBRARY_PATH",
            "LD_PRELOAD",
            "LIBRARY_PATH",
            "CMAKE_PREFIX_PATH",
        ):
            environment.pop(name, None)

        print(
            f"Building unpacked source: {unpacked}",
            flush=True,
        )

        subprocess.run(
            [
                "/bin/bash",
                str(
                    unpacked
                    / "scripts"
                    / "check_clean_build_m9.sh"
                ),
            ],
            cwd=unpacked,
            env=environment,
            check=True,
        )

        pointer = (
            unpacked
            / "run"
            / "m9-clean-current-session.txt"
        )

        build = Path(
            pointer.read_text(
                encoding="utf-8"
            ).strip()
        ).resolve(strict=True)

        require(
            build.is_relative_to(unpacked),
            "Unexpected build directory.",
        )

        # 检查构建测试没有改动快照中的原始源码。
        verify_files(unpacked, hashes)

        core = test_report(
            build / "logs" / "core-tests.xml"
        )

        gui = test_report(
            build / "logs" / "gui-tests.xml"
        )

        missing = (
            CONTRACTS - set(core["passed_names"])
        )

        require(
            not missing,
            "Required contracts missing or skipped: "
            f"{sorted(missing)}",
        )

        shutil.copytree(
            build / "logs",
            release / "logs",
        )

        (release / "build-directory.txt").write_text(
            str(build) + "\n",
            encoding="utf-8",
        )

        result.update(
            automated_checks="PASS",
            build_directory=str(build),
            core_tests={
                key: value
                for key, value in core.items()
                if key != "passed_names"
            },
            console_tests={
                key: value
                for key, value in gui.items()
                if key != "passed_names"
            },
        )

        write_json(result_path, result)

        print("PASS: release-source-build")
        print(f"Archive: {archive}")
        print(
            f"Console: {build / 'gui' / 'edge-console'}"
        )
        print(
            f"Agent: {build / 'core' / 'edge-agent'}"
        )
        print(
            f"Smoke config: {build / 'smoke' / 'config.yml'}"
        )
        print(
            "Manual GUI smoke check remains pending."
        )

        return 0

    except Exception as error:
        result.update(
            automated_checks="FAIL",
            error=str(error),
        )

        write_json(result_path, result)
        raise


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(
            f"FAIL: {error}",
            file=sys.stderr,
        )
        sys.exit(1)