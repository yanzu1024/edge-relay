cmake_minimum_required(VERSION 3.22)    # 指定 cmake 最低版本

project(EdgeRelay VERSION 0.1.0 LANGUAGES CXX)  # 声明工程, 版本号, 使用语言

set(CMAKE_CXX_STANDARD 17)  # 指定 cpp 版本
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 默认只构建 CLI；开发 Qt 控制台时通过构建配置开启。
option(EDGE_BUILD_GUI "Build the Qt6 console" OFF)

find_package(nlohmann_json 3.10 REQUIRED)
find_package(CURL 7.62 REQUIRED)
find_package(yaml-cpp 0.8 REQUIRED CONFIG)
find_package(SQLite3 3.11.0 REQUIRED)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

# 把指定源文件编译并组织为静态库
add_library(edge-core STATIC
    src/Version.cpp
    src/domain/EventId.cpp
    src/domain/EventJson.cpp
    src/validation/EventValidator.cpp
    src/protocol/FrameEncoder.cpp
    src/protocol/StreamDecoder.cpp
    src/app/EventStreamProcessor.cpp
    src/config/ConfigValidator.cpp
    src/config/ConfigYaml.cpp
    src/config/ConfigLoader.cpp
    src/net/SocketFd.cpp
    src/net/TcpListener.cpp
    src/net/TcpConnections.cpp
    src/net/TcpPoller.cpp
    src/app/TcpEventProcessor.cpp
    src/storage/SqliteDatabase.cpp
    src/storage/SqliteStatement.cpp
    src/storage/SqliteTransaction.cpp
    src/storage/OutboxSchema.cpp
    src/storage/OutboxStore.cpp
    src/storage/OutboxDatabase.cpp
    src/net/CurlRuntime.cpp
    src/net/HttpJsonPost.cpp
    src/upload/BatchResponse.cpp
    src/upload/BatchRequest.cpp
    src/upload/BatchUploader.cpp
    src/upload/RetryBackoff.cpp
    src/upload/OutboxUploader.cpp
    src/upload/UploadTrigger.cpp
    src/upload/UploadWorker.cpp
    src/upload/OutboxWorkerTask.cpp
    src/upload/UploadWaitPlan.cpp
    src/upload/OutboxUploadLoop.cpp
    src/upload/UploadReportChannel.cpp
    src/upload/HttpUploadPolicy.cpp
    src/status/StatusSnapshot.cpp
    src/status/StatusOutput.cpp
    src/status/JsonlProtocol.cpp
    src/status/JsonlWriter.cpp
    src/recording/RawRecordingSession.cpp
    src/recording/RawRecorder.cpp
    src/status/TcpActivityTracker.cpp
)

target_link_libraries(edge-core PRIVATE
    nlohmann_json::nlohmann_json
    CURL::libcurl
    yaml-cpp::yaml-cpp
    SQLite::SQLite3
)

target_link_libraries(edge-core PUBLIC
    Threads::Threads
)

# 编译时的宏定义, 并且指定源文件
target_compile_definitions(edge-core PRIVATE
    "EDGE_VERSION=\"${PROJECT_VERSION}\""
    _GNU_SOURCE
)

# 指定头文件搜索目录
target_include_directories(edge-core PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
)

add_executable(edge-agent
    apps/agent/main.cpp
    apps/agent/RunAgent.cpp
    apps/agent/RunOutput.cpp
    apps/agent/ExportDead.cpp
)

target_compile_definitions(edge-agent PRIVATE
    _POSIX_C_SOURCE=200809L
    _GNU_SOURCE
)

# 让可执行程序链接核心库, 从中找到 version() 的实现
target_link_libraries(edge-agent PRIVATE
    edge-core
    nlohmann_json::nlohmann_json
    SQLite::SQLite3
)

# 设置编译选项
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(edge-agent PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )

    target_compile_options(edge-core PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
endif()

# Qt 控制台：仅在显式开启时查找 Qt 并创建 GUI 目标。
if(EDGE_BUILD_GUI)
    find_package(Qt6 REQUIRED CONFIG COMPONENTS Core Widgets)

    # GUI 协议接收组件，独立于 Agent 业务实现。
    add_library(edge-console-protocol STATIC
        apps/console/protocol/JsonlLineFramer.h
        apps/console/protocol/JsonlLineFramer.cpp
        apps/console/protocol/JsonlMessageParser.h
        apps/console/protocol/JsonlMessageParser.cpp
        apps/console/protocol/StatusSummary.h
        apps/console/protocol/StatusSummary.cpp
        apps/console/protocol/JsonlReceiver.h
        apps/console/protocol/JsonlReceiver.cpp
        apps/console/protocol/RecordBuffer.h
        apps/console/protocol/RecordBuffer.cpp
    )

    target_include_directories(edge-console-protocol 
        PUBLIC
            "${CMAKE_CURRENT_SOURCE_DIR}/apps/console"
        PRIVATE
            "${CMAKE_CURRENT_SOURCE_DIR}/include"
    )

    target_link_libraries(edge-console-protocol PUBLIC
        nlohmann_json::nlohmann_json
    )

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(edge-console-protocol PRIVATE
            -Wall
            -Wextra
            -Wpedantic
        )
    endif()

    # Qt 进程管理层。协议库继续保持不依赖 Qt。
    add_library(edge-console-runtime STATIC
        apps/console/process/AgentProcess.h
        apps/console/process/AgentProcess.cpp

        apps/console/presentation/StatusText.h
        apps/console/presentation/StatusText.cpp

        apps/console/presentation/TcpActivityText.h
        apps/console/presentation/TcpActivityText.cpp

        apps/console/presentation/EventTableModel.h
        apps/console/presentation/EventTableModel.cpp
    )

    set_target_properties(edge-console-runtime PROPERTIES
        AUTOMOC ON
    )

    target_include_directories(edge-console-runtime 
        PUBLIC
            "${CMAKE_CURRENT_SOURCE_DIR}/apps/console"
        PRIVATE
            "${CMAKE_CURRENT_SOURCE_DIR}/include"
    )

    target_link_libraries(edge-console-runtime PUBLIC
        Qt6::Core
        edge-console-protocol
    )

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(edge-console-runtime PRIVATE
            -Wall
            -Wextra
            -Wpedantic
        )
    endif()

    add_executable(edge-console
        apps/console/main.cpp
        apps/console/mainwindow.h
        apps/console/mainwindow.cpp
        apps/console/mainwindow.ui
    )

    # 仅为 GUI 目标启用 Qt 代码生成。
    set_target_properties(edge-console PROPERTIES
        AUTOMOC ON
        AUTOUIC ON
    )

    target_link_libraries(edge-console PRIVATE
        Qt6::Widgets
        edge-console-runtime
    )

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(edge-console PRIVATE
            -Wall
            -Wextra
            -Wpedantic
        )
    endif()
endif()

# 引入测试支持, 并创建默认开启的 BUILD_TESTING 选项
# 开启时, 他会调用 enable_testing(), 使项目能够生成供 CTest 使用的测试信息
include(CTest)

if(BUILD_TESTING)
    find_package(Python3 REQUIRED COMPONENTS Interpreter)
    find_package(GTest REQUIRED)

    # 手工 HTTP 验收工具，不登记为自动 CTest。
    add_executable(edge-http-smoke
        tests/manual/http_post_smoke.cpp
    )

    target_link_libraries(edge-http-smoke PRIVATE
        edge-core
    )

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(edge-http-smoke PRIVATE
            -Wall
            -Wextra
            -Wpedantic
        )
    endif()

    add_executable(edge-batch-smoke
        tests/manual/batch_upload_smoke.cpp
    )

    target_link_libraries(edge-batch-smoke PRIVATE
        edge-core
    )

    add_executable(edge-outbox-smoke
        tests/manual/outbox_upload_smoke.cpp
    )

    target_link_libraries(edge-outbox-smoke PRIVATE
        edge-core
    )

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(edge-outbox-smoke PRIVATE
            -Wall
            -Wextra
            -Wpedantic
        )
    endif()

    add_test(
        NAME outbox_upload_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_outbox_upload.py"
            "$<TARGET_FILE:edge-outbox-smoke>"
            "${CMAKE_CURRENT_SOURCE_DIR}/scripts/mock_http_receiver.py"
    )

    set_tests_properties(outbox_upload_contract PROPERTIES
        TIMEOUT 180
    )

    add_test(
        NAME runtime_upload_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_runtime_upload.py"
            "$<TARGET_FILE:edge-agent>"
            "${CMAKE_CURRENT_SOURCE_DIR}/scripts/mock_http_receiver.py"
    )

    set_tests_properties(runtime_upload_contract PROPERTIES
        TIMEOUT 180
    )

    add_test(
        NAME status_output_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_status_output.py"
            "$<TARGET_FILE:edge-agent>"
    )

    set_tests_properties(status_output_contract PROPERTIES
        TIMEOUT 60
    )

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(edge-batch-smoke PRIVATE
            -Wall
            -Wextra
            -Wpedantic
        )
    endif()

    add_test(
        NAME http_upload_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_http_upload.py"
            "$<TARGET_FILE:edge-batch-smoke>"
            "$<TARGET_FILE:edge-http-smoke>"
            "${CMAKE_CURRENT_SOURCE_DIR}/scripts/mock_http_receiver.py"
    )

    set_tests_properties(http_upload_contract PROPERTIES
        TIMEOUT 180
    )

    # 保留 M0 的 CLI 契约检查。
    add_test(
        NAME cli_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_cli.py"
            "$<TARGET_FILE:edge-agent>"
            "${PROJECT_VERSION}"
    )

    # 脚本内部现在有 15 个场景, 每个子进程超时为 5 秒
    set_tests_properties(cli_contract PROPERTIES
        TIMEOUT 180
    )

    add_test(
        NAME dead_export_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_dead_export.py"
            "${CMAKE_CURRENT_SOURCE_DIR}/scripts/export_dead.py"
    )

    set_tests_properties(dead_export_contract PROPERTIES
        TIMEOUT 60
    )

    add_test(
        NAME drain_empty_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_drain_empty.py"
            "$<TARGET_FILE:edge-agent>"
    )

    set_tests_properties(drain_empty_contract PROPERTIES
        TIMEOUT 30
    )

    add_test(
        NAME drain_backlog_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_drain_backlog.py"
            "$<TARGET_FILE:edge-agent>"
            "${CMAKE_CURRENT_SOURCE_DIR}/scripts/mock_http_receiver.py"
    )

    set_tests_properties(drain_backlog_contract PROPERTIES
        TIMEOUT 120
    )

    add_test(
        NAME export_dead_cli_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_export_dead_cli.py"
            "$<TARGET_FILE:edge-agent>"
    )

    set_tests_properties(export_dead_cli_contract PROPERTIES
        TIMEOUT 90
    )

    add_test(
        NAME runtime_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_run.py"
            "$<TARGET_FILE:edge-agent>"
    )

    set_tests_properties(runtime_contract PROPERTIES
        TIMEOUT 120
    )

    add_test(
        NAME runtime_jsonl_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_runtime_jsonl.py"
            "$<TARGET_FILE:edge-agent>"
            "${CMAKE_CURRENT_SOURCE_DIR}/scripts/mock_http_receiver.py"
    )

    set_tests_properties(runtime_jsonl_contract PROPERTIES
        TIMEOUT 180
    )

    add_test(
        NAME runtime_recording_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_runtime_recording.py"
            "$<TARGET_FILE:edge-agent>"
            "${CMAKE_CURRENT_SOURCE_DIR}/scripts/mock_http_receiver.py"
    )

    set_tests_properties(runtime_recording_contract PROPERTIES
        TIMEOUT 180
    )

    add_test(
        NAME runtime_tcp_activity_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_runtime_tcp_activity.py"
            "$<TARGET_FILE:edge-agent>"
    )

    set_tests_properties(runtime_tcp_activity_contract PROPERTIES
        TIMEOUT 120
    )

    add_test(
        NAME runtime_m7_contract
        COMMAND
            "${Python3_EXECUTABLE}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/check_runtime_m7.py"
            "$<TARGET_FILE:edge-agent>"
            "${CMAKE_CURRENT_SOURCE_DIR}/scripts/mock_http_receiver.py"
    )

    set_tests_properties(runtime_m7_contract PROPERTIES
        TIMEOUT 120
    )

    # C++ 单元测试程序；后续模块的测试文件继续加入这个目标。
    add_executable(edge-core-tests
        tests/unit/EventValidatorTest.cpp
        tests/unit/EventJsonTest.cpp
        tests/unit/FrameEncoderTest.cpp
        tests/unit/StreamDecoderTest.cpp
        tests/unit/EventStreamProcessorTest.cpp
        tests/unit/ConfigValidatorTest.cpp
        tests/unit/ConfigYamlTest.cpp
        tests/unit/ConfigLoaderTest.cpp
        tests/unit/SocketFdTest.cpp
        tests/unit/TcpListenerTest.cpp
        tests/unit/TcpConnectionsTest.cpp
        tests/unit/TcpPollerTest.cpp
        tests/unit/TcpEventProcessorTest.cpp
        tests/unit/SqliteDatabaseTest.cpp
        tests/unit/SqliteStatementTest.cpp
        tests/unit/SqliteTransactionTest.cpp
        tests/unit/OutboxSchemaTest.cpp
        tests/unit/OutboxStoreTest.cpp
        tests/unit/OutboxCapacityTest.cpp
        tests/unit/OutboxCleanupTest.cpp
        tests/unit/OutboxDatabaseTest.cpp
        tests/unit/OutboxRecoveryTest.cpp
        tests/unit/BatchResponseTest.cpp
        tests/unit/BatchRequestTest.cpp
        tests/unit/OutboxClaimTest.cpp
        tests/unit/RetryBackoffTest.cpp
        tests/unit/OutboxUploaderTest.cpp
        tests/unit/UploadTriggerTest.cpp
        tests/unit/UploadWorkerTest.cpp
        tests/unit/OutboxWorkerTaskTest.cpp
        tests/unit/UploadWorkerWaitTest.cpp
        tests/unit/OutboxNextReadyTest.cpp
        tests/unit/UploadWaitPlanTest.cpp
        tests/unit/UploadReportChannelTest.cpp
        tests/unit/HttpUploadPolicyTest.cpp
        tests/unit/StatusSnapshotTest.cpp
        tests/unit/StatusOutputTest.cpp
        tests/unit/JsonlProtocolTest.cpp
        tests/unit/JsonlWriterTest.cpp
        tests/unit/RawRecordingSessionTest.cpp
        tests/unit/OutboxAccessGateTest.cpp
        tests/unit/RawRecorderTest.cpp
        tests/unit/RecordingStatusTest.cpp
        tests/unit/TcpActivityTrackerTest.cpp
        tests/unit/TcpActivityStatusTest.cpp
    )

    # edge-core 提供被测实现，gtest_main 提供测试入口。
    target_link_libraries(edge-core-tests PRIVATE
        edge-core
        GTest::gtest_main
        nlohmann_json::nlohmann_json
        yaml-cpp::yaml-cpp
        SQLite::SQLite3
    )

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(edge-core-tests PRIVATE
            -Wall -Wextra -Wpedantic
        )
    endif()

    # 查询编译后的测试程序，将每个 GoogleTest 测试分别登记到 CTest。
    include(GoogleTest)

    gtest_discover_tests(edge-core-tests
        DISCOVERY_TIMEOUT 10
        PROPERTIES TIMEOUT 5
    )

    # 当前仍位于 if(BUILD_TESTING) 内。
    if(EDGE_BUILD_GUI)
        add_executable(edge-console-tests
            tests/console/JsonlLineFramerTest.cpp
            tests/console/JsonlMessageParserTest.cpp
            tests/console/StatusSummaryTest.cpp
            tests/console/JsonlReceiverTest.cpp
            tests/console/RecordBufferTest.cpp
        )

        # 使用 QCoreApplication 的进程测试，不需要图形桌面。
        add_executable(edge-console-process-tests
            tests/console/AgentProcessTest.cpp
            tests/console/StatusTextTest.cpp
            tests/console/TcpActivityTextTest.cpp
            tests/console/EventTableModelTest.cpp
        )

        target_link_libraries(edge-console-process-tests PRIVATE
            edge-console-runtime
            GTest::gtest
        )

        if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
            target_compile_options(edge-console-process-tests PRIVATE
                -Wall
                -Wextra
                -Wpedantic
            )
        endif()

        gtest_discover_tests(edge-console-process-tests
            DISCOVERY_TIMEOUT 10
            PROPERTIES
                TIMEOUT 20
                LABELS console
        )

        target_link_libraries(edge-console-tests PRIVATE
            edge-console-protocol
            GTest::gtest_main
        )

        if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
            target_compile_options(edge-console-tests PRIVATE
                -Wall
                -Wextra
                -Wpedantic
            )
        endif()

        gtest_discover_tests(edge-console-tests
            DISCOVERY_TIMEOUT 10
            PROPERTIES
                TIMEOUT 5
                LABELS console
        )
    endif()

endif()