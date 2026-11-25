#include "edge/upload/HttpUploadPolicy.h"

#include <gtest/gtest.h>

#include <initializer_list>

namespace {

using Action = edge::HttpUploadAction;

void expectAction(
    std::initializer_list<long> statuses,
    Action expected
)
{
    for (const auto status : statuses) {
        SCOPED_TRACE(status);

        EXPECT_EQ(
            edge::classifyUploadHttpStatus(status),
            expected
        );
    }
}

TEST(
    HttpUploadPolicyTest,
    Http200RequiresResponseParsing
)
{
    EXPECT_EQ(
        edge::classifyUploadHttpStatus(200),
        Action::ParseResponse
    );
}

TEST(
    HttpUploadPolicyTest,
    Http429RemainsRetryable
)
{
    EXPECT_EQ(
        edge::classifyUploadHttpStatus(429),
        Action::Retry
    );
}

TEST(
    HttpUploadPolicyTest,
    All5xxRemainRetryable
)
{
    for (long status = 500; status <= 599; ++status) {
        SCOPED_TRACE(status);

        EXPECT_EQ(
            edge::classifyUploadHttpStatus(status),
            Action::Retry
        );
    }
}

TEST(
    HttpUploadPolicyTest,
    ConfigurationErrorsBlock
)
{
    expectAction(
        {
            400,
            401,
            403,
            404,
            413,
            422
        },
        Action::Block
    );
}

TEST(
    HttpUploadPolicyTest,
    Other4xxBlock
)
{
    // 本项目首版仅将 429 列为可自动重试的 4xx。
    // HTTP 409 也不能直接当成业务 conflict。
    expectAction(
        {
            408,
            409,
            415,
            418,
            499
        },
        Action::Block
    );
}

TEST(
    HttpUploadPolicyTest,
    Unsupported2xxBlock
)
{
    // 当前业务协议只接受 HTTP 200。
    expectAction(
        {
            201,
            202,
            204,
            206,
            299
        },
        Action::Block
    );
}

TEST(
    HttpUploadPolicyTest,
    RedirectsBlock
)
{
    expectAction(
        {
            300,
            301,
            302,
            303,
            304,
            307,
            308,
            399
        },
        Action::Block
    );
}

TEST(
    HttpUploadPolicyTest,
    OtherUnsupportedStatusesBlock
)
{
    expectAction(
        {
            100,
            101,
            199,
            600,
            999
        },
        Action::Block
    );
}

} // namespace