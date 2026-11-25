#include "edge/upload/HttpUploadPolicy.h"

namespace edge {

HttpUploadAction classifyUploadHttpStatus(
    long status_code
) noexcept
{
    if (status_code == 200) {
        return HttpUploadAction::ParseResponse;
    }

    if (status_code == 429
        || (status_code >= 500 && status_code <= 599)) {
        return HttpUploadAction::Retry;
    }

    return HttpUploadAction::Block;
}

} // namespace edge