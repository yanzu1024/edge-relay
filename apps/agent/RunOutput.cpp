#include "RunOutput.h"

#include "edge/status/JsonlWriter.h"

#include <nlohmann/json.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <streambuf>
#include <string>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <sys/random.h>
#include <unistd.h>

namespace edge {

namespace {

using Json = nlohmann::json;

class IgnoreSigpipe {
public:
    IgnoreSigpipe()
    {
        struct sigaction action{};
        action.sa_handler = SIG_IGN;

        if (::sigemptyset(&action.sa_mask) == -1
            || ::sigaction(SIGPIPE, &action, &previous_) == -1) {
            throw std::system_error(
                errno,
                std::generic_category(),
                "ignore SIGPIPE"
            );
        }
    }

    ~IgnoreSigpipe() noexcept
    {
        const int saved_errno = errno;
        (void)::sigaction(SIGPIPE, &previous_, nullptr);
        errno = saved_errno;
    }

    IgnoreSigpipe(const IgnoreSigpipe&) = delete;
    IgnoreSigpipe& operator=(const IgnoreSigpipe&) = delete;

private:
    struct sigaction previous_{};
};

std::int64_t outputWallNowMs()
{
    return std::chrono::duration_cast<
        std::chrono::milliseconds
    >(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

std::string makeRunId()
{
    std::array<unsigned char, 16> bytes{};
    std::size_t offset = 0;

    for (int attempt = 0;
         attempt < 32 && offset < bytes.size();
         ++attempt) {
        const auto count = ::getrandom(
            bytes.data() + offset,
            bytes.size() - offset,
            GRND_NONBLOCK
        );

        if (count > 0) {
            offset += static_cast<std::size_t>(count);
            continue;
        }

        if (count < 0 && errno == EINTR) {
            continue;
        }

        throw std::system_error(
            count == 0 ? EIO : errno,
            std::generic_category(),
            "generate run ID"
        );
    }

    if (offset != bytes.size()) {
        throw std::runtime_error(
            "Run ID generation made no progress"
        );
    }

    constexpr char hexadecimal[] = "0123456789abcdef";

    std::string result;
    result.reserve(32);

    for (const auto byte : bytes) {
        result.push_back(hexadecimal[byte >> 4]);
        result.push_back(hexadecimal[byte & 15]);
    }

    return result;
}

// 收集现有 ostream 输出中的完整行。
// 固定大小前缀缓冲，避免错误信息无限增长。
class OutputLineBuffer : public std::streambuf {
public:
    using LineHandler = std::function<void(std::string)>;
    using PumpHandler = std::function<void()>;

    OutputLineBuffer(
        bool discard,
        std::uint64_t& dropped,
        std::uint64_t& truncated,
        LineHandler line_handler,
        PumpHandler pump_handler
    )
        : discard_(discard),
          dropped_(dropped),
          truncated_count_(truncated),
          line_handler_(std::move(line_handler)),
          pump_handler_(std::move(pump_handler))
    {
    }

    void finishPartial() noexcept
    {
        if (size_ != 0 || truncated_) {
            complete(false);
        }
    }

protected:
    int_type overflow(int_type character) override
    {
        if (traits_type::eq_int_type(character, traits_type::eof())) {
            return traits_type::not_eof(character);
        }

        if (discard_) {
            return character;
        }

        const char value = traits_type::to_char_type(character);

        if (value == '\n') {
            complete(true);
        } else if (size_ < buffer_.size()) {
            buffer_[size_++] = value;
        } else {
            truncated_ = true;
        }

        return character;
    }

    std::streamsize xsputn(
        const char* data,
        std::streamsize size
    ) override
    {
        if (discard_) {
            return size;
        }

        for (std::streamsize index = 0; index < size; ++index) {
            (void)overflow(
                traits_type::to_int_type(data[index])
            );
        }

        return size;
    }

    int sync() override
    {
        try {
            pump_handler_();
        }
        catch (...) {
            // 展示输出不能向业务路径传播异常。
        }

        return 0;
    }

private:
    void complete(bool newline) noexcept
    {
        try {
            std::string line(buffer_.data(), size_);

            if (truncated_) {
                line += " [truncated]";
                ++truncated_count_;
            }

            if (newline) {
                line.push_back('\n');
            }

            line_handler_(std::move(line));
        }
        catch (...) {
            ++dropped_;
        }

        size_ = 0;
        truncated_ = false;
    }

    bool discard_;
    std::uint64_t& dropped_;
    std::uint64_t& truncated_count_;

    LineHandler line_handler_;
    PumpHandler pump_handler_;

    std::array<char, 8160> buffer_{};
    std::size_t size_ = 0;
    bool truncated_ = false;
};

} // namespace

struct RunOutput::Impl {
    using Clock = JsonlWriter::Clock;

    // 首先构造，最后销毁，覆盖全部描述符写入。
    IgnoreSigpipe sigpipe;

    const AgentConfig& config;
    AgentRunOptions options;

    std::string run_id;
    std::uint64_t next_sequence = 1;

    bool started = false;
    bool finished = false;

    std::array<int, 2> descriptors{STDOUT_FILENO, STDERR_FILENO};
    std::array<int, 2> original_flags{-1, -1};
    std::array<bool, 2> changed{false, false};
    std::array<bool, 2> enabled{false, false};

    std::array<std::deque<std::string>, 2> text;
    std::array<std::size_t, 2> offsets{0, 0};

    std::array<std::uint64_t, 2> dropped{0, 0};
    std::array<std::uint64_t, 2> truncated{0, 0};
    std::array<std::uint64_t, 2> io_errors{0, 0};
    std::array<int, 2> last_error{0, 0};

    std::uint64_t encoding_errors = 0;
    std::uint64_t record_dropped = 0;

    std::string last_diagnostic;

    std::unique_ptr<JsonlWriter> json;

    OutputLineBuffer stdout_buffer;
    OutputLineBuffer stderr_buffer;

    std::streambuf* previous_stdout = nullptr;
    std::streambuf* previous_stderr = nullptr;

    Impl(
        const AgentConfig& agent_config,
        const AgentRunOptions& requested
    )
        : config(agent_config),
          options{
              requested.jsonl || requested.events_jsonl,
              requested.events_jsonl
          },
          stdout_buffer(
              options.jsonl,
              dropped[0],
              truncated[0],
              [this](std::string line) {
                  onText(0, std::move(line));
              },
              [this] {
                  pump();
              }
          ),
          stderr_buffer(
              false,
              dropped[1],
              truncated[1],
              [this](std::string line) {
                  onText(1, std::move(line));
              },
              [this] {
                  pump();
              }
          )
    {
        run_id = makeRunId();

        // 先读取两端的原始标志，再修改。
        // 即使 stdout/stderr 引用了同一输出流，也能正确恢复。
        for (std::size_t index = 0; index < 2; ++index) {
            original_flags[index] = ::fcntl(
                descriptors[index],
                F_GETFL
            );

            if (original_flags[index] == -1) {
                ++io_errors[index];
                last_error[index] = errno;
            } else {
                enabled[index] = true;
            }
        }

        for (std::size_t index = 0; index < 2; ++index) {
            if (!enabled[index]) {
                continue;
            }

            if ((original_flags[index] & O_NONBLOCK) == 0) {
                if (::fcntl(
                        descriptors[index],
                        F_SETFL,
                        original_flags[index] | O_NONBLOCK
                    ) == -1) {
                    ++io_errors[index];
                    last_error[index] = errno;
                    enabled[index] = false;
                } else {
                    changed[index] = true;
                }
            }
        }

        if (options.jsonl && enabled[0]) {
            try {
                json = std::make_unique<JsonlWriter>(
                    STDOUT_FILENO
                );
            }
            catch (...) {
                ++io_errors[0];
                last_error[0] = EIO;
                enabled[0] = false;
            }
        }

        // 安装之后，现有 RunAgent 中的所有 cout/cerr 都被接管。
        previous_stdout = std::cout.rdbuf(&stdout_buffer);
        previous_stderr = std::cerr.rdbuf(&stderr_buffer);
    }

    ~Impl() noexcept
    {
        finish();

        std::cout.rdbuf(previous_stdout);
        std::cerr.rdbuf(previous_stderr);

        // JsonlWriter 在此之前销毁，之后不再进行输出。
        json.reset();

        for (int index = 1; index >= 0; --index) {
            if (changed[index]) {
                (void)::fcntl(
                    descriptors[index],
                    F_SETFL,
                    original_flags[index]
                );
            }
        }
    }

    JsonlMessageMeta metadata()
    {
        if (next_sequence
            == std::numeric_limits<std::uint64_t>::max()) {
            throw std::overflow_error(
                "JSONL sequence exhausted"
            );
        }

        return {
            run_id,
            next_sequence++,
            outputWallNowMs()
        };
    }

    void onText(std::size_t index, std::string line) noexcept
    {
        if (index == 1) {
            try {
                // 截取后若碰到 UTF-8 边界，替换不完整编码。
                // 原始 stderr 行仍保持原有字节。
                const auto encoded = Json(
                    line.substr(0, 2048)
                ).dump(
                    -1,
                    ' ',
                    false,
                    Json::error_handler_t::replace
                );

                last_diagnostic =
                    Json::parse(encoded).get<std::string>();
            }
            catch (...) {
                last_diagnostic.clear();
            }
        }

        if (!enabled[index] || text[index].size() >= 64) {
            ++dropped[index];
            return;
        }

        try {
            text[index].push_back(std::move(line));
        }
        catch (...) {
            ++dropped[index];
            return;
        }

        pump();
    }

    void abandonText(std::size_t index) noexcept
    {
        dropped[index] += text[index].size();
        text[index].clear();
        offsets[index] = 0;
    }

    void drainText(std::size_t index) noexcept
    {
        if (!enabled[index]) {
            return;
        }

        std::size_t budget = 32 * 1024;

        for (int operation = 0;
             operation < 16
                 && budget > 0
                 && !text[index].empty();
             ++operation) {
            const auto& line = text[index].front();

            const auto length = std::min(
                line.size() - offsets[index],
                budget
            );

            const auto written = ::write(
                descriptors[index],
                line.data() + offsets[index],
                length
            );

            if (written < 0) {
                const int code = errno;

                if (code == EINTR) {
                    continue;
                }

                if (code == EAGAIN || code == EWOULDBLOCK) {
                    return;
                }

                ++io_errors[index];
                last_error[index] = code;
                enabled[index] = false;
                abandonText(index);
                return;
            }

            if (written == 0) {
                ++io_errors[index];
                last_error[index] = EIO;
                enabled[index] = false;
                abandonText(index);
                return;
            }

            const auto count =
                static_cast<std::size_t>(written);

            offsets[index] += count;
            budget -= count;

            if (offsets[index] == line.size()) {
                text[index].pop_front();
                offsets[index] = 0;
            }
        }
    }

    void submit(
        JsonlEncodeResult result,
        bool is_record
    ) noexcept
    {
        try {
            if (auto* line = std::get_if<std::string>(&result)) {
                if (json) {
                    (void)json->enqueue(std::move(*line));
                } else if (is_record) {
                    ++record_dropped;
                }

                return;
            }

            ++encoding_errors;

            if (is_record) {
                ++record_dropped;
            } else if (json) {
                // 生命周期/状态编码异常后停用该展示流，
                // 避免继续产生缺少 ready 等前置条件的消息。
                json->abandon();
            }
        }
        catch (...) {
            ++encoding_errors;

            if (is_record) {
                ++record_dropped;
            }

            if (json) {
                json->abandon();
            }
        }
    }

    void pump() noexcept
    {
        if (json) {
            try {
                (void)json->drain();
            }
            catch (...) {
                ++encoding_errors;
                json->abandon();
            }
        }

        drainText(0);
        drainText(1);
    }

    bool pending() const noexcept
    {
        return (json && !json->empty())
            || !text[0].empty()
            || !text[1].empty();
    }

    void finish() noexcept
    {
        if (finished) {
            return;
        }

        stdout_buffer.finishPartial();
        stderr_buffer.finishPartial();

        const auto deadline =
            Clock::now() + std::chrono::milliseconds(200);

        do {
            pump();

            if (!pending()) {
                break;
            }

            struct pollfd descriptors_to_wait[2]{};

            descriptors_to_wait[0].fd =
                ((json && !json->empty()) || !text[0].empty())
                ? STDOUT_FILENO : -1;

            descriptors_to_wait[1].fd =
                !text[1].empty() ? STDERR_FILENO : -1;

            descriptors_to_wait[0].events = POLLOUT;
            descriptors_to_wait[1].events = POLLOUT;

            (void)::poll(descriptors_to_wait, 2, 10);
        } while (Clock::now() < deadline);

        if (json && !json->empty()) {
            json->abandon();
        }

        abandonText(0);
        abandonText(1);

        finished = true;
    }
};

RunOutput::RunOutput(
    const AgentConfig& config,
    const AgentRunOptions& options
)
    : impl_(std::make_unique<Impl>(config, options))
{
}

RunOutput::~RunOutput() noexcept = default;

const std::string& RunOutput::runId() const noexcept
{
    return impl_->run_id;
}

void RunOutput::ready() noexcept
{
    impl_->started = true;

    if (!impl_->options.jsonl) {
        return;
    }

    try {
        impl_->submit(
            encodeJsonlReady(
                impl_->metadata(),
                {
                    impl_->config.agent.id,
                    impl_->config.tcp.bind,
                    impl_->config.tcp.port,
                    impl_->options.events_jsonl
                }
            ),
            false
        );

        impl_->pump();
    }
    catch (...) {
        ++impl_->encoding_errors;

        if (impl_->json) {
            impl_->json->abandon();
        }
    }
}

void RunOutput::record(
    const SensorEvent& event,
    JsonlRecordDisposition disposition
) noexcept
{
    if (!impl_->options.events_jsonl) {
        return;
    }

    if (!impl_->started
        || !impl_->json
        || !impl_->json->enabled()) {
        ++impl_->record_dropped;
        return;
    }

    try {
        impl_->submit(
            encodeJsonlRecord(
                impl_->metadata(),
                JsonlRecordData{event, disposition}
            ),
            true
        );

        impl_->pump();
    }
    catch (...) {
        ++impl_->encoding_errors;
        ++impl_->record_dropped;
    }
}

void RunOutput::status(
    const StatusSnapshot& snapshot
) noexcept
{
    if (!impl_->options.jsonl || !impl_->started) {
        return;
    }

    try {
        impl_->submit(
            encodeJsonlStatus(
                impl_->metadata(),
                snapshot,
                impl_->config.status
            ),
            false
        );

        impl_->pump();
    }
    catch (...) {
        ++impl_->encoding_errors;

        if (impl_->json) {
            impl_->json->abandon();
        }
    }
}

void RunOutput::stopped(
    const StatusSnapshot& snapshot,
    int exit_code
) noexcept
{
    if (!impl_->options.jsonl || !impl_->started) {
        return;
    }

    try {
        impl_->submit(
            encodeJsonlStopped(
                impl_->metadata(),
                exit_code,
                snapshot,
                impl_->config.status
            ),
            false
        );

        impl_->pump();
    }
    catch (...) {
        ++impl_->encoding_errors;

        if (impl_->json) {
            impl_->json->abandon();
        }
    }
}

void RunOutput::decorate(StatusSnapshot& snapshot) const
{
    snapshot.run_id = impl_->run_id;

    StatusConsoleObservation console;

    console.jsonl = impl_->options.jsonl;
    console.stdout_enabled = impl_->options.jsonl
        ? (impl_->json && impl_->json->enabled())
        : impl_->enabled[0];

    console.stderr_enabled = impl_->enabled[1];
    console.encoding_errors = impl_->encoding_errors;

    console.stdout_text_dropped = impl_->dropped[0];
    console.diagnostic_dropped = impl_->dropped[1];
    console.text_truncated =
        impl_->truncated[0] + impl_->truncated[1];

    console.io_errors =
        impl_->io_errors[0] + impl_->io_errors[1];

    console.stdout_error = impl_->last_error[0];
    console.stderr_error = impl_->last_error[1];

    console.last_diagnostic = impl_->last_diagnostic;

    snapshot.display_dropped = impl_->record_dropped;

    if (impl_->json) {
        const auto& stats = impl_->json->stats();

        snapshot.display_dropped += stats.display_dropped;

        console.io_errors += stats.io_errors;
        console.status_coalesced = stats.status_coalesced;
        console.record_queue_drops = stats.record_queue_drops;
        console.record_rate_drops = stats.record_rate_drops;
        console.abandoned_lines = stats.abandoned_lines;
        console.partial_lines_abandoned =
            stats.partial_lines_abandoned;

        if (stats.last_error != 0) {
            console.stdout_error = stats.last_error;
        }
    }

    snapshot.console = std::move(console);
}

void RunOutput::pump() noexcept
{
    impl_->pump();
}

void RunOutput::finish() noexcept
{
    impl_->finish();
}

void reportRunOutputSetupError(const char* message) noexcept
{
    try {
        IgnoreSigpipe sigpipe;

        const int flags = ::fcntl(STDERR_FILENO, F_GETFL);

        if (flags == -1) {
            return;
        }

        if (::fcntl(
                STDERR_FILENO,
                F_SETFL,
                flags | O_NONBLOCK
            ) == -1) {
            return;
        }

        try {
            std::string text = "Runtime output setup failed: ";
            text += std::string(message).substr(0, 2048);
            text.push_back('\n');

            (void)::write(
                STDERR_FILENO,
                text.data(),
                text.size()
            );
        }
        catch (...) {
        }

        (void)::fcntl(STDERR_FILENO, F_SETFL, flags);
    }
    catch (...) {
    }
}

} // namespace edge