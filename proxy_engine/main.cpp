/*
 * proxy_engine – High-performance async MySQL TCP proxy
 *
 * Listens on 127.0.0.1:3307, forwards traffic to 127.0.0.1:3306.
 * Intercepts COM_QUERY (0x03) packets, times the round-trip to the
 * first response packet, and logs {query, duration_ms} as JSON to
 * query_logs.json.
 *
 * Built with C++20 / Boost.Asio / MSVC / vcpkg.
 */

#include <boost/asio.hpp>

#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>

namespace asio = boost::asio;
using tcp      = asio::ip::tcp;

// ────────────────────────────────────────────────────────────────
// Constants
// ────────────────────────────────────────────────────────────────
static constexpr std::string_view kListenAddr   = "127.0.0.1";
static constexpr std::uint16_t    kListenPort   = 3307;
static constexpr std::string_view kUpstreamAddr = "127.0.0.1";
static constexpr std::uint16_t    kUpstreamPort = 3306;
static constexpr std::size_t      kBufferSize   = 65536;   // 64 KiB
static constexpr std::uint8_t     kComQuery     = 0x03;
static constexpr char             kLogFile[]    = "query_logs.json";

// ────────────────────────────────────────────────────────────────
// Thread-safe JSON logger
// ────────────────────────────────────────────────────────────────
class QueryLogger {
public:
    static QueryLogger& instance() {
        static QueryLogger logger;
        return logger;
    }

    void log(const std::string& query, double duration_ms) {
        // Escape the query for JSON safety.
        std::string escaped = json_escape(query);

        std::lock_guard<std::mutex> lock(mtx_);
        std::ofstream ofs(kLogFile, std::ios::app);
        if (ofs.is_open()) {
            ofs << R"({"query": ")" << escaped
                << R"(", "duration_ms": )" << duration_ms << "}\n";
            ofs.flush();
        }
    }

private:
    QueryLogger() = default;

    static std::string json_escape(const std::string& s) {
        std::string out;
        out.reserve(s.size() + 16);
        for (char c : s) {
            switch (c) {
                case '"':  out += "\\\""; break;
                case '\\': out += "\\\\"; break;
                case '\b': out += "\\b";  break;
                case '\f': out += "\\f";  break;
                case '\n': out += "\\n";  break;
                case '\r': out += "\\r";  break;
                case '\t': out += "\\t";  break;
                default:
                    if (static_cast<unsigned char>(c) < 0x20) {
                        // Control character – encode as \u00XX
                        char buf[8];
                        std::snprintf(buf, sizeof(buf), "\\u%04x",
                                      static_cast<unsigned>(static_cast<unsigned char>(c)));
                        out += buf;
                    } else {
                        out += c;
                    }
            }
        }
        return out;
    }

    std::mutex mtx_;
};

// ────────────────────────────────────────────────────────────────
// ProxySession – one client↔server pair
// ────────────────────────────────────────────────────────────────
class ProxySession : public std::enable_shared_from_this<ProxySession> {
public:
    explicit ProxySession(tcp::socket client_socket,
                          asio::io_context& io_ctx)
        : client_socket_(std::move(client_socket))
        , server_socket_(io_ctx)
    {}

    // Connect to MySQL and start bidirectional relay.
    void start() {
        auto self = shared_from_this();

        tcp::endpoint upstream{
            asio::ip::make_address(kUpstreamAddr), kUpstreamPort};

        server_socket_.async_connect(
            upstream,
            [this, self](boost::system::error_code ec) {
                if (ec) {
                    std::cerr << "[proxy] upstream connect failed: "
                              << ec.message() << '\n';
                    close();
                    return;
                }
                // Begin two independent async read loops.
                read_from_client();
                read_from_server();
            });
    }

private:
    // ── Client → Server direction ──────────────────────────────
    void read_from_client() {
        auto self = shared_from_this();
        client_socket_.async_read_some(
            asio::buffer(client_buf_),
            [this, self](boost::system::error_code ec, std::size_t n) {
                if (ec) { close(); return; }
                inspect_client_data(client_buf_.data(), n);
                write_to_server(n);
            });
    }

    void write_to_server(std::size_t n) {
        auto self = shared_from_this();
        asio::async_write(
            server_socket_,
            asio::buffer(client_buf_.data(), n),
            [this, self](boost::system::error_code ec, std::size_t /*n*/) {
                if (ec) { close(); return; }
                read_from_client();
            });
    }

    // ── Server → Client direction ──────────────────────────────
    void read_from_server() {
        auto self = shared_from_this();
        server_socket_.async_read_some(
            asio::buffer(server_buf_),
            [this, self](boost::system::error_code ec, std::size_t n) {
                if (ec) { close(); return; }
                check_response_timing();
                write_to_client(n);
            });
    }

    void write_to_client(std::size_t n) {
        auto self = shared_from_this();
        asio::async_write(
            client_socket_,
            asio::buffer(server_buf_.data(), n),
            [this, self](boost::system::error_code ec, std::size_t /*n*/) {
                if (ec) { close(); return; }
                read_from_server();
            });
    }

    // ── MySQL packet inspection ────────────────────────────────
    //
    // MySQL packet format (little-endian):
    //   [3 bytes payload_length] [1 byte sequence_id] [payload ...]
    //
    // For COM_QUERY the first byte of the payload is 0x03 and the
    // remainder is the SQL query string (no NUL terminator).
    //
    void inspect_client_data(const char* data, std::size_t len) {
        // We need at least 5 bytes: 4-byte header + 1-byte command.
        if (len < 5) return;

        const auto* raw = reinterpret_cast<const std::uint8_t*>(data);

        // Decode 3-byte little-endian payload length.
        std::uint32_t payload_len =
              static_cast<std::uint32_t>(raw[0])
            | (static_cast<std::uint32_t>(raw[1]) << 8)
            | (static_cast<std::uint32_t>(raw[2]) << 16);

        // Sanity check: payload_len should fit within this read.
        // (For very large queries the packet may be split; we only
        //  capture what's available in the first read.)
        if (payload_len == 0) return;

        std::uint8_t cmd = raw[4];
        if (cmd != kComQuery) return;

        // Extract SQL string (starts at byte 5).
        std::size_t sql_len = std::min<std::size_t>(
            payload_len - 1,          // minus the command byte
            len - 5                   // what we actually received
        );
        pending_query_ = std::string(data + 5, sql_len);
        query_start_   = std::chrono::high_resolution_clock::now();
        awaiting_response_ = true;
    }

    void check_response_timing() {
        if (!awaiting_response_) return;
        awaiting_response_ = false;

        auto elapsed = std::chrono::high_resolution_clock::now() - query_start_;
        double ms = std::chrono::duration<double, std::milli>(elapsed).count();

        QueryLogger::instance().log(pending_query_, ms);

        std::cout << "[query] " << ms << " ms  →  "
                  << pending_query_.substr(0, 120) << '\n';
    }

    // ── Graceful shutdown ──────────────────────────────────────
    void close() {
        boost::system::error_code ignored;
        client_socket_.shutdown(tcp::socket::shutdown_both, ignored);
        client_socket_.close(ignored);
        server_socket_.shutdown(tcp::socket::shutdown_both, ignored);
        server_socket_.close(ignored);
    }

    // ── Data members ───────────────────────────────────────────
    tcp::socket client_socket_;
    tcp::socket server_socket_;

    std::array<char, kBufferSize> client_buf_{};
    std::array<char, kBufferSize> server_buf_{};

    // Query timing state
    bool        awaiting_response_ = false;
    std::string pending_query_;
    std::chrono::high_resolution_clock::time_point query_start_;
};

// ────────────────────────────────────────────────────────────────
// ProxyServer – accept loop
// ────────────────────────────────────────────────────────────────
class ProxyServer {
public:
    ProxyServer(asio::io_context& io_ctx,
                const tcp::endpoint& listen_ep)
        : io_ctx_(io_ctx)
        , acceptor_(io_ctx, listen_ep)
    {
        acceptor_.set_option(tcp::acceptor::reuse_address(true));
        do_accept();
    }

private:
    void do_accept() {
        acceptor_.async_accept(
            [this](boost::system::error_code ec, tcp::socket socket) {
                if (!ec) {
                    std::cout << "[proxy] new connection from "
                              << socket.remote_endpoint() << '\n';
                    std::make_shared<ProxySession>(
                        std::move(socket), io_ctx_)->start();
                } else {
                    std::cerr << "[proxy] accept error: "
                              << ec.message() << '\n';
                }
                do_accept();   // keep accepting
            });
    }

    asio::io_context& io_ctx_;
    tcp::acceptor     acceptor_;
};

// ────────────────────────────────────────────────────────────────
// main
// ────────────────────────────────────────────────────────────────
int main() {
    try {
        std::cout << "╔══════════════════════════════════════════╗\n"
                  << "║   AutoDBA Proxy Engine                   ║\n"
                  << "║   Listening on " << kListenAddr << ':' << kListenPort
                  << "              ║\n"
                  << "║   Upstream  -> " << kUpstreamAddr << ':' << kUpstreamPort
                  << "              ║\n"
                  << "╚══════════════════════════════════════════╝\n\n";

        asio::io_context io_ctx;

        tcp::endpoint listen_ep{
            asio::ip::make_address(kListenAddr), kListenPort};

        ProxyServer server(io_ctx, listen_ep);

        // Run the event loop on hardware_concurrency threads.
        const auto thread_count =
            std::max(1u, std::thread::hardware_concurrency());

        std::vector<std::thread> pool;
        pool.reserve(thread_count - 1);
        for (unsigned i = 1; i < thread_count; ++i) {
            pool.emplace_back([&io_ctx] { io_ctx.run(); });
        }
        std::cout << "[proxy] running on " << thread_count << " thread(s)\n";

        io_ctx.run();   // main thread also participates

        for (auto& t : pool) t.join();

    } catch (const std::exception& ex) {
        std::cerr << "[fatal] " << ex.what() << '\n';
        return 1;
    }
    return 0;
}
