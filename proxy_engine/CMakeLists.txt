cmake_minimum_required(VERSION 3.20)
project(proxy_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ── Boost (header-only Asio is sufficient, but we also pull in
#    boost::system for error_code support) ──────────────────────
find_package(Boost REQUIRED COMPONENTS system)

add_executable(proxy_engine main.cpp)

target_link_libraries(proxy_engine
    PRIVATE
        Boost::system
        Boost::headers        # pulls in all header-only libs (asio, etc.)
        ws2_32                # Winsock2 – required on Windows for Asio
)

# MSVC-specific: silence warnings about deprecated Winsock1 and enable
# big-object support for heavily-templated Asio code.
if(MSVC)
    target_compile_definitions(proxy_engine PRIVATE
        _WIN32_WINNT=0x0A00          # target Windows 10+
        BOOST_ASIO_NO_DEPRECATED     # remove deprecated Asio APIs
    )
    target_compile_options(proxy_engine PRIVATE /bigobj /W4)
endif()
