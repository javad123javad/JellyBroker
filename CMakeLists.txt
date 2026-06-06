cmake_minimum_required(VERSION 3.20)
project(mqtt_broker CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Coverage flags (Debug only)
if(CMAKE_BUILD_TYPE STREQUAL "Coverage" OR CMAKE_BUILD_TYPE STREQUAL "Debug")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        add_compile_options(-fprofile-arcs -ftest-coverage -g -O0)
        add_link_options(-fprofile-arcs)
    endif()
endif()

# ── Dependencies via FetchContent ──────────────────────────
include(FetchContent)

# spdlog (header-only, depends on fmt)
FetchContent_Declare(fmt GIT_REPOSITORY https://github.com/fmtlib/fmt.git GIT_TAG 10.2.1)
FetchContent_Declare(spdlog GIT_REPOSITORY https://github.com/gabime/spdlog.git GIT_TAG v1.14.1)
FetchContent_Declare(nlohmann_json GIT_REPOSITORY https://github.com/nlohmann/json.git GIT_TAG v3.11.3)

set(fmt_headers_only OFF CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(fmt spdlog nlohmann_json)

# Boost (required for Asio)
find_package(Boost REQUIRED COMPONENTS system)
if(NOT Boost_FOUND)
    message(FATAL_ERROR "Boost is required. Install via MSYS2: pacman -S mingw-w64-ucrt-x86_64-boost")
endif()

# libpqxx (optional — PostgreSQL auth)
find_package(libpqxx QUIET)
if(NOT libpqxx_FOUND)
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(libpqxx QUIET libpqxx)
    endif()
endif()
if(NOT libpqxx_FOUND)
    message(STATUS "libpqxx not found — building without PostgreSQL auth backend")
endif()

# OpenSSL (required for password hashing)
if(WIN32)
    set(OPENSSL_ROOT_DIR "C:/msys64/ucrt64" CACHE PATH "OpenSSL root directory")
endif()
find_package(OpenSSL REQUIRED)

# Google Test (tests only)
option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
    find_package(GTest REQUIRED)
endif()

# ── Subdirectories ─────────────────────────────────────────
add_subdirectory(src)

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
