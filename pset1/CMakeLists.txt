cmake_minimum_required(VERSION 3.16)
project(rpcgame CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

# Find msgpack (C++ headers are header-only; msgpack.hpp at top level).
find_path(MSGPACK_INCLUDE_DIR msgpack.hpp HINTS /usr/include)
find_library(MSGPACK_LIBRARY msgpackc HINTS /usr/lib/x86_64-linux-gnu)

if(NOT MSGPACK_INCLUDE_DIR OR NOT MSGPACK_LIBRARY)
    message(FATAL_ERROR "msgpack not found. Install with: sudo apt-get install libmsgpack-dev")
endif()

# Find xxhash
find_path(XXHASH_INCLUDE_DIR xxhash.h HINTS /usr/include)
find_library(XXHASH_LIBRARY xxhash HINTS /usr/lib/x86_64-linux-gnu)

if(NOT XXHASH_INCLUDE_DIR OR NOT XXHASH_LIBRARY)
    message(FATAL_ERROR "xxhash not found. Install with: sudo apt-get install libxxhash-dev")
endif()

# Server executable
add_executable(rpcg-server
    rpcg-server.cc
    serverstub.cc
)
target_include_directories(rpcg-server PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${MSGPACK_INCLUDE_DIR}
    ${XXHASH_INCLUDE_DIR}
)
target_link_libraries(rpcg-server PRIVATE
    ${MSGPACK_LIBRARY}
    ${XXHASH_LIBRARY}
    Threads::Threads
)

# Client executable
add_executable(rpcg-client
    rpcg-client.cc
    clientstub.cc
)
target_include_directories(rpcg-client PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${MSGPACK_INCLUDE_DIR}
    ${XXHASH_INCLUDE_DIR}
)
target_link_libraries(rpcg-client PRIVATE
    ${MSGPACK_LIBRARY}
    ${XXHASH_LIBRARY}
    Threads::Threads
)