cmake_minimum_required(VERSION 3.16)
project(market_data_feed_handler CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

option(FEED_NATIVE "Optimise for the build machine's CPU (-march=native)" ON)
option(FEED_SANITIZE "Build with AddressSanitizer + UndefinedBehaviorSanitizer" OFF)

find_package(Threads REQUIRED)

# P3 (order book + ITCH decoder) is vendored under external/lob; this project's code is in include/feed.
add_library(feed INTERFACE)
target_include_directories(feed INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include ${CMAKE_CURRENT_SOURCE_DIR}/external/lob/include)
target_compile_options(feed INTERFACE -Wall -Wextra -Wpedantic)
target_link_libraries(feed INTERFACE Threads::Threads)
if(FEED_NATIVE)
  target_compile_options(feed INTERFACE -march=native)
endif()
if(FEED_SANITIZE)
  target_compile_options(feed INTERFACE -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(feed INTERFACE -fsanitize=address,undefined)
endif()

add_executable(feed_publisher src/publisher.cpp)
target_link_libraries(feed_publisher PRIVATE feed)

add_executable(feed_handler src/subscriber.cpp)
target_link_libraries(feed_handler PRIVATE feed)

add_executable(queue_bench src/queue_bench.cpp)
target_link_libraries(queue_bench PRIVATE feed)

add_executable(feed_tests tests/test_main.cpp)
target_link_libraries(feed_tests PRIVATE feed)

enable_testing()
add_test(NAME feed_tests COMMAND feed_tests)
