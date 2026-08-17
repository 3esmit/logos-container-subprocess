# Logos Container Subprocess tests
#
# Exercises the parent-side container directly (launch/terminate/pid/output
# relay/crash detection) using real child processes (/bin/sleep, /bin/sh).
# Qt-free: only the parent library is needed.

add_executable(logos_container_subprocess_tests
    test_subprocess_container.cpp
)

target_link_libraries(logos_container_subprocess_tests PRIVATE
    logos_container_subprocess
    GTest::gtest
    GTest::gtest_main
)

target_include_directories(logos_container_subprocess_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

# DISCOVERY_MODE PRE_TEST: the default (POST_BUILD) RUNS the test binary at
# build time to enumerate cases, which is impossible when cross-compiling --
# the build host cannot execute a PE, and the attempt fails obscurely with
# "logos_container_subprocess_tests.exe: line 2: syntax error". PRE_TEST defers
# enumeration to ctest time; native behaviour is unchanged.
gtest_discover_tests(logos_container_subprocess_tests DISCOVERY_MODE PRE_TEST)
