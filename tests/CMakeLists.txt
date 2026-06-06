set(TEST_SOURCES
    test_buffer.cpp
    test_parser.cpp
    test_topic_filter.cpp
    test_topic_tree.cpp
)

add_executable(mqtt_broker_tests ${TEST_SOURCES})

target_link_libraries(mqtt_broker_tests PRIVATE
    mqtt_broker_lib
    GTest::gtest_main
    GTest::gmock
)

add_test(NAME mqtt_broker_tests COMMAND mqtt_broker_tests)
