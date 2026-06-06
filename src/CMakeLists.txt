set(COMMON_SOURCES
    config.h config.cpp
    logger.h logger.cpp
    utils/buffer.h utils/buffer.cpp
    utils/timer.h utils/timer.cpp
    packet/types.h
    packet/builder.h packet/builder.cpp
    packet/parser.h packet/parser.cpp
    packet/packets.h packet/packets.cpp
    topic/filter.h topic/filter.cpp
    topic/topic_tree.h topic/topic_tree.cpp
    subscription.h subscription.cpp
    auth/authenticator.h
    auth/allow_all_authenticator.h
    auth/pg_authenticator.h auth/pg_authenticator.cpp
    qos/qos0.h qos/qos0.cpp
    qos/qos1.h qos/qos1.cpp
    qos/qos2.h qos/qos2.cpp
    core/context.h
    core/delivery_engine.h core/delivery_engine.cpp
    mdns/mdns_responder.h mdns/mdns_responder.cpp
    session.h session.cpp
    server.h server.cpp
    broker.h broker.cpp
)

# Static library (without main) for tests to link against
add_library(mqtt_broker_lib STATIC ${COMMON_SOURCES})

target_include_directories(mqtt_broker_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(mqtt_broker_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(mqtt_broker_lib PUBLIC
    fmt::fmt
    spdlog::spdlog
    nlohmann_json::nlohmann_json
    OpenSSL::SSL
    OpenSSL::Crypto
)

# Boost
if(WIN32)
    target_link_libraries(mqtt_broker_lib PUBLIC ws2_32 mswsock)
endif()

if(TARGET Boost::system)
    target_link_libraries(mqtt_broker_lib PUBLIC Boost::system)
elseif(TARGET boost_system)
    target_link_libraries(mqtt_broker_lib PUBLIC boost_system)
else()
    target_include_directories(mqtt_broker_lib SYSTEM PUBLIC ${Boost_INCLUDE_DIRS})
    if(Boost_LIBRARIES)
        target_link_libraries(mqtt_broker_lib PUBLIC ${Boost_LIBRARIES})
    endif()
endif()

# libpqxx
if(TARGET pqxx::pqxx)
    target_link_libraries(mqtt_broker_lib PUBLIC pqxx::pqxx)
    target_compile_definitions(mqtt_broker_lib PUBLIC HAS_LIBPQXX=1)
else()
    message(STATUS "libpqxx not found — building without PostgreSQL auth backend")
    target_compile_definitions(mqtt_broker_lib PUBLIC HAS_LIBPQXX=0)
endif()

# Main executable
add_executable(mqtt_broker main.cpp)
target_link_libraries(mqtt_broker PRIVATE mqtt_broker_lib)
