#include "broker.h"
#include "config.h"
#include "logger.h"
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
    std::string config_path = "config/broker.json";

    if (argc > 1) {
        config_path = argv[1];
    }

    try {
        Config::instance().load(config_path);
        Logger::instance().init(
            Config::instance().log_level(),
            Config::instance().log_file());

        Broker broker;
        broker.run();

    } catch (const std::exception& e) {
        std::cerr << "Fatal: " << e.what() << std::endl;
        Logger::instance().shutdown();
        return 1;
    }

    return 0;
}
