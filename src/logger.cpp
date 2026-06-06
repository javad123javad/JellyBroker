#include "logger.h"

Logger& Logger::instance() {
    static Logger inst;
    return inst;
}

void Logger::init(const std::string& level, const std::string& file) {
    std::vector<spdlog::sink_ptr> sinks;
    sinks.push_back(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());

    if (!file.empty()) {
        sinks.push_back(std::make_shared<spdlog::sinks::rotating_file_sink_mt>(file, 5 * 1024 * 1024, 3));
    }

    logger_ = std::make_shared<spdlog::logger>("broker", sinks.begin(), sinks.end());
    spdlog::register_logger(logger_);
    logger_->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");

    if (level == "trace") logger_->set_level(spdlog::level::trace);
    else if (level == "debug") logger_->set_level(spdlog::level::debug);
    else if (level == "warn") logger_->set_level(spdlog::level::warn);
    else if (level == "error") logger_->set_level(spdlog::level::err);
    else logger_->set_level(spdlog::level::info);

    logger_->flush_on(spdlog::level::err);
}

void Logger::shutdown() {
    if (logger_) {
        spdlog::drop(logger_->name());
        logger_.reset();
    }
}
