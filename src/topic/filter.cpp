#include "filter.h"
#include "config.h"
#include <sstream>

namespace topic {

std::vector<std::string> split(const std::string& path) {
    std::vector<std::string> segments;
    std::istringstream stream(path);
    std::string segment;
    while (std::getline(stream, segment, '/')) {
        segments.push_back(segment);
    }
    return segments;
}

bool matches(const std::string& topic, const std::string& filter) {
    auto topic_segments = split(topic);
    auto filter_segments = split(filter);

    size_t ti = 0, fi = 0;
    while (ti < topic_segments.size() && fi < filter_segments.size()) {
        const auto& fs = filter_segments[fi];

        if (fs == "#") {
            return true;
        }

        if (fs == "+") {
            ++ti;
            ++fi;
            continue;
        }

        if (topic_segments[ti] != fs) {
            return false;
        }

        ++ti;
        ++fi;
    }

    return ti == topic_segments.size() && fi == filter_segments.size();
}

bool is_valid_topic(const std::string& topic) {
    if (topic.empty()) return false;

    auto max_len = Config::instance().max_topic_length();
    if (static_cast<int>(topic.size()) > max_len) return false;

    auto segments = split(topic);
    if (static_cast<int>(segments.size()) > Config::instance().max_topic_depth()) return false;

    for (const auto& seg : segments) {
        if (seg.empty()) return false;
        if (seg == "+" || seg == "#") return false;
    }
    return true;
}

bool is_valid_filter(const std::string& filter) {
    if (filter.empty()) return false;

    auto max_len = Config::instance().max_topic_length();
    if (static_cast<int>(filter.size()) > max_len) return false;

    auto segments = split(filter);
    if (static_cast<int>(segments.size()) > Config::instance().max_topic_depth()) return false;

    for (size_t i = 0; i < segments.size(); ++i) {
        const auto& seg = segments[i];

        if (seg.empty()) return false;

        // '+' must be an entire segment, not part of one
        if (seg.find('+') != std::string::npos && seg != "+") return false;

        // '#' must be entire segment and only in last position
        if (seg == "#") {
            return i == segments.size() - 1;
        }
        // '#' appearing inside a segment
        if (seg.find('#') != std::string::npos) return false;
    }
    return true;
}

}  // namespace topic
