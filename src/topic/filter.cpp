#include "filter.h"
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

    auto segments = split(topic);
    for (const auto& seg : segments) {
        if (seg.empty()) return false;
        if (seg == "+" || seg == "#") return false;
    }
    return true;
}

bool is_valid_filter(const std::string& filter) {
    if (filter.empty()) return false;

    auto segments = split(filter);
    for (size_t i = 0; i < segments.size(); ++i) {
        const auto& seg = segments[i];
        if (seg.empty()) return false;

        if (seg == "#") {
            return i == segments.size() - 1;
        }
    }
    return true;
}

}  // namespace topic
