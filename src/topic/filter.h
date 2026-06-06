#pragma once
#include <string>
#include <vector>

namespace topic {

bool matches(const std::string& topic, const std::string& filter);

std::vector<std::string> split(const std::string& path);

bool is_valid_topic(const std::string& topic);

bool is_valid_filter(const std::string& filter);

}  // namespace topic
