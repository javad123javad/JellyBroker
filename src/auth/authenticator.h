#pragma once
#include "packet/types.h"
#include <string>

enum class Access : uint8_t {
    NONE = 0,
    READ = 1,
    WRITE = 2,
    READWRITE = 3
};

struct AuthResult {
    bool success = false;
    ConnackCode reason = ConnackCode::REFUSED_NOT_AUTHORIZED;
};

class Authenticator {
public:
    virtual ~Authenticator() = default;

    virtual AuthResult authenticate(const std::string& client_id,
                                    const std::string& username,
                                    const std::string& password) = 0;

    virtual Access check_acl(const std::string& username,
                             const std::string& topic,
                             Access required) = 0;
};
