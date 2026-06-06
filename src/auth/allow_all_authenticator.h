#pragma once
#include "authenticator.h"

class AllowAllAuthenticator : public Authenticator {
public:
    AuthResult authenticate(const std::string& client_id,
                            const std::string& username,
                            const std::string& password) override {
        return AuthResult{true, ConnackCode::ACCEPTED};
    }

    Access check_acl(const std::string& username,
                     const std::string& topic,
                     Access required) override {
        return Access::READWRITE;
    }
};
