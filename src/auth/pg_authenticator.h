#pragma once
#include "authenticator.h"
#include <pqxx/pqxx>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

class PgAuthenticator : public Authenticator {
public:
    PgAuthenticator(const std::string& conn_str, int pool_size);
    ~PgAuthenticator() override;

    AuthResult authenticate(const std::string& client_id,
                            const std::string& username,
                            const std::string& password) override;

    Access check_acl(const std::string& username,
                     const std::string& topic,
                     Access required) override;

private:
    struct PooledConnection {
        std::unique_ptr<pqxx::connection> conn;
        bool in_use = false;
    };

    std::string conn_str_;
    std::vector<std::unique_ptr<PooledConnection>> pool_;
    std::mutex pool_mutex_;

    PooledConnection* acquire();
    void release(PooledConnection* pc);

    std::string hash_password(const std::string& password, const std::string& salt) const;
};
