CREATE TABLE IF NOT EXISTS clients (
    client_id TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    salt TEXT NOT NULL,
    enabled BOOLEAN DEFAULT true
);

CREATE TABLE IF NOT EXISTS acls (
    username TEXT NOT NULL,
    topic_filter TEXT NOT NULL,
    access INT NOT NULL
);
