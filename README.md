# MQTT Broker 3.1.1

A production-grade MQTT 3.1.1 broker in C++17 with Boost.Asio, supporting PostgreSQL authentication, ACLs, and TLS.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                       main.cpp                          │
│  loads Config → init Logger → start Broker              │
└──────────────┬──────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────┐
│                       Broker                             │
│  ┌──────────┬──────────┬──────────┬──────────────────┐   │
│  │ TopicTree│  SubMgr  │   Auth   │ DeliveryEngine   │   │
│  │ (trie)   │  (persist│   (iface)│ ┌────┬────┬────┐ │   │
│  │          │   subs)  │          │ │QoS0│QoS1│QoS2│ │   │
│  └──────────┴──────────┴──────────┘ └────┴────┴────┘ │   │
│  ┌────────────────────────────────────────────────────┐   │
│  │ Server (TCP acceptor + per-IP tracking)             │   │
│  │ ┌────────────────────────────────────────────────┐  │   │
│  │ │ Session (async read/write state machine)        │  │   │
│  │ │ ┌──────────┐ ┌───────────┐ ┌────────────────┐  │  │   │
│  │ │ │ Parser   │ │ Handlers  │ │ Write Queue    │  │  │   │
│  │ │ │(residue) │ │(connect,  │ │ (HWM enforced) │  │  │   │
│  │ │ │          │ │ publish,  │ │                │  │  │   │
│  │ │ │          │ │ subscribe,…││                │  │  │   │
│  │ │ └──────────┘ └───────────┘ └────────────────┘  │  │   │
│  │ └────────────────────────────────────────────────┘  │   │
│  └────────────────────────────────────────────────────┘   │
│                                                           │
│  Thread pool workers: io_context::run() × N               │
└───────────────────────────────────────────────────────────┘
```

### Layer Diagram

```
Layer 1: Utility         ┌─────────────────────────────┐
                         │ Buffer, Timer, Logger, Config│
                         └──────────┬──────────────────┘
                                    │
Layer 2: Packet          ┌──────────▼──────────────────┐
                         │ Parser, Builder, Packets     │
                         │ (Connect, Publish, Subscribe,│
                         │  Puback/rec/rel/comp, etc.)  │
                         └──────────┬──────────────────┘
                                    │
Layer 3: Topic           ┌──────────▼──────────────────┐
                         │ Filter (match/split/validate)│
                         │ TopicTree (trie + wildcards) │
                         └──────────┬──────────────────┘
                                    │
Layer 4: Auth            ┌──────────▼──────────────────┐
                         │ Authenticator interface     │
                         │ ├── AllowAllAuthenticator   │
                         │ └── PgAuthenticator         │
                         │     (SHA-256, ACLs via SQL) │
                         └──────────┬──────────────────┘
                                    │
Layer 5: QoS             ┌──────────▼──────────────────┐
                         │ DeliveryEngine              │
                         │ ├── Qos0Handler (fire-n-forget)
                         │ ├── Qos1Handler (retry+ack) │
                         │ └── Qos2Handler (4-way hndshk)
                         └──────────┬──────────────────┘
                                    │
Layer 6: Session         ┌──────────▼──────────────────┐
                         │ Session (strand-serialized)  │
                         │ ├── do_read/do_write/close  │
                         │ ├── keepalive timer         │
                         │ ├── write queue (HWM)       │
                         │ └── TLS wrapper (optional)  │
                         └──────────┬──────────────────┘
                                    │
Layer 7: Server          ┌──────────▼──────────────────┐
                         │ Server (TCP acceptor)        │
                         │ ├── per-IP connection limit │
                         │ ├── global max connections  │
                         │ └── rate-limit tracking     │
                         └──────────┬──────────────────┘
                                    │
Layer 8: Orchestrator    ┌──────────▼──────────────────┐
                         │ Broker                      │
                         │ ├── owns all services       │
                         │ ├── ServerState (shared)    │
                         │ ├── TLS context setup       │
                         │ └── signal handling         │
                         └─────────────────────────────┘
```

### Data Flow

```
Client ──TCP/TLS──→ Server accept
                         │
                    Session start
                         │
                    do_read() → parser_.feed()
                         │
                    try_extract() → handle_packet()
                         │
                    ┌────┴────┐
                    │ PUBLISH │  handle_publish()
                    └────┬────┘
                         │
                    ACL check (authenticator->check_acl)
                         │
                    topic validation
                         │
                    DeliveryEngine::publish_from_client()
                         │
                    topic_tree.lookup(topic) → subscribers
                         │
                    Qos0/1/2::deliver() → session->deliver()
                         │
                    write_queue_.push_back → do_write()
                         │
                    async_write(socket_/ssl_stream_)
```

### Key Design Decisions

| Decision | Rationale |
|---|---|
| **Single `io_context` + thread pool** | Socket transfer (`release()`/`assign()`) unreliable on Windows; single io_context is simpler and equally correct with strands |
| **`BrokerContext` (pointer members)** | Avoids construction ordering issues (auth_ set after context init); all dependencies aggregated in one struct |
| **`DeliveryEngine` owns Qos0/1/2** | Single entry point `publish_from_client()` replaces three-way dispatch in Session; cleaner SRP |
| **Raw pointers in context** | Non-owning; Broker owns the actual objects and guarantees they outlive sessions |
| **`boost::asio::strand` per session** | Lock-free serialized access to session state; shared state (TopicTree, QoS maps) uses `std::mutex` |
| **`PacketParser` with internal buffer** | Handles partial reads (TCP framing); accumulates until a complete MQTT packet is available |
| **Write queue with HWM** | Prevents OOM from slow consumers; configurable max queue depth |

## Build

### Prerequisites

- **Compiler:** GCC 10+ or MSVC 2022+ (C++17)
- **CMake** 3.20+
- **Boost** 1.80+ (system, asio)
- **OpenSSL** 1.1+ (for TLS + password hashing)
- **fmt** 10+ and **spdlog** 1.13+ (auto-fetched by CMake)
- **nlohmann_json** 3.11+ (auto-fetched)
- **GTest** 1.12+ (for tests)
- **libpqxx** 7+ (optional, for PostgreSQL auth)

### Quick Start

```bash
git clone <repo>
cd mqtt-broker
cmake -B build
cmake --build build
./build/src/mqtt_broker          # uses config/broker.json
./build/src/mqtt_broker my.json  # custom config
```

### Windows (MSYS2 UCRT64)

```bash
pacman -S mingw-w64-ucrt-x86_64-{gcc,cmake,boost,openssl,gtest}
cmake -B build -G "MSYS Makefiles"
cmake --build build
```

### CMake Options

| Option | Default | Description |
|---|---|---|
| `HAS_LIBPQXX` | auto | Enable PostgreSQL auth backend (requires libpqxx) |

## Configuration

See `config/broker.json` for the default config file.

### Broker Section

```json
{
    "broker": {
        "host": "0.0.0.0",
        "port": 1883,
        "thread_pool_size": 4,
        "max_connections": 100000,
        "max_connections_per_ip": 100,
        "session_expiry_seconds": 86400,
        "keepalive_multiplier": 1.5,
        "max_keepalive": 3600,
        "max_packet_size": 262144,
        "max_topic_depth": 128,
        "max_topic_length": 65536,
        "max_subscriptions_per_client": 1000,
        "max_retained_messages": 10000,
        "max_write_queue_size": 10000,
        "max_parser_buffer_size": 1048576,
        "max_auth_attempts": 10,
        "auth_ban_seconds": 60,
        "retry_interval_seconds": 5,
        "max_retry_count": 5
    }
}
```

| Key | Default | Description |
|---|---|---|
| `host` | `0.0.0.0` | Listen address |
| `port` | `1883` | MQTT port |
| `thread_pool_size` | `4` | Worker threads (clamped 1-128) |
| `max_connections` | `100000` | Global concurrent connection limit |
| `max_connections_per_ip` | `100` | Per-IP connection limit |
| `session_expiry_seconds` | `86400` | Persistent session expiry |
| `keepalive_multiplier` | `1.5` | Timeout = keepalive × multiplier |
| `max_keepalive` | `3600` | Maximum allowed keepalive (seconds) |
| `max_packet_size` | `262144` | Maximum MQTT packet size (bytes) |
| `max_topic_depth` | `128` | Maximum topic segments |
| `max_topic_length` | `65536` | Maximum topic string length (bytes) |
| `max_subscriptions_per_client` | `1000` | Max subscriptions per client |
| `max_retained_messages` | `10000` | Max retained messages server-wide |
| `max_write_queue_size` | `10000` | Max queued outbound packets per session |
| `max_parser_buffer_size` | `1048576` | Max parser internal buffer (bytes) |
| `max_auth_attempts` | `10` | Failed auth before temp ban |
| `auth_ban_seconds` | `60` | Ban duration (seconds) |
| `retry_interval_seconds` | `5` | QoS 1/2 retry interval |
| `max_retry_count` | `5` | QoS 1/2 max retries before drop |

### TLS Section

```json
{
    "tls": {
        "enabled": false,
        "cert_file": "/path/to/cert.pem",
        "key_file": "/path/to/key.pem",
        "ca_file": "/path/to/ca.pem"
    }
}
```

### Auth Section

```json
{
    "auth": {
        "backend": "allow_all",
        "postgres": {
            "connection_string": "host=localhost port=5432 dbname=mqtt user=mqtt password=mqtt",
            "pool_size": 4
        }
    }
}
```

## Authentication & ACLs

### AllowAll (default)

Accepts all connections, grants READWRITE to all topics. Intended for development only.

### PostgreSQL

Requires building with libpqxx. The schema expects two tables:

```sql
CREATE TABLE clients (
    client_id TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    salt TEXT NOT NULL,
    enabled BOOLEAN DEFAULT true
);

CREATE TABLE acls (
    username TEXT NOT NULL,
    topic_filter TEXT NOT NULL,
    access INT NOT NULL  -- 1=READ, 2=WRITE, 3=READWRITE
);
```

Password hashing: `SHA256(salt + password)`.

### ACL Enforcement

ACLs are checked on every PUBLISH (WRITE) and SUBSCRIBE (READ). If no auth backend is configured or the backend is unavailable, the broker refuses to start (fail-closed). Anonymous clients (no username) are checked as empty-string username.

### Brute Force Protection

After `max_auth_attempts` failed authentications from the same IP, that IP is banned for `auth_ban_seconds`. Failed attempt counters reset on successful authentication.

## Topic Model

- Supports `+` (single-level wildcard) and `#` (multi-level wildcard)
- `#` must be the last topic segment
- `+` must be an entire segment (`+bar` is invalid)
- Topic depth and length are capped by config

## Quality of Service

| QoS | Behavior |
|---|---|
| **0** | Fire-and-forget; no delivery guarantees |
| **1** | At-least-once with PUBACK; retry with configurable interval and max count |
| **2** | Exactly-once with 4-way handshake (PUBLISH→PUBREC→PUBREL→PUBCOMP); configurable retry |

Packet IDs are allocated from a per-client pool with collision avoidance: `in_use` tracking prevents reusing an ID that is still in-flight.

## Protocols and Standards

- **MQTT 3.1.1** (OASIS standard)
- No MQTT 5.0 support
- **WebSocket:** Not built-in; wrap with a WebSocket-to-TCP bridge if needed

## Security Features

| Feature | Mechanism |
|---|---|
| **Transport encryption** | TLS via `boost::asio::ssl::stream` (config.pem cert+key) |
| **Authentication** | AllowAll (dev) or PostgreSQL (SHA-256 password hash) |
| **Authorization** | Per-topic ACLs (READ/WRITE/READWRITE) |
| **Brute force protection** | Per-IP rate limiting with configurable ban |
| **Client-ID takeover** | Old session disconnected on duplicate CONNECT |
| **Connection limits** | Global and per-IP caps |
| **Write queue backpressure** | High-water mark disconnect |
| **Packet size limit** | Configurable maximum MQTT packet size |
| **Topic limits** | Max depth (segments) and length (bytes) |
| **Input validation** | All packet parsing wrapped in try-catch |
| **Remaining-length validation** | Overflow-safe decode with 4-byte max |

## Resource Limits

| Resource | Config Key | Default | Max |
|---|---|---|---|
| Concurrent connections | `max_connections` | 100,000 | — |
| Connections per IP | `max_connections_per_ip` | 100 | — |
| Write queue per session | `max_write_queue_size` | 10,000 | — |
| Parser buffer | `max_parser_buffer_size` | 1 MB | — |
| Packet size | `max_packet_size` | 256 KB | 268 MB |
| Topic depth | `max_topic_depth` | 128 | — |
| Topic length | `max_topic_length` | 65,536 | — |
| Subscriptions per client | `max_subscriptions_per_client` | 1,000 | — |
| Retained messages | `max_retained_messages` | 10,000 | — |

## Testing

### Unit Tests

```bash
cmake --build build
./build/tests/mqtt_broker_tests.exe
```

Test suites:
- `BufferTest` (9 tests) — Buffer read/write, remaining-length, edge cases
- `ParserTest` (9 tests) — Packet parsing, building round-trip, multi-packet
- `TopicFilterTest` (8 tests) — Exact match, wildcards, validation
- `TopicTreeTest` (6 tests) — Subscribe/lookup, wildcards, retained messages

### End-to-End Test

```bash
# Start broker
./build/src/mqtt_broker &

# Run test (requires paho-mqtt)
pip install paho-mqtt
python ClientPy/test_broker.py
```

### Stress Test

```bash
python ClientPy/stress_test.py 127.0.0.1 1883 \
    --connections 500 --clients 100 --messages 1000
```

## Performance

Measured on Windows 11, AMD Ryzen 5950X, MSYS2 UCRT64:

| Metric | Result |
|---|---|
| Connection storm | 500 connections in 10.5s (48/s) |
| Publish throughput | ~47,000 msg/s (10 clients × 200 msgs) |
| Delivery accuracy | 100% at 100,000 messages |
| Peak memory | ~86 MB after 100k messages + 500 connections |

## Project Structure

```
src/
├── main.cpp                  Entry point
├── broker.h/.cpp             Orchestrator
├── config.h/.cpp             Configuration
├── logger.h/.cpp             Logging
├── session.h/.cpp            Client session (state machine)
├── server.h/.cpp             TCP acceptor
├── subscription.h/.cpp       Persistent subscription storage
├── packet/                   MQTT packet layer
│   ├── types.h               Packet types, enums
│   ├── parser.h/.cpp         Stream parser (residue buffer)
│   ├── builder.h/.cpp        Packet builders
│   └── packets.h/.cpp        Packet structs + parse methods
├── topic/                    Topic matching
│   ├── filter.h/.cpp         Match/split/validate
│   └── topic_tree.h/.cpp     Trie-based subscription tree
├── auth/                     Authentication
│   ├── authenticator.h       Interface
│   ├── allow_all_authenticator.h  Dev mode
│   └── pg_authenticator.h/.cpp    PostgreSQL
├── qos/                      QoS handlers
│   ├── qos0.h/.cpp           Fire-and-forget
│   ├── qos1.h/.cpp           At-least-once (retry + ack)
│   └── qos2.h/.cpp           Exactly-once (4-way handshake)
├── core/                     Cross-cutting
│   ├── context.h             BrokerContext, ServerState
│   └── delivery_engine.h/.cpp  Publish fan-out router
└── utils/
    ├── buffer.h/.cpp         Binary buffer read/write
    └── timer.h/.cpp          Interval timer
```

## Troubleshooting

### Broker won't start: "Auth backend 'postgres' unavailable"

Either install libpqxx and rebuild, or set `auth.backend` to `"allow_all"` in the config.

### Broker won't start: "Config validation failed"

The config file is validated at startup. Check:
- `port` must be non-zero
- `thread_pool_size` must be 1–128
- If `tls.enabled` is true, `tls.cert_file` and `tls.key_file` must be set

### Clients keep getting disconnected

Check broker logs for:
- `Write queue overflow` — client is too slow to consume; increase `max_write_queue_size` or investigate client
- `Auth rate limit: X.X.X.X is banned` — too many failed auth attempts; wait for ban to expire or restart broker
- `Per-IP limit reached` — too many connections from one IP; increase `max_connections_per_ip`

### High memory usage

Check:
1. `max_retained_messages` — too many retained messages?
2. `max_write_queue_size` — slow consumers building queues?
3. Number of connected clients vs. `max_connections`

## License

MIT
