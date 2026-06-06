# ---- Build stage ----
FROM debian:bookworm-slim AS builder

RUN apt-get update -qq && apt-get install -y -qq \
    cmake \
    ninja-build \
    g++ \
    libboost-dev \
    libboost-system-dev \
    libssl-dev \
    libgtest-dev \
    libgmock-dev \
    nlohmann-json3-dev \
    libzstd-dev \
    zlib1g-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cmake -G Ninja -DCMAKE_BUILD_TYPE=Release \
    -DOPENSSL_ROOT_DIR=/usr \
    -DCMAKE_PREFIX_PATH="/usr/lib/x86_64-linux-gnu" \
    -DBUILD_TESTS=OFF \
    -Wno-dev \
    -B build

RUN cmake --build build

# ---- Runtime stage ----
FROM debian:bookworm-slim

RUN apt-get update -qq && apt-get install -y -qq \
    libboost-system1.74.0 \
    libssl3 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/build/src/mqtt_broker /usr/local/bin/mqtt_broker
COPY --from=builder /build/config/broker.json /etc/mqtt-broker/broker.json

EXPOSE 1883
EXPOSE 8883
EXPOSE 5353/udp

ENTRYPOINT ["mqtt_broker"]
CMD ["/etc/mqtt-broker/broker.json"]
