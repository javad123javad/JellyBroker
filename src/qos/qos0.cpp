#include "qos0.h"
#include "session.h"
#include "packet/builder.h"

void Qos0Handler::deliver(TopicTree& tree, const PublishPacket& pub) {
    auto subscribers = tree.lookup(pub.topic);
    auto packet = PacketBuilder::build_publish(
        pub.topic, pub.payload.data(), pub.payload.size(),
        0, false, false, 0);

    for (auto& entry : subscribers) {
        auto session = entry.session.lock();
        if (session) {
            session->deliver(packet);
        }
    }
}
