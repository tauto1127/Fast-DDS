// Copyright 2026 Proyectos y Sistemas de Mantenimiento SL (eProsima).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <asio.hpp>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <thread>

#include <fastdds/rtps/RTPSDomain.h>
#include <fastdds/rtps/attributes/HistoryAttributes.h>
#include <fastdds/rtps/attributes/ReaderAttributes.h>
#include <fastdds/rtps/attributes/RTPSParticipantAttributes.h>
#include <fastdds/rtps/attributes/WriterAttributes.h>
#include <fastdds/rtps/builtin/data/BuiltinEndpoints.hpp>
#include <fastdds/rtps/builtin/data/ParticipantProxyData.h>
#include <fastdds/rtps/history/ReaderHistory.h>
#include <fastdds/rtps/history/WriterHistory.h>
#include <fastdds/rtps/messages/RTPSMessageCreator.h>
#include <fastdds/rtps/participant/RTPSParticipant.h>
#include <fastdds/rtps/participant/RTPSParticipantListener.h>
#include <fastdds/rtps/transport/UDPv4TransportDescriptor.h>
#include <fastrtps/attributes/TopicAttributes.h>
#include <fastrtps/qos/ReaderQos.h>
#include <fastrtps/qos/WriterQos.h>
#include <fastrtps/utils/IPLocator.h>

using namespace eprosima::fastrtps;
using namespace eprosima::fastrtps::rtps;

namespace {
using udp = asio::ip::udp;

Locator_t locator(uint16_t port)
{
    Locator_t loc;
    loc.kind = LOCATOR_KIND_UDPv4;
    IPLocator::setIPv4(loc, 127, 0, 0, 1);
    loc.port = port;
    return loc;
}

class DiscoveryListener : public RTPSParticipantListener
{
public:
    void onParticipantDiscovery(RTPSParticipant*, ParticipantDiscoveryInfo&& info) override
    {
        if (info.info.m_guid.guidPrefix.value[0] == 0xa0 && !info.info.metatraffic_locators.unicast.empty())
        {
            std::lock_guard<std::mutex> guard(mutex_);
            port_ = info.info.metatraffic_locators.unicast.begin()->port;
            cv_.notify_all();
        }
    }

    bool wait_for(uint16_t port)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(5), [&] { return port_ == port; });
    }
private:
    std::mutex mutex_;
    std::condition_variable cv_;
    uint16_t port_ = 0;
};

class ParticipantGuard
{
public:
    explicit ParticipantGuard(RTPSParticipant* participant) : participant_(participant) {}
    ~ParticipantGuard() { if (participant_) RTPSDomain::removeRTPSParticipant(participant_); }
private:
    RTPSParticipant* participant_;
};

bool send_spdp(udp::socket& socket, const udp::endpoint& destination, const GuidPrefix_t& prefix,
        const Locator_t& remote_locator, uint32_t sequence, bool with_pdp_endpoints = false)
{
    RTPSParticipantAllocationAttributes allocation;
    ParticipantProxyData pdata(allocation);
    pdata.m_guid = GUID_t(prefix, c_EntityId_RTPSParticipant);
    pdata.m_protocolVersion = c_ProtocolVersion;
    pdata.m_VendorId = c_VendorId_eProsima;
    pdata.m_availableBuiltinEndpoints = DISC_BUILTIN_ENDPOINT_PUBLICATION_ANNOUNCER |
            DISC_BUILTIN_ENDPOINT_PUBLICATION_DETECTOR | DISC_BUILTIN_ENDPOINT_SUBSCRIPTION_ANNOUNCER |
            DISC_BUILTIN_ENDPOINT_SUBSCRIPTION_DETECTOR;
    if (with_pdp_endpoints)
    {
        pdata.m_availableBuiltinEndpoints |= DISC_BUILTIN_ENDPOINT_PARTICIPANT_ANNOUNCER |
                DISC_BUILTIN_ENDPOINT_PARTICIPANT_DETECTOR;
    }
    pdata.m_leaseDuration = Duration_t(60, 0);
    pdata.metatraffic_locators.unicast.push_back(remote_locator);
    pdata.default_locators.unicast.push_back(remote_locator);
    CacheChange_t change(pdata.get_serialized_size(true));
    change.kind = ALIVE;
    change.writerGUID = GUID_t(prefix, c_EntityId_SPDPWriter);
    change.sequenceNumber = SequenceNumber_t(0, sequence);
    CDRMessage_t payload(change.serializedPayload);
    if (!pdata.writeToCDRMessage(&payload, true)) return false;
    change.serializedPayload.length = payload.length;
    CDRMessage_t msg(change.serializedPayload.length + 512);
    GuidPrefix_t copy = prefix;
    if (!RTPSMessageCreator::addMessageData(&msg, copy, &change, WITH_KEY, c_EntityId_SPDPReader, false, nullptr))
    {
        return false;
    }
    return socket.send_to(asio::buffer(msg.buffer, msg.length), destination) == msg.length;
}

bool stimulate(udp::socket& socket, const udp::endpoint& destination, const GuidPrefix_t& remote,
        const GuidPrefix_t& local, int count)
{
    for (int i = 0; i < 2; ++i)
    {
        const EntityId_t& writer = i ? c_EntityId_SEDPSubWriter : c_EntityId_SEDPPubWriter;
        const EntityId_t& reader = i ? c_EntityId_SEDPSubReader : c_EntityId_SEDPPubReader;
        CDRMessage_t heartbeat(512);
        if (!RTPSMessageCreator::addMessageHeartbeat(&heartbeat, remote, reader, writer,
                SequenceNumber_t(0, 1), SequenceNumber_t(0, count + 2), count, false, false)) return false;
        socket.send_to(asio::buffer(heartbeat.buffer, heartbeat.length), destination);
        SequenceNumberSet_t missing(SequenceNumber_t(0, 1));
        missing.add(SequenceNumber_t(0, 1));
        CDRMessage_t ack(512);
        if (!RTPSMessageCreator::addMessageAcknack(&ack, remote, local, reader, writer, missing, count, false))
        {
            return false;
        }
        socket.send_to(asio::buffer(ack.buffer, ack.length), destination);
    }
    return true;
}

struct Observed
{
    bool ack[2] = {false, false};
    bool heartbeat[2] = {false, false};
    bool data[2] = {false, false};
    bool complete() const { return ack[0] && ack[1] && heartbeat[0] && heartbeat[1] && data[0] && data[1]; }
    bool empty() const { return !ack[0] && !ack[1] && !heartbeat[0] && !heartbeat[1] && !data[0] && !data[1]; }
};

Observed collect(udp::socket& socket, const GuidPrefix_t& local,
        std::chrono::seconds timeout = std::chrono::seconds(5))
{
    Observed result;
    socket.non_blocking(true);
    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::array<unsigned char, 65536> buffer;
    while (std::chrono::steady_clock::now() < deadline && !result.complete())
    {
        udp::endpoint from;
        asio::error_code ec;
        size_t size = socket.receive_from(asio::buffer(buffer), from, 0, ec);
        if (ec == asio::error::would_block || ec == asio::error::try_again)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        if (ec || size < 20 || std::memcmp(buffer.data(), "RTPS", 4) ||
                std::memcmp(buffer.data() + 8, local.value, 12)) continue;
        for (size_t pos = 20; pos + 4 <= size;)
        {
            unsigned kind = buffer[pos];
            unsigned length = (buffer[pos + 1] & 1) ? buffer[pos + 2] + 256 * buffer[pos + 3] :
                    256 * buffer[pos + 2] + buffer[pos + 3];
            size_t start = pos + 4, end = length ? start + length : size;
            if (end > size || end < start) break;
            size_t id_offset = kind == 0x15 ? 8 : 4;
            if ((kind == 0x06 || kind == 0x07 || kind == 0x15) && start + id_offset + 4 <= end)
            {
                const auto* id = buffer.data() + start + id_offset;
                int index = id[0] == 0 && id[1] == 0 && id[3] == 0xc2 && id[2] == 3 ? 0 :
                        id[0] == 0 && id[1] == 0 && id[3] == 0xc2 && id[2] == 4 ? 1 : -1;
                if (index >= 0)
                {
                    if (kind == 0x06) result.ack[index] = true;
                    else if (kind == 0x07) result.heartbeat[index] = true;
                    else result.data[index] = true;
                }
            }
            if (end == size) break;
            pos = end;
        }
    }
    return result;
}
// Counts DATA submessages written by the local SPDP writer (entity 0x000100c2) until the timeout,
// or until the first one if stop_at_first is set.
int count_spdp(udp::socket& socket, const GuidPrefix_t& local, std::chrono::milliseconds timeout,
        bool stop_at_first)
{
    int count = 0;
    socket.non_blocking(true);
    auto deadline = std::chrono::steady_clock::now() + timeout;
    std::array<unsigned char, 65536> buffer;
    while (std::chrono::steady_clock::now() < deadline && !(stop_at_first && count > 0))
    {
        udp::endpoint from;
        asio::error_code ec;
        size_t size = socket.receive_from(asio::buffer(buffer), from, 0, ec);
        if (ec == asio::error::would_block || ec == asio::error::try_again)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        if (ec || size < 20 || std::memcmp(buffer.data(), "RTPS", 4) ||
                std::memcmp(buffer.data() + 8, local.value, 12)) continue;
        for (size_t pos = 20; pos + 4 <= size;)
        {
            unsigned kind = buffer[pos];
            unsigned length = (buffer[pos + 1] & 1) ? buffer[pos + 2] + 256 * buffer[pos + 3] :
                    256 * buffer[pos + 2] + buffer[pos + 3];
            size_t start = pos + 4, end = length ? start + length : size;
            if (end > size || end < start) break;
            if (kind == 0x15 && start + 12 <= end)
            {
                const auto* id = buffer.data() + start + 8;
                if (id[0] == 0 && id[1] == 1 && id[2] == 0 && id[3] == 0xc2) ++count;
            }
            if (end == size) break;
            pos = end;
        }
    }
    return count;
}
} // namespace

TEST(RTPSLocatorUpdates, ExistingBuiltinEndpointsFollowParticipantMetatraffic)
{
    asio::io_context io;
    udp::socket old_socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    udp::socket new_socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    udp::socket reserved(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    const uint16_t old_port = old_socket.local_endpoint().port(), new_port = new_socket.local_endpoint().port();
    const uint16_t local_port = reserved.local_endpoint().port();
    reserved.close();
    DiscoveryListener listener;
    RTPSParticipantAttributes attributes;
    for (unsigned i = 0; i < 12; ++i) attributes.prefix.value[i] = static_cast<uint8_t>(0x10 + i);
    attributes.useBuiltinTransports = false;
    auto transport = std::make_shared<eprosima::fastdds::rtps::UDPv4TransportDescriptor>();
    transport->interfaceWhiteList.push_back("127.0.0.1");
    attributes.userTransports.push_back(transport);
    attributes.builtin.use_WriterLivelinessProtocol = false;
    attributes.builtin.avoid_builtin_multicast = true;
    attributes.builtin.metatrafficUnicastLocatorList.push_back(locator(local_port));
    attributes.builtin.initialPeersList.push_back(locator(old_port));
    HistoryAttributes history_attributes;
    history_attributes.payloadMaxSize = 255;
    history_attributes.maximumReservedCaches = 50;
    WriterHistory writer_history(history_attributes);
    ReaderHistory reader_history(history_attributes);
    RTPSParticipant* participant = RTPSDomain::createParticipant(0, attributes, &listener);
    ASSERT_NE(nullptr, participant);
    ParticipantGuard guard(participant);
    WriterAttributes writer_attributes;
    ReaderAttributes reader_attributes;
    RTPSWriter* writer = RTPSDomain::createRTPSWriter(participant, writer_attributes, &writer_history);
    RTPSReader* reader = RTPSDomain::createRTPSReader(participant, reader_attributes, &reader_history);
    ASSERT_NE(nullptr, writer);
    ASSERT_NE(nullptr, reader);
    TopicAttributes topic;
    topic.topicName = "LocatorUpdateRegression";
    topic.topicDataType = "LocatorUpdateType";
    ASSERT_TRUE(participant->registerWriter(writer, topic, WriterQos()));
    ASSERT_TRUE(participant->registerReader(reader, topic, ReaderQos()));

    GuidPrefix_t remote;
    for (unsigned i = 0; i < 12; ++i) remote.value[i] = static_cast<uint8_t>(0xa0 + i);
    const auto destination = udp::endpoint(asio::ip::address_v4::loopback(), local_port);
    ASSERT_TRUE(send_spdp(old_socket, destination, remote, locator(old_port), 1));
    ASSERT_TRUE(listener.wait_for(old_port));
    ASSERT_TRUE(stimulate(old_socket, destination, remote, attributes.prefix, 1));
    ASSERT_TRUE(collect(old_socket, attributes.prefix).complete());
    ASSERT_TRUE(send_spdp(new_socket, destination, remote, locator(new_port), 2));
    ASSERT_TRUE(listener.wait_for(new_port));
    ASSERT_TRUE(stimulate(new_socket, destination, remote, attributes.prefix, 2));
    EXPECT_TRUE(collect(new_socket, attributes.prefix).complete());
    // Drain datagrams already queued before the switch, then check for ongoing stale traffic.
    std::array<unsigned char, 65536> pending;
    udp::endpoint sender;
    asio::error_code ec;
    while (old_socket.receive_from(asio::buffer(pending), sender, 0, ec), !ec) {}
    EXPECT_TRUE(collect(old_socket, attributes.prefix, std::chrono::seconds(2)).empty());
}

// A known participant that re-announces a new metatraffic unicast locator (same GUID) must receive our
// periodic unicast SPDP DATA(p) at the new locator, and no longer at the old one.
TEST(RTPSLocatorUpdates, SpdpUnicastFollowsParticipantMetatraffic)
{
    asio::io_context io;
    udp::socket old_socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    udp::socket new_socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    // Initial peer that is neither the old nor the new locator, so fixed-locator announcements do not interfere.
    udp::socket peer_socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    udp::socket reserved(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    const uint16_t old_port = old_socket.local_endpoint().port(), new_port = new_socket.local_endpoint().port();
    const uint16_t peer_port = peer_socket.local_endpoint().port();
    const uint16_t local_port = reserved.local_endpoint().port();
    reserved.close();
    DiscoveryListener listener;
    RTPSParticipantAttributes attributes;
    for (unsigned i = 0; i < 12; ++i) attributes.prefix.value[i] = static_cast<uint8_t>(0x30 + i);
    attributes.useBuiltinTransports = false;
    auto transport = std::make_shared<eprosima::fastdds::rtps::UDPv4TransportDescriptor>();
    transport->interfaceWhiteList.push_back("127.0.0.1");
    attributes.userTransports.push_back(transport);
    attributes.builtin.use_WriterLivelinessProtocol = false;
    attributes.builtin.avoid_builtin_multicast = true;
    attributes.builtin.discovery_config.leaseDuration_announcementperiod = Duration_t(0, 500000000);
    attributes.builtin.metatrafficUnicastLocatorList.push_back(locator(local_port));
    attributes.builtin.initialPeersList.push_back(locator(peer_port));
    RTPSParticipant* participant = RTPSDomain::createParticipant(0, attributes, &listener);
    ASSERT_NE(nullptr, participant);
    ParticipantGuard guard(participant);

    GuidPrefix_t remote;
    for (unsigned i = 0; i < 12; ++i) remote.value[i] = static_cast<uint8_t>(0xa0 + i);
    const auto destination = udp::endpoint(asio::ip::address_v4::loopback(), local_port);
    const auto period = std::chrono::milliseconds(500);

    ASSERT_TRUE(send_spdp(old_socket, destination, remote, locator(old_port), 1, true));
    ASSERT_TRUE(listener.wait_for(old_port));
    ASSERT_GT(count_spdp(old_socket, attributes.prefix, 4 * period, true), 0);

    ASSERT_TRUE(send_spdp(new_socket, destination, remote, locator(new_port), 2, true));
    ASSERT_TRUE(listener.wait_for(new_port));
    EXPECT_GT(count_spdp(new_socket, attributes.prefix, 4 * period, true), 0);
    // Drain datagrams already queued before the switch, then check for ongoing stale announcements.
    std::array<unsigned char, 65536> pending;
    udp::endpoint sender;
    asio::error_code ec;
    old_socket.non_blocking(true);
    while (old_socket.receive_from(asio::buffer(pending), sender, 0, ec), !ec) {}
    EXPECT_EQ(0, count_spdp(old_socket, attributes.prefix, 5 * period, false));
    EXPECT_GT(count_spdp(new_socket, attributes.prefix, 5 * period, false), 0);
}
