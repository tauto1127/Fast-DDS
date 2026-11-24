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

/*
 * Regression tests for a known remote participant that keeps its GUID but announces different metatraffic
 * locators (e.g. after its IP address changed).
 * The remote participant is emulated with plain UDP sockets on localhost, so the same GUID can be announced from
 * two different locators. One socket plays the old locator and the other one the new locator.
 */

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include <asio.hpp>
#include <gtest/gtest.h>

#include <fastdds/dds/publisher/qos/WriterQos.hpp>
#include <fastdds/dds/subscriber/qos/ReaderQos.hpp>
#include <fastdds/rtps/attributes/HistoryAttributes.hpp>
#include <fastdds/rtps/attributes/ReaderAttributes.hpp>
#include <fastdds/rtps/attributes/RTPSParticipantAttributes.hpp>
#include <fastdds/rtps/attributes/WriterAttributes.hpp>
#include <fastdds/rtps/builtin/data/ParticipantBuiltinTopicData.hpp>
#include <fastdds/rtps/builtin/data/TopicDescription.hpp>
#include <fastdds/rtps/common/EntityId_t.hpp>
#include <fastdds/rtps/common/GuidPrefix_t.hpp>
#include <fastdds/rtps/history/ReaderHistory.hpp>
#include <fastdds/rtps/history/WriterHistory.hpp>
#include <fastdds/rtps/participant/RTPSParticipant.hpp>
#include <fastdds/rtps/participant/RTPSParticipantListener.hpp>
#include <fastdds/rtps/reader/RTPSReader.hpp>
#include <fastdds/rtps/RTPSDomain.hpp>
#include <fastdds/rtps/transport/UDPv4TransportDescriptor.hpp>
#include <fastdds/rtps/writer/RTPSWriter.hpp>
#include <fastdds/utils/IPLocator.hpp>

#include "BlackboxTests.hpp"

using namespace eprosima::fastdds;
using namespace eprosima::fastdds::rtps;

namespace {

using udp = asio::ip::udp;

// RTPS submessage kinds
constexpr uint8_t ACKNACK = 0x06;
constexpr uint8_t HEARTBEAT = 0x07;
constexpr uint8_t INFO_DST = 0x0e;
constexpr uint8_t DATA = 0x15;

// Builtin endpoint flags announced by the emulated remote participant
constexpr uint32_t PARTICIPANT_ANNOUNCER = 0x00000001;
constexpr uint32_t PARTICIPANT_DETECTOR = 0x00000002;
constexpr uint32_t PUBLICATION_ANNOUNCER = 0x00000004;
constexpr uint32_t PUBLICATION_DETECTOR = 0x00000008;
constexpr uint32_t SUBSCRIPTION_ANNOUNCER = 0x00000010;
constexpr uint32_t SUBSCRIPTION_DETECTOR = 0x00000020;

/**
 * Builds little endian RTPS messages coming from the emulated remote participant.
 */
class MessageBuilder
{
public:

    explicit MessageBuilder(
            const GuidPrefix_t& sender)
    {
        const uint8_t header[] = {'R', 'T', 'P', 'S', 2, 3, 0x01, 0x0f};
        buffer_.insert(buffer_.end(), std::begin(header), std::end(header));
        add_prefix(sender);
    }

    void info_destination(
            const GuidPrefix_t& destination)
    {
        begin_submessage(INFO_DST);
        add_prefix(destination);
        end_submessage();
    }

    void heartbeat(
            const EntityId_t& reader,
            const EntityId_t& writer,
            uint32_t first,
            uint32_t last,
            uint32_t count)
    {
        begin_submessage(HEARTBEAT);
        add_entity(reader);
        add_entity(writer);
        add_sequence_number(first);
        add_sequence_number(last);
        add_u32(count);
        end_submessage();
    }

    // Requests sequence number 1
    void acknack(
            const EntityId_t& reader,
            const EntityId_t& writer,
            uint32_t count)
    {
        begin_submessage(ACKNACK);
        add_entity(reader);
        add_entity(writer);
        add_sequence_number(1);
        add_u32(1);           // numBits
        add_u32(0x80000000);  // bitmap
        add_u32(count);
        end_submessage();
    }

    // SPDP DATA(p) with a single metatraffic and default unicast locator
    void participant_data(
            const GuidPrefix_t& prefix,
            uint32_t sequence,
            const Locator_t& locator,
            uint32_t builtin_endpoints)
    {
        begin_submessage(DATA, 0x04);
        add_u16(0);   // extraFlags
        add_u16(16);  // octetsToInlineQos
        add_entity(c_EntityId_SPDPReader);
        add_entity(c_EntityId_SPDPWriter);
        add_sequence_number(sequence);
        // PL_CDR_LE encapsulation
        add_bytes({0x00, 0x03, 0x00, 0x00});
        add_parameter(0x0015, {2, 3, 0, 0});           // PID_PROTOCOL_VERSION
        add_parameter(0x0016, {0x01, 0x0f, 0, 0});     // PID_VENDORID
        begin_parameter(0x0050, 16);                    // PID_PARTICIPANT_GUID
        add_prefix(prefix);
        add_entity(c_EntityId_RTPSParticipant);
        begin_parameter(0x0032, 24);                    // PID_METATRAFFIC_UNICAST_LOCATOR
        add_locator(locator);
        begin_parameter(0x0031, 24);                    // PID_DEFAULT_UNICAST_LOCATOR
        add_locator(locator);
        begin_parameter(0x0002, 8);                     // PID_PARTICIPANT_LEASE_DURATION
        add_u32(60);
        add_u32(0);
        begin_parameter(0x0058, 4);                     // PID_BUILTIN_ENDPOINT_SET
        add_u32(builtin_endpoints);
        begin_parameter(0x0001, 0);                     // PID_SENTINEL
        end_submessage();
    }

    const std::vector<uint8_t>& buffer() const
    {
        return buffer_;
    }

private:

    void begin_submessage(
            uint8_t kind,
            uint8_t flags = 0)
    {
        submessage_start_ = buffer_.size();
        buffer_.push_back(kind);
        buffer_.push_back(static_cast<uint8_t>(flags | 0x01));
        add_u16(0);
    }

    void end_submessage()
    {
        size_t length = buffer_.size() - submessage_start_ - 4;
        buffer_[submessage_start_ + 2] = static_cast<uint8_t>(length & 0xff);
        buffer_[submessage_start_ + 3] = static_cast<uint8_t>((length >> 8) & 0xff);
    }

    void begin_parameter(
            uint16_t pid,
            uint16_t length)
    {
        add_u16(pid);
        add_u16(length);
    }

    void add_parameter(
            uint16_t pid,
            std::initializer_list<uint8_t> value)
    {
        begin_parameter(pid, static_cast<uint16_t>(value.size()));
        add_bytes(value);
    }

    void add_bytes(
            std::initializer_list<uint8_t> bytes)
    {
        buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
    }

    void add_u16(
            uint16_t value)
    {
        buffer_.push_back(static_cast<uint8_t>(value & 0xff));
        buffer_.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
    }

    void add_u32(
            uint32_t value)
    {
        for (int i = 0; i < 4; ++i)
        {
            buffer_.push_back(static_cast<uint8_t>((value >> (8 * i)) & 0xff));
        }
    }

    void add_sequence_number(
            uint32_t low)
    {
        add_u32(0);
        add_u32(low);
    }

    void add_prefix(
            const GuidPrefix_t& prefix)
    {
        buffer_.insert(buffer_.end(), prefix.value, prefix.value + GuidPrefix_t::size);
    }

    void add_entity(
            const EntityId_t& entity)
    {
        buffer_.insert(buffer_.end(), entity.value, entity.value + EntityId_t::size);
    }

    void add_locator(
            const Locator_t& locator)
    {
        add_u32(static_cast<uint32_t>(locator.kind));
        add_u32(locator.port);
        buffer_.insert(buffer_.end(), locator.address, locator.address + 16);
    }

    std::vector<uint8_t> buffer_;
    size_t submessage_start_ = 0;
};

Locator_t localhost_locator(
        uint16_t port)
{
    Locator_t locator;
    locator.kind = LOCATOR_KIND_UDPv4;
    IPLocator::setIPv4(locator, 127, 0, 0, 1);
    locator.port = port;
    return locator;
}

uint16_t free_port(
        asio::io_context& io)
{
    udp::socket socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    return socket.local_endpoint().port();
}

/**
 * Waits for the local participant to report the emulated remote participant at a given metatraffic port.
 */
class RemoteParticipantListener : public RTPSParticipantListener
{
public:

    explicit RemoteParticipantListener(
            const GuidPrefix_t& remote)
        : remote_(remote)
    {
    }

    void on_participant_discovery(
            RTPSParticipant*,
            ParticipantDiscoveryStatus,
            const ParticipantBuiltinTopicData& info,
            bool&) override
    {
        if (info.guid.guidPrefix == remote_ && !info.metatraffic_locators.unicast.empty())
        {
            std::lock_guard<std::mutex> guard(mutex_);
            port_ = info.metatraffic_locators.unicast.begin()->port;
            cv_.notify_all();
        }
    }

    bool wait_for_port(
            uint32_t port)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, std::chrono::seconds(5), [&]()
                       {
                           return port_ == port;
                       });
    }

private:

    GuidPrefix_t remote_;
    std::mutex mutex_;
    std::condition_variable cv_;
    uint32_t port_ = 0;
};

/**
 * Builtin traffic sent by the local participant to one of the locators of the emulated remote participant.
 * Submessages are identified by the local writer entity they refer to.
 */
struct ObservedTraffic
{
    // Index 0: SEDP publications, index 1: SEDP subscriptions
    bool acknack[2] = {false, false};
    bool heartbeat[2] = {false, false};
    bool data[2] = {false, false};
    // SPDP DATA(p) submessages
    size_t spdp_data = 0;

    bool sedp_complete() const
    {
        return acknack[0] && acknack[1] && heartbeat[0] && heartbeat[1] && data[0] && data[1];
    }

    bool sedp_empty() const
    {
        return !acknack[0] && !acknack[1] && !heartbeat[0] && !heartbeat[1] && !data[0] && !data[1];
    }

};

int sedp_index(
        const uint8_t* entity)
{
    EntityId_t id;
    std::memcpy(id.value, entity, EntityId_t::size);
    if (id == c_EntityId_SEDPPubWriter)
    {
        return 0;
    }
    if (id == c_EntityId_SEDPSubWriter)
    {
        return 1;
    }
    return -1;
}

void parse_message(
        const uint8_t* message,
        size_t size,
        const GuidPrefix_t& local,
        ObservedTraffic& traffic)
{
    if (size < 20 || 0 != std::memcmp(message, "RTPS", 4) ||
            0 != std::memcmp(message + 8, local.value, GuidPrefix_t::size))
    {
        return;
    }

    size_t pos = 20;
    while (pos + 4 <= size)
    {
        uint8_t kind = message[pos];
        bool little_endian = 0 != (message[pos + 1] & 0x01);
        size_t length = little_endian ? message[pos + 2] + 256u * message[pos + 3] :
                256u * message[pos + 2] + message[pos + 3];
        size_t start = pos + 4;
        size_t end = (0 == length) ? size : start + length;
        if (end > size)
        {
            return;
        }

        // Offset of the writer entity inside the submessage
        size_t writer_offset = (DATA == kind) ? 8 : 4;
        if ((ACKNACK == kind || HEARTBEAT == kind || DATA == kind) && start + writer_offset + 4 <= end)
        {
            const uint8_t* writer = message + start + writer_offset;
            EntityId_t id;
            std::memcpy(id.value, writer, EntityId_t::size);
            int index = sedp_index(writer);
            if (DATA == kind && id == c_EntityId_SPDPWriter)
            {
                ++traffic.spdp_data;
            }
            else if (0 <= index)
            {
                // ACKNACKs are sent by our readers, so they refer to the remote writer, which has the same entity id
                if (ACKNACK == kind)
                {
                    traffic.acknack[index] = true;
                }
                else if (HEARTBEAT == kind)
                {
                    traffic.heartbeat[index] = true;
                }
                else
                {
                    traffic.data[index] = true;
                }
            }
        }
        pos = end;
    }
}

/**
 * Receives on a socket during a period of time, or until a condition on the observed traffic holds.
 */
template<typename Predicate>
ObservedTraffic observe(
        udp::socket& socket,
        const GuidPrefix_t& local,
        std::chrono::milliseconds timeout,
        Predicate stop)
{
    ObservedTraffic traffic;
    socket.non_blocking(true);
    std::array<uint8_t, 65536> buffer;
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline && !stop(traffic))
    {
        udp::endpoint from;
        asio::error_code ec;
        size_t size = socket.receive_from(asio::buffer(buffer), from, 0, ec);
        if (ec)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        parse_message(buffer.data(), size, local, traffic);
    }
    return traffic;
}

ObservedTraffic observe_during(
        udp::socket& socket,
        const GuidPrefix_t& local,
        std::chrono::milliseconds period)
{
    return observe(socket, local, period, [](const ObservedTraffic&)
                   {
                       return false;
                   });
}

// Discards datagrams already queued on a socket
void drain(
        udp::socket& socket)
{
    socket.non_blocking(true);
    std::array<uint8_t, 65536> buffer;
    udp::endpoint from;
    asio::error_code ec;
    do
    {
        socket.receive_from(asio::buffer(buffer), from, 0, ec);
    }
    while (!ec);
}

void send(
        udp::socket& socket,
        uint16_t port,
        const MessageBuilder& message)
{
    const auto& buffer = message.buffer();
    ASSERT_EQ(buffer.size(), socket.send_to(asio::buffer(buffer),
            udp::endpoint(asio::ip::address_v4::loopback(), port)));
}

/**
 * Local participant under test, only reachable on localhost through a known metatraffic unicast port.
 */
class LocalParticipant
{
public:

    LocalParticipant(
            asio::io_context& io,
            uint16_t initial_peer_port,
            RTPSParticipantListener* listener,
            bool with_user_endpoints)
        : port_(free_port(io))
    {
        RTPSParticipantAttributes attributes;
        for (uint8_t i = 0; i < GuidPrefix_t::size; ++i)
        {
            attributes.prefix.value[i] = static_cast<uint8_t>(0x30 + i);
        }
        attributes.useBuiltinTransports = false;
        auto transport = std::make_shared<UDPv4TransportDescriptor>();
        transport->interfaceWhiteList.push_back("127.0.0.1");
        attributes.userTransports.push_back(transport);
        attributes.builtin.use_WriterLivelinessProtocol = false;
        attributes.builtin.avoid_builtin_multicast = true;
        attributes.builtin.discovery_config.leaseDuration_announcementperiod = dds::Duration_t(0, 500000000);
        attributes.builtin.metatrafficUnicastLocatorList.push_back(localhost_locator(port_));
        attributes.builtin.initialPeersList.push_back(localhost_locator(initial_peer_port));
        participant_ = RTPSDomain::createParticipant(static_cast<uint32_t>(GET_PID()) % 230, attributes, listener);
        if (nullptr == participant_ || !with_user_endpoints)
        {
            return;
        }

        // A local writer and a local reader, so there is something to send on both SEDP writers
        HistoryAttributes history_attributes;
        history_attributes.payloadMaxSize = 255;
        writer_history_.reset(new WriterHistory(history_attributes));
        reader_history_.reset(new ReaderHistory(history_attributes));
        WriterAttributes writer_attributes;
        ReaderAttributes reader_attributes;
        writer_ = RTPSDomain::createRTPSWriter(participant_, writer_attributes, writer_history_.get());
        reader_ = RTPSDomain::createRTPSReader(participant_, reader_attributes, reader_history_.get());
        TopicDescription topic;
        topic.topic_name = TEST_TOPIC_NAME;
        topic.type_name = "LocatorUpdatesType";
        endpoints_registered_ = nullptr != writer_ && nullptr != reader_ &&
                participant_->register_writer(writer_, topic, dds::WriterQos()) &&
                participant_->register_reader(reader_, topic, dds::ReaderQos());
    }

    ~LocalParticipant()
    {
        if (nullptr != participant_)
        {
            RTPSDomain::removeRTPSParticipant(participant_);
        }
    }

    bool valid(
            bool with_user_endpoints) const
    {
        return nullptr != participant_ && (!with_user_endpoints || endpoints_registered_);
    }

    GuidPrefix_t prefix() const
    {
        return participant_->getGuid().guidPrefix;
    }

    uint16_t port() const
    {
        return port_;
    }

private:

    uint16_t port_;
    RTPSParticipant* participant_ = nullptr;
    std::unique_ptr<WriterHistory> writer_history_;
    std::unique_ptr<ReaderHistory> reader_history_;
    RTPSWriter* writer_ = nullptr;
    RTPSReader* reader_ = nullptr;
    bool endpoints_registered_ = false;
};

GuidPrefix_t remote_prefix()
{
    GuidPrefix_t prefix;
    for (uint8_t i = 0; i < GuidPrefix_t::size; ++i)
    {
        prefix.value[i] = static_cast<uint8_t>(0xa0 + i);
    }
    return prefix;
}

// Makes the local SEDP readers send ACKNACKs and the local SEDP writers send HEARTBEATs and DATAs
void stimulate_sedp(
        udp::socket& socket,
        const LocalParticipant& local,
        const GuidPrefix_t& remote,
        uint32_t count)
{
    MessageBuilder message(remote);
    message.info_destination(local.prefix());
    message.heartbeat(c_EntityId_SEDPPubReader, c_EntityId_SEDPPubWriter, 1, count + 2, count);
    message.heartbeat(c_EntityId_SEDPSubReader, c_EntityId_SEDPSubWriter, 1, count + 2, count);
    message.acknack(c_EntityId_SEDPPubReader, c_EntityId_SEDPPubWriter, count);
    message.acknack(c_EntityId_SEDPSubReader, c_EntityId_SEDPSubWriter, count);
    send(socket, local.port(), message);
}

} // namespace

/*
 * The builtin SEDP endpoints already matched with a remote participant must send to its new metatraffic locator
 * once it announces it with the same GUID, and stop sending to the old one.
 */
TEST(RTPSLocatorUpdates, sedp_follows_participant_metatraffic_locator)
{
    asio::io_context io;
    udp::socket old_socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    udp::socket new_socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    const uint16_t old_port = old_socket.local_endpoint().port();
    const uint16_t new_port = new_socket.local_endpoint().port();
    const GuidPrefix_t remote = remote_prefix();
    const uint32_t builtin_endpoints = PUBLICATION_ANNOUNCER | PUBLICATION_DETECTOR |
            SUBSCRIPTION_ANNOUNCER | SUBSCRIPTION_DETECTOR;

    RemoteParticipantListener listener(remote);
    LocalParticipant local(io, old_port, &listener, true);
    ASSERT_TRUE(local.valid(true));
    auto sedp_complete = [](const ObservedTraffic& traffic)
            {
                return traffic.sedp_complete();
            };

    // Remote participant discovered at the old locator
    MessageBuilder first_announcement(remote);
    first_announcement.participant_data(remote, 1, localhost_locator(old_port), builtin_endpoints);
    send(old_socket, local.port(), first_announcement);
    ASSERT_TRUE(listener.wait_for_port(old_port));
    stimulate_sedp(old_socket, local, remote, 1);
    ASSERT_TRUE(observe(old_socket, local.prefix(), std::chrono::seconds(5), sedp_complete).sedp_complete());
    // Let the retransmissions above leave the NACK suppression period, so the next ACKNACKs are not ignored
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Same remote participant now at the new locator
    MessageBuilder second_announcement(remote);
    second_announcement.participant_data(remote, 2, localhost_locator(new_port), builtin_endpoints);
    send(new_socket, local.port(), second_announcement);
    ASSERT_TRUE(listener.wait_for_port(new_port));
    drain(old_socket);
    stimulate_sedp(new_socket, local, remote, 2);
    EXPECT_TRUE(observe(new_socket, local.prefix(), std::chrono::seconds(5), sedp_complete).sedp_complete());
    EXPECT_TRUE(observe_during(old_socket, local.prefix(), std::chrono::seconds(2)).sedp_empty());
}

/*
 * The SPDP writer must send its periodic unicast announcements to the new metatraffic locator of a known remote
 * participant once it announces it with the same GUID, and stop sending them to the old one.
 */
TEST(RTPSLocatorUpdates, spdp_follows_participant_metatraffic_locator)
{
    asio::io_context io;
    udp::socket old_socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    udp::socket new_socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    // Initial peer which is neither the old nor the new locator, so its announcements are not counted
    udp::socket peer_socket(io, udp::endpoint(asio::ip::address_v4::loopback(), 0));
    const uint16_t old_port = old_socket.local_endpoint().port();
    const uint16_t new_port = new_socket.local_endpoint().port();
    const GuidPrefix_t remote = remote_prefix();
    const uint32_t builtin_endpoints = PARTICIPANT_ANNOUNCER | PARTICIPANT_DETECTOR |
            PUBLICATION_ANNOUNCER | PUBLICATION_DETECTOR | SUBSCRIPTION_ANNOUNCER | SUBSCRIPTION_DETECTOR;
    const std::chrono::milliseconds period(500);

    RemoteParticipantListener listener(remote);
    LocalParticipant local(io, peer_socket.local_endpoint().port(), &listener, false);
    ASSERT_TRUE(local.valid(false));
    auto any_spdp = [](const ObservedTraffic& traffic)
            {
                return 0 < traffic.spdp_data;
            };

    // Remote participant discovered at the old locator
    MessageBuilder first_announcement(remote);
    first_announcement.participant_data(remote, 1, localhost_locator(old_port), builtin_endpoints);
    send(old_socket, local.port(), first_announcement);
    ASSERT_TRUE(listener.wait_for_port(old_port));
    ASSERT_LT(0u, observe(old_socket, local.prefix(), 4 * period, any_spdp).spdp_data);

    // Same remote participant now at the new locator
    MessageBuilder second_announcement(remote);
    second_announcement.participant_data(remote, 2, localhost_locator(new_port), builtin_endpoints);
    send(new_socket, local.port(), second_announcement);
    ASSERT_TRUE(listener.wait_for_port(new_port));
    drain(old_socket);
    EXPECT_LT(0u, observe(new_socket, local.prefix(), 4 * period, any_spdp).spdp_data);
    drain(old_socket);
    EXPECT_EQ(0u, observe_during(old_socket, local.prefix(), 5 * period).spdp_data);
    EXPECT_LT(0u, observe_during(new_socket, local.prefix(), 5 * period).spdp_data);
}
