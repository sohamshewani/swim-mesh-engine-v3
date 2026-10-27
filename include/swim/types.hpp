#ifndef SWIM_TYPES_HPP
#define SWIM_TYPES_HPP

#include <cstdint>
#include <cstddef>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <string_view>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace swim {

using NodeId = uint64_t;
using SequenceNumber = uint64_t;

enum class NodeState : uint8_t {
    Alive = 0,
    Suspect = 1,
    Dead = 2,
    Left = 3
};

enum class MessageType : uint8_t {
    Ping = 1,
    Ack = 2,
    PingReq = 3,
    StateSync = 4
};

struct alignas(64) Endpoint {
    uint32_t ipv4_address; // Network byte order
    uint16_t port;         // Network byte order
    uint16_t padding;

    constexpr Endpoint() noexcept : ipv4_address(0), port(0), padding(0) {}
    constexpr Endpoint(uint32_t ip, uint16_t p) noexcept : ipv4_address(ip), port(p), padding(0) {}

    bool operator==(const Endpoint& other) const noexcept {
        return ipv4_address == other.ipv4_address && port == other.port;
    }

    bool operator!=(const Endpoint& other) const noexcept {
        return !(*this == other);
    }
};

struct alignas(64) VectorClock {
    std::atomic<SequenceNumber> counter{0};

    constexpr VectorClock() noexcept = default;
    
    VectorClock(SequenceNumber val) noexcept : counter(val) {}

    VectorClock(const VectorClock& other) noexcept {
        counter.store(other.counter.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }

    VectorClock& operator=(const VectorClock& other) noexcept {
        if (this != &other) {
            counter.store(other.counter.load(std::memory_order_relaxed), std::memory_order_relaxed);
        }
        return *this;
    }

    bool happens_before(const VectorClock& other) const noexcept {
        return counter.load(std::memory_order_acquire) < other.counter.load(std::memory_order_acquire);
    }

    bool concurrent_with(const VectorClock& other) const noexcept {
        return counter.load(std::memory_order_acquire) == other.counter.load(std::memory_order_acquire);
    }
};

struct alignas(64) NodeMetadata {
    NodeId id;
    Endpoint endpoint;
    std::atomic<NodeState> state;
    VectorClock vector_clock;
    std::atomic<int64_t> suspicion_timeout_ms;
    uint64_t incarnation;

    NodeMetadata() noexcept 
        : id(0), 
          endpoint(0, 0), 
          state(NodeState::Dead), 
          vector_clock(0), 
          suspicion_timeout_ms(0), 
          incarnation(0) {}

    NodeMetadata(NodeId node_id, Endpoint ep, NodeState st, SequenceNumber seq, int64_t timeout, uint64_t inc) noexcept
        : id(node_id),
          endpoint(ep),
          state(st),
          vector_clock(seq),
          suspicion_timeout_ms(timeout),
          incarnation(inc) {}

    NodeMetadata(const NodeMetadata& other) noexcept
        : id(other.id),
          endpoint(other.endpoint),
          state(other.state.load(std::memory_order_relaxed)),
          vector_clock(other.vector_clock),
          suspicion_timeout_ms(other.suspicion_timeout_ms.load(std::memory_order_relaxed)),
          incarnation(other.incarnation) {}

    NodeMetadata& operator=(const NodeMetadata& other) noexcept {
        if (this != &other) {
            id = other.id;
            endpoint = other.endpoint;
            state.store(other.state.load(std::memory_order_relaxed), std::memory_order_relaxed);
            vector_clock = other.vector_clock;
            suspicion_timeout_ms.store(other.suspicion_timeout_ms.load(std::memory_order_relaxed), std::memory_order_relaxed);
            incarnation = other.incarnation;
        }
        return *this;
    }
};

struct MessageHeader {
    MessageType type;
    uint8_t flags;
    uint16_t payload_length;
    NodeId sender_id;
    SequenceNumber sequence_number;

    constexpr MessageHeader() noexcept 
        : type(MessageType::Ping), flags(0), payload_length(0), sender_id(0), sequence_number(0) {}
} __attribute__((packed));

template <size_t Capacity = 1024>
class alignas(64) RingBufferArena {
private:
    alignas(64) uint8_t buffer_[Capacity];
    std::atomic<size_t> head_{0};
    std::atomic<size_t> tail_{0};

public:
    RingBufferArena() noexcept = default;
    ~RingBufferArena() noexcept = default;

    RingBufferArena(const RingBufferArena&) = delete;
    RingBufferArena& operator=(const RingBufferArena&) = delete;

    void* allocate(size_t size) noexcept {
        // 8-byte alignment alignment enforcement
        size_t aligned_size = (size + 7) & ~static_size_t_mask();
        size_t current_head = head_.load(std::memory_order_relaxed);
        
        while (true) {
            size_t current_tail = tail_.load(std::memory_order_acquire);
            if (current_head >= current_tail) {
                if (current_head + aligned_size > Capacity) {
                    if (aligned_size > current_tail) {
                        return nullptr; // Arena exhausted
                    }
                    // Wrap around
                    if (head_.compare_exchange_weak(current_head, aligned_size, std::memory_order_release, std::memory_order_relaxed)) {
                        return buffer_;
                    }
                } else {
                    if (head_.compare_exchange_weak(current_head, current_head + aligned_size, std::memory_order_release, std::memory_order_relaxed)) {
                        return &buffer_[current_head];
                    }
                }
            } else {
                if (current_head + aligned_size > current_tail) {
                    return nullptr; // Arena exhausted
                }
                if (head_.compare_exchange_weak(current_head, current_head + aligned_size, std::memory_order_release, std::memory_order_relaxed)) {
                    return &buffer_[current_head];
                }
            }
        }
    }

    void reset() noexcept {
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
    }

private:
    constexpr static size_t static_size_t_mask() noexcept {
        return 7;
    }
};

struct PiggybackMessage {
    NodeMetadata metadata;
    uint8_t priority; // Lower value = higher priority (e.g., Alive->Suspect transitions)
    uint8_t active;
};

template <size_t MaxEntries = 64>
class alignas(64) PriorityPiggybackQueue {
private:
    std::array<PiggybackMessage, MaxEntries> queue_;
    std::atomic<size_t> head_{0};
    std::atomic<size_t> tail_{0};
    std::atomic<size_t> size_{0};

public:
    PriorityPiggybackQueue() noexcept {
        for (auto& msg : queue_) {
            msg.active = 0;
            msg.priority = 0xFF;
        }
    }

    bool push(const NodeMetadata& meta, uint8_t priority) noexcept {
        size_t current_size = size_.load(std::memory_order_relaxed);
        if (current_size >= MaxEntries) {
            // Evict lowest priority if full, find worst priority
            size_t worst_index = 0;
            uint8_t max_prio = 0;
            for (size_t i = 0; i < MaxEntries; ++i) {
                if (queue_[i].priority > max_prio) {
                    max_prio = queue_[i].priority;
                    worst_index = i;
                }
            }
            if (priority < max_prio) {
                queue_[worst_index].metadata = meta;
                queue_[worst_index].priority = priority;
                queue_[worst_index].active = 1;
                return true;
            }
            return false;
        }

        size_t current_tail = tail_.load(std::memory_order_relaxed);
        size_t next_tail = (current_tail + 1) % MaxEntries;
        
        queue_[current_tail].metadata = meta;
        queue_[current_tail].priority = priority;
        queue_[current_tail].active = 1;
        
        tail_.store(next_tail, std::memory_order_release);
        size_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    bool pop(NodeMetadata& out_meta) noexcept {
        size_t current_size = size_.load(std::memory_order_relaxed);
        if (current_size == 0) {
            return false;
        }

        size_t current_head = head_.load(std::memory_order_relaxed);
        
        // Find highest priority item in queue
        size_t best_index = current_head;
        uint8_t min_prio = 0xFF;
        bool found = false;

        for (size_t i = 0; i < current_size; ++i) {
            size_t idx = (current_head + i) % MaxEntries;
            if (queue_[idx].active && queue_[idx].priority < min_prio) {
                min_prio = queue_[idx].priority;
                best_index = idx;
                found = true;
            }
        }

        if (!found) {
            return false;
        }

        out_meta = queue_[best_index].metadata;
        queue_[best_index].active = 0;
        queue_[best_index].priority = 0xFF;
        
        size_.fetch_sub(1, std::memory_order_relaxed);
        return true;
    }

    void clear() noexcept {
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
        size_.store(0, std::memory_order_relaxed);
        for (auto& msg : queue_) {
            msg.active = 0;
            msg.priority = 0xFF;
        }
    }
};

} // namespace swim

#endif // SWIM_TYPES_HPP