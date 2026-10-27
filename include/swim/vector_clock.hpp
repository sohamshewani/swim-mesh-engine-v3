#ifndef SWIM_VECTOR_CLOCK_HPP
#define SWIM_VECTOR_CLOCK_HPP

#include <cstdint>
#include <array>
#include <atomic>
#include <algorithm>
#include <stdexcept>
#include <string>
#include <cstring>
#include <system_error>

namespace swim {

using NodeId = uint64_t;
using ClockValue = uint64_t;

constexpr size_t MAX_VECTOR_CLOCK_ENTRIES = 64;

enum class ClockError {
    Success = 0,
    TableFull,
    InvalidNodeId,
    Overflow
};

class ClockErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "swim_vector_clock";
    }

    std::string message(int ev) const override {
        switch (static_cast<ClockError>(ev)) {
            case ClockError::Success: return "Success";
            case ClockError::TableFull: return "Vector clock capacity exhausted";
            case ClockError::InvalidNodeId: return "Invalid node identifier";
            case ClockError::Overflow: return "Clock value integer overflow detected";
            default: return "Unknown vector clock error";
        }
    }
};

inline const std::error_category& get_clock_error_category() noexcept {
    static ClockErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(ClockError e) noexcept {
    return {static_cast<int>(e), get_clock_error_category()};
}

} // namespace swim

namespace std {
template <>
struct is_error_code_enum<swim::ClockError> : std::true_type {};
} // namespace std

namespace swim {

enum class DominanceResult {
    Concurrent,
    Dominates,
    IsDominatedBy,
    Equal
};

alignas(64) class VectorClock {
public:
    struct Entry {
        NodeId node_id;
        ClockValue counter;
    };

    VectorClock() noexcept : size_(0) {
        std::memset(entries_, 0, sizeof(entries_));
    }

    VectorClock(const VectorClock& other) noexcept {
        std::atomic_thread_fence(std::memory_order_acquire);
        size_load_acquire(other);
    }

    VectorClock& operator=(const VectorClock& other) noexcept {
        if (this != &other) {
            std::atomic_thread_fence(std::memory_order_acquire);
            size_load_acquire(other);
        }
        return *this;
    }

    VectorClock(VectorClock&& other) noexcept {
        std::atomic_thread_fence(std::memory_order_acquire);
        size_load_acquire(other);
    }

    VectorClock& operator=(VectorClock&& other) noexcept {
        if (this != &other) {
            std::atomic_thread_fence(std::memory_order_acquire);
            size_load_acquire(other);
        }
        return *this;
    }

    ~VectorClock() noexcept = default;

    ClockValue get(NodeId id) const noexcept {
        std::atomic_thread_fence(std::memory_order_acquire);
        uint8_t current_size = size_.load(std::memory_order_relaxed);
        for (uint8_t i = 0; i < current_size; ++i) {
            if (entries_[i].node_id == id) {
                return entries_[i].counter;
            }
        }
        return 0;
    }

    std::error_code increment(NodeId id) noexcept {
        std::atomic_thread_fence(std::memory_order_acquire);
        uint8_t current_size = size_.load(std::memory_order_relaxed);
        
        for (uint8_t i = 0; i < current_size; ++i) {
            if (entries_[i].node_id == id) {
                if (entries_[i].counter == UINT64_MAX) {
                    return make_error_code(ClockError::Overflow);
                }
                entries_[i].counter++;
                std::atomic_thread_fence(std::memory_order_release);
                return make_error_code(ClockError::Success);
            }
        }

        if (current_size >= MAX_VECTOR_CLOCK_ENTRIES) {
            return make_error_code(ClockError::TableFull);
        }

        entries_[current_size].node_id = id;
        entries_[current_size].counter = 1;
        size_.store(current_size + 1, std::memory_order_release);
        return make_error_code(ClockError::Success);
    }

    std::error_code set(NodeId id, ClockValue value) noexcept {
        std::atomic_thread_fence(std::memory_order_acquire);
        uint8_t current_size = size_.load(std::memory_order_relaxed);

        for (uint8_t i = 0; i < current_size; ++i) {
            if (entries_[i].node_id == id) {
                entries_[i].counter = value;
                std::atomic_thread_fence(std::memory_order_release);
                return make_error_code(ClockError::Success);
            }
        }

        if (current_size >= MAX_VECTOR_CLOCK_ENTRIES) {
            return make_error_code(ClockError::TableFull);
        }

        entries_[current_size].node_id = id;
        entries_[current_size].counter = value;
        size_.store(current_size + 1, std::memory_order_release);
        return make_error_code(ClockError::Success);
    }

    void merge(const VectorClock& other) noexcept {
        std::atomic_thread_fence(std::memory_order_acquire);
        uint8_t other_size = other.size_.load(std::memory_order_relaxed);
        
        for (uint8_t i = 0; i < other_size; ++i) {
            NodeId o_id = other.entries_[i].node_id;
            ClockValue o_val = other.entries_[i].counter;
            
            uint8_t current_size = size_.load(std::memory_order_relaxed);
            bool found = false;
            for (uint8_t j = 0; j < current_size; ++j) {
                if (entries_[j].node_id == o_id) {
                    if (o_val > entries_[j].counter) {
                        entries_[j].counter = o_val;
                    }
                    found = true;
                    break;
                }
            }
            
            if (!found && current_size < MAX_VECTOR_CLOCK_ENTRIES) {
                entries_[current_size].node_id = o_id;
                entries_[current_size].counter = o_val;
                size_.store(current_size + 1, std::memory_order_release);
            }
        }
        std::atomic_thread_fence(std::memory_order_release);
    }

    DominanceResult compare(const VectorClock& other) const noexcept {
        std::atomic_thread_fence(std::memory_order_acquire);
        
        // Collect all unique node IDs from both clocks
        NodeId all_nodes[MAX_VECTOR_CLOCK_ENTRIES * 2];
        uint8_t total_nodes = 0;
        
        uint8_t s1 = size_.load(std::memory_order_relaxed);
        uint8_t s2 = other.size_.load(std::memory_order_relaxed);
        
        for (uint8_t i = 0; i < s1; ++i) {
            all_nodes[total_nodes++] = entries_[i].node_id;
        }
        for (uint8_t i = 0; i < s2; ++i) {
            NodeId id = other.entries_[i].node_id;
            bool exists = false;
            for (uint8_t j = 0; j < s1; ++j) {
                if (all_nodes[j] == id) {
                    exists = true;
                    break;
                }
            }
            if (!exists && total_nodes < (MAX_VECTOR_CLOCK_ENTRIES * 2)) {
                all_nodes[total_nodes++] = id;
            }
        }

        bool greater_or_equal = true;
        bool less_or_equal = true;

        for (uint8_t i = 0; i < total_nodes; ++i) {
            NodeId id = all_nodes[i];
            ClockValue v1 = get(id);
            ClockValue v2 = other.get(id);

            if (v1 < v2) {
                greater_or_equal = false;
            }
            if (v1 > v2) {
                less_or_equal = false;
            }
        }

        if (greater_or_equal && less_or_equal) {
            return DominanceResult::Equal;
        }
        if (greater_or_equal) {
            return DominanceResult::Dominates;
        }
        if (less_or_equal) {
            return DominanceResult::IsDominatedBy;
        }
        return DominanceResult::Concurrent;
    }

    bool causes_causal_rejection(const VectorClock& incoming) const noexcept {
        DominanceResult res = compare(incoming);
        // Reject if incoming is strictly dominated by current (stale update)
        return res == DominanceResult::Dominates;
    }

    uint8_t size() const noexcept {
        std::atomic_thread_fence(std::memory_order_acquire);
        return size_.load(std::memory_order_relaxed);
    }

    const Entry* data() const noexcept {
        return entries_;
    }

private:
    void size_load_acquire(const VectorClock& other) noexcept {
        uint8_t o_size = other.size_.load(std::memory_order_relaxed);
        for (uint8_t i = 0; i < o_size; ++i) {
            entries_[i] = other.entries_[i];
        }
        size_.store(o_size, std::memory_order_release);
    }

    Entry entries_[MAX_VECTOR_CLOCK_ENTRIES];
    std::atomic<uint8_t> size_;
};

} // namespace swim

#endif // SWIM_VECTOR_CLOCK_HPP