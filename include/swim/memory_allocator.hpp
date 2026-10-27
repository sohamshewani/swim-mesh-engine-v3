#ifndef SWIM_MEMORY_ALLOCATOR_HPP
#define SWIM_MEMORY_ALLOCATOR_HPP

#include <cstdint>
#include <cstddef>
#include <new>
#include <atomic>
#include <array>
#include <system_error>
#include <new>
#include <utility>

namespace swim {

enum class AllocatorError : uint8_t {
    Success = 0,
    OutOfMemory,
    InvalidAlignment,
    BlockExhausted,
    CorruptedHeader
};

class AllocatorException : public std::system_error {
public:
    explicit AllocatorException(AllocatorError err, const char* message)
        : std::system_error(static_cast<int>(err), std::generic_category(), message),
          error_code_(err) {}

    [[nodiscard]] AllocatorError error() const noexcept { return error_code_; }

private:
    AllocatorError error_code_;
};

class SlabArena {
public:
    static constexpr std::size_t kCacheLineSize = 64;

    SlabArena(std::size_t block_size, std::size_t block_count)
        : block_size_(AlignUp(block_size, kCacheLineSize)),
          block_count_(block_count),
          total_size_(block_size_ * block_count_),
          storage_(new alignas(kCacheLineSize) uint8_t[total_size_]),
          free_list_head_(0),
          allocated_blocks_(0) {
        
        InitializeFreeList();
    }

    ~SlabArena() noexcept {
        delete[] storage_;
    }

    SlabArena(const SlabArena&) = delete;
    SlabArena& operator=(const SlabArena&) = delete;
    SlabArena(SlabArena&& other) noexcept
        : block_size_(other.block_size_),
          block_count_(other.block_count_),
          total_size_(other.total_size_),
          storage_(other.storage_),
          free_list_head_(other.free_list_head_.load(std::memory_order_relaxed)),
          allocated_blocks_(other.allocated_blocks_.load(std::memory_order_relaxed)) {
        other.storage_ = nullptr;
        other.block_count_ = 0;
        other.total_size_ = 0;
    }

    SlabArena& operator=(SlabArena&& other) noexcept {
        if (this != &other) {
            delete[] storage_;
            block_size_ = other.block_size_;
            block_count_ = other.block_count_;
            total_size_ = other.total_size_;
            storage_ = other.storage_;
            free_list_head_.store(other.free_list_head_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            allocated_blocks_.store(other.allocated_blocks_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            
            other.storage_ = nullptr;
            other.block_count_ = 0;
            other.total_size_ = 0;
        }
        return *this;
    }

    [[nodiscard]] void* Allocate() noexcept {
        std::size_t current_head = free_list_head_.load(std::memory_order_acquire);
        
        while (current_head != kNullIndex) {
            Header* current_block = GetBlockHeader(current_head);
            std::size_t next_head = current_block->next_free;

            if (free_list_head_.compare_exchange_weak(
                current_head, 
                next_head, 
                std::memory_order_release, 
                std::memory_order_acquire)) {
                
                allocated_blocks_.fetch_add(1, std::memory_order_relaxed);
                return static_cast<void*>(reinterpret_cast<uint8_t*>(current_block) + sizeof(Header));
            }
        }
        
        return nullptr;
    }

    void Deallocate(void* ptr) noexcept {
        if (!ptr) {
            return;
        }

        auto* user_ptr = reinterpret_cast<uint8_t*>(ptr);
        auto* header = reinterpret_cast<Header*>(user_ptr - sizeof(Header));
        
        std::size_t block_index = GetBlockIndex(header);
        if (block_index >= block_count_) {
            return;
        }

        std::size_t current_head = free_list_head_.load(std::memory_order_relaxed);
        do {
            header->next_free = current_head;
        } while (!free_list_head_.compare_exchange_weak(
            current_head, 
            block_index, 
            std::memory_order_release, 
            std::memory_order_relaxed));

        allocated_blocks_.fetch_sub(1, std::memory_order_relaxed);
    }

    [[nodiscard]] std::size_t BlockSize() const noexcept { return block_size_ - sizeof(Header); }
    [[nodiscard]] std::size_t BlockCount() const noexcept { return block_count_; }
    [[nodiscard]] std::size_t AllocatedCount() const noexcept { return allocated_blocks_.load(std::memory_order_relaxed); }

private:
    struct Header {
        std::size_t next_free;
    };

    static constexpr std::size_t kNullIndex = static_cast<std::size_t>(-1);

    std::size_t block_size_;
    std::size_t block_count_;
    std::size_t total_size_;
    uint8_t* storage_;
    std::atomic<std::size_t> free_list_head_;
    std::atomic<std::size_t> allocated_blocks_;

    [[nodiscard]] static constexpr std::size_t AlignUp(std::size_t size, std::size_t alignment) noexcept {
        return (size + alignment - 1) & ~(alignment - 1);
    }

    void InitializeFreeList() noexcept {
        for (std::size_t i = 0; i < block_count_; ++i) {
            Header* header = GetBlockHeader(i);
            header->next_free = (i + 1 < block_count_) ? (i + 1) : kNullIndex;
        }
        free_list_head_.store(0, std::memory_order_relaxed);
        allocated_blocks_.store(0, std::memory_order_relaxed);
    }

    [[nodiscard]] Header* GetBlockHeader(std::size_t index) noexcept {
        uint8_t* block_address = storage_ + (index * block_size_);
        return reinterpret_cast<Header*>(block_address);
    }

    [[nodiscard]] std::size_t GetBlockIndex(const Header* header) const noexcept {
        auto byte_offset = reinterpret_cast<const uint8_t*>(header) - storage_;
        return static_cast<std::size_t>(byte_offset) / block_size_;
    }
};

template <typename T, std::size_t N>
class ThreadSafeRingArena {
public:
    ThreadSafeRingArena() : head_(0), tail_(0) {}

    ~ThreadSafeRingArena() = default;

    ThreadSafeRingArena(const ThreadSafeRingArena&) = delete;
    ThreadSafeRingArena& operator=(const ThreadSafeRingArena&) = delete;
    ThreadSafeRingArena(ThreadSafeRingArena&&) = delete;
    ThreadSafeRingArena& operator=(ThreadSafeRingArena&&) = delete;

    template <typename... Args>
    [[nodiscard]] T* Emplace(Args&&... args) noexcept {
        std::size_t current_tail = tail_.load(std::memory_order_relaxed);
        std::size_t current_head = head_.load(std::memory_order_acquire);

        do {
            std::size_t next_tail = (current_tail + 1) % Capacity();
            if (next_tail == current_head) {
                return nullptr;
            }
            if (tail_.compare_exchange_weak(current_tail, next_tail,
                                            std::memory_order_release,
                                            std::memory_order_relaxed)) {
                break;
            }
        } while (true);

        void* memory = static_cast<void*>(&buffer_[current_tail].storage);
        T* object = ::new (memory) T(std::forward<Args>(args)...);
        
        buffer_[current_tail].sequence.store(current_tail + 1, std::memory_order_release);
        return object;
    }

    bool Pop(T& value) noexcept {
        std::size_t current_head = head_.load(std::memory_order_relaxed);
        std::size_t current_tail = tail_.load(std::memory_order_acquire);

        do {
            if (current_head == current_tail) {
                return false;
            }
            std::size_t next_head = (current_head + 1) % Capacity();
            if (head_.compare_exchange_weak(current_head, next_head,
                                            std::memory_order_release,
                                            std::memory_order_relaxed)) {
                T* object = reinterpret_cast<T*>(&buffer_[current_head].storage);
                value = std::move(*object);
                object->~T();
                return true;
            }
        } while (true);
    }

    [[nodiscard]] constexpr std::size_t Capacity() const noexcept { return N + 1; }

    [[nodiscard]] bool Empty() const noexcept {
        return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_relaxed);
    }

private:
    struct Node {
        alignas(SlabArena::kCacheLineSize) char storage[sizeof(T)];
        std::atomic<std::size_t> sequence{0};
    };

    std::array<Node, N + 1> buffer_;
    alignas(SlabArena::kCacheLineSize) std::atomic<std::size_t> head_;
    alignas(SlabArena::kCacheLineSize) std::atomic<std::size_t> tail_;
};

template <typename T>
class ArenaAllocator {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    template <typename U>
    struct rebind {
        using other = ArenaAllocator<U>;
    };

    explicit ArenaAllocator(SlabArena& arena) noexcept : arena_(arena) {}

    template <typename U>
    ArenaAllocator(const ArenaAllocator<U>& other) noexcept : arena_(other.arena()) {}

    [[nodiscard]] pointer allocate(size_type n) {
        if (n != 1) {
            throw AllocatorException(AllocatorError::InvalidAlignment, "ArenaAllocator strictly supports single-object allocation.");
        }
        void* ptr = arena_.Allocate();
        if (!ptr) {
            throw AllocatorException(AllocatorError::OutOfMemory, "SlabArena out of memory blocks.");
        }
        return static_cast<pointer>(ptr);
    }

    void deallocate(pointer p, size_type n) noexcept {
        if (n != 1 || !p) {
            return;
        }
        arena_.Deallocate(static_cast<void*>(p));
    }

    template <typename... Args>
    void construct(pointer p, Args&&... args) {
        ::new (static_cast<void*>(p)) T(std::forward<Args>(args)...);
    }

    void destroy(pointer p) {
        if (p) {
            p->~T();
        }
    }

    [[nodiscard]] SlabArena& arena() const noexcept { return arena_; }

    template <typename U>
    [[nodiscard]] bool operator==(const ArenaAllocator<U>& other) const noexcept {
        return &arena_ == &other.arena();
    }

    template <typename U>
    [[nodiscard]] bool operator!=(const ArenaAllocator<U>& other) const noexcept {
        return &arena_ != &other.arena();
    }

private:
    SlabArena& arena_;
};

} // namespace swim

#endif // SWIM_MEMORY_ALLOCATOR_HPP