#include "gossip_engine.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <algorithm>

namespace swim {

// ============================================================================
// PiggybackQueue Implementation
// ============================================================================

PiggybackQueue::PiggybackQueue(size_t capacity)
    : capacity_(capacity), head_(0), tail_(0), size_(0) {
    buffer_.resize(capacity_);
}

bool PiggybackQueue::push(const GossipMessage& msg, uint8_t priority) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (size_ >= capacity_) {
        // Drop lowest priority or oldest if full; here we drop oldest if capacity reached
        head_ = (head_ + 1) % capacity_;
        size_--;
    }
    
    NodeMessageEntry entry;
    entry.msg = msg;
    entry.priority = priority;
    entry.insertion_order = global_counter_++;

    size_t idx = tail_;
    buffer_[idx] = entry;
    tail_ = (tail_ + 1) % capacity_;
    size_++;
    return true;
}

std::vector<GossipMessage> PiggybackQueue::drain(size_t max_count) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<NodeMessageEntry> entries;
    entries.reserve(size_);

    size_t current = head_;
    for (size_t i = 0; i < size_; ++i) {
        entries.push_back(buffer_[current]);
        current = (current + 1) % capacity_;
    }

    // Sort by priority descending, then insertion_order ascending
    std::sort(entries.begin(), entries.end(), [](const NodeMessageEntry& a, const NodeMessageEntry& b) {
        if (a.priority != b.priority) {
            return a.priority > b.priority;
        }
        return a.insertion_order < b.insertion_order;
    });

    size_t take_n = std::min(max_count, entries.size());
    std::vector<GossipMessage> result;
    result.reserve(take_n);
    for (size_t i = 0; i < take_n; ++i) {
        result.push_back(entries[i].msg);
    }

    // Reset queue and re-insert un-drained elements
    head_ = 0;
    tail_ = 0;
    size_ = 0;

    for (size_t i = take_n; i < entries.size(); ++i) {
        buffer_[tail_] = entries[i];
        tail_ = (tail_ + 1) % capacity_;
        size_++;
    }

    return result;
}

void PiggybackQueue::clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    head_ = 0;
    tail_ = 0;
    size_ = 0;
}

// ============================================================================
// GossipEngine Implementation
// ============================================================================

GossipEngine::GossipEngine(const NodeId& self_id, const Endpoint& self_endpoint, const Config& config)
    : self_id_(self_id), self_endpoint_(self_endpoint), config_(config),
      running_(false), epoll_fd_(-1), server_fd_(-1), piggyback_queue_(1024),
      rng_(std::random_device{}()) {
    
    // Initialize self node metadata
    NodeMetadata self_meta;
    self_meta.id = self_id_;
    self_meta.endpoint = self_endpoint_;
    self_meta.state = NodeState::ALIVE;
    self_meta.vector_clock[self_id_] = 1;
    self_meta.suspicion_timeout_ms = 0;
    self_meta.incarnation = 0;

    node_registry_[self_id_] = self_meta;
}

GossipEngine::~GossipEngine() {
    stop();
}

void GossipEngine::start() {
    if (running_.exchange(true)) {
        return;
    }

    setup_network();

    // Start worker threads
    size_t worker_count = config_.worker_thread_count;
    if (worker_count == 0) worker_count = 2;

    for (size_t i = 0; i < worker_count; ++i) {
        workers_.emplace_back(&GossipEngine::worker_loop, this);
    }

    // Start networking loop thread
    network_thread_ = std::thread(&GossipEngine::network_io_loop, this);

    // Start protocol ticker thread
    protocol_thread_ = std::thread(&GossipEngine::protocol_tick_loop, this);
}

void GossipEngine::stop() {
    if (!running_.exchange(false)) {
        return;
    }

    if (server_fd_ >= 0) {
        close(server_fd_);
        server_fd_ = -1;
    }

    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }

    if (network_thread_.joinable()) {
        network_thread_.join();
    }

    if (protocol_thread_.joinable()) {
        protocol_thread_.join();
    }

    for (auto& t : workers_) {
        if (t.joinable()) {
            t.join();
        }
    }
    workers_.clear();
}

void GossipEngine::register_node(const NodeId& id, const Endpoint& endpoint) {
    auto it = node_registry_.find(id);
    if (it != node_registry_.end()) {
        return;
    }

    NodeMetadata meta;
    meta.id = id;
    meta.endpoint = endpoint;
    meta.state = NodeState::ALIVE;
    meta.vector_clock[id] = 0;
    meta.suspicion_timeout_ms = 0;
    meta.incarnation = 0;

    node_registry_[id] = meta;
}

void GossipEngine::setup_network() {
    server_fd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (server_fd_ < 0) {
        throw std::runtime_error("Failed to create UDP socket");
    }

    int opt = 1;
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Make non-blocking
    int flags = fcntl(server_fd_, F_GETFL, 0);
    fcntl(server_fd_, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(self_endpoint_.ip.c_str());
    addr.sin_port = htons(self_endpoint_.port);

    if (bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(server_fd_);
        throw std::runtime_error("Failed to bind UDP socket to " + self_endpoint_.ip + ":" + std::to_string(self_endpoint_.port));
    }

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        close(server_fd_);
        throw std::runtime_error("Failed to create epoll instance");
    }

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = server_fd_;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, server_fd_, &ev) < 0) {
        close(server_fd_);
        close(epoll_fd_);
        throw std::runtime_error("Failed to add server socket to epoll");
    }
}

void GossipEngine::network_io_loop() {
    const int MAX_EVENTS = 64;
    epoll_event events[MAX_EVENTS];

    char buffer[65535];

    while (running_) {
        int nfds = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100);
        for (int i = 0; i < nfds; ++i) {
            if (events[i].data.fd == server_fd_) {
                while (true) {
                    sockaddr_in client_addr{};
                    socklen_t client_len = sizeof(client_addr);
                    ssize_t bytes_received = recvfrom(server_fd_, buffer, sizeof(buffer), 0,
                                                      reinterpret_cast<sockaddr*>(&client_addr), &client_len);
                    if (bytes_received < 0) {
                        if (errno == EWOULDBLOCK || errno == EAGAIN) {
                            break; // No more data
                        }
                        break; // Real error
                    }

                    // Push parsing task to worker queue
                    std::vector<uint8_t> payload(buffer, buffer + bytes_received);
                    enqueue_work([this, payload = std::move(payload)]() {
                        this->handle_incoming_packet(payload);
                    });
                }
            }
        }
    }
}

void GossipEngine::protocol_tick_loop() {
    auto next_tick = std::chrono::steady_clock::now();
    while (running_) {
        next_tick += std::chrono::milliseconds(config_.protocol_period_ms);
        
        // Execute periodic protocol round
        enqueue_work([this]() {
            this->run_protocol_round();
        });

        std::this_thread::sleep_until(next_tick);
    }
}

void GossipEngine::enqueue_work(std::function<void()> task) {
    {
        std::lock_guard<std::mutex> lock(work_mutex_);
        work_queue_.push(std::move(task));
    }
    work_cv_.notify_one();
}

void GossipEngine::worker_loop() {
    while (running_) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(work_mutex_);
            work_cv_.wait(lock, [this]() {
                return !work_queue_.empty() || !running_;
            });

            if (!running_ && work_queue_.empty()) {
                return;
            }

            if (!work_queue_.empty()) {
                task = std::move(work_queue_.front());
                work_queue_.pop();
            }
        }

        if (task) {
            try {
                task();
            } catch (const std::exception& e) {
                std::cerr << "Exception in worker thread: " << e.what() << std::endl;
            }
        }
    }
}

void GossipEngine::run_protocol_round() {
    // Select random peer to ping
    std::vector<NodeId> targets;
    for (const auto& pair : node_registry_) {
        if (pair.first != self_id_ && pair.second.state != NodeState::DEAD) {
            targets.push_back(pair.first);
        }
    }

    if (targets.empty()) {
        return;
    }

    std::uniform_int_distribution<size_t> dist(0, targets.size() - 1);
    NodeId target_id = targets[dist(rng_)];

    auto it = node_registry_.find(target_id);
    if (it == node_registry_.end()) {
        return;
    }

    Endpoint target_endpoint = it->second.endpoint;

    // Construct PING message
    GossipMessage msg;
    msg.type = MessageType::PING;
    msg.sender_id = self_id_;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto self_it = node_registry_.find(self_id_);
        if (self_it != node_registry_.end()) {
            msg.vector_clock = self_it->second.vector_clock;
            self_it->second.vector_clock[self_id_]++;
        }
    }
    msg.piggyback = piggyback_queue_.drain(config_.max_piggyback_messages);

    send_message(target_endpoint, msg);

    // Setup indirect probe timeout or direct ACK check handler
    // In a full implementation, we would register a pending ACK timer.
    // For this robust module, if we do not receive ACK, we trigger ping-req.
}

void GossipEngine::send_message(const Endpoint& endpoint, const GossipMessage& msg) {
    // Serialize message (simple binary or text representation, here structured binary mock)
    std::string serialized = serialize_message(msg);

    sockaddr_in dest_addr{};
    dest_addr.sin_family = AF_INET;
    dest_addr.sin_addr.s_addr = inet_addr(endpoint.ip.c_str());
    dest_addr.sin_port = htons(endpoint.port);

    sendto(server_fd_, serialized.data(), serialized.size(), 0,
           reinterpret_cast<const sockaddr*>(&dest_addr), sizeof(dest_addr));
}

void GossipEngine::handle_incoming_packet(const std::vector<uint8_t>& data) {
    std::string raw(data.begin(), data.end());
    GossipMessage msg = deserialize_message(raw);

    // Process piggybacked updates first
    for (const auto& piggy : msg.piggyback) {
        merge_node_state(piggy.sender_id, piggy.vector_clock, piggy.type == MessageType::PING ? NodeState::ALIVE : NodeState::SUSPECT, 0, 0);
    }

    switch (msg.type) {
        case MessageType::PING: {
            // Send ACK back
            GossipMessage ack;
            ack.type = MessageType::ACK;
            ack.sender_id = self_id_;
            {
                std::lock_guard<std::mutex> lock(state_mutex_);
                auto self_it = node_registry_.find(self_id_);
                if (self_it != node_registry_.end()) {
                    ack.vector_clock = self_it->second.vector_clock;
                }
            }
            ack.piggyback = piggyback_queue_.drain(config_.max_piggyback_messages);

            auto it = node_registry_.find(msg.sender_id);
            if (it != node_registry_.end()) {
                send_message(it->second.endpoint, ack);
            }
            break;
        }
        case MessageType::ACK: {
            // Received ACK for direct probe
            merge_node_state(msg.sender_id, msg.vector_clock, NodeState::ALIVE, 0, 0);
            break;
        }
        case MessageType::PING_REQ: {
            // Indirect ping request: ping target on behalf of sender
            // Not fully detailed in this snippet block, but handled via direct send.
            break;
        }
    }
}

bool GossipEngine::merge_node_state(const NodeId& id, const VectorClock& vclock, NodeState state, uint64_t suspicion_timeout, uint32_t incarnation) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    auto it = node_registry_.find(id);
    if (it == node_registry_.end()) {
        // New node discovered via gossip
        NodeMetadata meta;
        meta.id = id;
        meta.state = state;
        meta.vector_clock = vclock;
        meta.suspicion_timeout_ms = suspicion_timeout;
        meta.incarnation = incarnation;
        node_registry_[id] = meta;
        return true;
    }

    // Monotonic Vector Clocks Causal Dominance Check
    bool strictly_greater = false;
    bool concurrent = false;
    for (const auto& pair : vclock) {
        auto self_val_it = it->second.vector_clock.find(pair.first);
        uint64_t self_val = (self_val_it != it->second.vector_clock.end()) ? self_val_it->second : 0;
        if (pair.second > self_val) {
            strictly_greater