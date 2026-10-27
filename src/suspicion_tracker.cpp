#include "suspicion_tracker.hpp"
#include <chrono>
#include <algorithm>
#include <stdexcept>
#include <system_error>

namespace swim {

SuspicionTracker::SuspicionTracker(
    std::shared_ptr<NodeRegistry> registry,
    std::shared_ptr<PiggybackQueue> piggyback_queue,
    Config config)
    : registry_(std::move(registry)),
      piggyback_queue_(std::move(piggyback_queue)),
      config_(std::move(config)),
      running_(false) {
    if (!registry_) {
        throw std::invalid_argument("NodeRegistry cannot be null");
    }
    if (!piggyback_queue_) {
        throw std::invalid_argument("PiggybackQueue cannot be null");
    }
}

SuspicionTracker::~SuspicionTracker() {
    stop();
}

void SuspicionTracker::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        return; // Already running
    }
    worker_thread_ = std::thread(&SuspicionTracker::supervision_loop, this);
}

void SuspicionTracker::stop() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false, std::memory_order_acq_rel)) {
        return; // Not running
    }
    cv_.notify_all();
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

bool SuspicionTracker::handle_node_unresponsive(NodeId id) {
    auto node_opt = registry_->get(id);
    if (!node_opt.has_value()) {
        return false;
    }

    NodeMetadata current = node_opt.value();
    
    // Attempt state transition from Alive -> Suspect
    while (current.state == NodeState::Alive) {
        NodeMetadata updated = current;
        updated.state = NodeState::Suspect;
        
        // Calculate suspicion timeout dynamically based on cluster size or config
        auto now = std::chrono::steady_clock::now();
        auto timeout_duration = calculate_suspicion_timeout();
        updated.suspicion_timeout = now + timeout_duration;
        updated.vector_clock.increment(current.id);

        if (registry_->compare_exchange(current.id, current, updated)) {
            // Enqueue high-priority state transition for dissemination
            PiggybackMessage msg;
            msg.type = MessageType::StateTransition;
            msg.target_id = updated.id;
            msg.state = updated.state;
            msg.clock = updated.vector_clock;
            msg.priority = 100; // High priority for state changes

            piggyback_queue_->push(msg);
            return true;
        }
        
        // CAS failed, reload current
        auto recheck = registry_->get(id);
        if (!recheck.has_value()) {
            return false;
        }
        current = recheck.value();
    }

    return false;
}

bool SuspicionTracker::handle_ack_received(NodeId id, const VectorClock& incoming_clock) {
    auto node_opt = registry_->get(id);
    if (!node_opt.has_value()) {
        return false;
    }

    NodeMetadata current = node_opt.value();

    // If the node was Suspect or Dead, an ACK with valid causality heals it back to Alive
    if (current.state == NodeState::Suspect || current.state == NodeState::Dead) {
        NodeMetadata updated = current;
        updated.state = NodeState::Alive;
        updated.vector_clock.merge(incoming_clock);
        updated.vector_clock.increment(id);
        // Clear suspicion timeout
        updated.suspicion_timeout = std::chrono::steady_clock::time_point::max();

        if (registry_->compare_exchange(current.id, current, updated)) {
            PiggybackMessage msg;
            msg.type = MessageType::StateTransition;
            msg.target_id = updated.id;
            msg.state = updated.state;
            msg.clock = updated.vector_clock;
            msg.priority = 100;

            piggyback_queue_->push(msg);
            return true;
        }
    } else if (current.state == NodeState::Alive) {
        // Just merge vector clock if needed
        if (incoming_clock.dominates(current.vector_clock)) {
            NodeMetadata updated = current;
            updated.vector_clock.merge(incoming_clock);
            registry_->compare_exchange(current.id, current, updated);
        }
    }

    return false;
}

void SuspicionTracker::supervision_loop() {
    while (running_.load(std::memory_order_acquire)) {
        auto now = std::chrono::steady_clock::now();
        
        // Scan registry for expired suspicion timeouts
        auto all_nodes = registry_->get_all();
        for (const auto& node : all_nodes) {
            if (node.state == NodeState::Suspect) {
                if (now >= node.suspicion_timeout) {
                    transition_to_dead(node.id);
                }
            }
        }

        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, config_.sweep_interval, [this] {
            return !running_.load(std::memory_order_acquire);
        });
    }
}

void SuspicionTracker::transition_to_dead(NodeId id) {
    auto node_opt = registry_->get(id);
    if (!node_opt.has_value()) {
        return;
    }

    NodeMetadata current = node_opt.value();
    if (current.state == NodeState::Suspect) {
        NodeMetadata updated = current;
        updated.state = NodeState::Dead;
        updated.vector_clock.increment(id);

        if (registry_->compare_exchange(current.id, current, updated)) {
            PiggybackMessage msg;
            msg.type = MessageType::StateTransition;
            msg.target_id = updated.id;
            msg.state = updated.state;
            msg.clock = updated.vector_clock;
            msg.priority = 150; // Highest priority for declarations of death

            piggyback_queue_->push(msg);
        }
    }
}

std::chrono::milliseconds SuspicionTracker::calculate_suspicion_timeout() const {
    size_t cluster_size = registry_->size();
    if (cluster_size == 0) {
        return config_.base_suspicion_timeout;
    }
    
    // Scale timeout logarithmically with cluster size to bound false positives
    double scale_factor = std::log2(static_cast<double>(cluster_size + 1));
    auto calculated = static_cast<long long>(config_.base_suspicion_timeout.count() * scale_factor);
    
    return std::chrono::milliseconds(std::max(calculated, config_.base_suspicion_timeout.count()));
}

} // namespace swim