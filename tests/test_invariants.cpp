#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <unordered_set>
#include <shared_mutex>
#include <queue>
#include <cassert>
#include <algorithm>
#include <cstring>

// ==========================================
// MOCK IMPLEMENTATION OF SWIM MESH ENGINE
// ==========================================

namespace Swim {

enum class NodeState {
    Alive,
    Suspect,
    Dead,
    Left
};

struct NodeId {
    uint32_t ip;
    uint16_t port;

    bool operator==(const NodeId& o) const {
        return ip == o.ip && port == o.port;
    }
    bool operator<(const NodeId& o) const {
        return ip < o.ip || (ip == o.ip && port < o.port);
    }
};

struct NodeIdHash {
    size_t operator()(const NodeId& id) const {
        return std::hash<uint32_t>()(id.ip) ^ (std::hash<uint16_t>()(id.port) << 1);
    }
};

struct Member {
    NodeId id;
    NodeState state;
    uint64_t incarnation;
    std::chrono::steady_clock::time_point last_state_change;
};

class MeshEngine {
private:
    NodeId self_;
    mutable std::shared_mutex mutex_;
    std::unordered_map<NodeId, Member, NodeIdHash> members_;
    std::atomic<uint64_t> incarnation_{0};
    std::atomic<bool> running_{false};

public:
    explicit MeshEngine(NodeId self) : self_(self) {
        members_[self] = Member{self, NodeState::Alive, 0, std::chrono::steady_clock::now()};
    }

    ~MeshEngine() {
        stop();
    }

    void start() {
        running_ = true;
    }

    void stop() {
        running_ = false;
    }

    bool is_running() const {
        return running_;
    }

    NodeId self() const {
        return self_;
    }

    bool upsert_member(const NodeId& id, NodeState state, uint64_t incarnation) {
        std::unique_lock lock(mutex_);
        if (id == self_) return false; // Cannot override self via external upsert directly without local inc bump

        auto it = members_.find(id);
        if (it == members_.end()) {
            members_[id] = Member{id, state, incarnation, std::chrono::steady_clock::now()};
            return true;
        }

        // Conflict resolution: higher incarnation wins. If equal, Alive > Suspect > Dead > Left
        if (incarnation > it->second.incarnation) {
            it->second.incarnation = incarnation;
            it->second.state = state;
            it->second.last_state_change = std::chrono::steady_clock::now();
            return true;
        } else if (incarnation == it->second.incarnation) {
            if (state == NodeState::Alive && it->second.state != NodeState::Alive) {
                it->second.state = state;
                it->second.last_state_change = std::chrono::steady_clock::now();
                return true;
            }
        }
        return false;
    }

    void set_node_suspect(const NodeId& id) {
        std::unique_lock lock(mutex_);
        auto it = members_.find(id);
        if (it != members_.end() && it->second.state == NodeState::Alive) {
            it->second.state = NodeState::Suspect;
            it->second.last_state_change = std::chrono::steady_clock::now();
        }
    }

    void set_node_dead(const NodeId& id) {
        std::unique_lock lock(mutex_);
        auto it = members_.find(id);
        if (it != members_.end() && it->second.state != NodeState::Dead && it->second.state != NodeState::Left) {
            it->second.state = NodeState::Dead;
            it->second.last_state_change = std::chrono::steady_clock::now();
        }
    }

    void local_incarnation_bump() {
        std::unique_lock lock(mutex_);
        incarnation_++;
        members_[self_].incarnation = incarnation_;
        members_[self_].state = NodeState::Alive;
        members_[self_].last_state_change = std::chrono::steady_clock::now();
    }

    std::vector<Member> get_members() const {
        std::shared_lock lock(mutex_);
        std::vector<Member> result;
        result.reserve(members_.size());
        for (const auto& pair : members_) {
            result.push_back(pair.second);
        }
        return result;
    }

    size_t member_count() const {
        std::shared_lock lock(mutex_);
        return members_.size();
    }

    bool get_member(const NodeId& id, Member& out_member) const {
        std::shared_lock lock(mutex_);
        auto it = members_.find(id);
        if (it != members_.end()) {
            out_member = it->second;
            return true;
        }
        return false;
    }

    // Invariant check across internal data structures
    bool validate_invariants() const {
        std::shared_lock lock(mutex_);
        // 1. Self must always exist and be Alive
        auto self_it = members_.find(self_);
        if (self_it == members_.end()) return false;
        if (self_it->second.state != NodeState::Alive) return false;
        if (self_it->second.incarnation != incarnation_) return false;

        // 2. No duplicate IDs (guaranteed by unordered_map)
        // 3. Check logical consistency of states
        for (const auto& [id, member] : members_) {
            if (id.ip == 0 || id.port == 0) return false; // Invalid endpoints
        }
        return true;
    }
};

} // namespace Swim

// ==========================================
// TEST SUITE IMPLEMENTATION
// ==========================================

class TestSuite {
private:
    int passed_tests_ = 0;
    int failed_tests_ = 0;

    void assert_true(bool condition, const std::string& name) {
        if (condition) {
            passed_tests_++;
            std::cout << "[PASS] " << name << "\n";
        } else {
            failed_tests_++;
            std::cerr << "[FAIL] " << name << "\n";
            std::abort();
        }
    }

public:
    void run_all_tests() {
        std::cout << "=== STARTING EXHAUSTIVE SWIM MESH ENGINE TEST SUITE ===\n";
        
        test_unit_initialization();
        test_unit_state_transitions();
        test_unit_conflict_resolution();
        test_invariants();
        test_concurrent_contention();
        test_failure_and_recovery();

        std::cout << "=== TEST SUITE COMPLETE ===\n";
        std::cout << "Passed: " << passed_tests_ << "\n";
        std::cout << "Failed: " << failed_tests_ << "\n";
    }

private:
    void test_unit_initialization() {
        Swim::NodeId self{0x7F000001, 8000};
        Swim::MeshEngine engine(self);

        assert_true(engine.is_running() == false, "Engine should not be running upon construction");
        engine.start();
        assert_true(engine.is_running() == true, "Engine should run after start()");
        assert_true(engine.member_count() == 1, "Engine should initialize with exactly self member");
        
        Swim::Member m;
        assert_true(engine.get_member(self, m), "Self member must be retrievable");
        assert_true(m.state == Swim::NodeState::Alive, "Self member must start Alive");
        assert_true(m.incarnation == 0, "Initial incarnation must be 0");
        
        engine.stop();
        assert_true(engine.is_running() == false, "Engine should stop after stop()");
    }

    void test_unit_state_transitions() {
        Swim::NodeId self{0x7F000001, 8000};
        Swim::NodeId peer{0x7F000001, 8001};
        Swim::MeshEngine engine(self);

        // Add peer
        bool added = engine.upsert_member(peer, Swim::NodeState::Alive, 1);
        assert_true(added, "Peer should be added successfully");
        assert_true(engine.member_count() == 2, "Member count should be 2");

        // Transition: Alive -> Suspect
        engine.set_node_suspect(peer);
        Swim::Member m;
        engine.get_member(peer, m);
        assert_true(m.state == Swim::NodeState::Suspect, "Peer state must transition to Suspect");

        // Transition: Suspect -> Dead
        engine.set_node_dead(peer);
        engine.get_member(peer, m);
        assert_true(m.state == Swim::NodeState::Dead, "Peer state must transition to Dead");

        // Edge case: Dead -> Alive via lower incarnation should fail or be ignored
        bool updated_lower = engine.upsert_member(peer, Swim::NodeState::Alive, 1);
        engine.get_member(peer, m);
        assert_true(m.state == Swim::NodeState::Dead, "Dead node should not revert to Alive with equal/lower incarnation without override rules");
    }

    void test_unit_conflict_resolution() {
        Swim::NodeId self{0x7F000001, 8000};
        Swim::NodeId peer{0x7F000001, 8002};
        Swim::MeshEngine engine(self);

        engine.upsert_member(peer, Swim::NodeState::Alive, 5);
        Swim::Member m;
        engine.get_member(peer, m);
        assert_true(m.incarnation == 5, "Peer incarnation should be 5");

        // Lower incarnation update should be rejected
        engine.upsert_member(peer, Swim::NodeState::Dead, 3);
        engine.get_member(peer, m);
        assert_true(m.state == Swim::NodeState::Alive, "Lower incarnation update must not change state");

        // Higher incarnation update should be accepted
        engine.upsert_member(peer, Swim::NodeState::Suspect, 6);
        engine.get_member(peer, m);
        assert_true(m.state == Swim::NodeState::Suspect, "Higher incarnation update must be accepted");
        assert_true(m.incarnation == 6, "Incarnation must update to 6");
    }

    void test_invariants() {
        Swim::NodeId self{0x7F000001, 8000};
        Swim::MeshEngine engine(self);

        assert_true(engine.validate_invariants(), "Initial invariants must hold");

        engine.upsert_member({0x7F000001, 8003}, Swim::NodeState::Alive, 1);
        engine.upsert_member({0x7F000001, 8004}, Swim::NodeState::Suspect, 2);
        engine.upsert_member({0x7F000001, 8005}, Swim::NodeState::Dead, 3);

        assert_true(engine.validate_invariants(), "Invariants must hold after adding various states");

        engine.local_incarnation_bump();
        assert_true(engine.validate_invariants(), "Invariants must hold after local incarnation bump");
    }

    void test_concurrent_contention() {
        Swim::NodeId self{0x7F000001, 8000};
        Swim::MeshEngine engine(self);
        engine.start();

        const int num_threads = 16;
        const int iterations