#include <iostream>
#include <chrono>
#include <vector>
#include <numeric>
#include <algorithm>
#include <atomic>
#include <thread>
#include <iomanip>

// Mocking the 'swim-mesh-engine' structures and operations for a self-contained microbenchmark.
// In a real integration, include the actual SWIM protocol headers.
namespace swim {
    enum class MessageType { PING, ACK, PING_REQ, INDIRECT_ACK };

    struct NodeId {
        uint64_t id;
        bool operator==(const NodeId& other) const { return id == other.id; }
    };

    struct Message {
        MessageType type;
        NodeId sender;
        NodeId target;
        uint62_t sequence_number;
    };

    class MeshEngine {
    public:
        MeshEngine(NodeId self) : self_(self), seq_(0) {}

        // Simulates handling an incoming ping or state update (the core transaction)
        void process_message(const Message& msg) {
            // Simulate minimal work: state lookup / membership update logic
            if (msg.type == MessageType::PING) {
                // Generate ACK response internally (no-op for benchmark payload)
                volatile uint64_t dummy = msg.sequence_number ^ self_.id;
                (void)dummy;
            }
        }

        Message create_ping(const NodeId& target) {
            return Message{MessageType::PING, self_, target, seq_++};
        }

    private:
        NodeId self_;
        uint32_t seq_;
    }
}

int main() {
    using namespace std::chrono;

    constexpr size_t WARMUP_ITERATIONS = 50000;
    constexpr size_t BENCHMARK_ITERATIONS = 1000000;

    swim::NodeId self{1};
    swim::NodeId peer{2};
    swim::MeshEngine engine(self);

    // Prepare a standard message payload
    swim::Message ping_msg = engine.create_ping(peer);

    // Warmup phase to stabilize branch predictors, caches, and frequency scaling
    for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
        engine.process_message(ping_msg);
    }

    // Latency collection vector
    std::vector<double> latencies_ns;
    latencies_ns.reserve(BENCHMARK_ITERATIONS);

    // High-precision timing loop
    auto start_total = high_resolution_clock::now();

    for (size_t i = 0; i < BENCHMARK_ITERATIONS; ++i) {
        auto start_op = high_resolution_clock::now();
        
        engine.process_message(ping_msg);

        auto end_op = high_resolution_clock::now();
        double duration_ns = duration_cast<nanoseconds>(end_op - start_op).count();
        latencies_ns.push_back(duration_ns);
    }

    auto end_total = high_resolution_clock::now();
    double total_seconds = duration_cast<duration<double>>(end_total - start_total).count();

    // Compute metrics
    double ops_sec = static_cast<double>(BENCHMARK_ITERATIONS) / total_seconds;

    // Sort latencies for percentile calculations
    std::sort(latencies_ns.begin(), latencies_ns.end());

    double p50 = latencies_ns[static_cast<size_t>(BENCHMARK_ITERATIONS * 0.50)];
    double p90 = latencies_ns[static_cast<size_t>(BENCHMARK_ITERATIONS * 0.90)];
    double p99 = latencies_ns[static_cast<size_t>(BENCHMARK_ITERATIONS * 0.99)];
    double p999 = latencies_ns[static_cast<size_t>(BENCHMARK_ITERATIONS * 0.999)];

    // Format and output metrics to stdout strictly as requested
    std::cout << "\n=================================================================\n";
    std::cout << "         SWIM Mesh Engine - Throughput & Latency Benchmark       \n";
    std::cout << "=================================================================\n";
    std::cout << std::left << std::setw(25) << "Metric" << std::right << std::setw(20) << "Value" << "\n";
    std::cout << "-----------------------------------------------------------------\n";
    std::cout << std::left << std::setw(25) << "Total Iterations" << std::right << std::setw(20) << BENCHMARK_ITERATIONS << "\n";
    std::cout << std::left << std::setw(25) << "Throughput (ops/sec)" << std::right << std::setw(20) << static_cast<uint64_t>(ops_sec) << "\n";
    std::cout << std::left << std::setw(25) << "Latency p50 (ns)" << std::right << std::setw(20) << p50 << "\n";
    std::cout << std::left << std::setw(25) << "Latency p90 (ns)" << std::right << std::setw(20) << p90 << "\n";
    std::cout << std::left << std::setw(25) << "Latency p99 (ns)" << std::right << std::setw(20) << p99 << "\n";
    std::cout << std::left << std::setw(25) << "Latency p99.9 (ns)" << std::right << std::setw(20) << p999 << "\n";
    std::cout << "=================================================================\n";

    return 0;
}