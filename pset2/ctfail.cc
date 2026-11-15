#include "cotamer.hh"
#include "netsim.hh"
#include "ctconsensus_msgs.hh"
#include <list>
#include <print>
#include <getopt.h>

namespace cot = cotamer;
using namespace std::chrono_literals;
using namespace netsim;


namespace ctconsensus {

// The message definitions for Chandra-Toueg consensus are in
// `ctconsensus_msgs.hh`.

using network_type = netsim::network<message>;
using port_type = netsim::port<message>;

// the ID of the distinguished server that receives final decisions
constexpr int nancy_id = -1;


// server
//    Represents the state for a consensus server. The consensus algorithm
//    is in the `consensus()` function and its helpers. Communicates with
//    other servers over `net_`, the network.

struct server {
    server(int id, int N, network_type& net, std::string color)
        : id_(id), N_(N), net_(net), my_port_(net.input(id_)), color_(color) {
    }

    cot::task<> consensus();

private:
    // set at initialization
    int id_;                    // my ID; in [0, N)
    int N_;                     // number of servers
    network_type& net_;         // the network
    port_type& my_port_;        // my input interface
    std::string color_;         // current color

    // initial values
    uint64_t round_ = 1;
    uint64_t color_round_ = 0;

    // message stash (messages from current or future round
    // to be delivered later)
    std::deque<message> stash_;

    // set when we decide
    bool decided_ = false;

    cot::event failure_detector(int leader);
    inline message pop_stash() {
        auto m = stash_.front();
        stash_.pop_front();
        return m;
    }
    cot::task<message> receive(message_type mt);
};


// fail a server by failing all of its outbound links after `delay`
cot::task<> fail_server_after(ctconsensus::network_type& net,
                              int i /* server to fail */,
                              int N /* total number of servers */,
                              cot::clock::duration delay) {
    assert(i >= 0 && i < N);
    co_await cot::after(delay);
    for (int j = -1 /* Nancy */; j != N; ++j) {
        net.link(i, j).fail();       // fail() is for you to implement
    }
}


// Return a failure detector for the server with ID `leader`.
// The handout failure detector is a simple timeout. 10% of the time the
// detector misses the failure entirely and waits for 15 minutes.

cot::event server::failure_detector(int leader) {
    (void) leader;
    if (net_.coin_flip(0.10)) {
        return cot::after(15min);
    }
    return cot::after(1500ms);
}


// Receive a message of a specific type for the current round.
// Ignore old messages, except for DECIDE messages.

cot::task<message> server::receive(message_type mt) {
    size_t stash_size = stash_.size();

    while (!decided_) {
        // Process the stash first (not including re-stashed messages),
        // then receive from the network
        message m;
        if (stash_size > 0) {
            --stash_size;
            m = pop_stash();
        } else {
            m = co_await my_port_.receive();
        }

        // DECIDE messages cause us to decide
        if (m.type == m_decide) {
            color_ = m.color;
            decided_ = true;
            break;
        }

        // ignore or stash unwanted messages
        if (m.type != mt || m.round != round_) {
            if (m.round >= round_) {
                stash_.push_back(m);
            }
            continue;
        }

        // return wanted message
        co_return m;
    }

    // If we’ve already decided, then return a fake message of the
    // expected type. This simplifies coding.
    co_return message{mt, round_, color_, color_round_, true};
}


// The main Chandra-Toueg consensus algorithm!

cot::task<> server::consensus() {
    while (!decided_) {
        // Compute leader of current round
        int leader = round_ % N_;

        // Phase 1: Send PREPARE to leader
        co_await net_.link(id_, leader).send(
            prepare_message(round_, color_, color_round_)
        );

        // Phase 2: Leader waits to receive >N/2 PREPAREs, tracking latest color
        // & round
        if (id_ == leader) {
            int received_prepare = 0;
            while (received_prepare <= N_ / 2) {
                auto m = co_await receive(m_prepare);
                ++received_prepare;
                // maybe update color
                if (m.color_round > color_round_) {
                    color_ = m.color;
                    color_round_ = m.color_round;
                }
            }
            if (decided_) {
                // received a delayed DECIDE from a prior round; exit &
                // rebroadcast the DECIDE.
                break;
            }
        }

        // Phase 3: Leader sends PROPOSE to all
        if (id_ == leader) {
            auto propose = propose_message(round_, color_);
            for (int j = 0; j != N_; ++j) {
                co_await net_.link(id_, j).send(propose);
            }
        }

        // Phase 4: Wait for either leader failure
        // or PROPOSE from leader
        std::optional<message> maybe_propose =
            co_await cot::attempt(receive(m_propose),
                                  failure_detector(leader));
        if (decided_) {
            break;
        } else if (maybe_propose) {
            color_ = maybe_propose->color;
            color_round_ = round_;
        }
        co_await net_.link(id_, leader).send(
            ack_message(round_, (bool) maybe_propose)
        );

        // Phase 5: Leader waits for ACKs
        if (id_ == leader) {
            int success = 0, total = 0;
            while (total <= N_ / 2) {
                auto m = co_await receive(m_ack);
                if (m.ack) {
                    ++success;
                }
                ++total;
            }
            if (decided_) {
                break;
            } else if (success > N_ / 2) {
                // A majority acknowledged our color! Time to decide.
                decided_ = true;
                break;
            }
        }

        // Phase 6: Advance the round and continue
        ++round_;

        co_await cot::after(10ms);  // brief delay before next round
    }

    // We have decided!
    auto decide = decide_message(color_);
    // send DECIDE to Nancy
    co_await net_.link(id_, nancy_id).send(decide);
    // send DECIDE to everyone else
    for (int j = 0; j != N_; ++j) {
        if (j != id_) {
            co_await net_.link(id_, j).send(decide);
        }
    }
}


// Nancy is a distinguished observer that collects DECIDE messages
// and validates that (1) all servers agree on the same color, and
// (2) the consensus color is valid for this initialization.

bool nancy_approves = false;
bool nancy_be_quiet = false;

cot::task<> nancy_is_impatient();

cot::task<> nancy(port<message>& my_port, int N, std::string required_consensus) {
    int received = 0;
    std::string consensus;

    // Nancy initially disapproves.
    nancy_approves = false;

    // It's an error if we don't achieve consensus in 15 minutes.
    nancy_is_impatient().detach();

    // Wait until we get at least N/2 messages, and then a little longer
    // (in case more messages come in).
    cot::event stopper;
    while (!stopper.triggered()) {
        auto m = co_await cot::attempt(my_port.receive(), stopper);

        if (!m) { // timeout
            break;
        }

        if (m->type != m_decide
            || (m->color != "red" && m->color != "blue")) {
            std::print("*** ERROR! *** Nancy received unexpected \"{}\"\n", *m);
            goto done;
        }

        if ((!required_consensus.empty() && m->color != required_consensus)
            || (!consensus.empty() && m->color != consensus)) {
            std::print("*** CONSENSUS ERROR! *** Nancy received \"{}\"\n", *m);
            goto done;
        }

        consensus = m->color;

        ++received;
        // We've got more than N/2 messages! Set `stopper` to go off after 10s.
        if (received > N/2 && stopper.empty()) {
            stopper = cot::after(10s);
        }
    }

    if (received <= N/2) {
        std::print("*** ERROR! *** Nancy terminated before consensus\n");
        goto done;
    }

    if (!nancy_be_quiet) {
        std::print("*** CONSENSUS ACHIEVED *** {} x \"{}\"\n", received, consensus);
    }
    nancy_approves = true;

done:
    // cancel all outstanding tasks and end the event loop
    cot::clear();
}

cot::task<> nancy_is_impatient() {
    co_await cot::after(15min);
    std::print("*** ERROR! *** 15 minutes of virtual time without consensus\n");
    cot::clear();
}

} // namespace ctconsensus



// Main entry point

static int N = 3;

static bool try_one_seed(ctconsensus::network_type& net,
                         std::optional<unsigned long> seed) {
    net.clear();    // delete old network (might trigger some events)
    cot::reset();   // clear old events and coroutines

    if (seed) {
        net.randomness().seed(*seed);
    }

    // start N servers, each with a random initial color
    std::list<ctconsensus::server> servers;
    std::string required_consensus;
    for (int i = 0; i != N; ++i) {
        std::string color = net.uniform({"red", "blue"});
        if (i == 0) {
            required_consensus = color;
        } else if (required_consensus != color) {
            required_consensus = "";
        }
        servers.emplace_back(i, N, net, color);
        servers.back().consensus().detach();
    }

    // Fail at most N/2 - 1 servers (a strict minority) at some random point
    // in the sequence. Each candidate fails with a fixed probability; we stop
    // once we've already failed N/2 - 1 servers so a majority stays alive.
    {
        int max_failures = std::max(0, N / 2 - 1);
        int failed = 0;
        for (int i = 0; i != N && failed < max_failures; ++i) {
            if (net.coin_flip(0.3)) {   // probability a server fails
                ++failed;
                cot::clock::duration delay = 3ms + net.exponential(150ms);
                ctconsensus::fail_server_after(net, i, N, delay).detach();
            }
        }
    }

    // start Nancy, who collects DECIDE messages and validates them
    ctconsensus::nancy(net.input(ctconsensus::nancy_id), N, required_consensus)
        .detach();

    cot::loop();

    return ctconsensus::nancy_approves;
}


static struct option options[] = {
    { "count", required_argument, nullptr, 'n' },
    { "seed", required_argument, nullptr, 'S' },
    { "random-seeds", required_argument, nullptr, 'R' },
    { "verbose", no_argument, nullptr, 'V' },
    { "quiet", no_argument, nullptr, 'q' },
    { nullptr, 0, nullptr, 0 }
};

int main(int argc, char* argv[]) {
    // Declare the network
    network<ctconsensus::message> net;

    // Read program options: `-n N` sets the number of servers, `-S SEED` sets
    // the desired random seed, and `-R COUNT` runs COUNT times with different
    // random seeds, exiting on the first problem.
    // Add more options by extending the `options` structure.
    std::optional<unsigned long> first_seed;
    unsigned long seed_count = 0;

    auto shortopts = short_options_for(options);
    int ch;
    while ((ch = getopt_long(argc, argv, shortopts.c_str(), options, nullptr)) != -1) {
        if (ch == 'n') {
            N = from_str_chars<int>(optarg);
            if (N <= 2) {
                throw std::invalid_argument("`-n` must be 3 or more");
            }
        } else if (ch == 'S') {
            first_seed = from_str_chars<unsigned long>(optarg);
        } else if (ch == 'R') {
            seed_count = from_str_chars<unsigned long>(optarg);
        } else if (ch == 'V') {
            net.set_verbose(true);
        } else if (ch == 'q') {
            ctconsensus::nancy_be_quiet = true;
        } else {
            std::print(std::cerr, "Unknown option\n");
            return 1;
        }
    }

    bool ok;
    if (seed_count > 0) {
        std::mt19937_64 seed_generator = randomly_seeded<std::mt19937_64>();
        for (unsigned long i = 0; i != seed_count; ++i) {
            if (i > 0 && i % 1000 == 0 && ctconsensus::nancy_be_quiet) {
                std::print(std::cerr, ".");
            }
            unsigned long seed = seed_generator();
            ok = try_one_seed(net, seed);
            if (!ok) {
                std::print(std::cerr, "*** FAILURE on seed {}\n", seed);
                break;
            }
        }
        if (ok && seed_count >= 1000 && ctconsensus::nancy_be_quiet) {
            std::print(std::cerr, "\n");
        }
    } else {
        ok = try_one_seed(net, first_seed);
    }
    return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}