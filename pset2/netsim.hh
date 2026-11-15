#pragma once
#include "cotamer.hh"
#include <concepts>
#include <map>
#include <print>
#include "utils.hh"

// netsim.hh
//    Network simulator for messages of type T.
//
//    * channel<T> -- represents a link between two servers
//    * port<T> -- represents a receiving port on a server
//    * network<T> -- looks up channels and ports by integer ID


namespace netsim {
namespace cot = cotamer;
using namespace std::chrono_literals;
template <typename T> struct port;
template <typename T> struct network;
template <typename T> struct message_traits;

using id_type = int;       // type of server IDs


// channel<T>
//    A link from one server to another.
//
//    The critical function is `send`, which sends a message on the link to
//    the destination server. `send` is a coroutine; it returns when this
//    server (`source`) can send another message. `send` also starts a detached
//    coroutine to actually deliver the message after some delay.

template <typename T>
struct channel {
    using message_type = T;
    using message_traits_type = message_traits<T>;

    inline channel(id_type src, id_type dst, network<T>& net);
    channel(const channel<T>&) = delete;
    channel(channel<T>&&) = delete;
    channel<T>& operator=(const channel<T>&) = delete;
    channel<T>& operator=(channel<T>&&) = delete;


    id_type source() const noexcept { return from_; }
    id_type destination() const noexcept { return to_port_.id(); }

    bool verbose() const noexcept { return verbose_; }
    void set_verbose(bool verbose) noexcept { verbose_ = verbose; }

    // send a message on this channel
    cot::task<> send(message_type m);

    // permanently fail this channel: drop all future messages
    void fail() noexcept { failed_ = true; }


private:
    id_type from_;
    port<T>& to_port_;
    bool verbose_;
    bool failed_ = false;
    network<T>& net_;

    cot::clock::duration link_delay_ = 20ms; // base time for message to arrive
    cot::clock::duration send_delay_ = 1ms;  // time before sender can continue

    cot::task<> send_after(cot::clock::duration, message_type);
};


// port<T>
//    An input interface on a server.
//
//    The critical function is `receive`, which receives a message. `receive`
//    is a coroutine; it suspends processing until a message becomes available.

template <typename T>
struct port {
    using message_type = T;
    using message_traits_type = message_traits<T>;

    inline port(id_type id, network<T>& net);
    ~port() {
        // wake up any `receive` coroutines so that the driver cleanup code
        // will free their memory
        receiver_event_.trigger();
    }
    port(const port<T>&) = delete;
    port(port<T>&&) = delete;
    port<T>& operator=(const port<T>&) = delete;
    port<T>& operator=(port<T>&&) = delete;


    id_type id() const noexcept { return id_; }

    bool verbose() const noexcept { return verbose_; }
    void set_verbose(bool verbose) noexcept { verbose_ = verbose; }

    // receive a message on this port
    cot::task<T> receive();


private:
    friend struct channel<T>;

    id_type id_;
    bool verbose_;
    network<T>& net_;

    cot::clock::duration receive_delay_ = 5ms;  // time before receiver can continue
    std::deque<message_type> messageq_;
    cot::event receiver_event_;
};


// The central coroutines
// ======================

// channel<T>::send(m)
//    Send a message from source() to destination().

template <typename T>
cot::task<> channel<T>::send(message_type m) {
    if (verbose_) {
        std::print("{}: {} → {} \"{}\"\n", cot::now(), source(), destination(),
                   message_traits_type::print_transform(m));
    }

    // after a randomized delay, place the message in the receiver’s queue;
    // delay is a base plus an exponentially-distributed extra amount
    auto delay = link_delay_ + net_.exponential(link_delay_ / 2);
    send_after(delay, std::move(m)).detach();

    // sending a message takes time
    co_await cot::after(send_delay_);
}


// channel<T>::send_after(delay, m)
//    Delay for `delay`, then enqueue `m` on the destination port.

template <typename T>
cot::task<> channel<T>::send_after(cot::clock::duration delay, message_type m) {
    co_await cot::after(delay);

    // if this link has failed, drop the message
    if (failed_) {
        co_return;
    }

    // record this message in the destination message queue
    to_port_.messageq_.emplace_back(std::move(m));

    // wake up a blocked receiver
    to_port_.receiver_event_.trigger();
}


// port<T>::receive()
//    Suspend until a message is available, then dequeue and return it.

template <typename T>
cot::task<T> port<T>::receive() {
    // sleep until there’s a message
    while (messageq_.empty()) {
        // Register an event that senders will trigger on delivery.
        // Need a new one every time because events are one-shot.
        co_await (receiver_event_ = cot::event());
    }

    auto m = std::move(messageq_.front());
    messageq_.pop_front();

    if (verbose_) {
        std::print("{}: {} ← \"{}\"\n", cot::now(), id(),
                   message_traits_type::print_transform(m));
    }

    // NB could also model receive_delay_, like `send()`’s send_delay_,
    // but don’t bother in handout code

    co_await cot::after(receive_delay_);

    co_return m;
}



// network<T>
//    A collection of channel<T> and port<T> objects.
//
//    `link(src, dst)` returns the channel<T> for the link server `src` to
//    server `dst`, and `input(id)` returns the port<T> for server `id`’s input
//    interface. These functions create the relevant channels and ports as
//    necessary.

template <typename T>
struct network {
    using channel_type = channel<T>;
    using port_type = port<T>;
    using random_engine_type = std::mt19937_64;

    network();
    network(const network<T>&) = delete;
    network(network<T>&&) = delete;
    network<T>& operator=(const network<T>&) = delete;
    network<T>& operator=(network<T>&&) = delete;


    // - network components
    // return the channel from `src` to `dst`
    inline channel_type& link(id_type src, id_type dst);

    // return the input interface for receiving messages to `id`
    inline port_type& input(id_type id);


    // - verbosity
    bool verbose() const noexcept { return verbose_; }
    void set_verbose(bool verbose) noexcept { verbose_ = verbose; }


    // - source of randomness
    random_engine_type& randomness() { return randomness_; }

    // Helper functions for accessing randomness
    // - returning bool
    inline bool coin_flip();     // returns true with P = 0.5
    inline bool coin_flip(double probability_of_true);
    // - uniform distributions: select from a list of items or choose within a
    //   range
    template <typename U>
    inline U uniform(std::initializer_list<U> list);
    template <std::integral I>
    inline I uniform(I min, I max);
    template <std::floating_point FP>
    inline FP uniform(FP min, FP max);
    inline cot::clock::duration uniform(cot::clock::duration min,
                                        cot::clock::duration max);
    // - exponential distributions: useful for network delay, which can have
    //   occasional long tails
    template <std::floating_point FP>
    inline FP exponential(FP mean);
    inline cot::clock::duration exponential(cot::clock::duration mean);
    // - normal (Gaussian) distributions: useful for jitter around a mean
    //   delay; the duration overload clamps negative results to zero
    template <std::floating_point FP>
    inline FP normal(FP mean, FP stddev);
    inline cot::clock::duration normal(cot::clock::duration mean,
                                       cot::clock::duration stddev);


    // - erase network state
    void clear();


private:
    std::map<std::pair<id_type, id_type>, std::unique_ptr<channel_type>> links_;
    std::map<id_type, std::unique_ptr<port_type>> inputs_;
    bool verbose_ = false;
    random_engine_type randomness_;
};


// Construction functions
// (These functions are ugly because channels and ports cannot be copied.)

template <typename T>
inline channel<T>::channel(id_type from, id_type to, network<T>& net)
    : from_(from), to_port_(net.input(to)), verbose_(net.verbose()), net_(net) {
}

template <typename T>
inline port<T>::port(id_type id, network<T>& net)
    : id_(id), verbose_(net.verbose()), net_(net) {
}

// network<T>::link(from, to)
//    Return the channel in this network for the `from → to` link.

template <typename T>
inline channel<T>& network<T>::link(id_type from, id_type to) {
    auto& link = links_[{from, to}];
    if (!link) {
        link.reset(new channel_type(from, to, *this));
    }
    return *link;
}

// network<T>::input(id)
//    Return the port in this network that accepts messages for server `id`.

template <typename T>
inline port<T>& network<T>::input(id_type id) {
    auto& input = inputs_[id];
    if (!input) {
        input.reset(new port_type(id, *this));
    }
    return *input;
}


// network constructor
//    Seeds the random generator randomly.

template <typename T>
network<T>::network()
    : randomness_(randomly_seeded<random_engine_type>()) {
}


// Convenience functions for accessing network<T> randomness

template <typename T>
inline bool network<T>::coin_flip() {
    return std::uniform_int_distribution<int>(0, 1)(randomness_);
}

template <typename T>
inline bool network<T>::coin_flip(double probability_of_true) {
    constexpr uint64_t one = uint64_t(1) << 53;
    auto val = std::uniform_int_distribution<uint64_t>(0, one - 1)(randomness_);
    return val < static_cast<uint64_t>(probability_of_true * one);
}

template <typename T>
template <typename U>
inline U network<T>::uniform(std::initializer_list<U> list) {
    assert(list.size() > 0);
    auto idx = std::uniform_int_distribution<size_t>(0, list.size() - 1)(randomness_);
    return list.begin()[idx];
}

template <typename T>
template <std::integral I>
inline I network<T>::uniform(I min, I max) {
    return std::uniform_int_distribution<I>(min, max)(randomness_);
}

template <typename T>
template <std::floating_point FP>
inline FP network<T>::uniform(FP min, FP max) {
    return std::uniform_real_distribution<FP>(min, max)(randomness_);
}

template <typename T>
inline cot::clock::duration network<T>::uniform(
    cot::clock::duration min, cot::clock::duration max
) {
    using rep = cot::clock::duration::rep;
    std::uniform_int_distribution<rep> dist(min.count(), max.count());
    return cot::clock::duration(dist(randomness_));
}

template <typename T>
template <std::floating_point FP>
inline FP network<T>::exponential(FP mean) {
    return std::exponential_distribution<FP>(1.0 / mean)(randomness_);
}

template <typename T>
inline cot::clock::duration network<T>::exponential(cot::clock::duration mean) {
    using rep = cot::clock::duration::rep;
    std::exponential_distribution<double> dist(1.0 / mean.count());
    return cot::clock::duration(static_cast<rep>(dist(randomness_)));
}

template <typename T>
template <std::floating_point FP>
inline FP network<T>::normal(FP mean, FP stddev) {
    return std::normal_distribution<FP>(mean, stddev)(randomness_);
}

template <typename T>
inline cot::clock::duration network<T>::normal(
    cot::clock::duration mean, cot::clock::duration stddev
) {
    using rep = cot::clock::duration::rep;
    std::normal_distribution<double> dist(mean.count(), stddev.count());
    return cot::clock::duration(static_cast<rep>(std::max(dist(randomness_), 0.0)));
}


// network<T>::clear()
//    Clear the network state. Note that this may trigger some events, so it
//    should be followed by cotamer::clear() to clean everything up.

template <typename T>
void network<T>::clear() {
    links_.clear();
    inputs_.clear();
}


// message_traits<T>
//    This template lets us change the behavior of network functions based on
//    message type. We provide specializations that allow you to print
//    messages that are stored as pointers.

template <typename T>
struct message_traits {
    static inline const T& print_transform(const T& x) {
        return x;
    }
};

template <typename T>
struct message_traits<T*> {
    static inline const T& print_transform(T* x) {
        return *x;
    }
};

template <typename T>
struct message_traits<std::unique_ptr<T>> {
    static inline const T& print_transform(const std::unique_ptr<T>& x) {
        return *x;
    }
};

template <typename T>
struct message_traits<std::shared_ptr<T>> {
    static inline const T& print_transform(const std::shared_ptr<T>& x) {
        return *x;
    }
};

}
