# Chain Replication

*Development Status*: Experimental

[Chain replication](https://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf) is a replication protocol designed for high throughput, 
fault tolerant systems to achieve both fast
replication as well as, in some implementations, strong consistency. A chain is able to recover quickly from failure
by bypassing failed nodes in the chain, and new nodes can be added with minimal disruption.

This implementation is an experiment to build chain replication with Rust and [Tokio](https://tokio.rs). The goal is to build
an underlying primitive on which specific systems can be built (e.g. key value store, search engine, state machine replication).

## Building/Running

Must be using Rust nightly.

```shell
cargo build --release

# start a head node
./target/release/log-server &

# start a replica node
./target/release/log-server -r &

# start the CLI
./target/release/cli
```

The *benchit* tool allows a quick way to benchmark log insertion using multiple clients.

```shell
# Start 2 clients, 50 requests in flight per client
./target/release/benchit -w 2 -c 50
```


## Progress

- [ ] Chain Replication
    - [X] Replicated log
    - [X] Chained replication
    - [X] Multiplexed command protocol
    - [X] Multiplexed, streaming replication protocol
    - [ ] Tail replies
    - [ ] Tail queries
- [ ] Reconfiguration
    - [ ] Modes
        - [ ] Head node failure
        - [ ] Tail node failure
        - [ ] Middle node failure
    - [ ] Master Node
        - [ ] Failure detector
        - [ ] Reconfiguration Protocol
        - [ ] Partitioning
        - [ ] Chain reconfiguration
        - [ ] Backup Master Nodes (Requires consensus protocol)
- [ ] Framework Elements
    - [ ] Custom Commands
    - [ ] Tail node queries
- [ ] Other ideas
    - [ ] Complex chains
        - [ ] [Replex](https://www.cs.princeton.edu/~mfreed/docs/replex-atc16.pdf)
        - [ ] [HyperDex](https://www.cs.cornell.edu/people/egs/papers/hyperdex-sigcomm.pdf)
    - [ ] Multi-Data Center topology
    - [ ] Integration with non-replicated systems or poorly replicated systems
    - [ ] IoT offline mode (coalescing chains)
