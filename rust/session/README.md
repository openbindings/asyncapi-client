# Portable session policy

This crate contains no networking runtime. It owns compiled session plans,
checks connection/role/native-operation compatibility once, dispatches binary
messages to an unambiguous receive operation, and validates send operation
indices. Native and Wasm-host runtimes use the same policy.

`Budget` clones share one count/byte quota. Reservations admit both counters
atomically; a `Lease` returns capacity on drop. Runtime completion releases its
lease before waking a recipient. This accounts for library-owned pending bodies,
not application-owned results, protocol-library buffers or browser buffers.

Initial profile limits and refusals remain development policy. Sharing this
crate is not evidence that a host supports every transport described by a plan.
Each driver must check its capabilities before I/O.
