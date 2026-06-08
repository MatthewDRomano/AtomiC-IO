# AtomiC-IO

A high-performance broadcast server framework designed for generic, lock-minimal concurrent C networking. 

AtomiC-IO solves "Global Lock Contention"—a common bottleneck in traditional broadcast servers where a single thread transmitting messages to all users freezes the entire client ingestion and cleanup pipeline. By leveraging C11 hardware-native atomics and an asymmetric multi-threaded architecture, AtomiC-IO ensures high-throughput I/O without sacrificing thread safety or connection scalability.

### Architecture Overview

| Component | Implementation Details |
| :--- | :--- |
| **Language & Standard** | POSIX C11 |
| **Concurrency Model** | Asymmetric threading, CAS-based linked lists, `<stdatomic.h>` |
| **Networking** | IPv4 TCP Sockets, `poll()` I/O multiplexing |
| **Platform Target** | macOS & Linux (Cross-platform TCP resilience mechanisms) |

---

### Core Engineering Features

* **Lock-Minimal Concurrency:** Utilizes C11 `atomic_compare_exchange_weak/strong` for lock-free client ingestion. The main thread can continuously accept new users without blocking, even while heavy data broadcasts or memory cleanups are actively executing.
* **Asymmetric Threading Model:** Workloads are aggressively isolated. The server operates utilizing a dedicated Acceptor thread, a background Reaper thread for safe dead-node unlinking, and independent `poll()`-driven I/O threads for each client.
* **Opaque Context Architecture:** Built using an object-oriented C pattern. Both the Server (`atomicio_server_ctx`) and Client (`atomicio_cl_t`) contexts are entirely opaque and isolated, allowing you to instantiate an unlimited number of server/client instances safely within a single host process.
* **Cross-Platform TCP Resilience:** Network edge cases are explicitly handled at the kernel interface level. The framework seamlessly resolves `SIGPIPE` issues across both Linux (`MSG_NOSIGNAL`) and macOS (`SO_NOSIGPIPE`), alongside port reuse optimizations (`SO_REUSEPORT` / `SO_REUSEADDR`).
* **Asynchronous Developer Logging:** Includes a dedicated, thread-safe logging pipeline. Logs are queued via a custom linked list and flushed by an isolated I/O thread regulated by POSIX semaphores, ensuring disk writes never block network operations.

---

### The Problem vs. The AtomiC-IO Solution

**The Problem:** Traditional architectures rely on heavy reader/writer mutexes. When broadcasting state to 100+ clients, the global lock is held for the duration of the network write. If a new client attempts to connect, or if a dead client needs to be reaped, the entire application stalls, causing massive latency spikes. 

**The Solution:** AtomiC-IO separates structural changes from data transmission. Mutexes are strictly reserved for brief reader-integrity copy operations (ensuring a clean snapshot of outbound memory). Structural changes to the client pool use wait-free atomic mutators. A background Reaper thread safely handles resource deallocation via semaphore-signaling, preventing zombie threads without disrupting the main execution path.

---

### API Preview

The API is designed to be highly readable, enforcing safe lifecycles through "Magic Cookie" token validation to prevent stale pointer manipulation and internal state tracking to prevent invalid API calls.

```c
// 1. Initialize Configuration
atomicio_config_t config = {
    .port = 8080,
    .max_users = 128,
    .devlogs_enabled = true,
    .drop_late_packets = false,
    .log_path = "./server_logs"
};

// 2. Instantiate Opaque Server Context
atomicio_server_ctx* server = atomicio_create_server(&config);

// 3. Boot Acceptor, Reaper, and Logging Threads
atomicio_server_run(server);

// 4. Safe Teardown & Deallocation
atomicio_server_shutdown(server);
atomicio_server_destroy(&server);
```

---

### Getting Started

**Prerequisites:** A POSIX-compliant operating system (Linux/macOS) and `gcc` / `clang` (with support for C11).

**Building from source:**

```bash
# Clone the repository
git clone https://github.com/MatthewDRomano/AtomiC-IO.git
cd AtomiC-IO

# Create a build directory
mkdir build && cd build 

# Compile the Server (Assumes you have a server.c entry point)
gcc ../src/server.c ../src/atomicio.c ../src/log.c ../src/at_net.c -o at_server -pthread

# Compile the Client (Assumes you have a client.c entry point)
gcc ../src/client.c ../src/atomicio_cl.c ../src/log.c ../src/at_net.c -o at_client -pthread

# Execute
./at_server
```

