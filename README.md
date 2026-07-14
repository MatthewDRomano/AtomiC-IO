# AtomiC-IO

A high-performance broadcast server framework designed for generic, lock-minimal concurrent C networking. 

AtomiC-IO solves "Global Lock Contention"—a common bottleneck in traditional broadcast servers where a single thread transmitting messages to all users freezes the entire client ingestion and cleanup pipeline. By leveraging C11 hardware-native atomics and an asymmetric multi-threaded architecture, AtomiC-IO ensures high-throughput I/O without sacrificing thread safety or connection scalability.

### Architecture Overview

| Component | Implementation Details |
| :--- | :--- |
| **Language & Standard** | POSIX C11 |
| **Concurrency Model** | Asymmetric threading, CAS-based linked lists, `<pthread.h>`, `<stdatomic.h>` |
| **Networking** | IPv4 TCP Sockets, `poll()` I/O multiplexing,  |
| **Platform Target** | macOS & Linux (Cross-platform TCP resilience mechanisms) |

---

### Core Engineering Features

* **Lock-Minimal Concurrency:** Utilizes C11 `atomic_compare_exchange_weak/strong` for lock-free client ingestion. The main thread can continuously accept new users without blocking, even while heavy data broadcasts or memory cleanups are actively executing.
* **Asymmetric Threading Model:** Workloads are aggressively isolated. The server operates utilizing a dedicated Acceptor thread and independent `poll()`-driven I/O threads for each client. A background Reaper thread also spawns for safe dead-node unlinking—triggered via named kernel semaphores `<semaphore.h>`.
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

**Prerequisites:** A POSIX-compliant operating system (Linux/macOS) and `gcc` / `clang` (with support for C11 standard atomics).

#### Local Building (Library, Examples, & Tests)

Running the standard `make` command compiles all core source files into a static library (`libatomicio.a`) and automatically compiles all examples and tests, linking them directly against that local library.

```bash
# Clone the repository
git clone [https://github.com/MatthewDRomano/AtomiC-IO.git](https://github.com/MatthewDRomano/AtomiC-IO.git)
cd AtomiC-IO

# Compile the local library, examples, and tests
make
```

This populates the `bin/` folder with executable binaries. You can run them instantly:
* Execute an example: `./bin/example_name`
* Execute a test suite: `./bin/test_name`

#### Global Installation (System-Wide Includes & Libraries)

To make the framework accessible across your entire machine, run `make install`. This copies the public header files and the compiled library file into standard global system paths (`/usr/local/include/atomicio/` and `/usr/local/lib/`), making them available to any other project you write.

```bash
# Install headers and library system-wide
sudo make install
```

#### Using the Global Library

Once installed system-wide, you can include the server or client headers using system angle brackets, and link against the library anywhere on your computer using the `-latomicio` flag.

```c
#include <atomicio/atomicio.h>    // Global Server features
#include <atomicio/atomicio_cl.h> // Global Client features

int main() {
    atomicio_config_t config = { .port = 8080, .max_users = 128 };
    atomicio_server_ctx* server = atomicio_create_server(&config);
    // Application logic here...
    return 0;
}
```

**Global Compilation Command:**

```bash
gcc my_app.c -latomicio -pthread -DUSE_GLOBAL_HEADER -o my_app
```

---

#### Clean Up

To remove local build artifacts (`obj/` and `bin/`):

```bash
make clean
```

To completely strip the global includes and libraries from system paths:

```bash
sudo make uninstall
```
