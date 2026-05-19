# AtomiC-IO

A high-performance broadcast server model designed as a generic, lock-minimal framework for concurrent C networking.

### The Problem
Traditional broadcast servers often suffer from "Global Lock Contention," where a single thread sending a message to all users freezes the entire client list. This architecture prevents new connections or resource cleanup until the I/O is finished, causing significant latency spikes as the user count grows.

### Solution
AtomiC-IO solves this by allowing simultaneous client connection, data receiving/broadcast, and resource cleanup through efficient asymmetrical concurrency. 


### Features
* **Generic Broadcast Architecture**: Provides a reusable blueprint for 1-to-N message distribution across concurrent TCP streams.
* **Lock-Free Ingestion**: Utilizes `atomic_compare_exchange_weak` so the main thread can accept new users even while broadcasts or cleanup are in progress.
* **Conflict-Aware Reaper**: A background thread using a retry-on-conflict CAS loop to safely unlink dead nodes without blocking the primary network path.
* **Asymmetric Synchronization**: Reserves mutexes for reader-integrity during broadcasts while using atomic mutators for structural list changes.
* **Integrated Developer Logging**: Features an optional, real-time logging system to track thread lifecycles, CAS retries, and network state changes.
* **Post-Publication Handshake**: A safety mechanism that prevents "orphan" threads if a client disconnects immediately after joining the list.

### Installation
**Build from source:**

```bash
# Clone and enter the repository
git clone https://github.com/MatthewDRomano/AtomiC-IO.git
touch build && cd build 

# Compile using the following gcc commands
Server: gcc ../src/server.c ../src/log.c ../src/at_net.c -o at_server
Client: gcc ../src/client.c ../src/log.c ../src/at_net.c -o at_client

# Start the server (-h for help)
./at_server --ip IP --port PORT
