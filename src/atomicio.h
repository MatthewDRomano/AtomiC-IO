#ifndef ATOMICIO_H
#define ATOMICIO_H

#include <stdbool.h>
#include <stdint.h> 

// ========================================================
// 1. PUBLIC CONFIGURATION INTERFACE
// ========================================================
typedef struct {
	uint16_t port;			// Active binding port
	int max_users;			// Limit of simultaneous connections
	bool devlogs_enabled;		// Verbose server-performance logging flag
	bool drop_late_packets;		// Policy for handling latency
	const char* log_path;		// Desired path for logging - Must be null-terminated
} atomicio_config_t;

// ========================================================
// 2. LIFECYCLE CONTROLLERS
// ========================================================

/**
 * Configures the internal server structure and sets socket parameters,
 * and initiates server log thread.
 * If succesful, atomicio_shutdown() will need to be called eventually to cleanup server resources.
 */ 
int atomicio_init_server(const atomicio_config_t* init_settings);

/**
 * Executes the main blocking client accept loop.
 * Call this after a successful initialization to boot the server application.
 */ 
int atomicio_run(void);

/**
 * Sets execution flags to false and performs a graceful shutdown.
 * Shutdown order: Shutdown flags set, Signal reaper thread to cleanup client resources,
 * close all open connections, flush remaining log entries, and close log.
 */ 
int atomicio_shutdown(void);


// ========================================================
// 3. OOP-STYLE READ-ONLY STATUS GETTERS
// ========================================================

/**
 * Atomically loads and returns a snapshot count of currently connected clients.
 */
int atomicio_get_active_user_count(void);

/**
 * Returns total active server lifetime in milliseconds since calling atomicio_run().
 */
uint64_t atomicio_get_uptime_ms(void);

/**
 * Computes and returns the average relative latency of all connected clients.
 */ 
float atomicio_get_avg_latency_multiplier(void);

/**
 * Returns tracking metric for dropped packets under high load conditions.
 */ 
uint64_t atomicio_get_dropped_packets_count(void);


// ========================================================
// 4. USER LOGGING 
// ========================================================

/**
 * Thread-safe logging option allowing custom message control for the user.
 */ 
void atomicio_log(char* msg);

#endif
