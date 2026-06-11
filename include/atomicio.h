#ifndef ATOMICIO_H
#define ATOMICIO_H

#include <stdbool.h>
#include <stdint.h> 

// ========================================================
// 1. PUBLIC CONFIGURATION INTERFACE
// ========================================================

typedef struct atomicio_server_ctx atomicio_server_ctx; 

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
 * Configures the internal server structure and sets socket parameters (Port, Protocol, etc),
 * and initiates server log thread.
 * If succesful, atomicio_shutdown() will need to be called eventually to cleanup server resources.
 *
 * Returns a valid server context to the user, allocated on the heap
 */ 
atomicio_server_ctx* atomicio_create_server(const atomicio_config_t* init_settings);

/**
 * Begins listening on the internal socket, and spawns the client accept thread & client cleanup reaper thread.
 * Call this after a successful initialization to boot the server application.
 */ 
int atomicio_server_run(atomicio_server_ctx* server_ctx);

/**
 * Sets execution flags to false and performs a graceful shutdown.
 * Shutdown order: Shutdown flags set, Signal reaper thread to cleanup client resources,
 * close all open connections, flush remaining log entries, and close log.
 *
 * atomicio_run may be called again upon a successful shutdown
 */ 
int atomicio_server_shutdown(atomicio_server_ctx* server_ctx);

/**
 * Takes the address of a server_ctx pointer
 * Destroys server context resources (Pthread attribute, mutex, etc), and deallocates associated memory.
 * The server context is no longer valid after a successful call to atomicio_destroy and is set to null.
 */ 
int atomicio_server_destroy(atomicio_server_ctx** server_ctx_ptr);


// ========================================================
// 3. OOP-STYLE READ-ONLY STATUS GETTERS
// ========================================================

/**
 * Returns the current server state (running / not running)
 */
bool atomicio_is_running(atomicio_server_ctx* server_ctx);

/**
 * Atomically loads and returns a snapshot count of currently connected clients.
 */
int atomicio_get_active_user_count(atomicio_server_ctx* server_ctx);

/**
 * Returns total server object lifetime in milliseconds since calling atomicio_create_server().
 */
int64_t atomicio_get_overall_uptime_ms(atomicio_server_ctx* server_ctx);

/**
 * Returns the server's current active session uptime in milliseconds
 * Returns 0 if the server is OFFLINE
 */ 
int64_t atomicio_get_session_uptime_ms(atomicio_server_ctx* server_ctx);

/**
 * Computes and returns the average relative latency of all connected clients.
 */ 
float atomicio_get_avg_latency_multiplier(atomicio_server_ctx* server_ctx);

/**
 * Returns tracking metric for dropped packets under high load conditions.
 */ 
int64_t atomicio_get_dropped_packets_count(atomicio_server_ctx* server_ctx);


// ========================================================
// 4. Thread safe user logging APIs
// ========================================================

/**
 * Initializes atomicio log global fields, and spawns background logging thread.
 * Disk IO is handled in the background logging thread for maximum performance.
 */ 
int atomicio_log_init();

/**
 * Cleans up log resources, performs one final log entry sweep, and shuts down log.
 */ 
int atomicio_log_shutdown();

/**
 * Internally creates a message log entry with given server context's path.
 *
 * Returns 0 upon success, -1 on failure
 */
int atomicio_log_info(atomicio_server_ctx* server_ctx, const char* info);

/**
 * Internally creates an error log entry with an errno value and the given server context's path.
 *
 * Returns 0 upon success, -1 on failure
 */
int atomicio_log_error(atomicio_server_ctx* server_ctx, int errnum, const char* err_desc);


#endif
