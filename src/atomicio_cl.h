#ifndef ATOMICIO_CL_H
#define ATOMICIO_CL_H

#include <stdint.h>
#include <stdbool.n>

// 1. OPAQUE CLIENT CONTEXT STRUCT --> allows multiple client instances per process
// 	|-> Stores client data internally and encapsulates internal structure
typedef struct atomicio_client_ctx atomicio_cl_t;

// ========================================================
// 2. PUBLIC CONFIGURATION INTERFACE
// ========================================================

typedef struct {
	uint16_t port;			// Server port
	const char* ipv4_domain;	// Server ipv4 or domain sent as string
	const char* uuid;		// Client username / userID
	const char* log_path;		// Client log path
} atomicio_cl_config;

// ========================================================
// 2. LIFECYCLE CONTROLLERS
// ========================================================

/**
 * Configures the provided client struct with the specified config settings.
 * Client resources are initiated alongside client side log.
 */ 
int atomicio_cl_init(atomicio_cl_t* client, const atomicio_cl_config* cl_conf);


/**
 * Establishes a TCP connection with server and begins receiving packets from the broadcast server.
 * A receive thread is also spawned to update local client data sent periodically from broadcast server
 * Call this method after a successful call to atomicio_cl_init
 */
int atomicio_cl_connect(atomicio_cl_t* client);


/**
 * Sets connection flags to false and begins a graceful disconnect from the server.
 * After disconnecting, the client log is closed and client resources are cleaned up 
 */
int atomicio_cl_disconnect(atomicio_cl_t* client);

// ========================================================
// 3. RUNTIME DATA FUNCTIONS 
// ========================================================

/**
 * Takes a client context struct and void data pointer (accepts any type) as input.
 * Assuming the client has established a TCP connection with an AtomiC-IO server,
 * data_out is then packaged into a server-safe packet and sent to the server.
 *
 * See at_net.h to view network / packet sizing protocols
 */ 
int atomicio_cl_send_data(atomicio_cl_t* client, const void* data_out, uint16_t data_size);

// ========================================================
// 4. METADATA & DIAGNOSTICS
// ========================================================

/**
 * Checks if a given client is currently connected to an AtomiC-IO server.
 * Returns 1 if connected, 0 if disconnected or uninitialized
 */
bool atomicio_cl_is_connected(atomicio_cl_t* client);


/**
 * Returns how long a given client has been connected to an AtomiC-IO server
 */ 
uint64_t atomicio_cl_time_connected(atomicio_cl_t* client);


/**
 * Returns the total number of bytes successfully transmitted to the server
 */ 
uint64_t atomicio_cl_get_bytes_sent(atomicio_cl_t* client);


/**
 * Returns the total number of bytes received from the server
 */
uint64_t atomicio_ct_get_bytes_received(atomicio_cl_t* client);

// ========================================================
// 5. USER LOGGING
// ========================================================

/**
 * Thread-safe logging option allowing custom message control for the client.
 */ 
int atomicio_cl_log(atomicio_cl_t* client, const char* msg);

#endif
