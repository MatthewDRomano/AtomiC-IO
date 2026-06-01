#ifndef ATOMICIO_CL_H
#define ATOMICIO_CL_H

#include <stdint.h>
#include <stdbool.h>
#include "at_net.h"

// 1. OPAQUE CLIENT CONTEXT STRUCT --> allows multiple client instances per process
// 	|-> Stores client data internally and encapsulates internal structure
typedef struct atomicio_client_ctx atomicio_cl_t;


// ========================================================
// 2. LIFECYCLE CONTROLLERS
// ========================================================

/**
 * Takes a unique client uuid and desired log_path.
 * Returns a pointer to an opaque atomicio client struct allocated on the heap
 * atomicio_cl_destroy must be called eventually to free client memory
 */ 
atomicio_cl_t* atomicio_cl_create(const char* uuid, const char* log_path);


/**
 * Takes a 16-bit port and domain / IPv4.
 * Establishes a TCP connection with server and begins receiving packets from the broadcast server via a dedicated thread.
 * The aforementioned receive thread is spawned to update local client data.
 * Call this method after successfully creating a client object.
 */
int atomicio_cl_connect(atomicio_cl_t* client_ctx, uint16_t port, const char* ipv4_domain);


/**
 * Sets connection flags to false and begins a graceful disconnect from the server.
 * After disconnecting, the client log is closed and client resources are cleaned up 
 * ONLY returns -1 upon invalid / corrupted client_ctx arg. In practice the return value can be ignored.
 */
int atomicio_cl_disconnect(atomicio_cl_t* client_ctx);


/**
 * Cleans up internally allocated client data and frees associated client_ctx struct.
 * Calling destroy while connected to a server will also initiate a clean disconnect.
 * The passed in atomicio_cl_t* is no longer valid after this call
 */
int atomicio_cl_destroy(atomicio_cl_t* client_ctx);

// ========================================================
// 3. RUNTIME DATA FUNCTIONS 
// ========================================================

/**
 * Takes a client context struct and void data pointer (accepts any type) as input.
 * It also takes an unsigned data size specifier that is internally bounds checked. 
 * The client's local data is updated to the the void pointers data, otherwise returns -1 upon failure.
 *
 * See at_net.h for packet (client data) sizing protocol
 */ 
int atomicio_cl_data_update(atomicio_cl_t* client_ctx, const void* data, uint16_t data_size);


/**
 * Assuming the client has established a TCP connection with an AtomiC-IO server,
 * the client local data (settable with the above method) is then packaged into a server-safe packet and sent to the server.
 */ 
int atomicio_cl_send_data(atomicio_cl_t* client_ctx, message_type_t msg_type);

// ========================================================
// 4. METADATA & DIAGNOSTICS
// ========================================================

/**
 * Checks if a given client is currently connected to an AtomiC-IO server.
 * Returns true if connected, false if disconnected or uninitialized
 */
bool atomicio_cl_is_connected(atomicio_cl_t* client_ctx);


/**
 * Returns the active user count of the AtomiC-IO server the client is connected to.
 * Returns 0 as default if the client is either not connected, or has yet to receive a packet
 * Returns -1 if the client context is not valid
 */ 
int atomicio_cl_get_active_user_count(atomicio_cl_t* client_ctx);


/**
 * Returns how long a given client has been connected to an active AtomiC-IO server session in milliseconds.
 * Returns -1 if the client context is not valid 
*/ 
uint64_t atomicio_cl_session_uptime(atomicio_cl_t* client_ctx);


/**
 * Returns how long a given client has been alive (time since creation)
 * Returns -1 if the client context is not valid 
*/ 
uint64_t atomicio_cl_lifetime(atomicio_cl_t* client_ctx);


/**
 * Returns the total number of bytes successfully transmitted to the server
 * Returns -1 if the client context is not valid 
*/ 
uint64_t atomicio_cl_get_bytes_sent(atomicio_cl_t* client_ctx);


/**
 * Returns the total number of bytes received from the server
 * Returns -1 if the client context is not valid 
*/
uint64_t atomicio_cl_get_bytes_received(atomicio_cl_t* client_ctx);

// ========================================================
// 5. USER LOGGING
// ========================================================

/**
 * Thread-safe logging option allowing custom message control for the client.
 * Can safely call after config init
 */ 
int atomicio_cl_log(atomicio_cl_t* client_ctx, const char* msg);

#endif
