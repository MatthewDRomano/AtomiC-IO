#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <signal.h>
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>
#include <math.h>
#include <stdatomic.h>
#include <fcntl.h>

#include "../include/atomicio_cl.h"
#include "../include/at_net.h"


#define MAX_PORT 65535
#define ATOMICIO_CL_MAGIC_COOKIE 0x006174696F636C00 // " atiocl "



// Forward declaration
void* recv_thread(void* arg);


typedef enum {
	STATE_DISCONNECTED,
	STATE_CONNECTED,
	STATE_AWAITING_CLEANUP
} client_state_t;

typedef struct {
        struct sockaddr server;
        int socket_fd;

} network_t;

// Stores client collected metadata (per session, across connections)
typedef struct {
	_Atomic(uint64_t) bytes_sent;
	_Atomic(uint64_t) bytes_received;
	_Atomic(uint64_t) init_epoch;
	_Atomic(uint64_t) connection_epoch;
} telemetry_t;


struct atomicio_client_ctx {
	uint64_t token; 					// Used to ensure an atomicio_client_ctx (atomicio_cl_t*) is what's passed into methods

	packet_t my_client;					// Intenral packet struct holding local client data
 	packet_t all_clients_broadcast[MAX_CONNECTIONS];	// Internal packet struct holding all broadcast client data
	broadcast_view_t broadcast_data;	 		// Struct that holds relevant broadcast data accessible by the user
	pthread_mutex_t my_client_mutex;
	pthread_mutex_t all_clients_broadcast_mutex;

	network_t network;
	telemetry_t metadata;

	_Atomic bool recv_thread_active;
	pthread_t recv_tid;

	_Atomic int active_user_count;
	_Atomic(client_state_t) state;
};





// Returns current ms
static uint64_t now_ms() {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);

        // Sec / ns conversion
        return (uint64_t)ts.tv_sec * 1000
                + ts.tv_nsec / 1000000;
}



atomicio_cl_t* atomicio_cl_create(const char* uuid) {
	// 1. Allocate space for new client on heap, fields 0 by default
	atomicio_cl_t* new_client_ctx = (atomicio_cl_t*)calloc(1, sizeof(atomicio_cl_t));
	if (!new_client_ctx) {
		fprintf(stderr, "Unable to allocate memory for AtomiC-IO client\n");
		return NULL;
	}

	// 2. Set nonzero default fields
	new_client_ctx->token = ATOMICIO_CL_MAGIC_COOKIE;
        new_client_ctx->network.socket_fd = -1;

	// 3. Initialize mutexes
	int rc = pthread_mutex_init(&new_client_ctx->my_client_mutex, NULL);
	if (rc != 0) {
		fprintf(stderr, "Error initializing local client mutex: %d\n", rc);
		goto err_free_client_mem;
	}

	rc = pthread_mutex_init(&new_client_ctx->all_clients_broadcast_mutex, NULL);
	if (rc != 0) {
                fprintf(stderr, "Error initializing broadcast clients mutex: %d\n", rc);
                goto err_destroy_client_mutex;
        }

	// 4. Set specified uuid --> Bound constant defined in at_net.h
	snprintf(new_client_ctx->my_client.client_uuid, CLIENT_USERNAME_SIZE, "%s", uuid);

<<<<<<< HEAD
	// 5. Initialize necessary Atomics (Only nonzero ones or for explicit readibility)
=======
	// Set recv thread joined default value
	atomic_init(&new_client_ctx->recv_thread_active, false);
	
	
	// Init client metadata
	atomic_init(&new_client_ctx->active_user_count, 0);
	atomic_init(&new_client_ctx->metadata.bytes_sent, 0);
	atomic_init(&new_client_ctx->metadata.bytes_received, 0);
>>>>>>> 788593b88a131a203c214d9cbd7bf35d2c8e38f6
	atomic_init(&new_client_ctx->metadata.init_epoch, now_ms());
	atomic_init(&new_client_ctx->state, STATE_DISCONNECTED);
	atomic_init(&new_client_ctx->recv_thread_active, false);

	// NOTE: All network, metadata, and packet fields are 
	// already safely 0 due to calloc

	return new_client_ctx;


	// Error initializing broadcast clients mutex. Destroy local client mutex and cascade
	err_destroy_client_mutex:
	pthread_mutex_destroy(&new_client_ctx->my_client_mutex);

	// Error initializing local client mutex. Free allocated client context and return NULL
	err_free_client_mem:
	free(new_client_ctx);

	return NULL;
}

static int atomicio_connect_to_host(atomicio_cl_t* client_ctx, const char* ipv4_domain, const char* port_str) {
	struct addrinfo hints = { .ai_family = AF_INET, .ai_socktype = SOCK_STREAM }; 	// IPv4 & TCP
	struct addrinfo *result;

	// Resolve host into result
	int rc = getaddrinfo(ipv4_domain, port_str, &hints, &result);
	if (rc != 0) {
		fprintf(stderr, "%s\n", gai_strerror(rc));
		return -1;
	}


	// Iterate through returned hosts
	int fd = -1;
	struct addrinfo* current = result;	
	for (; current != NULL; current = current->ai_next) {
		fd = socket(current->ai_family, current->ai_socktype, current->ai_protocol); // Make new socket as linux socket state machine renders socket unusable w/ failed connection
		if (fd == -1) {
                	perror("Client socket creation failed");
                	continue;
        	}	

		// Set SO_REUSEADDR so clients can reestablish TCP connections with server from ports stuck in TIME_WAIT (TCP state)
                int reuse = 1;
                setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));	

		// Ensures sigpipe is disallowed on older macOS systems
        	#ifdef SO_NOSIGPIPE
        	int opt = 1;
        	setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &opt, sizeof(opt));
        	#endif
		
		// Attempt connection
		bool connect_failed = false;
		while (connect(fd, current->ai_addr, current->ai_addrlen) == -1) {
                	if (errno == EINTR) // Interrupted by signal
                        	continue;

                	// Fatal error occurred
                	connect_failed = true;
			break;
        	}	
	
		// Close fd and continue on connect() failure
		if (connect_failed) {
			close(fd);
			continue;
		}
	
		// Set client context fd / break out of loop upon success
		else {	
			client_ctx->network.socket_fd = fd;
			break;
		}
	}

	// Heap allocated
	freeaddrinfo(result); 

	// Upon no valid host	
	if (!current)
		return -1;

	// Valid host / connection
	return 0;

}

int atomicio_cl_connect(atomicio_cl_t* client_ctx, const char* port_str, const char* ipv4_domain) {
	
	// ========================================================
        // Ensures valid conditions to connect client to server
        // ========================================================

	// Ensures object is valid / returning -1 if not
	if (!client_ctx || client_ctx->token != ATOMICIO_CL_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error
	
	// Ensure object is not already connected
	if (atomic_load(&client_ctx->state) == STATE_CONNECTED) {
		fprintf(stderr, "Client is either uninitialized or already connected\n");
		return -1;
	}

	// If the client is awaiting cleanup from an internally severed connection,
	// Calls disconnect to join reaper thread, and enters client into proper disconnected state 
	if (atomic_load(&client_ctx->state) == STATE_AWAITING_CLEANUP)
		atomicio_cl_disconnect(client_ctx);


	// ========================================================
        // Establish connection to server
        // ========================================================	

	client_ctx->network.socket_fd = -1;
	if (atomicio_connect_to_host(client_ctx, ipv4_domain, port_str) != 0)
		return -1;	

	// Connection successful
        atomic_store(&client_ctx->state, STATE_CONNECTED);
	atomic_store(&client_ctx->metadata.connection_epoch, now_ms());	


        // Login message to the server
        client_ctx->my_client.payload_len = htons(1);
	atomicio_cl_send_data(client_ctx, LOGIN);


        // Spawn receive thread --> call disconnect on error
        if (pthread_create(&client_ctx->recv_tid, NULL, recv_thread, client_ctx) != 0) {
                fprintf(stderr, "Failed to receive send thread\n");
        	atomicio_cl_disconnect(client_ctx);
		return -1; 
	}	

	return 0;	
}


/**
 * Safely severs TCP connection to server and sets proper client disconnect flags.
 * If the client is not currently connected, this method does nothing.
 *
 * Upon successful teardown, the user must call atomicio_cl_disconnect() to finalize the disconnection process
 */ 
static void internal_connection_teardown(atomicio_cl_t* client_ctx) {
<<<<<<< HEAD

	// This will run once, even if both recv and main threads detect failure concurrently
	client_state_t expected = STATE_CONNECTED;
	if (!atomic_compare_exchange_strong(&client_ctx->state, &expected, STATE_AWAITING_CLEANUP))
=======
	// Ensures this block only runs if client state is connected
	// This will run once, even if both recv and send threads detect failure concurrently
	client_state_t expected = STATE_CONNECTED;
	if (atomic_compare_exchange_strong(&client_ctx->state, &expected, STATE_AWAITING_CLEANUP))
>>>>>>> 788593b88a131a203c214d9cbd7bf35d2c8e38f6
		return;
		
	// Interrupts blocking read / write
	int fd = client_ctx->network.socket_fd;
	if (fd >= 0)
		shutdown(fd, SHUT_RDWR);	
}


// Calls internal_connection_teardown() to sever TCP and frees receive thread memory
// If a client is not connected, this method is still safe to call and does nothing
int atomicio_cl_disconnect(atomicio_cl_t* client_ctx) {
	// Ensures object is valid / returning -1 if not
	if (!client_ctx || client_ctx->token != ATOMICIO_CL_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error
	
<<<<<<< HEAD
=======
	// Ensures object is able to be disconnected (in state CONNECTED or AWAITING_CLEANUP)
	//client_state_t old_state = atomic_exchange(&client_ctx->state, STATE_AWAITING_CLEANUP);
	//if (atomic_load(&client_ctx->state) == STATE_DISCONNECTED)
	//	return 0;

>>>>>>> 788593b88a131a203c214d9cbd7bf35d2c8e38f6
	// Severs TCP and sets state to awaiting cleanup
	internal_connection_teardown(client_ctx);

	// GUARANTEED REAP: Waits and joins receive thread and sets state to disconnected
	if (atomic_load(&client_ctx->recv_thread_active)) {
		pthread_join(client_ctx->recv_tid, NULL);
		atomic_store(&client_ctx->recv_thread_active, false); // Clears field
	}
	
	// Closes fd (socket resource) after thread is joined
	if (client_ctx->network.socket_fd >= 0) {
		close(client_ctx->network.socket_fd);
		client_ctx->network.socket_fd = -1;
	}

	
	// Reset necessary client runtime fields to default values
	atomic_store(&client_ctx->active_user_count, 0);

	// Clear network config
	client_ctx->network = (network_t){0};
	
	// Clear stale broadcast data
	pthread_mutex_lock(&client_ctx->all_clients_broadcast_mutex);
	memset(client_ctx->all_clients_broadcast, 0, sizeof(packet_t) * MAX_CONNECTIONS);
	pthread_mutex_unlock(&client_ctx->all_clients_broadcast_mutex);

	
	// Finalize state and return
	atomic_store(&client_ctx->state, STATE_DISCONNECTED);
	return 0;
}


int atomicio_cl_destroy(atomicio_cl_t** client_ctx_ptr) {
	if (!client_ctx_ptr)
		return -1;
	
	atomicio_cl_t* client_ctx = *client_ctx_ptr;
	if (!client_ctx || client_ctx->token != ATOMICIO_CL_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error

	// Force a disconnect on socket / TCP connection if client is not disconnected
	if (atomic_load(&client_ctx->state) != STATE_DISCONNECTED)
		atomicio_cl_disconnect(client_ctx);

	// Clean up client mutexes
	pthread_mutex_destroy(&client_ctx->my_client_mutex);
	pthread_mutex_destroy(&client_ctx->all_clients_broadcast_mutex);

	// Brick magic token to avoid stale pointers
	client_ctx->token = 0;

	// Nullify user's pointer / Free client context memory
	*client_ctx_ptr = NULL;
	free(client_ctx);		

	return 0;
}


int atomicio_cl_send_data(atomicio_cl_t* client_ctx, message_type_t msg_type) {
	// Ensures object is valid / returning -1 if not
	if (!client_ctx || client_ctx->token != ATOMICIO_CL_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error

	// Ensures proper connection to server	
	if (atomic_load(&client_ctx->state) != STATE_CONNECTED) {
		fprintf(stderr, "Cannot send data if no valid connection\n");
		return -1;
	}

	packet_t packet_out;
	
	// Locks mutex for copying local client data to outbound packet
	pthread_mutex_lock(&client_ctx->my_client_mutex);
        memcpy(&packet_out, &client_ctx->my_client, sizeof(packet_t));
        pthread_mutex_unlock(&client_ctx->my_client_mutex);

	// Sets outgoing packet header info (authenticator token, type, payload len)
	packet_out.token = htonl(ATOMICIO_PROTOCOL_MAGIC);
	packet_out.type = htons((uint16_t)msg_type);
        packet_out.active_users = htons(1);

	int result = full_write(client_ctx->network.socket_fd, &packet_out, 1);
        if (result != 0) {
		// Interrupts and ends recv thread
		internal_connection_teardown(client_ctx);
		return -1;
	}

	// Increment bytes sent metadata and return
	atomic_fetch_add(&client_ctx->metadata.bytes_sent, ntohs(client_ctx->my_client.payload_len));
	return 0;			
}


int atomicio_cl_update_data(atomicio_cl_t* client_ctx, const void* data, uint16_t data_size) {
	// Ensures object is valid / returning -1 if not
	if (!client_ctx || client_ctx->token != ATOMICIO_CL_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error

	// Ensures data size aligns with AtomiC-IO protocols (Unsigned specifier)	
	if (data_size > PAYLOAD_MAX) {
		fprintf(stderr, "Provided data is too large. (0-%d)\n", PAYLOAD_MAX);
                return -1;
	}


	// Sets payload length field in local client packet (Big endian conversion for network safety)
	// Byte-for-byte copy (data_size size) of provided data into packet,
	pthread_mutex_lock(&client_ctx->my_client_mutex);
	client_ctx->my_client.payload_len = htons(data_size);
	memcpy(client_ctx->my_client.payload, data, data_size);
	pthread_mutex_unlock(&client_ctx->my_client_mutex);
	return 0;
}


bool atomicio_cl_is_connected(atomicio_cl_t* client_ctx) {
	if (!client_ctx || client_ctx->token != ATOMICIO_CL_MAGIC_COOKIE)
                return false; // Returns false on structural API error

	return atomic_load(&client_ctx->state) == STATE_CONNECTED;
}

int atomicio_cl_get_active_user_count(atomicio_cl_t* client_ctx) {
	if (!client_ctx || client_ctx->token != ATOMICIO_CL_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error	

	return atomic_load(&client_ctx->active_user_count);
}

int64_t atomicio_cl_session_uptime(atomicio_cl_t* client_ctx) {
	if (!client_ctx || client_ctx->token != ATOMICIO_CL_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error

	// Returns 0 if the client has never connected to the server
	uint64_t client_connect_epoch = atomic_load(&client_ctx->metadata.connection_epoch);	
	return (client_connect_epoch == 0) ? 0 : now_ms() - client_connect_epoch;
}

int64_t atomicio_cl_lifetime(atomicio_cl_t* client_ctx) {
	if (!client_ctx || client_ctx->token != ATOMICIO_CL_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error

	// Returns valid lifetime if above check passes	
	return now_ms() - atomic_load(&client_ctx->metadata.init_epoch);
}

int atomicio_cl_get_broadcast_data(atomicio_cl_t* client_ctx, broadcast_view_t* view) {
	if (!client_ctx || client_ctx->token != ATOMICIO_CL_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error	

	// Protect read from simultaneous writes to broadcast_data
	// Performs struct copy
	pthread_mutex_lock(&client_ctx->all_clients_broadcast_mutex);
	*view = client_ctx->broadcast_data;	
	pthread_mutex_unlock(&client_ctx->all_clients_broadcast_mutex);

	return 0;
}







void* recv_thread(void* arg) {
	// Client context
	atomicio_cl_t* client_ctx = (atomicio_cl_t*)arg;	
	
	// Set thread active flag
	atomic_store(&client_ctx->recv_thread_active, true);       
 
	// Allocate temporary receive packet buffer onto the heap	
	packet_t* rx_pkt_buf = (packet_t*)malloc(sizeof(packet_t) * MAX_CONNECTIONS);	
	if (!rx_pkt_buf) {
		internal_connection_teardown(client_ctx);	
		return NULL;
	}
	
	int result = 0;
	while (atomic_load(&client_ctx->state) == STATE_CONNECTED) {

		// Full read ensures Range[1, MAX_CONNECTIONS] packets are read. No more no less
		if ((result = full_read(client_ctx->network.socket_fd, rx_pkt_buf)) != 0) {
			// Severs TCP connection to server due to internel socket errors
			internal_connection_teardown(client_ctx);			
			break;
		}


		// Handle message based on type
		message_type_t type = (message_type_t)ntohs(rx_pkt_buf[0].type);	
		bool should_exit_loop = false;
		switch (type) {
			case LOGOUT:
				// Initiates logout sequence -> User must finalize with atomicio_cl_disconnect() call
				internal_connection_teardown(client_ctx);
				should_exit_loop = true;
				break;
			case UPDATE_MESSAGE:
				break;
			
			// Types LOGIN and invalid types cannot be sent
			default:
				// Initiates logout sequence -> User must finalize with atomicio_cl_disconnect() call
				internal_connection_teardown(client_ctx);
				should_exit_loop = true;
				break;
		}

		// Exits if message type error
		if (should_exit_loop)
			break;


		// Update client context active user field
		uint16_t active_users = ntohs(rx_pkt_buf[0].active_users); // Alternatively: rx_pkt_buf->active_users
                atomic_store(&client_ctx->active_user_count, active_users);

		// Load broadcast clients data into local struct --> memcpy to client_ctx struct under mutex to save time
		broadcast_view_t local_view = {0};
                for (int i = 0; i < active_users; i++) {
                        uint16_t plen = ntohs(rx_pkt_buf[i].payload_len);
			local_view.snapshots[i].payload_len = plen;
			memcpy(local_view.snapshots[i].uuid, rx_pkt_buf[i].client_uuid, CLIENT_USERNAME_SIZE);
                        memcpy(local_view.snapshots[i].payload, rx_pkt_buf[i].payload, plen);
                }
                local_view.count = active_users;
		local_view.timestamp = at_ntohll(rx_pkt_buf[0].timestamp);

		// Copy rx_pkt_buf and local_view data into respective client context fields in thread safe manner
                pthread_mutex_lock(&client_ctx->all_clients_broadcast_mutex);
                memcpy(client_ctx->all_clients_broadcast, rx_pkt_buf, sizeof(packet_t) * active_users);
		client_ctx->broadcast_data = local_view;
                pthread_mutex_unlock(&client_ctx->all_clients_broadcast_mutex);
	}	

	// Free allocated temp buffer and return
	free(rx_pkt_buf);
	return NULL;
}



