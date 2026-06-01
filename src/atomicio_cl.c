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

#include "atomicio_cl.h"
#include "at_net.h"
#include "log.h"


#define MAX_PORT 65535


// Forward declaration
void* recv_thread(void* arg);


typedef enum {
	STATE_DISCONNECTED,
	STATE_CONNECTED,
	STATE_AWAITING_CLEANUP
} client_state_t;

typedef struct {
        struct sockaddr_in server;
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
	packet_t my_client;
 	packet_t all_clients_broadcast[MAX_CONNECTIONS];
	pthread_mutex_t my_client_mutex;
	pthread_mutex_t all_clients_broadcast_mutex;

	network_t network;
	telemetry_t metadata;

	pthread_t recv_tid;

	char log_path[MAX_PATH_LEN];
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



atomicio_cl_t* atomicio_cl_create(const char* uuid, const char* log_path) {
	// Allocate space for new client on heap
	atomicio_cl_t* new_client_ctx = (atomicio_cl_t*)malloc(sizeof(atomicio_cl_t));
	if (!new_client_ctx) {
		fprintf(stderr, "Unable to allocate memory for AtomiC-IO client\n");
		return NULL;
	}

	// Zero-set network fields
        new_client_ctx->network.server = (struct sockaddr_in){0};
        new_client_ctx->network.socket_fd = -1;

	// Zero-set local/broadcast client packet(s) and init respective mutexes
	memset(&new_client_ctx->my_client, 0, sizeof(packet_t));
	memset(new_client_ctx->all_clients_broadcast, 0, sizeof(packet_t) * MAX_CONNECTIONS);
	pthread_mutex_init(&new_client_ctx->my_client_mutex, NULL);
	pthread_mutex_init(&new_client_ctx->all_clients_broadcast_mutex, NULL);

	// Set specified client fields -> uuid and log_path
	// Bound constants defined in at_net.h / log.h respectively
	snprintf(new_client_ctx->my_client.client_uuid, CLIENT_USERNAME_SIZE, "%s", uuid);
	snprintf(new_client_ctx->log_path, MAX_PATH_LEN, "%s", log_path);
	
	// Init client metadata
	atomic_init(&new_client_ctx->active_user_count, 0);
	new_client_ctx->metadata = (telemetry_t){0};
	new_client_ctx->metadata.init_epoch = now_ms();

	// Client is now fully initialized and address is returned
	atomic_init(&new_client_ctx->state, STATE_DISCONNECTED);
	return new_client_ctx;
	
}

int atomicio_cl_connect(atomicio_cl_t* client_ctx, uint16_t port, const char* ipv4_domain) {
	
	// ========================================================
        // Ensures valid conditions to connect client to server
        // ========================================================

	// Ensures object is valid
        if (!client_ctx)
		return -1;
	
	// Ensure object is not uninitialized or already connected
	if (atomic_load(&client_ctx->state) == STATE_CONNECTED) {
		fprintf(stderr, "Client is either uninitialized or already connected\n");
		return -1;
	}

	// If the client is awaiting cleanup from an internally severed connection,
	// Calls disconnect to join reaper thread, and enters client into proper disconnected state 
	if (atomic_load(&client_ctx->state) == STATE_AWAITING_CLEANUP)
		atomicio_cl_disconnect(client_ctx);

	// Sets Domain / IPv4 if valid
  	struct hostent* h_ent = gethostbyname(ipv4_domain);
	if (h_ent == NULL) {
		fprintf(stderr, "Invalid IPv4 / Domain\n");
		return -1;
	}
	
	// Sets: Port -> IP type -> desired host IP
	client_ctx->network.server.sin_port = htons(port);		
	client_ctx->network.server.sin_family = AF_INET;
	memcpy(&client_ctx->network.server.sin_addr, h_ent->h_addr_list[0], h_ent->h_length);

	// Zero out the server padding array
	memset(client_ctx->network.server.sin_zero, 0, sizeof(client_ctx->network.server.sin_zero));

	// ========================================================
        // Establish connection to server
        // ========================================================	

	// Create socket
	int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd == -1) { // Frees client memory upon error
                perror("Client socket creation failed");
                return -1;
        }
        client_ctx->network.socket_fd = fd;

	// Connects socket to server
	struct sockaddr* server_st = (struct sockaddr*)(&client_ctx->network.server);
        while (connect(client_ctx->network.socket_fd, server_st, sizeof(*server_st)) == -1) {
                if (errno == EINTR)
                        continue;

                // Error occurred or shutdown requested
                perror("Failed to connect socket to server");
                goto err_close_fd;
        }

	// Connection successful
        atomic_store(&client_ctx->state, STATE_CONNECTED);
	atomic_store(&client_ctx->metadata.connection_epoch, now_ms());	

	/*
	if (init_log(client_ctx->log_path) == -1)
                goto err_close_fd;
	*/

        // Login message to the server
        client_ctx->my_client.payload_len = htons(1);
	atomicio_cl_send_data(client_ctx, LOGIN);
        //msglog("-==CONNECTED==-");


        // Spawn receive thread
        if (pthread_create(&client_ctx->recv_tid, NULL, recv_thread, client_ctx) != 0) {
                fprintf(stderr, "Failed to receive send thread\n");
                goto err_close_log;
        }	



	return 0;	

	// ========================================================
        // Cascading error resource cleanup
        // ========================================================
	
	err_close_log:
	//end_log();
	
	err_close_fd:
	int fd_to_close = client_ctx->network.socket_fd;
        if (fd_to_close >= 0) {
                client_ctx->network.socket_fd = -1;
                close(fd_to_close);
        }
	
	atomic_store(&client_ctx->state, STATE_DISCONNECTED);
	return -1;
}


/**
 * Safely severs TCP connection to server and sets proper client disconnect flags.
 * If the client is not currently connected, this method does nothing.
 *
 * Upon successful teardown, the user must call atomicio_cl_disconnect() to finalize the disconnection process
 */ 
static void internal_connection_teardown(atomicio_cl_t* client_ctx) {
	// Ensures this block only runs if client state is connected
	// This will run once, even if both recv and send threads detect failure concurrently
	if (atomic_exchange(&client_ctx->state, STATE_AWAITING_CLEANUP) == STATE_CONNECTED) {	
		
		// Interrupts blocking read / write
		int fd_to_close;
		if ((fd_to_close = client_ctx->network.socket_fd) >= 0) {
			shutdown(client_ctx->network.socket_fd, SHUT_RDWR);	
			client_ctx->network.socket_fd = -1;
			close(fd_to_close);
			//msglog("-==DISCONNECTED==-");
		}
	}
}


// Calls internal_connection_teardown() to sever TCP and frees receive thread memory
// If a client is not connected, this method is still safe to call and does nothing
void atomicio_cl_disconnect(atomicio_cl_t* client_ctx) {
	// Ensures object is valid
	if (!client_ctx)
		return;
	
	// Ensures object is able to be disconnected (in state CONNECTED or AWAITING_CLEANUP)
	if (atomic_load(&client_ctx->state) == STATE_DISCONNECTED)
		return;

	// Severs TCP and sets state to awaiting cleanup
	internal_connection_teardown(client_ctx);

	// GUARANTEED REAP: Waits and joins receive thread and sets state to disconnected
	pthread_join(client_ctx->recv_tid, NULL);

	atomic_store(&client_ctx->state, STATE_DISCONNECTED);
	//msglog("Background receiver thread cleanly joined");
	
	// Reset necessary client runtime fields to default values
	atomic_store(&client_ctx->active_user_count, 0);

	// Clear network condif
	client_ctx->network = (network_t){0};
	
	// Clear stale broadcast data
	pthread_mutex_lock(&client_ctx->all_clients_broadcast_mutex);
	memset(client_ctx->all_clients_broadcast, 0, sizeof(packet_t) * MAX_CONNECTIONS);
	pthread_mutex_unlock(&client_ctx->all_clients_broadcast_mutex);

	// Close log upon dc
	//end_log();
}


int atomicio_cl_destroy(atomicio_cl_t* client_ctx) {
	// Ensures object is valid
	if (!client_ctx) 
		return 0;

	// Ensures clean disconnect if the client is connected / awaiting cleanup
	client_state_t current_state = atomic_load(&client_ctx->state);
	if (current_state == STATE_CONNECTED || current_state == STATE_AWAITING_CLEANUP)
		atomicio_cl_disconnect(client_ctx);

	// Clean up client mutexes
	pthread_mutex_destroy(&client_ctx->my_client_mutex);
	pthread_mutex_destroy(&client_ctx->all_clients_broadcast_mutex);

	// Close client log
	//end_log();

	// Free client context memory
	free(client_ctx);		
	return 0;
}


int atomicio_cl_send_data(atomicio_cl_t* client_ctx, message_type_t msg_type) {
	// Must have a valid client 
	if (!client_ctx) {
		fprintf(stderr, "Client must be created\n");
		return -1;
	}

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

	// Sets outgoing packet header info (type, payload len)
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


int atomicio_cl_data_update(atomicio_cl_t* client_ctx, const void* data, uint16_t data_size) {
	// Must have valid client
	if (!client_ctx) {
		fprintf(stderr, "Client must be created\n");
                return -1;
	}

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
	if (client_ctx)
		return atomic_load(&client_ctx->state) == STATE_CONNECTED;
	else
		return false;
}

int atomicio_cl_get_active_user_count(atomicio_cl_t* client_ctx) {
	return (client_ctx) ? atomic_load(&client_ctx->active_user_count) : 0;
}

uint64_t atomicio_cl_session_uptime(atomicio_cl_t* client_ctx) {
	if (!client_ctx)
		return 0;

	// Returns 0 if the client has never connected to the server
	uint64_t client_connect_epoch = atomic_load(&client_ctx->metadata.connection_epoch);	
	return (client_connect_epoch == 0) ? 0 : now_ms() - client_connect_epoch: 
}

uint64_t atomicio_cl_lifetime(atomicio_cl_t* client_ctx) {
	return (client_ctx) ? now_ms() - client_ctx->metadata.init_epoch : 0;
}







void* recv_thread(void* arg) {
	// Client context
	atomicio_cl_t* client_ctx = (atomicio_cl_t*)arg;	
        
	// Allocate temporary receive packet buffer onto the heap	
	packet_t* rx_pkt_buf = (packet_t*)malloc(sizeof(packet_t) * MAX_CONNECTIONS);	
	if (!rx_pkt_buf) {
		//errlog...
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
	
		uint16_t active_users = ntohs(rx_pkt_buf[0].active_users); // Alternatively: rx_pkt_buf->active_users
		atomic_store(&client_ctx->active_user_count, active_users);
		
		pthread_mutex_lock(&client_ctx->all_clients_broadcast_mutex);
		memcpy(client_ctx->all_clients_broadcast, rx_pkt_buf, sizeof(packet_t) * active_users);
		pthread_mutex_unlock(&client_ctx->all_clients_broadcast_mutex);	
		
		message_type_t type = (message_type_t)ntohs(rx_pkt_buf[0].type);	
		bool should_exit_loop = false;
		switch (type) {
			case LOGOUT:
				// Initiates logout sequence -> User must finalize with atomicio_cl_disconnect() call
				internal_connection_teardown(client_ctx);
				//msglog("-==SERVER REQUESTING LOGOUT==-");
				should_exit_loop = true;
				break;
			case UPDATE_MESSAGE:
				break;
			
			// Types LOGIN and invalid types cannot be sent
			default:
				// Initiates logout sequence -> User must finalize with atomicio_cl_disconnect() call
				internal_connection_teardown(client_ctx);
				//errlog("Recv", "msg parse", client_ctx->network.socket_fd, -1, "Inv msg type", client_ctx->my_client.client_uuid);
				should_exit_loop = true;
				break;
		}

		if (should_exit_loop)
			break;
	}	

	// Free allocated temp buffer and return
	free(rx_pkt_buf);
	return NULL;
}



