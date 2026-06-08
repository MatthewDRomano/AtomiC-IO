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
#include <poll.h>
#include <stdatomic.h>
#include <semaphore.h>
#include <fcntl.h>
#include <time.h>

#include "at_net.h"
#include "atomicio.h"
#include "log.h"

#define MIN_PORT 1024
#define MAX_PORT 49151
#define CLIENT_STACK_SIZE (1024 * 256)  // 256 KB

#define ATOMICIO_MAGIC_COOKIE 0x006174696F737600 // " atiosv " 



// Used to vary unnamed semaphore names per server context (for kernel)
static _Atomic(unsigned long long) sem_counter = 0ull;


// Client data struct
typedef struct client_thread {
	pthread_t thread;
	int client_fd;
	packet_t data_packet;
	atomic_bool finished;
	
	_Atomic(struct client_thread*) next;
} client_thread_t;

// Used to package multiple arguments into client io thread method
typedef struct {
	atomicio_server_ctx* server_ctx;
	client_thread_t* ct;
} cthread_args_t;

// Tracked runtime values
typedef struct {
	_Atomic(uint64_t) server_init_epoch;
	_Atomic(uint64_t) server_run_epoch;
	_Atomic(uint64_t) packets_dropped;
	_Atomic(uint64_t) late_packets;
	_Atomic(uint64_t) total_latency;
} telemetry_t;

// Server settings struct
typedef struct {
        int socket_fd; // for listening / accept only
        int max_users;
        struct sockaddr_in server;
        bool devlogs_enabled;
        bool drop_late_packets;
        char log_path[MAX_PATH_LEN];
} settings_t;


typedef enum {
	STATE_OFFLINE,
	STATE_ONLINE
} server_state_t;

// Server context struct
struct atomicio_server_ctx {
	uint64_t token;					// Used to ensure an atomicio_server_ctx* is what's passed into methods

	// Client fields
	_Atomic(client_thread_t*) clients;		// client_thread_t linked list
	pthread_mutex_t clients_mutex;			// Used to avoid races that stem from editing linked list structure
	pthread_attr_t client_attr;			// Used for limiting thread stack size
	sem_t* clients_cleanup_sem;			// Used to signal reaper thread to cleanup client resources
	char sem_name[MAX_PATH_LEN];

	// Thread IDs
	pthread_t reaper_tid;
	pthread_t accepter_tid;	
	
	// Server settings and tracked metadata
	settings_t settings;
	telemetry_t metadata;

	// Runtime status fields
	_Atomic(server_state_t) state;	
	_Atomic int connected_users;
	_Atomic bool shutdown_requested;
};






// Returns current ms
static uint64_t now_ms() {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);

        // Sec / ns conversion
        return (uint64_t)ts.tv_sec * 1000
                + ts.tv_nsec / 1000000;
}


// Signals socket connection shutdown / frees allocated client memory
// Mutex not needed, *ct assumed to be removed from global list prior to call
static void cleanup_client(client_thread_t* ct) {

	// Closes connection & fd / ensures client thread ends
	shutdown(ct->client_fd, SHUT_RDWR);
        close(ct->client_fd);
        //	//ct->client_fd = -1;

	// Avoids deadlock in other threads if join stalls
        pthread_join(ct->thread, NULL);

        // Frees associated memory / only after client thread ends
        free(ct);
}


// Handles cleaning up client_thread_t resources when set to finished
static void* reaper_thread(void* arg) {
	atomicio_server_ctx* server_ctx = (atomicio_server_ctx*)arg;	

	// Ensures a final sweep happens when the server is shutting down
	int final_sweep_flag = 1;
	while(atomic_load(&server_ctx->state) == STATE_ONLINE || final_sweep_flag-- == 1) {
	
		// Upon spurious wakeups, reaper thread safely falls through the below iteration loop back to sem_wait.
		// Thread waits until sem is posted (client marked finished)
		if (atomic_load(&server_ctx->state) == STATE_ONLINE)
			sem_wait(server_ctx->clients_cleanup_sem);

		// At this point a final sweep is already occurring
		if (atomic_load(&server_ctx->state) == STATE_OFFLINE) {
			final_sweep_flag = 0; // Prevents outer loop from allowing another sweep
		}
		
		while (sem_trywait(server_ctx->clients_cleanup_sem) == 0) { // sem_trywait returns 0 if successful decrement
			; // Drains clients_cleanup_sem to 0; Only one iteration is needed to remove ALL dead clients
		}


		// pp tracks list element addresses (manages list), ct is atomically loaded client_thread_t
		// This approach works under the assumption no other thread can add to / remove from the list asides from prepending the head
		_Atomic(client_thread_t*)* pp = &server_ctx->clients;
		client_thread_t* ct = (client_thread_t*)atomic_load(pp);

		while (ct != NULL) {
			// Removes client from clients list
			if (atomic_load(&ct->finished)) {	
				client_thread_t* next_node = (client_thread_t*)atomic_load(&ct->next);
				
				// Mutex ensures a dead client is removed before other threads access clients list
				pthread_mutex_lock(&server_ctx->clients_mutex);
				
				// This block executes if ct no longer matches *pp. Update ct and try the loop iteration again
				// exchange_strong ensures no spurious failures, and catches linked list updates due to untimely context switches
				if (!atomic_compare_exchange_strong(pp, &ct, next_node)) {
					pthread_mutex_unlock(&server_ctx->clients_mutex);
					ct = (client_thread_t*)atomic_load(pp);
					continue;
				}

				// Exchange/client removal was a success. Clean up client resources
				pthread_mutex_unlock(&server_ctx->clients_mutex);
	
				// Closes client connection and frees associated client_thread_t data
				cleanup_client(ct);
				atomic_fetch_sub(&server_ctx->connected_users, 1);	
	
			}
				
			else {
				pp = &ct->next;
			}

			// Update ct to next client			
			ct = (client_thread_t*)atomic_load(pp);

		}
		
	}	

	return NULL;
}

// Copies all client data into broadcast_buffer (heap allocated buffer with MAX_CONNECTIONS amount of packet_t's)
static int send_by_type(atomicio_server_ctx* server_ctx, packet_t* broadcast_buffer, int sock_fd, message_type_t msg_type) {
	const size_t n = sizeof(packet_t);
	int i = 0;

	pthread_mutex_lock(&server_ctx->clients_mutex);
	client_thread_t* c = (client_thread_t*)atomic_load(&server_ctx->clients);

	uint16_t net_type = htons((uint16_t)msg_type);
	while (c != NULL && i < MAX_CONNECTIONS) {
		memcpy(broadcast_buffer + i, &c->data_packet, n);

		broadcast_buffer[i].type = net_type;

		i++;
		c = (client_thread_t*)atomic_load(&c->next);
	}
	pthread_mutex_unlock(&server_ctx->clients_mutex);

	// Set user (packet) count after confirming EXACTLY how many packets are being sent
	// Also set timestamp with atomicio custom htonll endian conversion (defined in at_net.h)
	uint16_t net_act_usrs = htons((uint16_t)i);
	uint64_t net_timestamp = at_htonll(now_ms());
	for (int j = 0; j < i; j++) {
		broadcast_buffer[j].active_users = net_act_usrs;
		broadcast_buffer[j].timestamp = net_timestamp;
	}
	
	// Sends all 'i' clients' data processed above	
	return full_write(sock_fd, broadcast_buffer, i);
}


// Performs IO with client
static void* client_io_thread(void* args) {	

	// Put thread args into local variables (args format defined in static struct)
	atomicio_server_ctx* server_ctx = ((cthread_args_t*)args)->server_ctx;
	client_thread_t* ct = ((cthread_args_t*)args)->ct;

	// Allows for the send_by_type() write buffer to be on the heap instead of TLS
	packet_t* broadcast_buffer = (packet_t*)malloc(sizeof(packet_t) * MAX_CONNECTIONS);
	if (!broadcast_buffer) {
		errlog("Client", "malloc", ct->client_fd, errno, "N/A", "N/A"); // Not worried about race. ct->client_fd is effectively read only
		goto err_kill_client;
	}

	struct pollfd pfd;

	/* 
	 * Duplicates client socket file descriptor.   
	 * Ensures when connection is closed, read/write 
	 * does not persist to potential new socket assigned to client_fd 
	*/
	
	int io_fd = dup(ct->client_fd);

	// ms timestamp of last write to client
	uint64_t last_send_time = now_ms();
	
	// Temp rx_pkt_buf to read client data into. Later mutex memcpy into ct->data_packet
	packet_t rx_pkt_buf = {0};
	while (!atomic_load(&ct->finished)) {
		pfd.fd = io_fd;
        	pfd.events = POLLIN;
	
		uint64_t now = now_ms();
		uint64_t elapsed = now - last_send_time;
		int timeout_ms = 0;
		if (elapsed < NETWORK_TRANSFER_PERIOD) {
			timeout_ms = (int)(NETWORK_TRANSFER_PERIOD - elapsed);
		}
			
		// Waits for available data to read from client socket or if it is time to send data
		int ret = poll(&pfd, 1, timeout_ms);
		
		// Error during poll()
		if (ret < 0)
                        if (errno != EINTR) {
                                errlog("Client", "poll", io_fd, errno, "N/A", rx_pkt_buf.client_uuid);
                                break;
                        }

		if (ret > 0) {
			// Socket err	
			if (pfd.revents & (POLLHUP | POLLERR)) {
				//send_by_type(io_fd, LOGOUT); // socket broken
				break;
                        }
			
			// Reads from client	
			if (pfd.revents & POLLIN) {
				
				// Full read ensures number of packets read is between 1 and MAX_CONNECTIONS. (Guaranteed to be 1 here from client)
				int result = 0;
				if ((result = full_read(io_fd, &rx_pkt_buf)) != 0) {
					char dc_msg[MAX_MSG_LEN];
					snprintf(dc_msg, MAX_MSG_LEN, "DISCONNECTED: %s", rx_pkt_buf.client_uuid);
					msglog(dc_msg);	
					
					break;
				}
				

				// Checks if the packet's token is equal to AtomiC-IO's protocol magic number
				if (ntohl(rx_pkt_buf.token) != ATOMICIO_PROTOCOL_MAGIC) {
					errlog("Recv", "packet auth", io_fd, -1, "Inv auth token", rx_pkt_buf.client_uuid);
					break;			
				}


				// Parses / Handles different packet types
				bool should_disconnect = false;
				switch ((message_type_t)ntohs(rx_pkt_buf.type)) {
					case LOGIN:
						char connect_msg[MAX_MSG_LEN];
                                        	snprintf(connect_msg, MAX_MSG_LEN, "CONNECTED: %s", rx_pkt_buf.client_uuid);
                                        	msglog(connect_msg);
						break;
					case UPDATE_MESSAGE:
						break;
					// Invalid types are logged and treated as catastrophic
					default:
						errlog("Recv", "msg parse", io_fd, -1, "Inv msg type", rx_pkt_buf.client_uuid);
						should_disconnect = true;
						break;
				}	
				
				// Disconnects / shutdowns client if invalid packet type is detected
				if (should_disconnect)
					break; 

				
				// Copy inbound packet data to individual client struct
				pthread_mutex_lock(&server_ctx->clients_mutex);
                                memcpy(&ct->data_packet, &rx_pkt_buf, sizeof(packet_t));
                                pthread_mutex_unlock(&server_ctx->clients_mutex);
			}
		
		}
		
		now = now_ms();
		uint64_t delay;
		if ((delay = (now - last_send_time)) >= NETWORK_TRANSFER_PERIOD) {
			// Update server metadata (late packets / latency)
			float latency_ratio = (float)delay / NETWORK_TRANSFER_PERIOD;
			float tl = atomic_load(&server_ctx->metadata.total_latency);
			atomic_store(&server_ctx->metadata.total_latency, tl + latency_ratio);
			atomic_fetch_add(&server_ctx->metadata.late_packets, 1);

			// Devlogs for packets that reached drop threshold
			if (delay >= PACKET_DROP_THRESHOLD && server_ctx->settings.devlogs_enabled) {
				// Determine status tag based on policy
				const char* status = (server_ctx->settings.drop_late_packets) ? "[PACKETS DROPPED]" : "";
				char msg[MAX_MSG_LEN];
				snprintf(msg, MAX_MSG_LEN, "%s: %.3fx Latency %s", rx_pkt_buf.client_uuid, latency_ratio, status);
				msglog(msg);
			}
			
			// Drop packets if specified
			if (delay >= PACKET_DROP_THRESHOLD && server_ctx->settings.drop_late_packets) {
				atomic_fetch_add(&server_ctx->metadata.packets_dropped, 1);			
				last_send_time = now_ms();
				continue;
			}
			
			// Send packet to client	
			last_send_time = now;
			int result;
                        if ((result = send_by_type(server_ctx, broadcast_buffer, io_fd, UPDATE_MESSAGE)) != 0) {
                        	char dc_msg[MAX_MSG_LEN];
                                snprintf(dc_msg, MAX_MSG_LEN, "DISCONNECTED: %s", rx_pkt_buf.client_uuid);
                                msglog(dc_msg); 
                                break;
                        }
		}
	}


	// Clean up client IO resources
	free(broadcast_buffer);
	close(io_fd);

	// Goto symbol for if broadcast buffer allocation fails --> Kills client before any IO occurs
	err_kill_client:
	atomic_store(&ct->finished, true);

	free(args);	
	sem_post(server_ctx->clients_cleanup_sem);

	return NULL;
}


static void* client_accept_thread(void* arg) {
	atomicio_server_ctx* server_ctx = (atomicio_server_ctx*)arg;
	
	// Server accept loop
        atomic_store(&server_ctx->metadata.server_run_epoch, now_ms());
        while (!atomic_load(&server_ctx->shutdown_requested)) {

                struct sockaddr_in new_client;
                socklen_t adderlen = sizeof(new_client);
                int client_fd = -1;

                // Checks for accept() error / BLOCKING
                while (!atomic_load(&server_ctx->shutdown_requested) && ((client_fd = accept(atomic_load(&server_ctx->settings.socket_fd),
                        (struct sockaddr*)&new_client,
                        &adderlen)) == -1)) {

                        // Another thread closed listening fd (settings.socket_fd) break instantly
                        if (errno == EBADF)
                                break;

                        else if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS)
                                sleep(1);

                        else if (errno == ENOMEM) {
                                errlog("Main", "Accept", client_fd, errno, "No Mem", "New User");
                                atomic_store(&server_ctx->shutdown_requested, true);
                        }

                        // Depending on error, client either stays or is removed by kernel from accept queue
                        // Retry accept() always --- EMFILE or ENFILE (fd limit) and ENOBUFS or ENOMEM are network limits. Sleep & retry
                }

                // Exits server loop if terminated
                if (atomic_load(&server_ctx->shutdown_requested) || client_fd == -1)
                        break;

                // Server is full
                if (atomic_load(&server_ctx->connected_users) >= server_ctx->settings.max_users) {
                        //send_by_type(client_fd, LOGOUT); // THIS IS EXPENSIVE / TIME CONSUMING FOR SOMEONE NOT EVEN CONNECTED (abusable aswell)
                        shutdown(client_fd, SHUT_RDWR);
                        close(client_fd);
                        continue;
                }


		// On older macOS systems, setting SO_NOSIGPIPE flag is needed to ensure sigpipe is not raised on write errors due to disrupted connection
		int opt = 1;
		#ifdef SO_NOSIGPIPE
		setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &opt, sizeof(opt));
		#endif		

                // Create new client once accepted
                client_thread_t* ct = (client_thread_t*)calloc(1, sizeof(client_thread_t));
                if (!ct) {
                        errlog("Main", "calloc", -1, errno, "N/A", "N/A");
                        close(client_fd);
                        break;
                }

                // Sets file descriptor / finished to false
                ct->client_fd = client_fd;
                atomic_store(&ct->finished, false);

		// Allocate memory for client thread arguments --> server_ctx and ct
		// To be freed by the client thread upon success
		cthread_args_t* ct_thread_args = (cthread_args_t*)malloc(sizeof(cthread_args_t));
		if (!ct_thread_args) {
			errlog("Main", "malloc", -1, errno, "Client thread args allocation", "N/A");
			close(client_fd);
			free(ct);
			break;
		}

		// Set arg fields
		ct_thread_args->server_ctx = server_ctx;
		ct_thread_args->ct = ct;

                // Creates thread & starts IO
                int rc = pthread_create(&ct->thread, &server_ctx->client_attr, client_io_thread, (void*)ct_thread_args);
		if (rc == 0) {
                        // PUBLISHES TO CLIENTS ONLY ON SUCCESS
                        client_thread_t* old_head;
                        do {
                                old_head = (client_thread_t*)atomic_load(&server_ctx->clients);
                                atomic_store(&ct->next, old_head);
                                // Indivisible swap if the head hasn't changed / acts as MIPS load-linked & store-conditional
                        } while (!atomic_compare_exchange_weak(&server_ctx->clients, &old_head, ct));

                        atomic_fetch_add(&server_ctx->connected_users, 1);

                        // Prevents a client from potentially avoiding reaper
                        // Exact Situation: Client finishes and calls sem_post() before the reaper can see client in the list (because pthread_Create() before adding to list)
                        if (atomic_load(&ct->finished))
                                sem_post(server_ctx->clients_cleanup_sem);
                }

                // Failed to create thread (before adding to global list)
                else {
                        errlog("Main", "pthread_create", ct->client_fd, rc, "N/A", "N/A"); // Errno not set

                        // Manual cleanup since we cannot reap a nonexistent thread
                        close(client_fd);
                        free(ct_thread_args);
			free(ct);
                }

	}
		
	
	return NULL;	
}



// Calls in the beginning of atomicio_init_server() to make a clean slate for config
static void atomicio_config_init(atomicio_server_ctx* server_ctx) {
	
	// Clear linked list head and semaphore
        atomic_init(&server_ctx->clients, NULL);
        server_ctx->clients_cleanup_sem = NULL;

	// Set clean slate network fields
	memset(&server_ctx->settings.server, 0, sizeof(server_ctx->settings.server));
	server_ctx->settings.socket_fd = -1;
	
	// Clear runtime metrics
	atomic_init(&server_ctx->connected_users, 0);
	atomic_init(&server_ctx->metadata.server_run_epoch, -1);
	atomic_init(&server_ctx->metadata.packets_dropped, 0);
	atomic_init(&server_ctx->metadata.late_packets, 0);
	atomic_init(&server_ctx->metadata.total_latency, 0.0f);

	// Set the default core execution flags
        atomic_init(&server_ctx->shutdown_requested, false);
}



atomicio_server_ctx* atomicio_create_server(const atomicio_config_t* init_settings) {
	
	// ========================================================
	// Invalid config settings
	// ========================================================
	
	if (init_settings->port < MIN_PORT || init_settings->port > MAX_PORT) {
		fprintf(stderr, "Port not in range: [%d, %d]", MIN_PORT, MAX_PORT);
		return NULL;
	}	
	
	if (init_settings->max_users < 1 || init_settings->max_users > MAX_CONNECTIONS) {
		fprintf(stderr, "Max_users not valid. Range: [%d, %d]\n", 1, MAX_CONNECTIONS);
                return NULL;
	}


	// ========================================================
	// Server context creation --> Field instantiation
	// ========================================================

	// Allocate memory for new server context
	atomicio_server_ctx* new_server_ctx = (atomicio_server_ctx*)malloc(sizeof(atomicio_server_ctx));
	if (!new_server_ctx) {
		fprintf(stderr, "Error allocating memory for server\n");
		return NULL;
	}

	// Sets internal server data/config to clean slate
	atomicio_config_init(new_server_ctx);

	// Sets server settings based on user args - fields zeroed out by default
	new_server_ctx->settings.max_users = init_settings->max_users;
	new_server_ctx->settings.devlogs_enabled = init_settings->devlogs_enabled;	
	new_server_ctx->settings.drop_late_packets = init_settings->drop_late_packets;
	new_server_ctx->settings.server.sin_family = AF_INET;
        new_server_ctx->settings.server.sin_port = htons(init_settings->port);
        new_server_ctx->settings.server.sin_addr.s_addr = INADDR_ANY;
	snprintf(new_server_ctx->settings.log_path, MAX_PATH_LEN, "%s", init_settings->log_path); // MAX_PATH_LEN defined in log.h
	
	// Creates stack size attribute for client threads --> stack + TLS (256KB)
        int rc = pthread_attr_init(&new_server_ctx->client_attr);
	if (rc != 0) {
		fprintf(stderr, "Error initializing pthread attribute: %d\n", rc);
		goto err_free_server_mem;
	}
	
	rc = pthread_attr_setstacksize(&new_server_ctx->client_attr, CLIENT_STACK_SIZE);
        if (rc != 0) {
                fprintf(stderr, "Error setting client thread stack size: %d\n", rc);
                goto err_destroy_thread_attr;
        }

	// Initialize the clients linked list mutex
	rc = pthread_mutex_init(&new_server_ctx->clients_mutex, NULL);
	if (rc != 0) {
		fprintf(stderr, "Error initializing pthread_mutex: %d\n", rc);
		goto err_destroy_thread_attr;
	}
	


        // Creates semaphore w/ owner read / write, otherwise read only
	unsigned long long unique_sem_id = atomic_fetch_add(&sem_counter, 1ull);
	snprintf(new_server_ctx->sem_name, MAX_PATH_LEN, "/reaper_sem%llu", unique_sem_id); // MacOs and Linux differ in Posix named semaphore name lengths. (Some MacOs versions allow 31 chars max)
        
	new_server_ctx->clients_cleanup_sem = sem_open(new_server_ctx->sem_name, O_CREAT, 0644, 0);
        if (new_server_ctx->clients_cleanup_sem == SEM_FAILED) {
                perror("Failed to create client cleanup semaphore");
		// Do not subtract sem_count, just burn the id value
                goto err_destroy_mutex;
        }


        // Error opening error log; treated as catastrophic
        if (init_log(new_server_ctx->settings.log_path) != 0)
                goto err_close_sem;

	
	// No error --> set proper state and stamp server context token
	atomic_init(&new_server_ctx->state, STATE_OFFLINE);
	new_server_ctx->token = ATOMICIO_MAGIC_COOKIE;
	atomic_init(&new_server_ctx->metadata.server_init_epoch, now_ms());
	
	return new_server_ctx;


	// ========================================================
	// Cascading error resource cleanup. Resets anything done.
	// ========================================================


	// Closes semaphore
	err_close_sem:
	// Unlinks named semaphore from kernel
	sem_unlink(new_server_ctx->sem_name); 		// NEED TO ADD SEM_NAME TO CONTEXT AND UNLINK WHENEVER DESTROY
	sem_close(new_server_ctx->clients_cleanup_sem);
	
	err_destroy_mutex:
	pthread_mutex_destroy(&new_server_ctx->clients_mutex);

	err_destroy_thread_attr:
	pthread_attr_destroy(&new_server_ctx->client_attr);

	// Finally free allocated context and return
	err_free_server_mem:
	free(new_server_ctx);	

	return NULL;
}


int atomicio_server_run(atomicio_server_ctx* server_ctx) {
	
	// ========================================================
	// Invalid conditions to begin running server
	// ========================================================

	if (!server_ctx || server_ctx->token != ATOMICIO_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error
	
	if (atomic_load(&server_ctx->state) == STATE_ONLINE) {
		fprintf(stderr, "Server already running!!!\n");
		return -1;
	}
	
	
	// ========================================================
	// Server resources finalized and run begins
	// ========================================================


	// Creates IPv4 TCP default protocol socket
        int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd == -1) {
                perror("Socket creation failed");
                return -1;
        }


        // Copy fd to global settings
        atomic_store(&server_ctx->settings.socket_fd, listen_fd);

        // SO_REUSEADDR Allows rebind to same port after server termination w/o a delay
        // regardless if clients in TIME_WAIT or not
        int opt = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

	// Required for macOS quick server restarts on wildcard address (INADDR_ANY a.k.a 0.0.0.0)
	// MacOS vs Linux posix implementation 
	#ifdef SO_REUSEPORT
	setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
	#endif


        // Binds server to socket w/ above socket config
        while (bind(listen_fd, (struct sockaddr*)(&server_ctx->settings.server), sizeof(server_ctx->settings.server)) == -1) {
                if (errno == EINTR)
                        continue;
                perror("Bind to port failed");
                goto err_close_server_rsrcs;
        }
		
	// Starts listening on socket
        while (listen(server_ctx->settings.socket_fd, server_ctx->settings.max_users) == -1) {
                if (errno == EINTR)
                        continue;
                perror("Error while attmepting to listen on socket");
                goto err_close_server_rsrcs;
        }
	
	// Server is "running"
        atomic_store(&server_ctx->state, STATE_ONLINE);
        fprintf(stdout, "Server Listening on port: %d\n", ntohs(server_ctx->settings.server.sin_port));
        fprintf(stdout, "Max users: %d\n", server_ctx->settings.max_users);
        fflush(stdout);

        // Spawns reaper thread to cleanup dead client threads
        if (pthread_create(&server_ctx->reaper_tid, NULL, reaper_thread, server_ctx) != 0) {
                perror("Failed to spawn reaper thread");
                //atomic_store(&settings.running, false);
		goto err_close_server_rsrcs;
        }



	if (pthread_create(&server_ctx->accepter_tid, NULL, client_accept_thread, server_ctx) != 0) {
		perror("Failed to spawn main accept thread");
		atomic_store(&server_ctx->state, STATE_OFFLINE);
		goto err_kill_reaper;	
	}
	

	// Successfully begins run
	atomic_store(&server_ctx->metadata.server_run_epoch, now_ms());
	return 0;



	// ========================================================
	// Cascading error resources cleanup upon failed run
	// ========================================================


	// Kills reaper thread and joins all client threads
	err_kill_reaper:
	sem_post(server_ctx->clients_cleanup_sem);
        pthread_join(server_ctx->reaper_tid, NULL);
        sem_close(server_ctx->clients_cleanup_sem);	
	
	// Closes log and listening fd
	err_close_server_rsrcs:
	end_log();
	int fd_to_close = atomic_exchange(&server_ctx->settings.socket_fd, -1);
	if (fd_to_close >= 0)
		close(fd_to_close);

	// Ensures init flag is false to allow subsequent server init/run
	atomic_store(&server_ctx->state, STATE_OFFLINE);	
	
	// Returns on failure	
	return -1;		
}


int atomicio_server_shutdown(atomicio_server_ctx* server_ctx) {

	// Valid context arg check
	if (!server_ctx || server_ctx->token != ATOMICIO_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error

	// Ensures proper server state
	if (atomic_load(&server_ctx->state) == STATE_OFFLINE) {
		fprintf(stderr, "Cannot shutdown, as server is not connected\n");
		return -1;
	}

	// ========================================================
	// Proper shutdown sequence of a running server below
	// ========================================================	


	// Set exit flag for main accept thread -> Begins shutdown
	atomic_store(&server_ctx->shutdown_requested, true);		

	// Close listening client to break blocking accept() loop
	int fd_to_close = atomic_exchange(&server_ctx->settings.socket_fd, -1);
	if (fd_to_close >= 0) {
		shutdown(fd_to_close, SHUT_RDWR); // needed to wake accept() block
		close(fd_to_close);
	}


	// Waits for accepter thread to finish 
	pthread_join(server_ctx->accepter_tid, NULL);
	

	// ========================================================
        // Clean up server resources and reset global fields
        // ========================================================


        // Server is closing / finishes all threads & shuts down connection for reaper to join
        pthread_mutex_lock(&server_ctx->clients_mutex);
        client_thread_t* ct = (client_thread_t*)atomic_load(&server_ctx->clients);
        while (ct != NULL) {
                //send_by_type(c->client_fd, LOGOUT);
                atomic_store(&ct->finished, true);
                //shutdown(c->client_fd, SHUT_RDWR);
		ct = (client_thread_t*)atomic_load(&ct->next);
        }
        pthread_mutex_unlock(&server_ctx->clients_mutex);

        // Shutdown requested. Updates running value to false for all threads
        atomic_store(&server_ctx->state, STATE_OFFLINE);


        // Kills reaper thread and joins all client threads
        sem_post(server_ctx->clients_cleanup_sem);
        pthread_join(server_ctx->reaper_tid, NULL);
        //sem_close(server_ctx->clients_cleanup_sem); PUT IN DESTROY METHOD

	// Defensive store to ensure linked list is not pointing to garbage data
	atomic_store(&server_ctx->clients, NULL);

        // Destroys client thread attribute
        //pthread_attr_destroy(&server_ctx->client_attr); PUT IN DESTROY METHOD

	// RESET shutdown flag to allow subsequent runs
	atomic_store(&server_ctx->shutdown_requested, false);

        // Close log
        //end_log(); PUT IN DESTROY METHOD

	return 0;
}


int atomicio_server_destroy(atomicio_server_ctx** server_ctx_ptr) {
	if (!server_ctx_ptr)
		return -1;

	atomicio_server_ctx* server_ctx = *server_ctx_ptr;	

	// Ensures a valid server is passed
	if (!server_ctx || server_ctx->token != ATOMICIO_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error

	// Shutsdwon server if necessary 
	if (atomic_load(&server_ctx->state) == STATE_ONLINE)
		atomicio_server_shutdown(server_ctx);

	// Destroy server context internal data
	pthread_mutex_destroy(&server_ctx->clients_mutex);
	pthread_attr_destroy(&server_ctx->client_attr);
	
	// Unlinks named semaphore from kernel and closes it
	sem_unlink(server_ctx->sem_name);
	sem_close(server_ctx->clients_cleanup_sem);
	
	// Close log instance
	end_log();

	// Brick the magic cookie to invalidate stale pointers
	server_ctx->token = 0ull;

	free(server_ctx);
	*server_ctx_ptr = NULL;
	return 0;
}



// Returns true if server is running, otherwise false
bool atomicio_is_running(atomicio_server_ctx* server_ctx) {
	if (!server_ctx || server_ctx->token != ATOMICIO_MAGIC_COOKIE)
		return false; // Returns false on structural API error
	
	return atomic_load(&server_ctx->state) == STATE_ONLINE;
}

// Logs a message
void atomicio_log(const char* msg) {

	// Passes along log message
	msglog(msg);		
}


// Returns current user count
int atomicio_get_active_user_count(atomicio_server_ctx* server_ctx) {
	if (!server_ctx || server_ctx->token != ATOMICIO_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error

	return atomic_load(&server_ctx->connected_users);
}

// Returns overall server context uptime
int64_t atomicio_get_overall_uptime_ms(atomicio_server_ctx* server_ctx) {
	if (!server_ctx || server_ctx->token != ATOMICIO_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error


	return now_ms() - atomic_load(&server_ctx->metadata.server_init_epoch);
}

// Returns current session server context up time
int64_t atomicio_get_session_uptime_ms(atomicio_server_ctx* server_ctx) {
	if (!server_ctx || server_ctx->token != ATOMICIO_MAGIC_COOKIE)
		return -1; // Returns -1 on structural API error
	
	// Server not running. Uptime is 0
        if (atomic_load(&server_ctx->state) == STATE_OFFLINE)
                return 0;

	return now_ms() - atomic_load(&server_ctx->metadata.server_run_epoch);	
}

float atomicio_get_avg_latency_multiplier(atomicio_server_ctx* server_ctx) {
	if (!server_ctx || server_ctx->token != ATOMICIO_MAGIC_COOKIE)
                return 1.0f; // Returns default 1.0 on structural API error

	uint64_t late_pkts = atomic_load(&server_ctx->metadata.late_packets);

	// Returns avg latency or baseline latency of 1.0f if 0 late packets
	return (late_pkts == 0) ? 1.0f : (float)atomic_load(&server_ctx->metadata.total_latency) / late_pkts;
}

// Returns total packets dropped
int64_t atomicio_get_dropped_packets_count(atomicio_server_ctx* server_ctx) {
	if (!server_ctx || server_ctx->token != ATOMICIO_MAGIC_COOKIE)
                return -1; // Returns -1 on structural API error

	return atomic_load(&server_ctx->metadata.packets_dropped);
}	
