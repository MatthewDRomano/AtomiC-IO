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
#define CLIENT_STACK (1024 * 1024)  // 1 MB

typedef struct client_thread {
	pthread_t thread;
	int client_fd;
	packet_t net_msg;
	atomic_bool finished;
	
	_Atomic(struct client_thread*) next; // Atomic ptr
} client_thread_t;

static _Atomic(client_thread_t*) clients = NULL;
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;
static sem_t* client_cleanup_sem;


typedef struct {
	atomic_int connected_users;
	atomic_bool running;
	atomic_bool initialized;
	atomic_int socket_fd; // for listening only
	int max_users;
	struct sockaddr_in server;
	bool devlogs_enabled;
	bool drop_late_packets;
	char* log_path;
} settings_t;

// Internal settings struct / fields set to 0
static settings_t settings = {0};

// Atomic shutdown flag - thread safe and natively lock-free (async safe) on most systems
static _Atomic bool shutdown_requested = false;



// Checks for finished clients to remove
bool has_dead_clients() {
	// Grabs a snapshot of the current list (list cannot be changed during loop as this method halts reaper)
	client_thread_t* ct = atomic_load(&clients);
	while (ct != NULL) {
		if (atomic_load(&ct->finished)) 
			return true;

		ct = (client_thread_t*)atomic_load(&ct->next);
	}
	
	return false;
}


// Signals socket connection shutdown / frees allocated client memory
// Mutex not needed, *ct assumed to be removed from global list prior to call
void cleanup_client(client_thread_t* ct) {

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
void* reaper_thread(void* arg) {
	
	while(atomic_load(&settings.running)) {
	
		// Ignores spurious wake ups	
		while (!has_dead_clients()) {
			sem_wait(client_cleanup_sem);

			// Ensures reaper does not infinitely sleep upon shutdown with no clients
			if (!atomic_load(&settings.running) && atomic_load(&settings.connected_users) == 0)
				break;
		}
		
		while (sem_trywait(client_cleanup_sem) == 0) { // sem_trywait returns 0 if successful decrement
			; // Drains client_cleanup_sem to 0; Only one iteration is needed to remove ALL dead clients
		}


		// pp tracks list element addresses (manages list), ct is atomically loaded client_thread_t
		_Atomic(client_thread_t*)* pp = &clients;
		client_thread_t* ct = (client_thread_t*)atomic_load(pp);

		while (ct != NULL) {
			// Removes client from clients list
			if (atomic_load(&ct->finished)) {	
				client_thread_t* next_node = (client_thread_t*)atomic_load(&ct->next);
				
				// Mutex ensures a dead client is removed before other threads access clients list
				pthread_mutex_lock(&clients_mutex);
				if (!atomic_compare_exchange_weak(pp, &ct, next_node)) {
					ct = (client_thread_t*)atomic_load(pp);
					pthread_mutex_unlock(&clients_mutex);
					continue;
				}
				pthread_mutex_unlock(&clients_mutex);
	
                               	errlog("Reaper", "--DISCONNECT--", -1, -1, "User dc", ct->net_msg.username);
	
				// Closes client connection and frees associated client_thread_t data
				cleanup_client(ct);
				atomic_fetch_sub(&settings.connected_users, 1);	
	
			}
				
			else {
				pp = &ct->next;
			}
			
			ct = (client_thread_t*)atomic_load(pp);

		}
		
	}	

	return NULL;
}

// Assumes clients mutex unlocked
int send_by_type(int sock_fd, message_type_t msg_type) {
	packet_t msg[MAX_CONNECTIONS] = {0};
	const size_t n = sizeof(packet_t);
	int i = 0;

	pthread_mutex_lock(&clients_mutex);
	client_thread_t* c = (client_thread_t*)atomic_load(&clients);
	while (c != NULL && i < MAX_CONNECTIONS) {
		memcpy(msg + (i++), &c->net_msg, n);
		c = (client_thread_t*)atomic_load(&c->next);
	}
	pthread_mutex_unlock(&clients_mutex);
	
	msg[0].type = (uint8_t)msg_type;
	return full_write(sock_fd, msg, MAX_CONNECTIONS);
}

// Returns current ms
uint64_t now_ms() {
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
		
	// Sec / ns conversion
	return (uint64_t)ts.tv_sec * 1000
		+ ts.tv_nsec / 1000000;
}


// Performs IO with client
void* client_io_thread(void* arg) {	
	client_thread_t* ct = (client_thread_t*)arg;	

	struct pollfd pfd;

	/* 
	 * Duplicates client socket file descriptor.   
	 * Ensures when connection is closed, read/write 
	 * does not persist to potential new socket assigned to client_fd 
	*/
	
	int io_fd = dup(ct->client_fd);

	// ms timestamp of last write to client
	uint64_t last_send_time = now_ms();
	
	// Temp buffer to read client data into. Later mutex memcpy into ct->net_msg
	packet_t buf;
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
                                errlog("Client", "poll", io_fd, errno, "N/A", buf.username);
                                atomic_store(&ct->finished, true);
                                break;
                        }

		if (ret > 0) {
			// Socket err	
			if (pfd.revents & (POLLHUP | POLLERR)) {
				send_by_type(io_fd, LOGOUT);
				atomic_store(&ct->finished, true);
				break;
                        }
			
			// Reads from client	
			if (pfd.revents & POLLIN) {
				int result = 0;
				if ((result = full_read(io_fd, &buf, 1)) != 0) {
					const char* err_status = (result == EOF) ? "User dc (EOF)" : "N/A";
					
					errlog("Client", "read", io_fd, result, err_status, buf.username);
					
					atomic_store(&ct->finished, true);
					break;
				}
				
				pthread_mutex_lock(&clients_mutex);
				memcpy(&ct->net_msg, &buf, sizeof(packet_t));
				pthread_mutex_unlock(&clients_mutex);
				

				switch ((message_type_t)buf.type) {
					case LOGIN:
						errlog("Client", "--JOIN--", -1, -1, "User connected", buf.username);
						break;
					case UPDATE_MESSAGE:
						break;
					// LOGOUT and invalid types are logged but disregarded	
					default:
						errlog("Recv", "msg parse", io_fd, -1, "Inv msg type", buf.username);
						break;
				}	
			}
		
		}
		
		now = now_ms();
		uint64_t delay;
		if ((delay = (now - last_send_time)) >= NETWORK_TRANSFER_PERIOD) {
			// Devlogs for late packets
			float latency_ratio = (float)delay / NETWORK_TRANSFER_PERIOD;
			if (delay >= PACKET_DROP_THRESHOLD && settings.devlogs_enabled) {
				// Determine status tag based on policy
				const char* status = (settings.drop_late_packets) ? "[PACKETS DROPPED]" : "";
				char msg[128];
				snprintf(msg, 128, "%s: %.3fx Latency %s", buf.username, latency_ratio, status);
				msglog(msg);
			}
			
			// Drop packets if specified
			if (delay >= PACKET_DROP_THRESHOLD && settings.drop_late_packets) {
				last_send_time = now_ms();
				continue;
			}
			
			// Send to client	
			last_send_time = now;
			int result = 0;
                        if ((result = send_by_type(io_fd, UPDATE_MESSAGE)) != 0) {
        	                errlog("Client", "write", io_fd, result, "User dc", buf.username);
                                atomic_store(&ct->finished, true);
                                break;
                        }
		}
	}
	
	close(io_fd);
	sem_post(client_cleanup_sem);

	return NULL;
}

// Allows graceful shutdown for SIGINT / SIGTERM
void shutdown_handler(int signum) {
	atomic_store(&shutdown_requested, true);
	//close(settings.socket_fd); // Interrupts accept()
}

// Calls in the beginning of atomicio_init_server() to make a clean slate for config
void atomicio_config_reset() {
	// Reset the core execution flags
	atomic_store(&shutdown_requested, false);
	atomic_store(&settings.running, false);
	atomic_store(&settings.initialized, false);

	// Reset runtime metrics
	atomic_store(&settings.connected_users, 0);
	atomic_store(&settings.socket_fd, -1);

	// Clear linked list head and semaphore
	atomic_store(&clients, NULL);	
	client_cleanup_sem = NULL;	
}

void atomicio_sighandlers_reset() {
	struct sigaction sa_def;
	sa_def.sa_handler = SIG_DFL;
	sigemptyset(&sa_def.sa_mask);
	sa_def.sa_flags = 0;
	sigaction(SIGINT, &sa_def, NULL);
	sigaction(SIGTERM, &sa_def, NULL);
	sigaction(SIGHUP, &sa_def, NULL);
	sigaction(SIGPIPE, &sa_def, NULL);
}


int atomicio_init_server(const atomicio_config_t* init_settings) {
	
	// ========================================================
	// Invalid conditions to begin initialization
	// ========================================================
	
	if (atomic_load(&settings.running) || atomic_load(&settings.initialized)) {
                fprintf(stderr, "Server already running and/or initialized\n");
		return -1;
	}

	// Invalid fields
	if (init_settings->port < MIN_PORT || init_settings->port > MAX_PORT) {
		fprintf(stderr, "Port not in range: [%d, %d]", MIN_PORT, MAX_PORT);
		return -1;
	}	
	
	if (init_settings->max_users < 1 || init_settings->max_users > MAX_CONNECTIONS) {
		fprintf(stderr, "Max_users not valid. Range: [%d, %d]\n", 1, MAX_CONNECTIONS);
                return -1;
	}


	// ========================================================
	// Initialize global fields and server resources
	// ========================================================


	// Resets internal server data to clean slate
	atomicio_config_reset();
	
	// Handles termination / def flags
	struct sigaction shutdown_sa = {0};
        shutdown_sa.sa_handler = shutdown_handler;
        sigemptyset(&shutdown_sa.sa_mask); // Init mask to empty

	sigaction(SIGINT, &shutdown_sa, NULL);
	sigaction(SIGTERM, &shutdown_sa, NULL);

	struct sigaction ign = {0};
        ign.sa_handler = SIG_IGN;
        // Ensures Server stays open if terminal session closes
	sigaction(SIGHUP, &ign, NULL);
        // Ensures failed write() sets errno EPIPE and returns -1 instead of crashing with SIGPIPE
        sigaction(SIGPIPE, &ign, NULL);


	// Sets server settings - fields zeroed out by default
	settings.max_users = init_settings->max_users;
	settings.devlogs_enabled = init_settings->devlogs_enabled;	
	settings.drop_late_packets = init_settings->drop_late_packets;
	settings.server.sin_family = AF_INET;
        settings.server.sin_port = htons(init_settings->port);
        settings.server.sin_addr.s_addr = INADDR_ANY;
	// snprintf(settings.log_path, 128, "%s", init_settings->log_path);
	
	// IPv4 TCP default protocol
	int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd == -1) {
                perror("Socket creation failed");
                return -1;
        }
	
	// Copy fd to global settings 
	atomic_store(&settings.socket_fd, listen_fd);

	// Allows rebind to same port after server termination w/o delay
        // regardless if clients in TIME_WAIT or not
        int opt = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        // Binds server to socket
        while (bind(listen_fd, (struct sockaddr*)(&settings.server), sizeof(settings.server)) == -1) {
                if (errno == EINTR)
                        continue;
                perror("Bind to port failed");
                goto err_close_socket;
        }

        // Starts listening on socket
        while (listen(listen_fd, settings.max_users) == -1) {
                if (errno == EINTR)
                        continue;
                perror("Error while attmepting to listen on socket");
                goto err_close_socket;
        }


        // Creates semaphore w/ owner read / write, otherwise read only
        client_cleanup_sem = sem_open("/client_cleanup_sem", O_CREAT, 0644, 0);
        if (client_cleanup_sem == SEM_FAILED) {
                perror("Failed to create client cleanup semaphore");
                goto err_close_socket;
        }
        // Unlinks named semaphore from kernel
        // Semaphore now persists for server lifetime instead of in kernel indefinitely
        sem_unlink("/client_cleanup_sem");

        // Error opening error log; treated as catastrophic
        if (init_log("Server") != 0)
                goto err_close_log;

	
	// No error
	atomic_store(&settings.initialized, true);
	return 0;


	// ========================================================
	// Cascading error resource cleanup. Resets anything done.
	// ========================================================


	// Closes log
	err_close_log:
	end_log();
	sem_close(client_cleanup_sem);
	
	// Closes socket - safely scrubs global state if initialization fails
	err_close_socket:
	int fd_to_close = atomic_exchange(&settings.socket_fd, -1);
	if (fd_to_close >= 0)
		close(fd_to_close);
	
	// Returns with error after resetting signal handlers
	atomicio_sighandlers_reset();
	return -1;
}


int atomicio_run() {
	// ========================================================
	// Invalid conditions to begin running server
	// ========================================================
	
	if (atomic_load(&settings.running)) {
		fprintf(stderr, "Server already running\n");
		return -1;
	}
	
	if (!atomic_load(&settings.initialized)) {
		fprintf(stderr, "Server not initialized\n");
		return -1;
	}
	
	// ========================================================
	// Server resources finalized and run begins
	// ========================================================
	
	// Used to ensure proper return value	
	bool run_error = false;
	
	// Creates stack size attribute for client threads (1MB)
        pthread_attr_t client_attr;
        pthread_attr_init(&client_attr);
        if (pthread_attr_setstacksize(&client_attr, CLIENT_STACK) != 0) {
                fprintf(stderr, "Error setting client thread stack size\n");
                run_error = true;
		goto err_destroy_thread_resources;
        }
	
	// Server is "running"
        atomic_store(&settings.running, true);
        fprintf(stdout, "Server Listening on port: %d\n", ntohs(settings.server.sin_port));
        fprintf(stdout, "Max users: %d\n", settings.max_users);
        fflush(stdout);

        // Spawns reaper thread to cleanup dead client threads
        pthread_t reaper;
        if (pthread_create(&reaper, NULL, reaper_thread, NULL) != 0) {
                perror("Failed to spawn reaper thread");
                atomic_store(&settings.running, false);
                run_error = true;
		goto err_kill_reaper;
        }


	// Server loop
        while (!atomic_load(&shutdown_requested)) {

                struct sockaddr_in new_client;
                socklen_t adderlen = sizeof(new_client);
                int client_fd;

                // Checks for accept() error / BLOCKING
                while (!atomic_load(&shutdown_requested) && ((client_fd = accept(atomic_load(&settings.socket_fd),
                        (struct sockaddr*)&new_client,
                        &adderlen)) == -1)) {
	
			// Another thread closed listening fd (settings.socket_fd) break instantly
			if (errno == EBADF)
				break;
	
                        if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM)
                                sleep(1);

                        // Depending on error, client either stays or is removed by kernel from accept queue
                        // Retry accept() always --- EMFILE or ENFILE (fd limit) and ENOBUFS or ENOMEM are network limits. Sleep & retry
                }

                // Exits server loop if terminated
                if (atomic_load(&shutdown_requested) || client_fd == -1)
                        break;

                // Server is full
                if (atomic_load(&settings.connected_users) >= settings.max_users) {
                        //send_by_type(client_fd, LOGOUT); // THIS IS EXPENSIVE / TIME CONSUMING FOR SOMEONE NOT EVEN CONNECTED (abusable aswell)
                        shutdown(client_fd, SHUT_RDWR);
                        close(client_fd);
                        continue;
                }

                // Create new client once accepted
                client_thread_t* ct = (client_thread_t*)calloc(1, sizeof(client_thread_t));
                if (!ct) {
                        errlog("Main", "Calloc", -1, errno, "N/A", "N/A");
                        close(client_fd);
                        break;
                }

                // Sets file descriptor
                ct->client_fd = client_fd;
                atomic_store(&ct->finished, false);
			
                // Creates thread & starts IO
                if (pthread_create(&ct->thread, &client_attr, client_io_thread, ct) == 0) {
                        // PUBLISHES TO CLIENTS ONLY ON SUCCESS
                        client_thread_t* old_head;
                        do {
                                old_head = (client_thread_t*)atomic_load(&clients);
                                atomic_store(&ct->next, old_head);
                                // Indivisible swap if the head hasn't changed / acts as MIPS load-linked & store-conditional
                        } while (!atomic_compare_exchange_weak(&clients, &old_head, ct));

                        atomic_fetch_add(&settings.connected_users, 1);

                        // Prevents a client from potentially avoiding reaper
                        // Exact Situation: Client finishes and calls sem_post() before the reaper can see client in the list (because pthread_Create() before adding to list)
                        if (atomic_load(&ct->finished))
                                sem_post(client_cleanup_sem);
                }

                // Failed to create thread (before adding to global list)
                else {
                        errlog("Main", "pthread_create", ct->client_fd, errno, "N/A", "N/A");

                        // Manual cleanup since we cannot reap a nonexistent thread
                        close(client_fd);
                        free(ct);
                }

        }	


	// ========================================================
	// Clean up server resources and reset global fields
	// ========================================================


        // Server is closing / finishes all threads for reaper to join
        pthread_mutex_lock(&clients_mutex);
        client_thread_t* c = (client_thread_t*)atomic_load(&clients);
        while (c != NULL) {
                //send_by_type(c->client_fd, LOGOUT);
                atomic_store(&c->finished, true);
                c = (client_thread_t*)atomic_load(&c->next);
        }
        pthread_mutex_unlock(&clients_mutex);

	// Shutdown requested. Updates running value to false for all threads
	atomic_store(&settings.running, false);	



	// Kills reaper thread and joins all client threads
	err_kill_reaper:
	sem_post(client_cleanup_sem);
        pthread_join(reaper, NULL);
        sem_close(client_cleanup_sem);	
	
	// Destroys client thread attribute
	err_destroy_thread_resources:
        pthread_attr_destroy(&client_attr);

		
	// Core library systems always close
	end_log();
	int fd_to_close = atomic_exchange(&settings.socket_fd, -1);
	if (fd_to_close >= 0)
		close(fd_to_close);

	// Sets init flag to false / resets sig handlers to allow subsequent server init/run
	atomic_store(&settings.initialized, false);	
	atomicio_sighandlers_reset();
	
	// Return success (0) or failure (-1) based on flags
	return (run_error) ? -1 : 0;		
}


int atomicio_shutdown() {
	if (!atomic_load(&settings.running))
		return -1;

	// Set exit flag
	atomic_store(&shutdown_requested, true);		

	// Close listening client to break blocking accept() loop
	int fd_to_close = atomic_exchange(&settings.socket_fd, -1);
	if (fd_to_close >= 0) {
		close(fd_to_close);
		return 0; // Successful closure
	}
	
	return -1; // Already closed in a parallel call
}


void atomicio_log(char* msg) {
	if (!atomic_load(&settings.initialized)) {
		fprintf(stderr, "Log is not open. Must initialize AtomiC-IO server\n");
		return;
	}

	// Passes along log message
	msglog(msg);		
}
