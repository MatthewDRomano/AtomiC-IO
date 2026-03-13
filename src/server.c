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

#include "anera_net.h"
#include "log.h"

#define MIN_PORT 1024
#define MAX_PORT 49151
#define CLIENT_STACK (1024 * 1024)  // 1 MB

typedef struct client_thread {
	pthread_t thread;
	int client_fd;
	user_data_t net_msg;
	atomic_bool finished;
	
	struct client_thread *next;
} client_thread_t;

static client_thread_t *clients = NULL;
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;
static sem_t* client_cleanup_sem;


typedef struct {
	atomic_int connected_players;
	atomic_bool running;
	int socket_fd; // for listening only
	int max_players;
	struct sockaddr_in server;

} settings_t;

static settings_t settings;
// Volatile ignores compiler opitmizations due to updating in async signal handler
static volatile sig_atomic_t shutdown_requested = 0;

void init_def_settings() {
	settings.server.sin_family = AF_INET;
	settings.server.sin_port = htons(DEFAULT_PORT);
	settings.server.sin_addr.s_addr = INADDR_ANY;

	settings.socket_fd = 0;
	settings.connected_players = ATOMIC_VAR_INIT(0);
	settings.running = ATOMIC_VAR_INIT(false);
	settings.max_players = MAX_CONNECTIONS;

}



int parse_args(int argc, char *argv[]) {
	
	for (int i = 1; i < argc; i++) {
		// Usage menu
		if (strcmp("-h", argv[i]) == 0 || strcmp("--help", argv[i]) == 0) {
			fprintf(stdout, "usage: ./server [-h] [--port PORT] [--max-players PLAYER_COUNT]\n");
			exit(0);
		}

		else if	(strcmp("--port", argv[i]) == 0) {
			// Missing arg
			if (++i == argc) {
				fprintf(stderr, "Missing port\n");
				return -1;
			}
			
			char *end;
			long int port = strtol(argv[i], &end, 10);
			
			// Invalid Port
			if (port < MIN_PORT || port > MAX_PORT || *end != '\0') {
				fprintf(stderr, "Invalid Port (Must be %d-%d)\n", MIN_PORT, MAX_PORT);
				return -1;
			}

			settings.server.sin_port = htons((uint16_t)port);
		}

		else if (strcmp("--max-players", argv[i]) == 0) {
			// Missing arg
			if (++i == argc) {
				fprintf(stderr, "Missing max player count\n");
				return -1;
			}

			char *end;
			long int n = strtol(argv[i], &end, 10);
			
			// Invalid player count
			if (n < 1 || n > MAX_CONNECTIONS || *end != '\0') {
				fprintf(stderr, "Invalid max player count (Must be %d-%d)\n", 1, MAX_CONNECTIONS);
				return -1;
			}

			settings.max_players = (int)n;
		}

		else {
			fprintf(stderr, "Invalid argument \"%s\"\n", argv[i]);
			fprintf(stdout, "usage: ./server [-h] [--port PORT] [--max-players PLAYER_COUNT]\n");
			return -1;
		}
	}

	return 0;
}


// Checks for finished clients to remove / needs mutex
bool has_dead_clients() {
	pthread_mutex_lock(&clients_mutex);
	client_thread_t *ct = clients;
	while (ct != NULL) {
		if (atomic_load(&ct->finished)) {
			pthread_mutex_unlock(&clients_mutex);
			return true;
		}
		ct = ct->next;
	}
	
	pthread_mutex_unlock(&clients_mutex);
	return false;
}


// Removes client from clients pointer list / frees calloced data
// Assumes clients mutex is locked upon call
void cleanup_client(client_thread_t* ct) {

	// Closes connection / fd
	shutdown(ct->client_fd, SHUT_RDWR);
        close(ct->client_fd);
        ct->client_fd = -1;

	// Avoids deadlock in other threads if join stalls
        pthread_mutex_unlock(&clients_mutex);
        pthread_join(ct->thread, NULL);
        pthread_mutex_lock(&clients_mutex);

        // Frees associated memory / only after client thread ends
        free(ct);
}


// Handles cleaning up client_thread_t resources when set to finished
void* reaper_thread(void *arg) {
	
	while(atomic_load(&settings.running)) {
	
		// Ignores spurious wake ups	
		while (!has_dead_clients()) {
			sem_wait(client_cleanup_sem);

			// Ensures reaper does not infinitely sleep upon shutdown with no clients
			if (!atomic_load(&settings.running) && atomic_load(&settings.connected_players) == 0)
				break;
		}
		
		while (sem_trywait(client_cleanup_sem) == 0) { // sem_trywait returns 0 if successful decrement
			; // Drains client_cleanup_sem to 0; Only one iteration is needed to remove ALL dead clients
		}

		pthread_mutex_lock(&clients_mutex);
		client_thread_t **pp = &clients;
		while (*pp != NULL) {
			client_thread_t *ct = *pp;
			
			if (atomic_load(&ct->finished)) {
				// Removes client from clients list
				*pp = ct->next;
	
				// Prints user disconnected
                                errlog("Reaper", "--DISCONNECT--", -1, -1, "Player dc", ct->net_msg.username);
				
				// Closes client connection and frees associated client_thread_t data
				cleanup_client(ct);
	
				// Updates global connected_players counter	
				atomic_fetch_sub(&settings.connected_players, 1);	
			}
				
			else
				pp = &ct->next;

		}
		
		pthread_mutex_unlock(&clients_mutex);
	}	

	return NULL;
}

// Assumes clients mutex unlocked
int send_by_type(int sock_fd, message_type_t msg_type) {
	user_data_t msg[MAX_CONNECTIONS] = {0};
	const size_t n = sizeof(user_data_t);
	int i = 0;

	client_thread_t *c = clients;
	pthread_mutex_lock(&clients_mutex);
	while (c != NULL && i < MAX_CONNECTIONS) {
		memcpy(msg + (i++), &c->net_msg, n);
		c = c->next;
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
	client_thread_t *ct = (client_thread_t*)arg;	

	struct pollfd pfd;

	/* 
	 * Duplicates client socket file descriptor.   
	 * Ensures when connection is closed, read/write 
	 * does not persist to potential new socket assigned to client_fd
	*/
	
	pthread_mutex_lock(&clients_mutex);
	int io_fd = dup(ct->client_fd);
	pthread_mutex_unlock(&clients_mutex);

	// ms timestamp of last write to client
	uint64_t last_send_time = now_ms();
	
	// Temp buffer to read client data into. Later mutex memcpy into ct->net_msg
	user_data_t buf;
	while (!atomic_load(&ct->finished)) {
		pfd.fd = io_fd;
        	pfd.events = POLLIN | POLLOUT;
		
		// Waits indiefinitely for ability to write / available data to read from client socket
		int ready = poll(&pfd, 1, -1);

		if (ready > 0) {
			
			if (pfd.revents & (POLLHUP | POLLERR)) {
				send_by_type(io_fd, LOGOUT);
				atomic_store(&ct->finished, true);
				break;
                        }
			
			// Reads from client	
			if (pfd.revents & POLLIN) {
				int result = 0;
				if ((result = full_read(io_fd, &buf, 1)) != 0) {
					if (result == EOF) 
						errlog("Client", "read", io_fd, result, "User dc (EOF)", buf.username);
					
					else 
						errlog("Client", "read", io_fd, result, "N/A", buf.username);
					
					
					atomic_store(&ct->finished, true);
					break;
				}
				
				pthread_mutex_lock(&clients_mutex);
				memcpy(&ct->net_msg, &buf, sizeof(user_data_t));
				pthread_mutex_unlock(&clients_mutex);
				

				switch ((message_type_t)buf.type) {
					case LOGIN:
						errlog("Client", "--JOIN--", -1, -1, "Player connected", buf.username);
						break;
					case UPDATE_MESSAGE:
						break;
					// LOGOUT and invalid types are logged but disregarded	
					default:
						errlog("Recv", "msg parse", io_fd, -1, "Inv msg type", buf.username);
						break;
				}	
			}
		
			// Writes to client
			if (pfd.revents & POLLOUT) {
				// Fixed transfer period 
				// NETWORK_TRANSFER_PERIOD defined in anera_net.h 
				if (now_ms() - last_send_time < NETWORK_TRANSFER_PERIOD)
					continue;
		
				else
					last_send_time = now_ms();

				int result = 0;
				if ((result = send_by_type(io_fd, UPDATE_MESSAGE)) != 0) {
					errlog("Client", "write", io_fd, result, "Player dc", buf.username);
					atomic_store(&ct->finished, true);
					break;
				}
			}

				
			
		}
		
		// Error occurred during poll()
		else if (ready < 0)
			if (errno != EINTR) {
				errlog("Client", "poll", io_fd, errno, "N/A", buf.username);
				atomic_store(&ct->finished, true);
				break;
			}
	
	}
	
	close(io_fd);
	sem_post(client_cleanup_sem);

	return NULL;
}

// Allows graceful shutdown for SIGINT / SIGTERM
void shutdown_handler(int signum) {
	shutdown_requested = 1; // Async safe sig_atomic_t
	//close(settings.socket_fd); // Interrupts accept()
}

int main (int argc, char *argv[]) {
	
	// Handles termination	
	struct sigaction shutdown = {0};
	shutdown.sa_handler = shutdown_handler;
	
	sigaction(SIGINT, &shutdown, NULL);
	sigaction(SIGTERM, &shutdown, NULL);

	struct sigaction ign = {0};
	ign.sa_handler = SIG_IGN;
	// Ensures Server stays open if terminal session closes
	sigaction(SIGHUP, &ign, NULL);
	// Ensures failed write() sets errno EPIPE and returns -1 instead of crashing with SIGPIPE
	sigaction(SIGPIPE, &ign, NULL);


	// Sets default settings
	init_def_settings();

	// Parses CLI arguments and sets user specified settings
	if (parse_args(argc, argv) == -1) 
		return -1;

	// IPv4 TCP default protocol
	settings.socket_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (settings.socket_fd == -1) {
		perror("Socket creation failed");
		return -1;
	}

	// Allows rebind to same port after server termination
	// regardless if clients in TIME_WAIT or not
	int opt = 1;
	setsockopt(settings.socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
	
	// Binds server to socket
	while (bind(settings.socket_fd, (struct sockaddr*)(&settings.server), sizeof(settings.server)) == -1) {
		if (errno == EINTR)
			continue;
		perror("Bind to port failed");
		goto err_close_socket;
	}

	// Starts listening on socket
	while (listen(settings.socket_fd, settings.max_players) == -1) {
		if (errno == EINTR)
			continue;
		perror("Error while attmepting to listen on socket");
		goto err_close_socket;
	}


	// Creates semaphore w/ owner read / write, otherwise read only
	client_cleanup_sem = sem_open("/client_cleanup_sem", O_CREAT, 0644, 1);
	// Unlinks named semaphore from kernel 
	// Semaphore now persists for server lifetime instead of in kernel indefinitely
	sem_unlink("/client_cleanup_sem");

	// Error opening error log; treated as catastrophic
	if (init_log("Anera_server") != 0)
		goto err_close_log;	
	
	// Creates stack size attribute for client threads (1MB)
	pthread_attr_t client_attr;
        pthread_attr_init(&client_attr);
        if (pthread_attr_setstacksize(&client_attr, CLIENT_STACK) != 0) {
                fprintf(stderr, "Error setting client thread stack size\n");
                goto err_destroy_thread_resources;
        }

	// Server is "running"
	atomic_store(&settings.running, true);
        fprintf(stdout, "Server Listening on port: %d\n", ntohs(settings.server.sin_port));
        fprintf(stdout, "Max players: %d\n", settings.max_players);
        fflush(stdout);
	

	// Spawns reaper thread to cleanup dead client threads
	pthread_t reaper;
	if (pthread_create(&reaper, NULL, reaper_thread, NULL) != 0) {
		perror("Failed to spawn reaper thread");
		atomic_store(&settings.running, false);
		goto err_kill_reaper;
	}


	
	// Server loop
	while (!shutdown_requested) {

		struct sockaddr_in new_client;
		socklen_t adderlen = sizeof(new_client);
		int client_fd;

		// Checks for accept() error / BLOCKING
		while (!shutdown_requested && ((client_fd = accept(settings.socket_fd, 
			(struct sockaddr*)&new_client, 
			&adderlen)) == -1)) {
			
			if (errno == EMFILE || errno == ENFILE || errno == ENOBUFS || errno == ENOMEM)
				sleep(1);
			

			// Depending on error, client either stays or is removed by kernel from accept queue
			// Retry accept() always --- EMFILE or ENFILE (fd limit) and ENOBUFS or ENOMEM are network limits. Sleep & retry
		}
		
		// Exits server loop if terminated
		if (shutdown_requested)
			break;

		// Server is full
		if (atomic_load(&settings.connected_players) >= settings.max_players) {
			send_by_type(client_fd, LOGOUT);
			shutdown(client_fd, SHUT_RDWR);
			close(client_fd);
			continue;
		}

		// Create new client once accepted
		client_thread_t *ct = (client_thread_t*)calloc(1, sizeof(client_thread_t));
		if (!ct) {
			errlog("Main", "Calloc", -1, errno, "N/A", "N/A");
			close(client_fd);
			break;
		}
		
	
		// Adds client to front of list / Ensures no race
		// Sets file descriptor; client_io_thread also has mutex protection
		if (pthread_create(&ct->thread, &client_attr, client_io_thread, ct) == 0) {
			ct->client_fd = client_fd;	
			ct->finished = ATOMIC_VAR_INIT(false);
			atomic_fetch_add(&settings.connected_players, 1);

			pthread_mutex_lock(&clients_mutex);
			ct->next = clients;
                        clients = ct;
			pthread_mutex_unlock(&clients_mutex);
		}
		
		// Failed to create thread			
		else {
			errlog("Main", "pthread_create", ct->client_fd, errno, "N/A", "N/A");
			close(client_fd);
                        free(ct);
		}

	}
	
	// Shutdown requested. Updates running value to false for all threads
	atomic_store(&settings.running, false);
	
	// Server is closing / finishes all threads for reaper to join
	// Sends logout message
	client_thread_t *c = clients;
	pthread_mutex_lock(&clients_mutex);
	while (c != NULL) {
		//send_by_type(c->client_fd, LOGOUT);
		atomic_store(&c->finished, true);
		c = c->next;
	}

	// Wakes up reaper if sleeping; ready to join
	pthread_mutex_unlock(&clients_mutex);


	// Error creating reaper --> kills thread
	err_kill_reaper:	
	sem_post(client_cleanup_sem);
	pthread_join(reaper, NULL);
	sem_close(client_cleanup_sem);

	err_destroy_thread_resources:
        pthread_attr_destroy(&client_attr);

	err_close_log:	
	end_log();

	// Close server port connection
	err_close_socket:
	close(settings.socket_fd);
	
	return 0;
}
