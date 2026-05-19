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

#include "at_net.h"
#include "log.h"


#define MAX_PORT 65535



// Holds MAX_CONNECTIONS slots of user_data_t. Unused slots store 0 and are ignored
static user_data_t users[MAX_CONNECTIONS] = {0};
static pthread_mutex_t users_mutex = PTHREAD_MUTEX_INITIALIZER;

static user_data_t client_info = {0};
static pthread_mutex_t client_mutex = PTHREAD_MUTEX_INITIALIZER;

typedef struct settings {
        struct sockaddr_in server;
        int socket_fd;
        atomic_bool connected;

} settings_t;

static settings_t settings = {0};
static volatile sig_atomic_t shutdown_requested = 0;

// Default tcp server connection Port / Ip
void init_def_settings() {
        settings.server.sin_family = AF_INET;
        settings.server.sin_port = htons(DEFAULT_PORT);
        inet_pton(AF_INET, "127.0.0.1", &settings.server.sin_addr);
	
	atomic_init(&settings.connected, false);
}


// Parses in-line client arguments
int parse_args(int argc, char* argv[]) {
		
	bool ip_set = false;
	bool domain_set = false;
	bool username_set = false;
	for (int i = 1; i < argc; i++) {
		
		// Usage menu
		if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
			fprintf(stdout, "usage: ./client [-h] [-u USERNAME] [--port PORT] [--ip IP] [--domain DOMAIN]\n");
			exit(0);
		}
		
		// Username
		else if (strcmp(argv[i], "-u") == 0) {
			// Missing arg
			if (++i == argc) {
				fprintf(stderr, "Missing username\n");
				return -1;
			}
			
			// Upper bound username size declared in at_net.h	
			snprintf(client_info.username, CLIENT_USERNAME_SIZE, "%s", argv[i]);
			username_set = true;
		}

		// Port
		else if (strcmp(argv[i], "--port") == 0) {
			// Missing arg
			if (++i == argc) {
				fprintf(stderr, "Missing port\n");
				return -1;
			}	
			
			char* end = NULL;
			long int port = strtol(argv[i], &end, 10);
			if (port < 0 || port > MAX_PORT || *end != '\0') {
				fprintf(stderr, "Invalid port \"%s\" (must be 0-%d)\n", argv[i], MAX_PORT);
				return -1;
			}

			settings.server.sin_port = htons((uint16_t)port);
		}

		// Ip
		else if (strcmp(argv[i], "--ip") == 0) {
			// Missing arg
			if (++i == argc) {
				fprintf(stderr, "Missing IP\n");
				return -1;
			}
		
			if (domain_set) {
				fprintf(stderr, "Cannot specify BOTH domain and IP\n");
				return -1;
			}

			struct hostent* h_ent = gethostbyname(argv[i]);
			if (h_ent == NULL) {
				fprintf(stderr, "Invalid IP\n");
				return -1;
			}
			
			ip_set = true;
			memcpy(&settings.server.sin_addr, h_ent->h_addr_list[0], h_ent->h_length);
		}

		else if (strcmp(argv[i], "--domain") == 0) {
			// Missing arg
			if (++i == argc) {
                                fprintf(stderr, "Missing Domain\n");
                                return -1;
                        }	
				
			if (ip_set) {
				fprintf(stderr, "Cannot specify BOTH domain and IP\n");
                                return -1;
			}

                        struct hostent* h_ent = gethostbyname(argv[i]);
                        if (h_ent == NULL) {
                                fprintf(stderr, "Invalid Domain\n");
                                return -1;
                        }
			
			domain_set = true;
                        memcpy(&settings.server.sin_addr, h_ent->h_addr_list[0], h_ent->h_length);
		}

		// Invalid argument
		else {
			fprintf(stderr, "Invalid argument \"%s\"\n", argv[i]);
			return -1;
		}

	}

	if (!username_set) {
		fprintf(stderr, "Must specify username (-h for more details)\n");
		return -1;
	}

	return 0;
}


// Assumes clients mutex unlocked
int send_by_type(int sock_fd, message_type_t msg_type) {
	user_data_t msg;

	pthread_mutex_lock(&client_mutex);
	memcpy(&msg, &client_info, sizeof(user_data_t));
	pthread_mutex_unlock(&client_mutex);

	msg.type = (uint8_t)msg_type;
	return full_write(sock_fd, &msg, 1);
}


void* recv_thread(void *arg) {
	while (atomic_load(&settings.connected)) {
		int result = 0;
		user_data_t buf[MAX_CONNECTIONS];

		if ((result = full_read(settings.socket_fd, buf, MAX_CONNECTIONS)) != 0) {
			const char* err_status = (result == EOF) ? "EOF" : "N/A";
			
			errlog("Recv", "read", settings.socket_fd, result, err_status, client_info.username);

			atomic_store(&settings.connected, false);	
			break;
		}
		
		pthread_mutex_lock(&users_mutex);
		memcpy(users, buf, sizeof(user_data_t) * MAX_CONNECTIONS);
		pthread_mutex_unlock(&users_mutex);	
		
		message_type_t type = (message_type_t)buf[0].type;	
		switch (type) {
			case LOGOUT:
				errlog("Recv", "msg parse", settings.socket_fd, -1, "Disconnect msg recv", client_info.username);
				atomic_store(&settings.connected, false);
				break;
			case UPDATE_MESSAGE:
				break;
			// Types LOGIN and invalid types cannot be sent
			default:
				errlog("Recv", "msg parse", settings.socket_fd, -1, "Inv msg type", client_info.username);
				break;
		}
	}	
	
	return NULL;
}

void* send_thread(void *arg) {	
	struct timespec ts;
	ts.tv_sec = 0;
	ts.tv_nsec = NETWORK_TRANSFER_PERIOD * 1000000; // 25 ms
	
	while (atomic_load(&settings.connected)) {
		
		int result = 0;
		if ((result = send_by_type(settings.socket_fd, UPDATE_MESSAGE)) != 0) {
			atomic_store(&settings.connected, false);
			errlog("Send", "write", settings.socket_fd, result, "Disconnected", client_info.username);
			break;
		}
		
        	nanosleep(&ts, NULL);
	}

	return NULL;
}

// Graceful shutdown on SIGINT SIGTERM SIGHUP
void shutdown_handler(int signum) {
	shutdown_requested = 1;
}


int main(int argc, char* argv[]) {
	// Set signal handler to shutdown gracefully (Default flags)
	struct sigaction shutdown_sa = {0};
	shutdown_sa.sa_handler = shutdown_handler;
	sigaction(SIGINT, &shutdown_sa, NULL);
	sigaction(SIGTERM, &shutdown_sa, NULL);
	sigaction(SIGHUP, &shutdown_sa, NULL);
	
	// ENSURES the write() within send thread return -1 if server closes
	struct sigaction sa = {0};
        sa.sa_handler = SIG_IGN;
        sigaction(SIGPIPE, &sa, NULL);
	
	// Parse in-line arguments and set default settings
	init_def_settings(); 
	
	if (parse_args(argc, argv) == -1)
		return -1;
		
	// IPv4 TCP default protocol
	settings.socket_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (settings.socket_fd == -1) {
		perror("Socket creation failed");
		return -1;
	}
	
	// Connects socket to server
	while (connect(settings.socket_fd, (struct sockaddr*)(&settings.server), sizeof(settings.server)) == -1) {
		if (errno == EINTR && !shutdown_requested)
			continue;

		// Error occurred or shutdown requested
		perror("Failed to connect socket to server");
		return -1;
	}	
	
	// Connection successful
	atomic_store(&settings.connected, true);

	// Login message
        send_by_type(settings.socket_fd, LOGIN);


	if (init_log(client_info.username) == -1)
                goto err_close_log;


	// Thread #1: send to server
	pthread_t send_tid;
	if (pthread_create(&send_tid, NULL, send_thread, NULL) != 0) {
		fprintf(stderr, "Failed to create send thread\n");
		goto err_kill_send_thread;
	}
	
	// Thread #2: Receive from server
	pthread_t recv_tid;
        if (pthread_create(&recv_tid, NULL, recv_thread, NULL) != 0) {
		fprintf(stderr, "Failed to receive send thread\n");
		goto err_kill_recv_thread;
	}

	

	// Thread Main: Client Loop
	fprintf(stdout, "Connected to server\n");
        fflush(stdout);
	while (atomic_load(&settings.connected)) {
		if (shutdown_requested) {
			atomic_store(&settings.connected, false);
			break;
		}


		sleep(1);		
	}
	shutdown(settings.socket_fd, SHUT_RDWR);
	
	err_kill_recv_thread:
        pthread_join(recv_tid, NULL);

	err_kill_send_thread:
	pthread_join(send_tid, NULL);
	
	err_close_log:
	end_log();

	close(settings.socket_fd);
	return 0;
}
