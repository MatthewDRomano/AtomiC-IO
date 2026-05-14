#ifndef ANERA_NET_H
#define ANERA_NET_H

#include <stdint.h>	// uint8_t, uint16_t

#define MAX_CONNECTIONS 15
#define DEFAULT_PORT 5555
#define CLIENT_USERNAME_SIZE 32

// Bounded Server->Client data transfer rate (every __ ms)
#define NETWORK_TRANSFER_PERIOD 25
#define PACKET_DROP_THRESHOLD (NETWORK_TRANSFER_PERIOD * 2)


// Message type identifier
typedef enum {
	LOGIN,
	LOGOUT,
	UPDATE_MESSAGE
} message_type_t;

// Network safe player info struct
typedef struct __attribute__((packed)) {
	uint8_t type;
	char username[CLIENT_USERNAME_SIZE];
	uint16_t pos_x, pos_y;
} user_data_t;

// Shared client / server IO methods
int full_read(int socket_fd, user_data_t *player_info, int users);
int full_write(int socket_fd, user_data_t *player_info, int users);

#endif
