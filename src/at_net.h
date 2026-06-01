#ifndef AT_NET_H
#define AT_NET_H

#include <stdint.h>	// uint8_t, uint16_t

#define MAX_CONNECTIONS 128
#define PACKET_HEADER_SIZE 38 // uuid, type, payload_len
#define PAYLOAD_MAX (1024 - PACKET_HEADER_SIZE) // 986 1KB total
#define CLIENT_USERNAME_SIZE 32

#define ERR_PAYLOAD_OOB -2
#define ERR_USERS_OOB -3

// Bounded Server->Client data transfer rate (every __ ms)
#define NETWORK_TRANSFER_PERIOD 25
#define PACKET_DROP_THRESHOLD (NETWORK_TRANSFER_PERIOD * 2)


/**
 * Message type identifer
 */ 
typedef enum {
	LOGIN,
	LOGOUT,
	UPDATE_MESSAGE
} message_type_t;

/**
 *  Network safe packet structure for server/client IO
 *  htons / htonl needed for fields accordingly
 */  
typedef struct __attribute__((packed)) {
	char client_uuid[CLIENT_USERNAME_SIZE];		//   32 bytes
	uint16_t type;					//    2 bytes
	uint16_t active_users;				//    2 bytes / functionally identical to total packets sent
	uint16_t payload_len;				//    2 bytes
	char payload[PAYLOAD_MAX];
} packet_t;

/**
 * Standardized read / write protocols for both server and clients
 */ 
int full_read(int socket_fd, packet_t* packet_buffer);
int full_write(int socket_fd, packet_t* packet_buffer, int users);

#endif
