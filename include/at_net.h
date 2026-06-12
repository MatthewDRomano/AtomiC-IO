#ifndef AT_NET_H
#define AT_NET_H

#include <stdint.h>	// uint8_t, uint16_t



// Packet struct / connection specifiers
#define MAX_CONNECTIONS 128
#define PACKET_HEADER_SIZE 50 // uuid, type, payload_len
#define PAYLOAD_MAX (1024 - PACKET_HEADER_SIZE) // 974 1KB total
#define CLIENT_USERNAME_SIZE 32

// Magic cookie used to verify user packets
#define ATOMICIO_PROTOCOL_MAGIC 0x61746F6D			// "atom" in hex

// Atomicio IO internal errors
#define ERR_PAYLOAD_OOB -2
#define ERR_USERS_OOB -3

// Network time protocols
#define NETWORK_BROADCAST_PERIOD 25				// Bounded Server->Client data transfer rate (every __ ms)
#define PACKET_DROP_THRESHOLD (NETWORK_TRANSFER_PERIOD * 2)     // Threshold to drop packets if specified by user (server side)


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
	uint32_t token;					//    4 bytes 
	uint64_t timestamp;				//    8 bytes
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


// Custom network <--> host endian conversion methods
uint64_t at_htonll(uint64_t ui64);
uint64_t at_ntohll(uint64_t ui64);

#endif
