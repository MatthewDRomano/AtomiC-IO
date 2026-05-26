#include "at_net.h"

#include <arpa/inet.h>    // ntohs / ntohl for network endian
#include <unistd.h>     // read, write
#include <errno.h>      // errno, EINTR
#include <stddef.h>     // size_t
#include <unistd.h>     // ssize_t
#include <stdio.h>      // EOF enum

// Ensures full read
int full_read(int socket_fd, packet_t* packet_buffer) {
        ssize_t result;
	int packet_count = 1; // MUST read the first packet as it contains true packet_count	

	for (int i = 0; i < packet_count; i++) {		
		packet_t* current_packet = (packet_buffer + i);

		// 1.) Read static packet header
		size_t bytes_read = 0;
		char* header_ptr = (char*)current_packet;
		while (bytes_read < PACKET_HEADER_SIZE) {

                	result = read(socket_fd, header_ptr + bytes_read, PACKET_HEADER_SIZE - bytes_read);
		
			// Error while reading
			if (result < 0) {
                        	if (errno == EINTR)
                                	continue;
                        	// Propagates error number
				return errno;
                	}

			// EOF signaled; connection closed
			else if (result == 0)
                        	return EOF;
		
			bytes_read += result;
		}
		
		// 2.) Ensures protocol compliance (plength is unsigned 16 bit)
		uint16_t plength = ntohs(current_packet->payload_len);
		if (plength > PAYLOAD_MAX)
			return ERR_PAYLOAD_OOB;
		
		// 3.) If this is the first packet, update packet_count with true value
		if (i == 0) {
			uint16_t active_users = ntohs(current_packet->active_users);
			
			// Checks for invalid user count		
			if (active_users < 0 || active_users > MAX_CONNECTIONS)	
				return ERR_USERS_OOB;
			
			packet_count = active_users;
		}

		// 4.) Read dynamic packet payload
		bytes_read = 0;
		char* payload_ptr = current_packet->payload;
		while (bytes_read < plength) {
				
			result = read(socket_fd, payload_ptr + bytes_read, plength - bytes_read);	
			
			if (result < 0) {
				if (errno == EINTR)
					continue;
				return errno;
			}

			else if (result == 0)
				return EOF;

			bytes_read += result;
		}
	}
	
	return 0;
}


// Ensures full write
int full_write(int socket_fd, packet_t* packet_buffer, int packet_count) {
	ssize_t result;
        for (int i = 0; i < packet_count; i++) {
                packet_t* current_packet = (packet_buffer + i);

		// Ensures protocol compliance (plength is unsigned 16 bit)
                uint16_t plength = ntohs(current_packet->payload_len);
                if (plength > PAYLOAD_MAX)
                        return ERR_PAYLOAD_OOB;

		// Write entire packet
		size_t n = PACKET_HEADER_SIZE + plength;
                size_t bytes_written = 0;
                while (bytes_written < n) {

                        result = write(socket_fd, (char*)current_packet + bytes_written, n - bytes_written);

                        // Error while writing
                        if (result < 0) {
                                if (errno == EINTR)
                                        continue;
                                // Propagates error number
                                return errno;
                        }

                        bytes_written += result;
                }
        }

        return 0;
}
