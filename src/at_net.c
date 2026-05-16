#include "anera_net.h"
#include <unistd.h>     // read, write
#include <errno.h>      // errno, EINTR
#include <stddef.h>     // size_t
#include <unistd.h>     // ssize_t
#include <stdio.h>      // EOF enum

// Ensures full read
int full_read(int socket_fd, user_data_t* user_info, int users) {
	const size_t n = users * sizeof(user_data_t);
	size_t bytes_read = 0;
        ssize_t result;
	
	// Read data for all users
	while (bytes_read < n) {

                result = read(socket_fd, (char*)user_info + bytes_read, n - bytes_read);
		
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
	
	return 0;
}


// Ensures full write
int full_write(int socket_fd, user_data_t* user_info, int users) {
        const size_t n = users * sizeof(user_data_t);
        size_t bytes_written = 0;
	ssize_t result;

	// Writes data for all users	
	while (bytes_written < n) {
                result = write(socket_fd, (char*)user_info + bytes_written, n - bytes_written);
		
		// Error while writing
		if (result < 0) {
                        if (errno == EINTR)
                                continue;
			// Propagates error number
                        return errno;
                }

                bytes_written += result;
	}

	return 0;
}
