#ifndef LOG_H
#define LOG_H

#define TIMESTAMP_LEN 32
#define MAX_PATH_LEN 96
#define MAX_MSG_LEN 128

// Creates new log specific to server instance
int init_log(char* path);

// Closes file descriptor to log upon shutdown
int end_log();

// Inserts human readable unix timestamp into time buffer
// Seconds-Precision
void set_timestamp(char* time);

/* 
 * Logs an error entry
 * thread   -> thread name
 * call     -> function that produced error
 * fd 	    -> file descriptor num
 * errnum   -> errno value
 * err_desc -> string describing error
 * client   -> client name
*/
int errlog(const char* thread, 
	   const char* call, 
	   int fd, 
	   int errnum,
	   const char* err_desc,
	   const char* client);


// Used to log info / track response time
// msg is internally bounds checked and auto null terminated
// so msg can safely be passed as any length
int msglog(char* msg);

#endif
