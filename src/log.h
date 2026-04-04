#ifndef LOG_H
#define LOG_H

#define TIMESTAMP_LEN 32


// Creates new log specific to server instance
int init_log(char* log_name);

// Closes file descriptor to log upon shutdown
int end_log();

// Inserts human readable unix timestamp into time buffer
// Seconds-Precision
int set_timestamp(char* time);

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
int msglog(char* msg);

#endif
