#include "../include/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h> // For O_CREAT and open flags 
#include <unistd.h>
#include <stdatomic.h>
#include <stdalign.h>
#include <time.h>
#include <stdbool.h>

#define SEM_NAME_MAX 31		// Older macOS versions limit named semaphore length to 31 chars

// Forward func dec
static void* perform_logging(void* arg);


// write_log_entry() waits on this sem 
static sem_t* log_sem; 
static char sem_name[SEM_NAME_MAX];
static pthread_t log_thread;
static atomic_bool log_open = false;


// Log entry with error format
typedef struct err_entry {
	int fd;
	int errnum;
	char err_str[MAX_MSG_LEN];
	char desc[MAX_MSG_LEN];
	char client[MAX_MSG_LEN];
} err_entry_t;

// Log entry for standard messages
typedef struct msg_entry {
	char text[MAX_MSG_LEN];
} msg_entry_t;


typedef struct log_entry {
	//FILE* file;	
	char path[MAX_PATH_LEN];
	char timestamp[TIMESTAMP_LEN];
	bool isError;
	union {
		msg_entry_t message;
		err_entry_t error;
	} format;
	struct log_entry* next_entry;
} log_entry_t;


// Global linked list of log entries / log_thread will log upon sem_post
static log_entry_t* log_head = NULL;
static log_entry_t* log_tail = NULL;
static log_entry_t dummy_node = {0};
static pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;

  
// perform_logging knows when not to interrupt the process of adding to list
//static const log_entry_t SENTINEL = {0};

// Sets time buffer to second-precision timestamp 
void set_timestamp(char* time) {
	// Gets unix time
	struct timespec ts;
        struct tm tm;
        clock_gettime(CLOCK_REALTIME, &ts);

	// Unix -> Human timestamp conversion Null terminated
        gmtime_r(&ts.tv_sec, &tm);
        strftime(time, TIMESTAMP_LEN, "%Y-%m-%d-%H:%M:%S", &tm);
}


int log_init() {
	// Log already initialized
	if (atomic_load(&log_open))
		return -1;
	
	// Creates semaphore w/ owner read / write, otherwise read only
	snprintf(sem_name, SEM_NAME_MAX, "/log_%d", getpid());
        log_sem = sem_open(sem_name, O_CREAT, 0644, 0);
	
	log_head = &dummy_node;	
	log_tail = log_head;

	// Spawns logging thread / Uses global log_path to store name
	atomic_store(&log_open, true);
	if (pthread_create(&log_thread, NULL, perform_logging, NULL) != 0) {
		// Error spawning log thread
		fprintf(stderr, "Error spawning log thread\n");
		atomic_store(&log_open, false);
		sem_unlink(sem_name);
		sem_close(log_sem);	
		return -1;
	}

	return 0;
}

int log_shutdown() {
	// Ignore end request; Already ended or never initialized
	if (!atomic_load(&log_open)) 
		return -1;

	// Prevents new log entries
	// Wakes up log thread -> terminates -> joins
	atomic_store(&log_open, false);
	sem_post(log_sem);	
	pthread_join(log_thread, NULL);

	// Safety drain of any leaked log_entries (skips dummy_node)
	// No new nodes / no active logging thread allows for no mutex needed
	log_entry_t* current = log_head->next_entry;
	while (current != NULL) {
		log_entry_t* prev = current;
		current = current->next_entry;
		free(prev);
	}
	
	// Resets nodes in case of reinitializing a log
	dummy_node.next_entry = NULL;
	// Unlinks named semaphore from kernel
	// Semaphore now persists for server lifetime instead of in kernel indefinitely
	sem_unlink(sem_name);
	sem_close(log_sem);
	return 0;
}

int errlog(int fd, int errnum, const char* desc, const char* client, const char* path) {
	if (atomic_load(&log_open) == false)
		return -1;
	
	log_entry_t* new_entry = (log_entry_t*)malloc(sizeof(log_entry_t));
        if (!new_entry) {
                fprintf(stderr, "Error initializing err log entry");
                return -1;
        }

	// Set log entry path
        snprintf(new_entry->path, MAX_PATH_LEN, "%s.txt", path);


	new_entry->isError = true;
	new_entry->next_entry = NULL;	

	// Set fields
	// Ensures max length of TIMESTAMP_LEN
        set_timestamp(new_entry->timestamp);
	new_entry->format.error.fd = fd;
	new_entry->format.error.errnum = errnum;
		
	// If errnum is a valid errno value (>0), set desc as errno description
	// Otherwise, use custom desc text provided by user 
	if (errnum > 0)
		strerror_r(errnum, new_entry->format.error.err_str, MAX_MSG_LEN);
	else
		*new_entry->format.error.err_str = '\0';
	
	snprintf(new_entry->format.error.desc, MAX_MSG_LEN, "%s", desc);
	snprintf(new_entry->format.error.client, MAX_MSG_LEN, "%s", client);	

	// Waits for potential other threads to finish adding new entry
	pthread_mutex_lock(&queue_mutex);
	// Final check to ensure log isn't mid shutdown
	if (!atomic_load(&log_open)) {
                pthread_mutex_unlock(&queue_mutex);
		free(new_entry); // Safely discard the allocated node
                return -1;
        }
	log_entry_t* prev = log_tail;
	log_tail = new_entry;
	prev->next_entry = log_tail;
	pthread_mutex_unlock(&queue_mutex);
	

        sem_post(log_sem);
        return 0;
}

int msglog(const char* msg, const char* path) {
        if (atomic_load(&log_open) == false)
                return -1;

	log_entry_t* new_entry = (log_entry_t*)malloc(sizeof(log_entry_t));
	if (!new_entry) {
		fprintf(stderr, "Error initializing msg log entry");
		return -1;
	}

	// Set log entry path
	snprintf(new_entry->path, MAX_PATH_LEN, "%s.txt", path);
	
	new_entry->isError = false;
	new_entry->next_entry = NULL;

        set_timestamp(new_entry->timestamp); // Ensures max length of TIMESTAMP_LEN
	snprintf(new_entry->format.message.text, MAX_MSG_LEN, "%s", msg);
	
	// Waits for potential other threads to finish adding new entry	
	pthread_mutex_lock(&queue_mutex);
	// Final check to ensure log isn't mid shutdown
	if (!atomic_load(&log_open)) {
    		pthread_mutex_unlock(&queue_mutex);
    		free(new_entry); // Safely discard the allocated node
		return -1;
	}
	log_entry_t* prev = log_tail;
        log_tail = new_entry;
        prev->next_entry = log_tail;
	pthread_mutex_unlock(&queue_mutex);	


	sem_post(log_sem);
	return 0;
}

static void* perform_logging(void* arg) {
	(void)arg; // Silence unused parameter compiler warning
	

	// Begin; handle logging
	bool final_sweep_flag = true;
	while (atomic_load(&log_open) || final_sweep_flag) {
		// Waits for a log entry while the server is running
		if (atomic_load(&log_open))
			sem_wait(log_sem);

		while (sem_trywait(log_sem) == 0) {
			; // sem_trywait drains log_sem until it becomes 0
		}
		
		if (!atomic_load(&log_open))
			final_sweep_flag = false;
		
		// Ensures log is not in the middle of adding entry
		// Sets current equal to the true head (first non-dummy node)
		// Severs the tie from dummy_node in the global linked list, and sets new head/tail as unlinked dummy
		pthread_mutex_lock(&queue_mutex);
		log_entry_t* current = log_head->next_entry;
		dummy_node.next_entry = NULL;
		log_tail = &dummy_node;
		log_head = log_tail;	
		pthread_mutex_unlock(&queue_mutex);

		// Logs all entries, ignoring first dummy node
		while (current != NULL) {	
			FILE* log_file = fopen(current->path, "a");
			if (!log_file) 		// Invalid path or other fopen error
				goto skip_log;
	
			// Prints appropriate log type (msg/err) based on union format value 		
			if (!current->isError) {
				msg_entry_t* message = &current->format.message;
				// Prints message / timestampe is red
				if (fprintf(log_file, "\e[0;32m[%s]\e[0m | %s\n", current->timestamp, message->text) == EOF)
        		      		fprintf(stderr, "Error writing msg log entry\n");
        		}

			else if (current->isError) {
				err_entry_t* error = &current->format.error;
				// Prints error / timestamp is red
				if (fprintf(log_file, "\e[0;31m[%s]\e[0m | fd=%d | Client name: %s | Error: %s | Desc: %s |\n",
                                current->timestamp, error->fd, error->client, error->err_str, error->desc) == EOF)
                                        fprintf(stderr, "Error writing err log entry\n");
			}

			fclose(log_file);
			
			skip_log:
			; // Declaration CANNOT follow label prior to C23
			log_entry_t* prev = current;	
			current = current->next_entry;
			free(prev);	
		}
		
	}	
	
	atomic_store(&log_open, false);	
	return NULL;
}

