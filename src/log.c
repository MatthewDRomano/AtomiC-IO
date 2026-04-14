#include "log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h> // For O_CREAT and open flags 
#include <unistd.h>
#include <stdatomic.h>
#include <time.h>
#include <stdbool.h>

#define MAX_PATH_LEN 64
#define MAX_MSG_LEN 128

// Server-instance specific log file path
static char log_path[MAX_PATH_LEN] = {0};
// Log file
static FILE* log_f = NULL;
// Forward func dec
static void* perform_logging(void* arg);


// write_log_entry() waits on this sem 
static sem_t* log_sem; 
static pthread_t log_thread;
static atomic_bool log_open = ATOMIC_VAR_INIT(false);


// Log entry with error format
typedef struct err_entry {
	char timestamp[TIMESTAMP_LEN];
	const char* thread;
 	const char* call;
	int fd;
	int errnum;
	char err_desc[MAX_MSG_LEN];
	const char* client;
	//struct err_entry* _Atomic next_entry;
} err_entry_t;

// Log entry for standard messages
typedef struct msg_entry {
	char timestamp[TIMESTAMP_LEN];
	char text[MAX_MSG_LEN];
	//struct msg_entry* _Atomic next_entry; 
} msg_entry_t;


typedef struct log_entry {
	bool isError;
	union {
		msg_entry_t message;
		err_entry_t error;
	} format;
	struct log_entry* _Atomic next_entry;
} log_entry_t;


// Global linked list of log entries / log_thread will log upon sem_post
// Supports atomic pointer swaps
static log_entry_t* _Atomic log_head = NULL;
static log_entry_t* _Atomic log_tail = NULL;

// perform_logging knows when not to interrupt the process of adding to list
static const log_entry_t SENTINEL = {0};

// Sets time buffer to second-precision timestamp 
int set_timestamp(char* time) {
	// Gets unix time
	struct timespec ts;
        struct tm tm;
        clock_gettime(CLOCK_REALTIME, &ts);

	// Unix -> Human timestamp conversion Null terminated
        gmtime_r(&ts.tv_sec, &tm);
        strftime(time, TIMESTAMP_LEN, "%Y-%m-%d-%H:%M:%S", &tm);
}


int init_log(char* log_name) {
	// Log already initialized
	if (atomic_load(&log_open))
		return -1;
	
	// Creates semaphore w/ owner read / write, otherwise read only
	char sem_name[MAX_PATH_LEN];
	snprintf(sem_name, MAX_PATH_LEN, "/log_%d", getpid());
        log_sem = sem_open(sem_name, O_CREAT, 0644, 1);
        // Unlinks named semaphore from kernel
        // Semaphore now persists for server lifetime instead of in kernel indefinitely
        sem_unlink(sem_name);

	log_head = (log_entry_t*)calloc(1, sizeof(log_entry_t));
	if (!log_head) {
		fprintf(stderr, "Error allocating memory for log\n");
		sem_close(log_sem);
		return -1;
	}	
	log_tail = log_head;

	// Spawns logging thread
	snprintf(log_path, MAX_PATH_LEN, "%s", log_name);
	if (pthread_create(&log_thread, NULL, perform_logging, NULL) != 0) {
		// Error spawning log thread
		fprintf(stderr, "Error spawning log thread\n");
		sem_close(log_sem);	
		return -1;
	}

	return 0;
}

int end_log() {
	// Ignore end request; Already ended or never initialized
	if (!atomic_load(&log_open)) 
		return -1;

	// Wakes up log thread -> terminates -> joins
	atomic_store(&log_open, false);
	sem_post(log_sem);	
	pthread_join(log_thread, NULL);

	// After thread finishes, head = tail	
	free(log_head);
	//free(err_entries_head);
	sem_close(log_sem);
	return 0;
}

int errlog(const char* thread, const char* call, int fd, int errnum, const char* err_desc, const char* client) {
	if (atomic_load(&log_open) == false)
		return -1;
	
	char time_s[TIMESTAMP_LEN] = {0};
	set_timestamp(time_s);

	log_entry_t* new_entry = (log_entry_t*)malloc(sizeof(log_entry_t));
        if (!new_entry) {
                fprintf(stderr, "Error initializing err log entry");
                return -1;
        }
	new_entry->isError = true;
	new_entry->next_entry = &SENTINEL;
	
	// Set fields
	memcpy(new_entry->format.error.timestamp, time_s, TIMESTAMP_LEN);
	// const char* always safe (saved in data segment)
	new_entry->format.error.thread = thread;
	new_entry->format.error.call = call;
	new_entry->format.error.fd = fd;
	new_entry->format.error.errnum = errnum;
		
	// Null terminated
	snprintf(new_entry->format.error.err_desc, MAX_MSG_LEN, "%s", strerror(errnum));
	new_entry->format.error.client = client;
	

	log_entry_t* prev_tail = atomic_exchange(&log_tail, new_entry); 
        atomic_store(&prev_tail->next_entry, new_entry);

	atomic_store(&log_tail->next_entry, NULL);
	

        sem_post(log_sem);
        return 0;
}

int msglog(char* msg) {
        if (atomic_load(&log_open) == false)
                return -1;

	char time_s[TIMESTAMP_LEN] = {0};
	set_timestamp(time_s);

	log_entry_t* new_entry = (log_entry_t*)malloc(sizeof(log_entry_t));
	if (!new_entry) {
		fprintf(stderr, "Error initializing msg log entry");
		return -1;
	}
	new_entry->isError = false;
	new_entry->next_entry = &SENTINEL;
	
	memcpy(new_entry->format.message.timestamp, time_s, TIMESTAMP_LEN);
	memcpy(new_entry->format.message.text, msg, MAX_MSG_LEN);
	new_entry->format.message.text[MAX_MSG_LEN-1] = '\0';

	log_entry_t* prev_tail = atomic_exchange(&log_tail, new_entry);
	atomic_store(&prev_tail->next_entry, new_entry);
	
	atomic_store(&log_tail->next_entry, NULL);

	sem_post(log_sem);
	return 0;
}

static void* perform_logging(void* arg) {
	
	/* 
	 * Creates instance specific log w/ log_name
	 * Waits on semaphore; Writes log entry to log upon sem_post
	*/
	
        char log_name[MAX_PATH_LEN];
	snprintf(log_name, MAX_PATH_LEN, "%s", log_path);

	char time_s[TIMESTAMP_LEN] = {0};
        set_timestamp(time_s);
        snprintf(log_path, MAX_PATH_LEN, "../logs/%s_log_%s.txt", log_name, time_s);

        log_f = fopen(log_path, "w");
        if (log_f == NULL) {
                fprintf(stderr, "Error creating log file. Ironic\n");
                goto term_thread;
        }

        fprintf(log_f, "==========%s START==========\n\n", log_name);	
	

	// Begin; handle logging
	atomic_store(&log_open, true);
	while (atomic_load(&log_open)) {
		sem_wait(log_sem);

		while (sem_trywait(log_sem) == 0) {
			; // sem_trywait drains log_sem until it becomes 0
		}
		
		// All fields NULL -> log adds new entries to dummy tail
		log_entry_t* dummy_tail = (log_entry_t*)calloc(1, sizeof(log_entry_t));
		// Ensures log is not in the middle of adding entry
		while (atomic_load(&log_tail->next_entry) == &SENTINEL);
			
		log_entry_t* old_tail = atomic_exchange(&log_tail, dummy_tail);
		log_entry_t* old_head = atomic_exchange(&log_head, dummy_tail);

		// Logs all entries, ignoring first dummy node
		while (old_head->next_entry != NULL) {
			log_entry_t* next = old_head->next_entry;
			free(old_head);
			
			// Prints appropriate log type (msg/err) based on union format value 	
			if (!next->isError) {
				msg_entry_t* message = &next->format.message;
				if (fprintf(log_f, "[%s] | %s\n", message->timestamp, message->text) == EOF)
        		      		fprintf(stderr, "Error writing msg log entry\n");
        		}

			else if (next->isError) {
				err_entry_t* error = &next->format.error;
				if (fprintf(log_f, "[%s] | thread: %s | %s | fd=%d | Error: %s | Client name: %s\n",
                                error->timestamp, error->thread, error->call, error->fd, error->err_desc, error->client) == EOF)
                                        fprintf(stderr, "Error writing err log entry\n");
			}	
			old_head = next;
		}
		// Frees last node 
		free(old_head);	
		fflush(log_f);
	}	
	
	term_thread:

	fclose(log_f);
	return NULL;
}

