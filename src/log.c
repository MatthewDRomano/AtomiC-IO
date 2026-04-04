#include "log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h> // For O_CREAT and open flags 
#include <stdatomic.h>
#include <time.h>
#include <stdbool.h>

#define MAX_PATH_LEN 128
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
	struct err_entry* _Atomic next_entry;
} err_entry_t;

// Log entry for standard messages
typedef struct msg_entry {
	char timestamp[TIMESTAMP_LEN];
	char msg[MAX_MSG_LEN];
	struct msg_entry* _Atomic next_entry; 
} msg_entry_t;



// Global linked lists of log entries / log_thread will log upon sem_post
// Supports atomic pointer swaps
static err_entry_t* _Atomic err_entries_head = NULL;
static err_entry_t* _Atomic err_entries_tail = NULL;
static msg_entry_t* _Atomic msg_entries_head = NULL;
static msg_entry_t* _Atomic msg_entries_tail = NULL;



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
        log_sem = sem_open("/log_sem", O_CREAT, 0644, 1);
        // Unlinks named semaphore from kernel
        // Semaphore now persists for server lifetime instead of in kernel indefinitely
        sem_unlink("/log_sem");

	msg_entries_head = (msg_entry_t*)calloc(sizeof(msg_entry_t));
	err_entries_head = (msg_entry_t*)calloc(sizeof(msg_entry_t));
	if (!msg_entries_head || !err_entries_head) {
		fprintf(stderr, "Error allocating memory for logs\n");
		sem_close(log_sem);
		return -1;
	}	
	msg_entries_tail = msg_entries_head;
	err_entries_tail = err_entries_head;

	// Spawns logging thread
	if (pthread_create(&log_thread, NULL, perform_logging, log_name) != 0) {
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

	
	free(msg_entries_head);
	free(err_entries_head);
	sem_close(log_sem);
	return 0;
}

int errlog(const char* thread, const char* call, int fd, int errnum, const char* err_desc, const char* client) {
	if (atomic_load(&log_open) == false)
		return -1;
	
	char time_s[TIMESTAMP_LEN] = {0};
	set_timestamp(time_s);

	err_entry_t* _Atomic new_err_entry = (err_entry_t* _Atomic)malloc(sizeof(err_entry_t));
        if (!new_err_entry) {
                fprintf(stderr, "Error initializing err log entry");
                return -1;
        }
	
	// Set fields

	memcpy(new_err_entry, time_s, TIMESTAMP_LEN);
	//new_err_entry->timestamp[TIMESTAMP_LEN-1] = '\0';
	// const char* always safe (saved in data segment)
	new_err_entry->thread = thread;
	new_err_entry->call = call;
	new_err_entry->fd = fd;
	new_err_entry->errnum = errnum;
		
	char buf[MAX_MSG_LEN];
	// Null terminated
	strerror(errnum, buf, sizeof(buf));
	snprintf(new_err_entry->err_desc, MAX_MSG_LEN, "%s", buf);
	new_err_entry->client = client;
	new_err_entry->next_entry = NULL;
	

	err_entry_t* prev_tail = atomic_exchange(&err_entries_tail, new_err_entry); 
        //if (!atomic_load(&prev_tail)) {
                //atomic_store(&err_entries_head, new_err_entry);
                ////atomic_store(&err_entries_tail, new_err_entry);
        //}

        //else {
                atomic_store(&prev_tail->next_entry, new_err_entry);
                //atomic_exchange(&err_entries_tail, new_err_entry);
        //}

        sem_post(log_sem);
        return 0;
}

int msglog(char* msg) {
        if (atomic_load(&log_open) == false)
                return -1;

	char time_s[TIMESTAMP_LEN] = {0};
	set_timestamp(time_s);

	msg_entry_t* _Atomic new_msg_entry = (msg_entry_t* _Atomic)malloc(sizeof(msg_entry_t));
	if (!new_msg_entry) {
		fprintf(stderr, "Error initializing msg log entry");
		return -1;
	}
	
	memcpy(new_msg_entry->timestamp, time_s, TIMESTAMP_LEN);
	//new_msg_entry->timestamp[TIMESTAMP_LEN-1] = '\0';
	memcpy(new_msg_entry->msg, msg, MAX_MSG_LEN);
	new_msg_entry->msg[MAX_MSG_LEN-1] = '\0';
	new_msg_entry->next_entry = NULL;

	msg_entry_t* prev_tail = atomic_exchange(&msg_entries_tail, new_msg_entry);
	//if (!atomic_load(&prev_tail)) {
		//atomic_store(&msg_entries_head, new_msg_entry);
		//atomic_store(&msg_entries_tail, new_msg_entry);
	//}
	
	//else {
		
		atomic_store(&prev_tail->next_entry, new_msg_entry);
		//atomic_exchange(&msg_entries_tail, new_msg_entry);
        //}

	sem_post(log_sem);
	return 0;
}

static void* perform_logging(void* arg) {
	
	/* 
	 * Creates instance specific log w/ log_name
	 * Waits on semaphore; Writes log entry to log upon sem_post
	*/
	
	char* log_name = (char*)arg;
        char name[32] = {0};
        memcpy(name, log_name, sizeof(name) - 1);

	char time_s[TIMESTAMP_LEN] = {0};
        set_timestamp(time_s);
        snprintf(log_path, sizeof(log_path), "../logs/%s_log_%s.txt", name, time_s);

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
		
		// All fields NULL
		msg_entry_t* dummy_msg_tail = (msg_entry_t*)calloc(sizeof(msg_entry_t));
		atomic_exchange(&msg_entries_tail, dummy_msg_tail);

		msg_entry_t* curr_msg_head = msg_entries_head->next_entry;
		while (atomic_load(&curr_msg_head)) {
			msg_entry_t* old_head = atomic_exchange(&curr_msg_head, curr_msg_head->next_entry);
			
			if (fprintf(log_f, "[%s] | %s\n", old_head->timestamp, old_head->msg) == EOF)
        		      fprintf(stderr, "Error writing msg log entry\n");
        		
			fflush(log_f);
			//atomic_compare_exchange_strong(&msg_entries_tail, &old_head, NULL);
			free(old_head);
		}
		atomic_exchange(&msg_entries_head, dummy_msg_tail);	

		// All fields NULL
		err_entry_t* dummy_err_tail = (err_entry_t*)calloc(sizeof(err_entry_t));
		atomic_exchange(&err_entries_tail, dummy_err_tail);

		err_entry_t* curr_err_head = err_entries_head->next_entry; 	
		while (atomic_load(&curr_err_head)) {
			err_entry_t* old_head = atomic_exchange(&curr_err_head, curr_err_head->next_entry);
			
			if (fprintf(log_f, "[%s] | thread: %s | %s | fd=%d | Error: %s | Client name: %s\n",
        	          	old_head->timestamp, old_head->thread, old_head->call, old_head->fd, old_head->err_desc, old_head->client) == EOF)
					fprintf(stderr, "Error writing err log entry\n");

			fflush(log_f);
			//atomic_compare_exchange_strong(&err_entries_tail, &old_head, NULL);
			free(old_head);
		}
		atomic_exchange(&err_entries_head, dummy_err_tail);
		
	}	
	
	term_thread:

	fclose(log_f);
	return NULL;
}

