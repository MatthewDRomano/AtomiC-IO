/*
 * stress_test.c
 *
 * Self-contained stress tester that runs an AtomiC-IO server AND
 * the maxmimum amount of clients in a single process, to evaluate how
 * effective the AtomiC-IO server is at handling large throughput,
 * and aggressive simultaneous logging.
 *
 *
 * To be tested:
 *   - Running a server and maximum amt of clients side by side
 *   - Each client sending the maximum amount of data possible, and observing everyone else's data with little delay
 *   - The background logger to see how efficiently it handles logging from various threads at the same time
 *
 * Build (run from the project's tests folder, assuming this layout):
 *
 *   project_root/
 *         include/   (at_net.h, atomicio.h, atomicio_cl.h, log.h)
 *         src/           (at_net.c, atomicio.c, atomicio_cl.c, log.c)
 *         tests/  (currently in this directory)
 *
 *   gcc -Wall -Wextra -pthread \
 *           stress_test.c \
 *           ../src/atomicio.c ../src/atomicio_cl.c ../src/at_net.c ../src/log.c \
 *           -o stress_tester
 *
 * Run:
 *   ./stress_tester
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>

#include "../include/atomicio.h"
#include "../include/atomicio_cl.h"

#define TEST_PORT_STR   "5555"		/* atomicio_cl_connect() wants a string port */
#define TEST_PORT_NUM   5555         	/* atomicio_config_t wants a numeric port        */
#define TEST_MAX_USERS  MAX_CONNECTIONS
#define TEST_LOG_PATH   "stress_logs"
#define TEST_CLIENTS    TEST_MAX_USERS
#define TEST_TICKS      100             /* number of update/broadcast cycles each client runs */



/* Runs one simulated client end-to-end: connect, exchange a handful
 * of update ticks while printing what it sees, then disconnect. */
static void* run_demo_client(void* arg) {
        int id = *((int*)arg);
        free(arg);

        char uuid[CLIENT_USERNAME_SIZE];
        snprintf(uuid, sizeof(uuid), "client-%d", id);

        atomicio_cl_t* client = atomicio_cl_create(uuid);
        if (!client) {
                fprintf(stderr, "[%s] failed to create client context\n", uuid);
                return NULL;
        }

        if (atomicio_cl_connect(client, TEST_PORT_STR, "127.0.0.1") != 0) {
                fprintf(stderr, "[%s] failed to connect\n", uuid);
                atomicio_cl_destroy(&client);
                return NULL;
        }

        for (int tick = 0; tick < TEST_TICKS && atomicio_cl_is_connected(client); tick++) {
        	char payload[PAYLOAD_MAX] = {0};              // PAYLOAD_MAX is specified in the AtomiC-IO protocol header: "at_net.h"
                
		snprintf(payload, PAYLOAD_MAX, "%s has sent 1KB of data... (tick %d)", uuid, tick);

                atomicio_cl_update_data(client, payload, PAYLOAD_MAX);
                atomicio_cl_send_data(client, UPDATE_MESSAGE);

                broadcast_view_t view;
                if (atomicio_cl_get_broadcast_data(client, &view) == 0) {
                        printf("[%s] DATA SENT SUCCESSFULLY --> sees %u active user(s):\n", uuid, view.count);
			/*                        
  			for (uint16_t i = 0; i < view.count; i++) {
				client_data_snapshot_t* snap = &view.snapshots[i];
                                printf("    - %s: %.*s\n",
                                	snap->uuid,
                                        snap->payload_len,
                                        snap->payload);
                        }
			*/
                }
		
		// NO SLEEP.
		// Stress testing rapid fire network IO and server stability 	
        }

        atomicio_cl_disconnect(client);
        atomicio_cl_destroy(&client);
        printf("[Client %s] disconnected\n", uuid);
        return NULL;
}

int main(void) {

        /* ------------------------------------------------------------------
         * 1. Start up logging and the server, exactly as in server_example.c
         * ------------------------------------------------------------------ */
        if (atomicio_log_init() != 0) {
                fprintf(stderr, "Failed to initialize logging\n");
                return -1;
        }

        atomicio_config_t config = {
                .port              = TEST_PORT_NUM,
                .max_users         = TEST_MAX_USERS,
                .devlogs_enabled   = true,
                .drop_late_packets = true,
                .log_path          = TEST_LOG_PATH
        };

        atomicio_server_ctx* server = atomicio_create_server(&config);
        if (!server) {
                fprintf(stderr, "Failed to create server\n");
                atomicio_log_shutdown();
                return -1;
        }

        if (atomicio_server_run(server) != 0) {
                fprintf(stderr, "Failed to start server\n");
                atomicio_server_destroy(&server);
                atomicio_log_shutdown();
                return -1;
        }

        printf("Demo server running on port %s\n\n", TEST_PORT_STR);


        /* ------------------------------------------------------------------
         * 2. Spawn several clients that all talk to the embedded server.
         * ------------------------------------------------------------------ */
        pthread_t client_threads[TEST_CLIENTS];
        for (int i = 0; i < TEST_CLIENTS; i++) {
                int* arg = (int*)malloc(sizeof(int));
                if (!arg) {
                        fprintf(stderr, "Failed to allocate client thread arg\n");
                        continue;
                }
                *arg = i;
                if (pthread_create(&client_threads[i], NULL, run_demo_client, arg) != 0) {
                        fprintf(stderr, "Failed to spawn client thread %d\n", i);
                        free(arg);
                }
        }


	// Waits for clients to finish up
        for (int i = 0; i < TEST_CLIENTS; i++) {
		char msg[32];
		snprintf(msg, 32, "LOG SPAM SUCCESS %d", i);
        	atomicio_log_info(server, msg); 
		pthread_join(client_threads[i], NULL);
	}

	// Prints server efficiency metrics
	printf("\n[Server efficiency metrics] | dropped packets: %llu | avg_latency: %.2fx    \n", (long long)atomicio_get_dropped_packets_count(server), (double)atomicio_get_avg_latency_multiplier(server));	


        /* ------------------------------------------------------------------
         * 3. All clients are done - now shut the server down cleanly.
         * ------------------------------------------------------------------ */
        printf("\nAll clients finished, shutting down server...\n");
        atomicio_server_shutdown(server);
        atomicio_server_destroy(&server);
        atomicio_log_shutdown();

        printf("Demo complete.\n");
        return 0;
}
