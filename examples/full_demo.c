/*
 * full_demo.c
 *
 * Self-contained example that runs an AtomiC-IO server AND several
 * clients in a single process, so you can watch the whole protocol
 * work end-to-end without needing multiple terminals.
 *
 * Demonstrates:
 *   - Running a server and multiple clients side by side
 *   - Each client publishing its own data and observing everyone else's
 *   - A clean, ordered shutdown: clients first, then the server, then logging
 *
 * Build (run from the project root, with Makefile):
 *
 * make
 *
 * cd ./bin
 *
 * Run:
 *   ./loopback_demo
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>

#include "../include/atomicio.h"
#include "../include/atomicio_cl.h"

#define DEMO_PORT_STR   "5555"		/* atomicio_cl_connect() wants a string port */
#define DEMO_PORT_NUM   5555         	/* atomicio_config_t wants a numeric port        */
#define DEMO_MAX_USERS  16
#define DEMO_LOG_PATH   "atomicio_demo"
#define DEMO_CLIENTS    3
#define DEMO_TICKS      5               /* number of update/broadcast cycles each client runs */



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

        if (atomicio_cl_connect(client, DEMO_PORT_STR, "127.0.0.1") != 0) {
                fprintf(stderr, "[%s] failed to connect\n", uuid);
                atomicio_cl_destroy(&client);
                return NULL;
        }

        for (int tick = 0; tick < DEMO_TICKS && atomicio_cl_is_connected(client); tick++) {
        	char payload[PAYLOAD_MAX];              // PAYLOAD_MAX is specified in the AtomiC-IO protocol header: "at_net.h"

                int len = snprintf(payload, PAYLOAD_MAX, "%s says hello (tick %d)", uuid, tick);
                len = (len < 0) ? 0 : (len > PAYLOAD_MAX) ? PAYLOAD_MAX : len; 

                atomicio_cl_update_data(client, payload, (uint16_t)len);
                atomicio_cl_send_data(client, UPDATE_MESSAGE);

                broadcast_view_t view;
                if (atomicio_cl_get_broadcast_data(client, &view) == 0) {
                        printf("[%s] sees %u active user(s):\n", uuid, view.count);
                        for (uint16_t i = 0; i < view.count; i++) {
				client_data_snapshot_t* snap = &view.snapshots[i];
                                printf("    - %s: %.*s\n",
                                	snap->uuid,
                                        snap->payload_len,
                                        snap->payload);
                        }
                }

                sleep(1);
        }

        atomicio_cl_disconnect(client);
        atomicio_cl_destroy(&client);
        printf("[Client %s] disconnected\n", uuid);
        return NULL;
}

int main(void) {

        /* ------------------------------------------------------------------
         * 1. Bring up logging and the server, exactly as in server_example.c
         * ------------------------------------------------------------------ */
        if (atomicio_log_init() != 0) {
                fprintf(stderr, "Failed to initialize logging\n");
                return EXIT_FAILURE;
        }

        atomicio_config_t config = {
                .port              = DEMO_PORT_NUM,
                .max_users         = DEMO_MAX_USERS,
                .devlogs_enabled   = false,
                .drop_late_packets = true,
                .log_path          = DEMO_LOG_PATH
        };

        atomicio_server_ctx* server = atomicio_create_server(&config);
        if (!server) {
                fprintf(stderr, "Failed to create server\n");
                atomicio_log_shutdown();
                return EXIT_FAILURE;
        }

        if (atomicio_server_run(server) != 0) {
                fprintf(stderr, "Failed to start server\n");
                atomicio_server_destroy(&server);
                atomicio_log_shutdown();
                return EXIT_FAILURE;
        }

        printf("Demo server running on port %s\n\n", DEMO_PORT_STR);

        /* Give the accept thread a moment to start listening before clients dial in. */
        usleep(100 * 1000);

        /* ------------------------------------------------------------------
         * 2. Spawn several clients that all talk to the embedded server.
         * ------------------------------------------------------------------ */
        pthread_t client_threads[DEMO_CLIENTS];
        for (int i = 0; i < DEMO_CLIENTS; i++) {
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
        for (int i = 0; i < DEMO_CLIENTS; i++)
                pthread_join(client_threads[i], NULL);

        /* ------------------------------------------------------------------
         * 3. All clients are done - now shut the server down cleanly.
         * ------------------------------------------------------------------ */
        printf("\nAll clients finished, shutting down server...\n");
        atomicio_server_shutdown(server);
        atomicio_server_destroy(&server);
        atomicio_log_shutdown();

        printf("Demo complete.\n");
        return EXIT_SUCCESS;
}
