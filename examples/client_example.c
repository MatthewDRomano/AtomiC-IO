/*
 * client_example.c
 *
 * Comprehensive usage example for the AtomiC-IO CLIENT API (atomicio_cl.h).
 *
 * Demonstrates:
 *   - Client context lifecycle (create -> connect -> disconnect -> destroy)
 *   - Staging and sending local payload data
 *   - Reading the broadcast view published back by the server
 *   - Connection diagnostics / metadata getters
 *   - Graceful shutdown on SIGINT/SIGTERM
 *
 * Build (run from the project root with Makefile):
 *
 * make
 *
 * cd ./bin
 *
 * Run:
 *   ./client_example <server_ip / domain name> <port> <your_uuid>
 *   ./client_example 127.0.0.1 5555 alice
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <signal.h>
#include <unistd.h>

#include "../include/atomicio_cl.h"
#include "../include/at_net.h"

static volatile sig_atomic_t g_shutdown_requested = 0;


/* Prints every connected client's most recently broadcast payload.
 * Note: this includes your own client too - the server treats every
 * connected client uniformly when building the broadcast. */
static void print_broadcast_view(const broadcast_view_t* view) {
        printf("--- Broadcast snapshot (%u active user%s) ---\n",
                   view->count, view->count == 1 ? "" : "s");
	
        for (uint16_t i = 0; i < view->count; i++) {
                const client_data_snapshot_t* snap = &view->snapshots[i];
                printf("  [%s] (%u bytes): %.*s\n",
                           snap->uuid,
                           snap->payload_len,
                           snap->payload_len, snap->payload);
        }
        printf("---------------------------------------------\n");
}


static void handle_shutdown_signal(int signum) {
        (void)signum;
        g_shutdown_requested = 1;
}


int main(int argc, char** argv) {
        if (argc != 4) {
                fprintf(stderr, "Usage: %s <server_ip> <port> <uuid>\n", argv[0]);
                return -1;
        }

        const char* server_ip = argv[1];
        const char* port_str  = argv[2];
        const char* uuid      = argv[3];

	struct sigaction sa = {0};
	sa.sa_handler = handle_shutdown_signal;
	sigaction(SIGINT, &sa, NULL);
	sigaction(SIGTERM, &sa, NULL);

        /* ------------------------------------------------------------------
         * 1. Create the client context.
         * ------------------------------------------------------------------ */
        atomicio_cl_t* client = atomicio_cl_create(uuid);
        if (!client) {
                fprintf(stderr, "Failed to create client context\n");
                return -1;
        }

        /* ------------------------------------------------------------------
         * 2. Connect. This sends the LOGIN packet and spawns the background
         *        receive thread internally - no extra setup needed.
         * ------------------------------------------------------------------ */
        if (atomicio_cl_connect(client, port_str, server_ip) != 0) {
                fprintf(stderr, "Failed to connect to %s:%s\n", server_ip, port_str);
                atomicio_cl_destroy(&client);
                return -1;
        }

        printf("Connected to %s:%s as \"%s\"\n", server_ip, port_str, uuid);
        printf("Press Ctrl+C to disconnect.\n\n");

        /* ------------------------------------------------------------------
         * 3. Main loop: stage local data, push it to the server, then
         * 	inspect the server's most recently broadcast. (One time per second)
         * ------------------------------------------------------------------ */
        int tick = 0;
        while (!g_shutdown_requested && atomicio_cl_is_connected(client)) {
                char payload[PAYLOAD_MAX];		// PAYLOAD_MAX is specified in the AtomiC-IO protocol header: "at_net.h"

                int len = snprintf(payload, PAYLOAD_MAX, "tick #%d from %s", tick++, uuid);
		len = (len < 0) ? 0 : (len > PAYLOAD_MAX) ? PAYLOAD_MAX : len;
	
                /* Stage the payload locally */
                if (atomicio_cl_update_data(client, payload, (uint16_t)len) != 0) {
                        fprintf(stderr, "Failed to stage outgoing data\n");
                        break;
                }

                /* Then push it to the server as an UPDATE_MESSAGE. 
 		   Message types also specified in "at_net.h" */
                if (atomicio_cl_send_data(client, UPDATE_MESSAGE) != 0) {
                        fprintf(stderr, "Send failed - connection likely dropped\n");
                        break;
                }

                /* Read back the latest broadcast (filled in by the background
                 * receive thread) and display it. */
                broadcast_view_t view;
                if (atomicio_cl_get_broadcast_data(client, &view) == 0)
                        print_broadcast_view(&view);

                printf("[diagnostics] active_users=%d | session_uptime=%llds | client_lifetime=%llds\n\n",
                           atomicio_cl_get_active_user_count(client),
                           (long long)(atomicio_cl_session_uptime(client) / 1000),
                           (long long)(atomicio_cl_lifetime(client) / 1000));

                sleep(1);
        }

        printf("\nDisconnecting...\n");

        /* ------------------------------------------------------------------
         * 4. Graceful disconnect, then destroy frees all context memory
         *        (and nulls the pointer for you).
         * ------------------------------------------------------------------ */
        atomicio_cl_disconnect(client);
        atomicio_cl_destroy(&client);

        printf("Disconnected.\n");
        return 0;
}
