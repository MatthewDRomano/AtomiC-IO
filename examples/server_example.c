/*
 * server_example.c
 *
 * Comprehensive usage example for the AtomiC-IO SERVER API (atomicio.h).
 *
 * Demonstrates:
 *   - Initialize the logging subsystem (independent of any server context)
 *   - Configuring and creating a server context
 *   - Booting the server (atomicio_server_run() is non-blocking)
 *   - Polling live status / telemetry getters while it runs
 *   - Logging custom info/error entries through the server's own log
 *   - A clean, ordered shutdown on SIGINT/SIGTERM
 *
 * Build (run from the project root, with Makefile):
 *
 * make
 *
 * cd ./bin
 *  
 * Run:
 *   ./server_example
 *   (then connect with client_example.c from another terminal)
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>

#include "../include/atomicio.h"
#include "../include/at_net.h"


// Helpful server config constants
#define SERVER_PORT          5555
#define SERVER_LOG_PATH  "atomicio_server"   /* log.c appends ".txt" automatically */

// guaranteed safe to use in a signal handler
static volatile sig_atomic_t g_shutdown_requested = 0;



// Prints server metadata
static void print_status(atomicio_server_ctx* server) {
        printf("\r[status] users=%d/%d | session_uptime=%llds | dropped=%lld | avg_latency=%.2fx   ",
        	atomicio_get_active_user_count(server),
                MAX_CONNECTIONS,
                (long long)(atomicio_get_session_uptime_ms(server) / 1000),
                (long long)atomicio_get_dropped_packets_count(server),
                (double)atomicio_get_avg_latency_multiplier(server));
        fflush(stdout);
}

// Sets shutdown flag
static void handle_shutdown_signal(int signum) {
        (void)signum;
        g_shutdown_requested = 1;
}


int main(void) {

        /* Catch sigterm / sigint so the server has a graceful shutdown
         * instead of leaking sockets, the log thread, or client threads. */
        struct sigaction sa = {0};
        sa.sa_handler = handle_shutdown_signal;
        sigaction(SIGINT, &sa, NULL);
        sigaction(SIGTERM, &sa, NULL);

        /* ------------------------------------------------------------------
         * 1. Logging is global and independent of any single server context.
         *        Start it before anything else that might log.
         * ------------------------------------------------------------------ */
        if (atomicio_log_init() != 0) {
                fprintf(stderr, "Failed to initialize logging subsystem\n");
                return -1;
        }

        /* ------------------------------------------------------------------
         * 2. Configure and create the server context.
         * ------------------------------------------------------------------ */
        atomicio_config_t config = {
                .port              = SERVER_PORT,
                .max_users         = MAX_CONNECTIONS,
                .devlogs_enabled   = true,   /* verbose latency / drop logging   */
                .drop_late_packets = true,   /* drop instead of backing up slow clients */
                .log_path          = SERVER_LOG_PATH
        };

        atomicio_server_ctx* server = atomicio_create_server(&config);
        if (!server) {
                fprintf(stderr, "Failed to create server context\n");
                atomicio_log_shutdown();
                return -1;
        }

        /* ------------------------------------------------------------------
         * 3. Boot the server. This is non-blocking --> it spawns the client
         *        accept thread, IO thread, and the reaper thread, then returns.
         * ------------------------------------------------------------------ */
        if (atomicio_server_run(server) != 0) {
                fprintf(stderr, "Failed to start server\n");
                atomicio_log_error(server, errno, "atomicio_server_run failed");
                atomicio_server_destroy(&server);
                atomicio_log_shutdown();
                return -1;
        }

        atomicio_log_info(server, "Server booted successfully");
        printf("AtomiC-IO server running on port %d (max users: %d)\n", SERVER_PORT, MAX_CONNECTIONS);
        printf("Press Ctrl+C to shut down gracefully.\n\n");

        /* ------------------------------------------------------------------
         * 4. The main thread is free to do whatever it wants while the
         *        server runs in the background - here we just poll status.
         * ------------------------------------------------------------------ */
        while (!g_shutdown_requested && atomicio_is_running(server)) {
                print_status(server);
                sleep(1);
        }

        printf("\n\nShutdown requested, draining connections...\n");

        /* ------------------------------------------------------------------
         * 5. Graceful shutdown sequence: closes the listener, signals every
         *        connected client, and waits for the accept/reaper threads.
         * ------------------------------------------------------------------ */
        if (atomicio_server_shutdown(server) != 0)
                fprintf(stderr, "Server shutdown reported an error\n");
        else
                atomicio_log_info(server, "Server shut down gracefully");

        /* ------------------------------------------------------------------
         * 6. Destroy frees all context memory and nulls the pointer for you.
         * ------------------------------------------------------------------ */
        atomicio_server_destroy(&server);

        /* ------------------------------------------------------------------
         * 7. Logging is independent of the server context - shut it down
         *        last so any entries queued during shutdown still get flushed.
         * ------------------------------------------------------------------ */
        atomicio_log_shutdown();

        printf("Server stopped.\n");
        return 0;
}
