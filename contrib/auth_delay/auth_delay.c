/*
 * auth_delay.c
 *
 * This module provides a way to introduce a delay in the authentication
 * process before reporting authentication failure. It can be useful for
 * thwarting timing attacks.
 */

#include "postgres.h"

#include <limits.h>

#include "libpq/auth.h"
#include "port.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;  /* PostgreSQL extension magic */

void        _PG_init(void);

/* GUC Variables */
static int    auth_delay_milliseconds;  /* Milliseconds to delay before reporting authentication failure */

/* Original Hook */
static ClientAuthentication_hook_type original_client_auth_hook = NULL;

/*
 * Check authentication
 *
 * This function is called during the authentication process. It checks the
 * status of authentication and introduces a delay if authentication failed.
 */
static void
auth_delay_checks(Port *port, int status)
{
    /*
     * Call any other plugins which use ClientAuthentication_hook.
     */
    if (original_client_auth_hook)
        original_client_auth_hook(port, status);

    /*
     * Inject a short delay if authentication failed.
     */
    if (status != STATUS_OK)
    {
        pg_usleep(1000L * auth_delay_milliseconds);  /* Delay specified milliseconds */
    }
}

/*
 * Module Load Callback
 *
 * This function is called when the module is loaded. It defines custom GUC
 * variables and installs hooks for authentication.
 */
void
_PG_init(void)
{
    /* Define custom GUC variable */
    DefineCustomIntVariable("auth_delay.milliseconds",
                            "Milliseconds to delay before reporting authentication failure",
                            NULL,
                            &auth_delay_milliseconds,
                            0,  /* Default value */
                            0, INT_MAX / 1000,  /* Min and max values */
                            PGC_SIGHUP,  /* Context */
                            GUC_UNIT_MS,  /* Unit */
                            NULL,
                            NULL,
                            NULL);

    /* Install Hooks */
    original_client_auth_hook = ClientAuthentication_hook;
    ClientAuthentication_hook = auth_delay_checks;
}
