/* -------------------------------------------------------------------------
 *
 * auth_delay.c
 *
 * Copyright (c) 2010-2017, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *        contrib/auth_delay/auth_delay.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "libpq/auth.h"
#include "port.h"
#include "utils/guc.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

void        _PG_init(void);

/* GUC Variables */
static int    auth_delay_milliseconds;  // 认证延迟的毫秒数

/* Original Hook */
static ClientAuthentication_hook_type original_client_auth_hook = NULL; // 原始客户端认证钩子

/*
 * Check authentication
 */
static void
auth_delay_checks(Port *port, int status)
{
    /*
     * Any other plugins which use ClientAuthentication_hook.
     */
    if (original_client_auth_hook)
        original_client_auth_hook(port, status);  // 调用任何其他使用 ClientAuthentication_hook 的插件

    /*
     * Inject a short delay if authentication failed.
     */
    if (status != STATUS_OK)
    {
        pg_usleep(1000L * auth_delay_milliseconds); // 如果认证失败，注入短暂延迟
    }
}

/*
 * Module Load Callback
 */
void
_PG_init(void)
{
    /* Define custom GUC variables */
    DefineCustomIntVariable("auth_delay.milliseconds",
                            "Milliseconds to delay before reporting authentication failure",
                            NULL,
                            &auth_delay_milliseconds,
                            0,
                            0, INT_MAX / 1000,
                            PGC_SIGHUP,
                            GUC_UNIT_MS,
                            NULL,
                            NULL,
                            NULL); // 定义自定义 GUC 变量
    /* Install Hooks */
    original_client_auth_hook = ClientAuthentication_hook;
    ClientAuthentication_hook = auth_delay_checks; // 安装钩子
}
