/************************************************************\
 * Copyright 2018 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <flux/core.h>

#include "src/common/libjob/job_hash.h"
#include "src/common/libczmqcontainers/czmq_containers.h"

#include "job.h"
#include "submit.h"
#include "restart.h"
#include "raise.h"
#include "kill.h"
#include "list.h"
#include "urgency.h"
#include "alloc.h"
#include "start.h"
#include "event.h"
#include "drain.h"
#include "wait.h"
#include "annotate.h"
#include "journal.h"
#include "getattr.h"
#include "jobtap-internal.h"

#include "job-manager.h"

void getinfo_handle_request (flux_t *h,
                             flux_msg_handler_t *mh,
                             const flux_msg_t *msg,
                             void *arg)
{
    struct job_manager *ctx = arg;

    if (flux_request_decode (msg, NULL, NULL) < 0)
        goto error;
    if (flux_respond_pack (h,
                           msg,
                           "{s:I}",
                           "max_jobid",
                           ctx->max_jobid) < 0)
        flux_log_error (h, "%s: flux_respond_pack", __FUNCTION__);
    return;
error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

void disconnect_rpc (flux_t *h,
                     flux_msg_handler_t *mh,
                     const flux_msg_t *msg,
                     void *arg)
{
    /* disconnects occur once per client, there is no way to know
     * which services a client used, so we must check all services for
     * cleanup */
    alloc_disconnect_rpc (h, mh, msg, arg);
    wait_disconnect_rpc (h, mh, msg, arg);
    journal_listeners_disconnect_rpc (h, mh, msg, arg);
}

static void stats_cb (flux_t *h, flux_msg_handler_t *mh,
                      const flux_msg_t *msg, void *arg)
{
    struct job_manager *ctx = arg;
    int journal_listeners = journal_listeners_count (ctx->journal);
    if (flux_respond_pack (h, msg, "{s:{s:i}}",
                           "journal",
                             "listeners", journal_listeners) < 0) {
        flux_log_error (h, "%s: flux_respond_pack", __FUNCTION__);
        goto error;
    }

    return;
 error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static void config_reload_cb (flux_t *h,
                              flux_msg_handler_t *mh,
                              const flux_msg_t *msg,
                              void *arg)
{
    const flux_conf_t *conf;
    const char *errstr = NULL;

    if (flux_conf_reload_decode (msg, &conf) < 0)
        goto error;
    if (flux_set_conf (h, flux_conf_incref (conf)) < 0) {
        errstr = "error updating cached configuration";
        goto error;
    }
    if (flux_respond (h, msg, NULL) < 0)
        flux_log_error (h, "error responding to config-reload request");
    return;
error:
    if (flux_respond_error (h, msg, errno, errstr) < 0)
        flux_log_error (h, "error responding to config-reload request");
}

static const struct flux_msg_handler_spec htab[] = {
    {
        FLUX_MSGTYPE_REQUEST,
        "job-manager.list",
        list_handle_request,
        0
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "job-manager.urgency",
        urgency_handle_request,
        FLUX_ROLE_USER
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "job-manager.getattr",
        getattr_handle_request,
        FLUX_ROLE_USER
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "job-manager.getinfo",
        getinfo_handle_request,
        FLUX_ROLE_USER
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "job-manager.jobtap",
        jobtap_handler,
        FLUX_ROLE_OWNER,
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "job-manager.disconnect",
        disconnect_rpc,
        0
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "job-manager.stats.get",
        stats_cb,
        0
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "job-manager.config-reload",
        config_reload_cb,
        0,
    },

    FLUX_MSGHANDLER_TABLE_END,
};

int mod_main (flux_t *h, int argc, char **argv)
{
    flux_reactor_t *r = flux_get_reactor (h);
    int rc = -1;
    struct job_manager ctx;

    memset (&ctx, 0, sizeof (ctx));
    ctx.h = h;

    if (!(ctx.active_jobs = job_hash_create ())) {
        flux_log_error (h, "error creating active_jobs hash");
        goto done;
    }
    zhashx_set_destructor (ctx.active_jobs, job_destructor);
    zhashx_set_duplicator (ctx.active_jobs, job_duplicator);
    if (!(ctx.event = event_ctx_create (&ctx))) {
        flux_log_error (h, "error creating event batcher");
        goto done;
    }
    if (!(ctx.submit = submit_ctx_create (&ctx))) {
        flux_log_error (h, "error creating submit interface");
        goto done;
    }
    if (!(ctx.alloc = alloc_ctx_create (&ctx))) {
        flux_log_error (h, "error creating scheduler interface");
        goto done;
    }
    if (!(ctx.start = start_ctx_create (&ctx))) {
        flux_log_error (h, "error creating exec interface");
        goto done;
    }
    if (!(ctx.drain = drain_ctx_create (&ctx))) {
        flux_log_error (h, "error creating drain interface");
        goto done;
    }
    if (!(ctx.wait = wait_ctx_create (&ctx))) {
        flux_log_error (h, "error creating wait interface");
        goto done;
    }
    if (!(ctx.raise = raise_ctx_create (&ctx))) {
        flux_log_error (h, "error creating raise interface");
        goto done;
    }
    if (!(ctx.kill = kill_ctx_create (&ctx))) {
        flux_log_error (h, "error creating kill interface");
        goto done;
    }
    if (!(ctx.annotate = annotate_ctx_create (&ctx))) {
        flux_log_error (h, "error creating annotate interface");
        goto done;
    }
    if (!(ctx.journal = journal_ctx_create (&ctx))) {
        flux_log_error (h, "error creating journal interface");
        goto done;
    }
    if (!(ctx.jobtap = jobtap_create (&ctx))) {
        flux_log_error (h, "error creating jobtap interface");
        goto done;
    }
    if (flux_msg_handler_addvec (h, htab, &ctx, &ctx.handlers) < 0) {
        flux_log_error (h, "flux_msghandler_add");
        goto done;
    }
    if (restart_from_kvs (&ctx) < 0) {
        flux_log_error (h, "restart_from_kvs");
        goto done;
    }
    if (flux_reactor_run (r, 0) < 0) {
        flux_log_error (h, "flux_reactor_run");
        goto done;
    }
    if (checkpoint_to_kvs (&ctx) < 0) {
        flux_log_error (h, "checkpoint_to_kvs");
        goto done;
    }
    rc = 0;
done:
    flux_msg_handler_delvec (ctx.handlers);
    journal_ctx_destroy (ctx.journal);
    annotate_ctx_destroy (ctx.annotate);
    kill_ctx_destroy (ctx.kill);
    raise_ctx_destroy (ctx.raise);
    wait_ctx_destroy (ctx.wait);
    drain_ctx_destroy (ctx.drain);
    start_ctx_destroy (ctx.start);
    alloc_ctx_destroy (ctx.alloc);
    submit_ctx_destroy (ctx.submit);
    event_ctx_destroy (ctx.event);
    jobtap_destroy (ctx.jobtap);
    zhashx_destroy (&ctx.active_jobs);
    return rc;
}

MOD_NAME ("job-manager");

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
