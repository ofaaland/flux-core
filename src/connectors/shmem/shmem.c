/************************************************************\
 * Copyright 2014 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

/* Note:
 * This connector creates a 0MQ inproc socket that communicates with another
 * inproc socket in the same process (normally the flux broker).  Pairs of
 * inproc sockets must share a common 0MQ context.  This connector uses the
 * libczmq zsock class, which hides creation/sharing of the 0MQ context;
 * therefore, the other inproc socket should be created with zsock also.
 */

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <assert.h>
#include <errno.h>
#include <czmq.h>
#include <argz.h>
#if HAVE_CALIPER
#include <caliper/cali.h>
#endif
#include <flux/core.h>

#include "src/common/libzmqutil/msg_zsock.h"
#include "src/common/libutil/log.h"

#define MODHANDLE_MAGIC    0xfeefbe02
typedef struct {
    int magic;
    zsock_t *sock;
    char *uuid;
    flux_t *h;
    char *argz;
    size_t argz_len;
} shmem_ctx_t;

static const struct flux_handle_ops handle_ops;

static int op_pollevents (void *impl)
{
    shmem_ctx_t *ctx = impl;
    assert (ctx->magic == MODHANDLE_MAGIC);
    uint32_t e;
    int revents = 0;

    e = zsock_events (ctx->sock);
    if (e & ZMQ_POLLIN)
        revents |= FLUX_POLLIN;
    if (e & ZMQ_POLLOUT)
        revents |= FLUX_POLLOUT;
    if (e & ZMQ_POLLERR)
        revents |= FLUX_POLLERR;

    return revents;
}

static int op_pollfd (void *impl)
{
    shmem_ctx_t *ctx = impl;
    assert (ctx->magic == MODHANDLE_MAGIC);

    return zsock_fd (ctx->sock);
}

static int op_send (void *impl, const flux_msg_t *msg, int flags)
{
    shmem_ctx_t *ctx = impl;
    assert (ctx->magic == MODHANDLE_MAGIC);

    return zmqutil_msg_send (ctx->sock, msg);
}

static flux_msg_t *op_recv (void *impl, int flags)
{
    shmem_ctx_t *ctx = impl;
    assert (ctx->magic == MODHANDLE_MAGIC);
    zmq_pollitem_t zp = {
        .events = ZMQ_POLLIN,
        .socket = zsock_resolve (ctx->sock),
        .revents = 0,
        .fd = -1,
    };
    flux_msg_t *msg = NULL;

    if ((flags & FLUX_O_NONBLOCK)) {
        int n;
        if ((n = zmq_poll (&zp, 1, 0L)) <= 0) {
            if (n == 0)
                errno = EWOULDBLOCK;
            goto done;
        }
    }
    msg = zmqutil_msg_recv (ctx->sock);
done:
    return msg;
}

static int op_event_subscribe (void *impl, const char *topic)
{
    shmem_ctx_t *ctx = impl;
    assert (ctx->magic == MODHANDLE_MAGIC);
    flux_future_t *f;
    int rc = -1;

    if (!(f = flux_rpc_pack (ctx->h, "broker.sub", FLUX_NODEID_ANY, 0,
                             "{ s:s }", "topic", topic)))
        goto done;
    if (flux_future_get (f, NULL) < 0)
        goto done;
    rc = 0;
done:
    flux_future_destroy (f);
    return rc;
}

static int op_event_unsubscribe (void *impl, const char *topic)
{
    shmem_ctx_t *ctx = impl;
    assert (ctx->magic == MODHANDLE_MAGIC);
    flux_future_t *f = NULL;
    int rc = -1;

    if (!(f = flux_rpc_pack (ctx->h, "broker.unsub", FLUX_NODEID_ANY, 0,
                             "{ s:s }", "topic", topic)))
        goto done;
    if (flux_future_get (f, NULL) < 0)
        goto done;
    rc = 0;
done:
    flux_future_destroy (f);
    return rc;
}


static void op_fini (void *impl)
{
    shmem_ctx_t *ctx = impl;
    assert (ctx->magic == MODHANDLE_MAGIC);
    zsock_destroy (&ctx->sock);
    free (ctx->argz);
    ctx->magic = ~MODHANDLE_MAGIC;
    free (ctx);
}

flux_t *connector_init (const char *path, int flags)
{
#if HAVE_CALIPER
    cali_id_t uuid   = cali_create_attribute ("flux.uuid",
                                              CALI_TYPE_STRING,
                                              CALI_ATTR_SKIP_EVENTS);
    size_t length = strlen(path);
    cali_push_snapshot ( CALI_SCOPE_PROCESS | CALI_SCOPE_THREAD,
                         1, &uuid, (const void **)&path, &length);
#endif

    shmem_ctx_t *ctx = NULL;
    char *item;
    int e;
    int bind_socket = 0; // if set, call bind on socket, else connect

    if (!path) {
        errno = EINVAL;
        goto error;
    }
    if (!(ctx = calloc (1, sizeof (*ctx)))) {
        errno = ENOMEM;
        goto error;
    }
    ctx->magic = MODHANDLE_MAGIC;
    if ((e = argz_create_sep (path, '&', &ctx->argz, &ctx->argz_len)) != 0) {
        errno = e;
        goto error;
    }
    ctx->uuid = item = argz_next (ctx->argz, ctx->argz_len, NULL);
    if (!ctx->uuid) {
        errno = EINVAL;
        goto error;
    }
    while ((item = argz_next (ctx->argz, ctx->argz_len, item))) {
        if (!strcmp (item, "bind"))
            bind_socket = 1;
        else if (!strcmp (item, "connect"))
            bind_socket = 0;
        else {
            errno = EINVAL;
            goto error;
        }
    }
    if (!(ctx->sock = zsock_new_pair (NULL)))
        goto error;
    zsock_set_unbounded (ctx->sock);
    zsock_set_linger (ctx->sock, 5);
    if (bind_socket) {
        if (zsock_bind (ctx->sock, "inproc://%s", ctx->uuid) < 0)
            goto error;
    }
    else {
        if (zsock_connect (ctx->sock, "inproc://%s", ctx->uuid) < 0)
            goto error;
    }
    if (!(ctx->h = flux_handle_create (ctx, &handle_ops, flags)))
        goto error;
    return ctx->h;
error:
    if (ctx) {
        int saved_errno = errno;
        op_fini (ctx);
        errno = saved_errno;
    }
    return NULL;
}

static const struct flux_handle_ops handle_ops = {
    .pollfd = op_pollfd,
    .pollevents = op_pollevents,
    .send = op_send,
    .recv = op_recv,
    .getopt = NULL,
    .setopt = NULL,
    .event_subscribe = op_event_subscribe,
    .event_unsubscribe = op_event_unsubscribe,
    .impl_destroy = op_fini,
};

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
