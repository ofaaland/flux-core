/************************************************************\
 * Copyright 2018 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

/* publisher.c - event publishing service on rank 0 */

#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <errno.h>
#include <flux/core.h>

#include "src/common/libczmqcontainers/czmq_containers.h"
#include "src/common/libccan/ccan/base64/base64.h"

#include "publisher.h"


struct publisher {
    flux_t *h;
    flux_msg_handler_t **handlers;
    int seq;
    zlist_t *senders;
    publisher_send_f send;
    void *arg;
};

static flux_msg_t *encode_event (const char *topic, int flags,
                                 struct flux_msg_cred cred,
                                 uint32_t seq, const char *src)
{
    flux_msg_t *msg;
    char *dst = NULL;
    int saved_errno;

    if (!(msg = flux_msg_create (FLUX_MSGTYPE_EVENT)))
        goto error;
    if (flux_msg_set_topic (msg, topic) < 0)
        goto error;
    if (flux_msg_set_cred (msg, cred) < 0)
        goto error;
    if (flux_msg_set_seq (msg, seq) < 0)
        goto error;
    if ((flags & FLUX_MSGFLAG_PRIVATE)) {
        if (flux_msg_set_private (msg) < 0)
            goto error;
    }
    if (src) { // optional payload
        int srclen = strlen (src);
        size_t dstbuflen = base64_decoded_length (srclen);
        ssize_t dstlen;

        if (!(dst = malloc (dstbuflen)))
            goto error;
        if ((dstlen = base64_decode (dst, dstbuflen, src, srclen)) < 0) {
            errno = EPROTO;
            goto error;
        }
        if (flux_msg_set_payload (msg, dst, dstlen) < 0) {
            if (errno == EINVAL)
                errno = EPROTO;
            goto error;
        }
    }
    free (dst);
    return msg;
error:
    saved_errno = errno;
    free (dst);
    flux_msg_destroy (msg);
    errno = saved_errno;
    return NULL;
}

/* Broadcast event using all senders.
 * Log failure, but don't abort the event at this point.
 */
static void send_event (struct publisher *pub, const flux_msg_t *msg)
{
    if (pub->send (pub->arg, msg) < 0)
        flux_log_error (pub->h, "error publishing event message");
}

void pub_cb (flux_t *h, flux_msg_handler_t *mh,
             const flux_msg_t *msg, void *arg)
{
    struct publisher *pub = arg;
    const char *topic;
    const char *payload = NULL; // optional
    int flags;
    struct flux_msg_cred cred;
    flux_msg_t *event = NULL;

    if (flux_request_unpack (msg, NULL, "{s:s s:i s?:s}",
                                        "topic", &topic,
                                        "flags", &flags,
                                        "payload", &payload) < 0)
        goto error;
    if ((flags & ~(FLUX_MSGFLAG_PRIVATE)) != 0) {
        errno = EPROTO;
        goto error;
    }
    if (flux_msg_get_cred (msg, &cred) < 0)
        goto error;
    if (!(event = encode_event (topic, flags, cred, ++pub->seq, payload)))
        goto error_restore_seq;
    send_event (pub, event);
    if (flux_respond_pack (h, msg, "{s:i}", "seq", pub->seq) < 0)
        flux_log_error (h, "%s: flux_respond", __FUNCTION__);
    flux_msg_destroy (event);
    return;
error_restore_seq:
    pub->seq--;
error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
    flux_msg_destroy (event);
}

int publisher_send (struct publisher *pub, const flux_msg_t *msg)
{
    flux_msg_t *cpy;

    if (!(cpy = flux_msg_copy (msg, true)))
        return -1;
    flux_msg_route_disable (cpy);
    if (flux_msg_set_seq (cpy, ++pub->seq) < 0)
        goto error_restore_seq;
    send_event (pub, cpy);
    flux_msg_destroy (cpy);
    return 0;
error_restore_seq:
    pub->seq--;
    flux_msg_destroy (cpy);
    return -1;
}

static const struct flux_msg_handler_spec htab[] = {
    { FLUX_MSGTYPE_REQUEST, "event.pub",  pub_cb, FLUX_ROLE_USER },
    FLUX_MSGHANDLER_TABLE_END,
};

void publisher_destroy (struct publisher *pub)
{
    if (pub) {
        int saved_errno = errno;
        flux_msg_handler_delvec (pub->handlers);
        free (pub);
        errno = saved_errno;
    }
}

struct publisher *publisher_create (flux_t *h, publisher_send_f cb, void *arg)
{
    struct publisher *pub;

    if (!(pub = calloc (1, sizeof (*pub))))
        return NULL;
    pub->h = h;
    pub->send = cb;
    pub->arg = arg;
    if (flux_msg_handler_addvec (h, htab, pub, &pub->handlers) < 0) {
        publisher_destroy (pub);
        return NULL;
    }
    return pub;
}


/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
