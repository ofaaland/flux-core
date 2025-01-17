/************************************************************\
 * Copyright 2014 Lawrence Livermore National Security, LLC
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
#include <limits.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <stdbool.h>
#include <dlfcn.h>
#include <sys/epoll.h>
#include <poll.h>
#if HAVE_CALIPER
#include <caliper/cali.h>
#include <sys/syscall.h>
#endif

#include "src/common/libczmqcontainers/czmq_containers.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/dirwalk.h"
#include "src/common/libutil/aux.h"
#include "src/common/libutil/errno_safe.h"
#include "src/common/libutil/monotime.h"

#include "handle.h"
#include "reactor.h"
#include "connector.h"
#include "message.h"
#include "tagpool.h"
#include "msg_handler.h" // for flux_sleep_on ()
#include "flog.h"
#include "conf.h"

#if HAVE_CALIPER
struct profiling_context {
    int initialized;
    cali_id_t msg_type;
    cali_id_t msg_seq;
    cali_id_t msg_topic;
    cali_id_t msg_sender;
    cali_id_t msg_rpc;
    cali_id_t msg_rpc_nodeid;
    cali_id_t msg_rpc_resp_expected;
    cali_id_t msg_action;
    cali_id_t msg_match_type;
    cali_id_t msg_match_tag;
    cali_id_t msg_match_glob;
};
#endif

struct flux_handle_struct {
    flux_t          *parent; // if FLUX_O_CLONE, my parent
    struct aux_item *aux;
    int             usecount;
    int             flags;

    /* element below are unused in cloned handles */
    const struct flux_handle_ops *ops;
    void            *impl;
    void            *dso;
    struct flux_msglist *queue;
    int             pollfd;

    struct tagpool  *tagpool;
    flux_msgcounters_t msgcounters;
    flux_fatal_f    fatal;
    void            *fatal_arg;
    bool            fatality;
    bool            destroy_in_progress;
#if HAVE_CALIPER
    struct profiling_context prof;
#endif
};

static flux_t *lookup_clone_ancestor (flux_t *h)
{
    while ((h->flags & FLUX_O_CLONE))
        h = h->parent;
    return h;
}

void tagpool_grow_notify (void *arg, uint32_t old, uint32_t new);

#if HAVE_CALIPER
void profiling_context_init (struct profiling_context* prof)
{
    prof->msg_type = cali_create_attribute ("flux.message.type",
                                            CALI_TYPE_STRING,
                                            CALI_ATTR_DEFAULT | CALI_ATTR_ASVALUE);
    prof->msg_seq = cali_create_attribute ("flux.message.seq",
                                           CALI_TYPE_INT,
                                           CALI_ATTR_SKIP_EVENTS);
    prof->msg_topic = cali_create_attribute ("flux.message.topic",
                                             CALI_TYPE_STRING,
                                             CALI_ATTR_DEFAULT | CALI_ATTR_ASVALUE);
    prof->msg_sender = cali_create_attribute ("flux.message.sender",
                                              CALI_TYPE_STRING,
                                              CALI_ATTR_SKIP_EVENTS);
    // if flux.message.rpc is set, we're inside an RPC, it will be set to a
    // type, single or multi
    prof->msg_rpc = cali_create_attribute ("flux.message.rpc",
                                           CALI_TYPE_STRING,
                                           CALI_ATTR_SKIP_EVENTS);
    prof->msg_rpc_nodeid = cali_create_attribute ("flux.message.rpc.nodeid",
                                                  CALI_TYPE_INT,
                                                  CALI_ATTR_SKIP_EVENTS);
    prof->msg_rpc_resp_expected =
        cali_create_attribute ("flux.message.response_expected",
                               CALI_TYPE_INT,
                               CALI_ATTR_SKIP_EVENTS);
    prof->msg_action = cali_create_attribute ("flux.message.action",
                                              CALI_TYPE_STRING,
                                              CALI_ATTR_DEFAULT | CALI_ATTR_ASVALUE);
    prof->msg_match_type = cali_create_attribute ("flux.message.match.type",
                                                  CALI_TYPE_INT,
                                                  CALI_ATTR_SKIP_EVENTS);
    prof->msg_match_tag = cali_create_attribute ("flux.message.match.tag",
                                                 CALI_TYPE_INT,
                                                 CALI_ATTR_SKIP_EVENTS);
    prof->msg_match_glob = cali_create_attribute ("flux.message.match.glob",
                                                  CALI_TYPE_STRING,
                                                  CALI_ATTR_SKIP_EVENTS);
    prof->initialized=1;
}

static void profiling_msg_snapshot (flux_t *h,
                          const flux_msg_t *msg,
                          int flags,
                          const char *msg_action)
{
    h = lookup_clone_ancestor (h);
    cali_id_t attributes[3];
    const void * data[3];
    size_t size[3];

    // This can get called before the handle is really ready
    if(! h->prof.initialized) return;

    int len = 0;

    if (msg_action) {
        attributes[len] = h->prof.msg_action;
        data[len] = msg_action;
        size[len] = strlen(msg_action);
        ++len;
    }

    int type;
    flux_msg_get_type (msg, &type);
    const char *msg_type = flux_msg_typestr (type);
    if (msg_type) {
        attributes[len] = h->prof.msg_type;
        data[len] = msg_type;
        size[len] = strlen(msg_type);
        ++len;
    }

    const char *msg_topic;
    if (type != FLUX_MSGTYPE_KEEPALIVE)
        flux_msg_get_topic (msg, &msg_topic);
    else
        msg_topic = "NONE";
    /* attributes[len] = h->prof.msg_topic; */
    /* data[len] = msg_topic; */
    /* size[len] = strlen(msg_topic); */
    /* ++len; */

    if (type == FLUX_MSGTYPE_EVENT) {
        uint32_t seq;
        flux_msg_get_seq (msg, &seq);
        cali_begin_int (h->prof.msg_seq, seq);
    }
    cali_push_snapshot (CALI_SCOPE_PROCESS | CALI_SCOPE_THREAD,
                        len /* n_entries */,
                        attributes /* event_attributes */,
                        data /* event_data */,
                        size /* event_size */);
    if (type == FLUX_MSGTYPE_EVENT)
        cali_end (h->prof.msg_seq);
}


#endif

static char *find_file (const char *name, const char *searchpath)
{
    char *path;
    zlist_t *l;
    if (!(l = dirwalk_find (searchpath, DIRWALK_REALPATH, name, 1, NULL, NULL)))
        return NULL;
    path = zlist_pop (l);
    zlist_destroy (&l);
    return path;
}

static connector_init_f *find_connector (const char *scheme, void **dsop)
{
    char name[PATH_MAX];
    const char *searchpath = getenv ("FLUX_CONNECTOR_PATH");
    char *path = NULL;
    void *dso = NULL;
    connector_init_f *connector_init = NULL;

    if (!searchpath)
        searchpath = flux_conf_builtin_get ("connector_path", FLUX_CONF_AUTO);
    if (snprintf (name, sizeof (name), "%s.so", scheme) >= sizeof (name)) {
        errno = ENAMETOOLONG;
        return NULL;
    }
    if (!(path = find_file (name, searchpath))) {
        errno = ENOENT;
        goto error;
    }
    if (!(dso = dlopen (path, RTLD_LAZY | RTLD_LOCAL | FLUX_DEEPBIND))) {
        errno = EINVAL;
        goto error;
    }
    if (!(connector_init = dlsym (dso, "connector_init"))) {
        errno = EINVAL;
        goto error_dlopen;
    }
    *dsop = dso;
    free (path);
    return connector_init;
error_dlopen:
    ERRNO_SAFE_WRAP (dlclose, dso);
error:
    ERRNO_SAFE_WRAP (free, path);
    return NULL;
}

static char *strtrim (char *s, const char *trim)
{
    char *p = s + strlen (s) - 1;
    while (p >= s && strchr (trim, *p))
        *p-- = '\0';
    return *s ? s : NULL;
}

flux_t *flux_open (const char *uri, int flags)
{
    char *default_uri = NULL;
    char *path = NULL;
    char *scheme = NULL;
    void *dso = NULL;
    connector_init_f *connector_init = NULL;
    const char *s;
    flux_t *h = NULL;

    /* Try to get URI from (in descending precedence):
     *   argument > environment > builtin
     */
    if (!uri)
        uri = getenv ("FLUX_URI");
    if (!uri) {
        if (asprintf (&default_uri, "local://%s/local",
                      flux_conf_builtin_get ("rundir",
                                             FLUX_CONF_INSTALLED)) < 0)
            goto error;
        uri = default_uri;
    }
    if (!(scheme = strdup (uri)))
        goto error;
    path = strstr (scheme, "://");
    if (path) {
        *path = '\0';
        path = strtrim (path + 3, " \t");
    }
    if (!(connector_init = find_connector (scheme, &dso)))
        goto error;
    if (getenv ("FLUX_HANDLE_TRACE"))
        flags |= FLUX_O_TRACE;
    if (getenv ("FLUX_HANDLE_MATCHDEBUG"))
        flags |= FLUX_O_MATCHDEBUG;
    if (!(h = connector_init (path, flags))) {
        ERRNO_SAFE_WRAP (dlclose, dso);
        goto error;
    }
    h->dso = dso;
#if HAVE_CALIPER
    profiling_context_init(&h->prof);
#endif
    if ((s = getenv ("FLUX_HANDLE_USERID"))) {
        uint32_t userid = strtoul (s, NULL, 10);
        if (flux_opt_set (h, FLUX_OPT_TESTING_USERID, &userid,
                                                      sizeof (userid)) < 0)
            goto error_handle;
    }
    if ((s = getenv ("FLUX_HANDLE_ROLEMASK"))) {
        uint32_t rolemask = strtoul (s, NULL, 0);
        if (flux_opt_set (h, FLUX_OPT_TESTING_ROLEMASK, &rolemask,
                                                    sizeof (rolemask)) < 0)
            goto error_handle;
    }
    free (scheme);
    free (default_uri);
    return h;
error_handle:
    flux_handle_destroy (h);
error:
    ERRNO_SAFE_WRAP (free, scheme);
    ERRNO_SAFE_WRAP (free, default_uri);
    return NULL;
}

void flux_close (flux_t *h)
{
    flux_handle_destroy (h);
}

flux_t *flux_handle_create (void *impl, const struct flux_handle_ops *ops, int flags)
{
    flux_t *h;

    if (!(h = calloc (1, sizeof (*h))))
        return NULL;
    h->usecount = 1;
    h->flags = flags;
    h->ops = ops;
    h->impl = impl;
    if (!(h->tagpool = tagpool_create ()))
        goto error;
    tagpool_set_grow_cb (h->tagpool, tagpool_grow_notify, h);
    if (!(h->queue = flux_msglist_create ()))
        goto error;
    h->pollfd = -1;
    return h;
error:
    flux_handle_destroy (h);
    return NULL;
}

flux_t *flux_clone (flux_t *orig)
{
    flux_t *h;
    if (!orig) {
        errno = EINVAL;
        return NULL;
    }
    if (!(h = calloc (1, sizeof (*h))))
        return NULL;
    h->parent = flux_incref (orig);
    h->usecount = 1;
    h->flags = orig->flags | FLUX_O_CLONE;
    return h;
}

static void report_leaked_matchtags (struct tagpool *tp)
{
    uint32_t count = tagpool_getattr (tp, TAGPOOL_ATTR_SIZE) -
                     tagpool_getattr (tp, TAGPOOL_ATTR_AVAIL);
    if (count > 0)
        fprintf (stderr,
                 "MATCHDEBUG: pool destroy with %d allocated\n", count);
}

void flux_handle_destroy (flux_t *h)
{
    if (h && --h->usecount == 0) {
        int saved_errno = errno;
        if (h->destroy_in_progress)
            return;
        h->destroy_in_progress = true;
        aux_destroy (&h->aux);
        if ((h->flags & FLUX_O_CLONE)) {
            flux_handle_destroy (h->parent); // decr usecount
        }
        else {
            if (h->ops->impl_destroy)
                h->ops->impl_destroy (h->impl);
            if ((h->flags & FLUX_O_MATCHDEBUG))
                report_leaked_matchtags (h->tagpool);
            tagpool_destroy (h->tagpool);
#ifndef __SANITIZE_ADDRESS__
            if (h->dso)
                dlclose (h->dso);
#endif
            flux_msglist_destroy (h->queue);
            if (h->pollfd >= 0)
                (void)close (h->pollfd);
        }
        free (h);
        errno = saved_errno;
    }
}

flux_t *flux_incref (flux_t *h)
{
    if (h)
        h->usecount++;
    return h;
}

void flux_decref (flux_t *h)
{
    flux_handle_destroy (h);
}

void flux_flags_set (flux_t *h, int flags)
{
    h->flags |= flags;
}

void flux_flags_unset (flux_t *h, int flags)
{
    h->flags &= ~flags;
}

int flux_flags_get (flux_t *h)
{
    return h->flags;
}

int flux_opt_get (flux_t *h, const char *option, void *val, size_t len)
{
    h = lookup_clone_ancestor (h);
    if (!h->ops->getopt) {
        errno = EINVAL;
        return -1;
    }
    return h->ops->getopt (h->impl, option, val, len);
}

int flux_opt_set (flux_t *h, const char *option, const void *val, size_t len)
{
    h = lookup_clone_ancestor (h);
    if (!h->ops->setopt) {
        errno = EINVAL;
        return -1;
    }
    return h->ops->setopt (h->impl, option, val, len);
}

void *flux_aux_get (flux_t *h, const char *name)
{
    if (!h) {
        errno = EINVAL;
        return NULL;
    }
    return aux_get (h->aux, name);
}

int flux_aux_set (flux_t *h, const char *name, void *aux, flux_free_f destroy)
{
    if (!h) {
        errno = EINVAL;
        return -1;
    }
    return aux_set (&h->aux, name, aux, destroy);
}

void flux_fatal_set (flux_t *h, flux_fatal_f fun, void *arg)
{
    h = lookup_clone_ancestor (h);
    h->fatal = fun;
    h->fatal_arg = arg;
    h->fatality = false;
}

void flux_fatal_error (flux_t *h, const char *fun, const char *msg)
{
    h = lookup_clone_ancestor (h);
    if (!h->fatality) {
        h->fatality = true;
        if (h->fatal) {
            char buf[256];
            snprintf (buf, sizeof (buf), "%s: %s", fun, msg);
            h->fatal (buf, h->fatal_arg);
        }
    }
}

bool flux_fatality (flux_t *h)
{
    h = lookup_clone_ancestor (h);
    return h->fatality;
}

void flux_get_msgcounters (flux_t *h, flux_msgcounters_t *mcs)
{
    h = lookup_clone_ancestor (h);
    *mcs = h->msgcounters;
}

void flux_clr_msgcounters (flux_t *h)
{
    h = lookup_clone_ancestor (h);
    memset (&h->msgcounters, 0, sizeof (h->msgcounters));
}

void tagpool_grow_notify (void *arg, uint32_t old, uint32_t new)
{
    flux_t *h = arg;
    flux_log (h, LOG_INFO, "tagpool expanded from %u to %u entries", old, new);
}

uint32_t flux_matchtag_alloc (flux_t *h)
{
    h = lookup_clone_ancestor (h);
    uint32_t tag;

    tag = tagpool_alloc (h->tagpool);
    if (tag == FLUX_MATCHTAG_NONE) {
        flux_log (h, LOG_ERR, "tagpool temporarily out of tags");
        errno = EBUSY; /* appropriate error? */
    }
    return tag;
}

void flux_matchtag_free (flux_t *h, uint32_t matchtag)
{
    h = lookup_clone_ancestor (h);
    tagpool_free (h->tagpool, matchtag);
}

uint32_t flux_matchtag_avail (flux_t *h)
{
    h = lookup_clone_ancestor (h);
    return tagpool_getattr (h->tagpool, TAGPOOL_ATTR_AVAIL);
}

static void update_tx_stats (flux_t *h, const flux_msg_t *msg)
{
    int type;
    if (flux_msg_get_type (msg, &type) == 0) {
        switch (type) {
            case FLUX_MSGTYPE_REQUEST:
                h->msgcounters.request_tx++;
                break;
            case FLUX_MSGTYPE_RESPONSE:
                h->msgcounters.response_tx++;
                break;
            case FLUX_MSGTYPE_EVENT:
                h->msgcounters.event_tx++;
                break;
            case FLUX_MSGTYPE_KEEPALIVE:
                h->msgcounters.keepalive_tx++;
                break;
        }
    } else
        errno = 0;
}

static void update_rx_stats (flux_t *h, const flux_msg_t *msg)
{
    int type;
    if (flux_msg_get_type (msg, &type) == 0) {
        switch (type) {
            case FLUX_MSGTYPE_REQUEST:
                h->msgcounters.request_rx++;
                break;
            case FLUX_MSGTYPE_RESPONSE:
                h->msgcounters.response_rx++;
                break;
            case FLUX_MSGTYPE_EVENT:
                h->msgcounters.event_rx++;
                break;
        case FLUX_MSGTYPE_KEEPALIVE:
            h->msgcounters.keepalive_rx++;
            break;
        }
    } else
        errno = 0;
}

static void handle_trace (flux_t *h, const flux_msg_t *msg)
{
    const char *auxkey = "flux::trace_start";
    struct timespec *ts;

    if (!(ts = flux_aux_get (h, auxkey))) {
        if ((ts = calloc (1, sizeof (*ts)))) {
            monotime (ts);
            if (flux_aux_set (h, auxkey, ts, free) < 0) {
                free (ts);
                ts = NULL;
            }
        }
    }
    flux_msg_fprint_ts (stderr, msg, ts ? monotime_since (*ts)/1000 : -1);
}

int flux_send (flux_t *h, const flux_msg_t *msg, int flags)
{
    h = lookup_clone_ancestor (h);
    if (!h->ops->send || h->destroy_in_progress) {
        errno = ENOSYS;
        goto fatal;
    }
    flags |= h->flags;
    update_tx_stats (h, msg);
    if ((flags & FLUX_O_TRACE))
        handle_trace (h, msg);
    if (h->ops->send (h->impl, msg, flags) < 0)
        goto fatal;
#if HAVE_CALIPER
    profiling_msg_snapshot(h, msg, flags, "send");
#endif
    return 0;
fatal:
    FLUX_FATAL (h);
    return -1;
}

static int defer_enqueue (zlist_t **l, flux_msg_t *msg)
{
    if ((!*l && !(*l = zlist_new ())) || zlist_append (*l, msg) < 0) {
        errno = ENOMEM;
        return -1;
    }
    return 0;
}

static int defer_requeue (zlist_t **l, flux_t *h)
{
    flux_msg_t *msg;
    if (*l) {
        while ((msg = zlist_pop (*l))) {
            int rc = flux_requeue (h, msg, FLUX_RQ_TAIL);
            flux_msg_destroy (msg);
            if (rc < 0)
                return -1;
        }
    }
    return 0;
}

static void defer_destroy (zlist_t **l)
{
    flux_msg_t *msg;
    if (*l) {
        while ((msg = zlist_pop (*l)))
            flux_msg_destroy (msg);
        zlist_destroy (l);
    }
}

static flux_msg_t *flux_recv_any (flux_t *h, int flags)
{
    flux_msg_t *msg = NULL;
    if (flux_msglist_count (h->queue) > 0)
        msg = (flux_msg_t *)flux_msglist_pop (h->queue);
    else if (h->ops->recv)
        msg = h->ops->recv (h->impl, flags);
    else
        errno = ENOSYS;
    return msg;
}

/* N.B. the do loop below that reads messages and compares them to match
 * criteria may have to read a few non-matching messages before finding
 * a match.  On return, those non-matching messages have to be requeued
 * in the handle, hence the defer_*() helper calls.
 */
flux_msg_t *flux_recv (flux_t *h, struct flux_match match, int flags)
{
    h = lookup_clone_ancestor (h);
    zlist_t *l = NULL;
    flux_msg_t *msg = NULL;
    int saved_errno;

    flags |= h->flags;
    do {
        if (!(msg = flux_recv_any (h, flags))) {
            if (errno != EAGAIN && errno != EWOULDBLOCK)
                goto fatal;
            if (defer_requeue (&l, h) < 0)
                goto fatal;
            defer_destroy (&l);
            errno = EWOULDBLOCK;
            return NULL;
        }
        if (!flux_msg_cmp (msg, match)) {
            if (defer_enqueue (&l, msg) < 0)
                goto fatal;
            msg = NULL;
        }
    } while (!msg);
    update_rx_stats (h, msg);
    if ((flags & FLUX_O_TRACE))
        handle_trace (h, msg);
    if (defer_requeue (&l, h) < 0)
        goto fatal;
    defer_destroy (&l);
#if HAVE_CALIPER
    cali_begin_int (h->prof.msg_match_type, match.typemask);
    cali_begin_int (h->prof.msg_match_tag, match.matchtag);
    cali_begin_string (h->prof.msg_match_glob,
                       match.topic_glob ? match.topic_glob : "NONE");
    const char *sender;
    sender = flux_msg_route_first (msg);
    if (sender)
        cali_begin_string (h->prof.msg_sender, sender);
    profiling_msg_snapshot (h, msg, flags, "recv");
    if (sender)
        cali_end (h->prof.msg_sender);
    cali_end (h->prof.msg_match_type);
    cali_end (h->prof.msg_match_tag);
    cali_end (h->prof.msg_match_glob);
#endif
    return msg;
fatal:
    saved_errno = errno;
    FLUX_FATAL (h);
    if (msg)
        flux_msg_destroy (msg);
    defer_destroy (&l);
    errno = saved_errno;
    return NULL;
}

/* FIXME: FLUX_O_TRACE will show these messages being received again
 * So will message counters.
 */
int flux_requeue (flux_t *h, const flux_msg_t *msg, int flags)
{
    h = lookup_clone_ancestor (h);
    int rc;

    if (flags != FLUX_RQ_TAIL && flags != FLUX_RQ_HEAD) {
        errno = EINVAL;
        goto fatal;
    }
    if (flags == FLUX_RQ_TAIL)
        rc = flux_msglist_append (h->queue, msg);
    else
        rc = flux_msglist_push (h->queue, msg);
    if (rc < 0)
        goto fatal;
    return 0;
fatal:
    FLUX_FATAL (h);
    return -1;
}

int flux_event_subscribe (flux_t *h, const char *topic)
{
    h = lookup_clone_ancestor (h);
    if (h->ops->event_subscribe) {
        if (h->ops->event_subscribe (h->impl, topic) < 0)
            goto fatal;
    }
    return 0;
fatal:
    FLUX_FATAL (h);
    return -1;
}

int flux_event_unsubscribe (flux_t *h, const char *topic)
{
    h = lookup_clone_ancestor (h);
    if (h->ops->event_unsubscribe) {
        if (h->ops->event_unsubscribe (h->impl, topic) < 0)
            goto fatal;
    }
    return 0;
fatal:
    FLUX_FATAL (h);
    return -1;
}

int flux_pollfd (flux_t *h)
{
    h = lookup_clone_ancestor (h);
    if (h->pollfd < 0) {
        struct epoll_event ev = {
            .events = EPOLLET | EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP,
        };
        if ((h->pollfd = epoll_create1 (EPOLL_CLOEXEC)) < 0)
            goto fatal;
        /* add queue pollfd */
        ev.data.fd = flux_msglist_pollfd (h->queue);
        if (ev.data.fd < 0)
            goto fatal;
        if (epoll_ctl (h->pollfd, EPOLL_CTL_ADD, ev.data.fd, &ev) < 0)
            goto fatal;
        /* add connector pollfd (if defined) */
        if (h->ops->pollfd) {
            ev.data.fd = h->ops->pollfd (h->impl);
            if (ev.data.fd < 0)
                goto fatal;
            if (epoll_ctl (h->pollfd, EPOLL_CTL_ADD, ev.data.fd, &ev) < 0)
                goto fatal;
        }
    }
    return h->pollfd;
fatal:
    if (h->pollfd >= 0) {
        (void)close (h->pollfd);
        h->pollfd = -1;
    }
    FLUX_FATAL (h);
    return -1;
}

int flux_pollevents (flux_t *h)
{
    h = lookup_clone_ancestor (h);
    int e, events = 0;

    /* wait for handle event */
    if (h->pollfd >= 0) {
        struct epoll_event ev;
        (void)epoll_wait (h->pollfd, &ev, 1, 0);
    }
    /* get connector events (if applicable) */
    if (h->ops->pollevents) {
        if ((events = h->ops->pollevents (h->impl)) < 0)
            goto fatal;
    }
    /* get queue events */
    if ((e = flux_msglist_pollevents (h->queue)) < 0)
        goto fatal;
    if ((e & POLLIN))
        events |= FLUX_POLLIN;
    if ((e & POLLOUT))
        events |= FLUX_POLLOUT;
    if ((e & POLLERR))
        events |= FLUX_POLLERR;
    return events;
fatal:
    FLUX_FATAL (h);
    return -1;
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
