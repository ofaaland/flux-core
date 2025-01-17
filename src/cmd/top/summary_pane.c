/************************************************************\
 * Copyright 2021 Lawrence Livermore National Security, LLC
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
#include <sys/types.h>
#include <unistd.h>
#include <time.h>
#include <jansson.h>

#include "src/common/libutil/fsd.h"
#include "src/common/librlist/rlist.h"

#include "top.h"

static const struct dimension win_dim = { 0, 0, 80, 5 };
static const struct dimension level_dim = { 0, 0, 2, 1 };
static const struct dimension jobid_dim = { 36, 0, 16, 1 };
static const struct dimension timeleft_dim = { 70, 0, 10, 1 };
static const struct dimension resource_dim = { 4, 1, 36, 3 };
static const struct dimension heart_dim = { 77, 3, 1, 1 };
static const struct dimension stats_dim = { 60, 1, 15, 3 };

static const double heartblink_duration = 0.5;

struct resource_count {
    int total;
    int down;
    int used;
};

struct stats {
    int depend;
    int priority;
    int sched;
    int run;
    int cleanup;
    int inactive;
    int total;
};

struct summary_pane {
    struct top *top;
    WINDOW *win;
    unsigned long instance_level;
    flux_jobid_t jobid;
    double expiration;
    struct stats stats;
    struct resource_count node;
    struct resource_count core;
    struct resource_count gpu;
    flux_watcher_t *heartblink;
    bool heart_visible;
};

static void draw_timeleft (struct summary_pane *sum)
{
    double now = flux_reactor_now (flux_get_reactor (sum->top->h));
    double timeleft = sum->expiration - now;
    char buf[32] = "";

    if (timeleft > 0)
        fsd_format_duration_ex (buf, sizeof (buf), timeleft, 2);

    mvwprintw (sum->win,
               timeleft_dim.y_begin,
               timeleft_dim.x_begin,
               "%*s%s",
               timeleft_dim.x_length - 2,
               buf,
               timeleft > 0 ? "⌚" : "∞");
}

static void draw_level (struct summary_pane *sum)
{
    const char *sup[] = { "", "²", "³", "⁴", "⁵", "⁶", "⁷", "⁸", "⁹", "⁺" };
    int size = sizeof (sup) / sizeof (sup[0]);
    unsigned long level = sum->instance_level;

    wattron (sum->win, COLOR_PAIR (TOP_COLOR_YELLOW));
    mvwprintw (sum->win,
               level_dim.y_begin,
               level_dim.x_begin,
               "%s%s",
               "ƒ",
               sup[level < size ? level : size - 1]);
    wattroff (sum->win, COLOR_PAIR (TOP_COLOR_YELLOW));
}

static void draw_jobid (struct summary_pane *sum)
{
    if (sum->jobid != FLUX_JOBID_ANY) {
        char buf[jobid_dim.x_length + 1];
        flux_job_id_encode (sum->jobid, "f58", buf, sizeof (buf));
        wattron (sum->win, A_BOLD);
        mvwprintw (sum->win, jobid_dim.y_begin, jobid_dim.x_begin, "%s", buf);
        wattroff (sum->win, A_BOLD);
    }
}

static void draw_stats (struct summary_pane *sum)
{
    mvwprintw (sum->win,
               stats_dim.y_begin,
               stats_dim.x_begin,
               "%*d pending",
               stats_dim.x_length - 10,
               sum->stats.depend + sum->stats.priority + sum->stats.sched);
    mvwprintw (sum->win,
               stats_dim.y_begin + 1,
               stats_dim.x_begin,
               "%*d running",
               stats_dim.x_length - 10,
               sum->stats.run + sum->stats.cleanup);
    mvwprintw (sum->win,
               stats_dim.y_begin + 2,
               stats_dim.x_begin,
               "%*d inactive",
               stats_dim.x_length - 10,
               sum->stats.inactive);
}

/* Create a little graph like this that fits in bufsz:
 *     name [||||||||||        |||32/128]
 * "used" grows from the left in yellow; "down" grows from the right in red.
 * Fraction is used/total.
 */
static void draw_bargraph (WINDOW *win, int y, int x, int x_length,
                           const char *name, struct resource_count res)
{
    char prefix[16];
    char suffix[16];

    if (x_length > 80)
        x_length = 80;
    if (res.used > res.total)
        res.used = res.total;

    snprintf (prefix, sizeof (prefix), "%5s [", name);
    snprintf (suffix, sizeof (suffix), "%d/%d]", res.used, res.total);

    int slots = x_length - strlen (prefix) - strlen (suffix) - 1;
    mvwprintw (win,
               y,
               x,
               "%s%*s%s",
               prefix,
               slots, "",
               suffix);
    /* Graph used */
    wattron (win, COLOR_PAIR (TOP_COLOR_YELLOW));
    for (int i = 0; i < ((double)res.used / res.total) * slots; i++)
        mvwaddch (win, y, x + strlen (prefix) + i, '|');
    wattroff (win, COLOR_PAIR (TOP_COLOR_YELLOW));

    /* Graph down */
    wattron (win, COLOR_PAIR (TOP_COLOR_RED));
    for (int i = slots - 1;
         i >= slots - ((double)res.down / res.total) * slots; i--) {
        mvwaddch (win, y, x + strlen (prefix) + i, '|');
    }
    wattroff (win, COLOR_PAIR (TOP_COLOR_RED));
}

static void draw_resource (struct summary_pane *sum)
{
    draw_bargraph (sum->win,
                   resource_dim.y_begin,
                   resource_dim.x_begin,
                   resource_dim.x_length,
                   "nodes",
                   sum->node);
    draw_bargraph (sum->win,
                   resource_dim.y_begin + 1,
                   resource_dim.x_begin,
                   resource_dim.x_length,
                   "cores",
                   sum->core);
    draw_bargraph (sum->win,
                   resource_dim.y_begin + 2,
                   resource_dim.x_begin,
                   resource_dim.x_length,
                   "gpus",
                   sum->gpu);
}

static void draw_heartbeat (struct summary_pane *sum)
{
    mvwprintw (sum->win,
               heart_dim.y_begin,
               heart_dim.x_begin,
               "%s",
               sum->heart_visible ? "♡" : " ");
}

/* Fetch expiration time (abs time relative to UNIX epoch) from resource.R.
 * If unavailable (e.g. we are a guest in the system instance), return 0.
 */
static double get_expiration (flux_t *h)
{
    flux_future_t *f;
    double val = 0;

    if (!(f = flux_kvs_lookup (h, NULL, 0, "resource.R"))
        || flux_kvs_lookup_get_unpack (f,
                                       "{s:{s:f}}",
                                       "execution",
                                       "expiration", &val) < 0) {
        if (errno == EPERM)
            goto done;
        fatal (errno, "error fetching or decoding resource.R");
    }
done:
    flux_future_destroy (f);
    return val;
}

static int get_instance_level (flux_t *h)
{
    const char *s;
    unsigned long level;

    if (!(s = flux_attr_get (h, "instance-level")))
        fatal (errno, "error fetching instance-level broker attribute");
    errno = 0;
    level = strtoul (s, NULL, 10);
    if (errno != 0)
        fatal (errno, "error parsing instance level");
    return level;
}

static flux_jobid_t get_jobid (flux_t *h)
{
    const char *s;
    flux_jobid_t jobid;

    if (!(s = flux_attr_get (h, "jobid")))
        return FLUX_JOBID_ANY;
    if (flux_job_id_parse (s, &jobid) < 0)
        fatal (errno, "error parsing value of jobid attribute: %s", s);
    return jobid;
}

static int resource_count (json_t *o,
                           const char *name,
                           int *nnodes,
                           int *ncores,
                           int *ngpus)
{
    json_t *R;
    struct rlist *rl;

    if (!(R = json_object_get (o, name)))
        return -1;
    if (json_is_null (R)) { // N.B. fluxion sets objects to json null if empty
        *nnodes = *ncores = *ngpus = 0;
        return 0;
    }
    if (!(rl = rlist_from_json (R, NULL)))
        return -1;
    *nnodes = rlist_nnodes (rl);
    *ncores = rlist_count (rl, "core");
    *ngpus = rlist_count (rl, "gpu");
    rlist_destroy (rl);
    return 0;
}

static void resource_continuation (flux_future_t *f, void *arg)
{
    struct summary_pane *sum = arg;
    json_t *o;

    if (flux_rpc_get_unpack (f, "o", &o) < 0)
        fatal (errno, "sched.resource-status RPC failed");
    if (resource_count (o,
                        "all",
                        &sum->node.total,
                        &sum->core.total,
                        &sum->gpu.total) < 0
        || resource_count (o,
                           "allocated",
                           &sum->node.used,
                           &sum->core.used,
                           &sum->gpu.used) < 0
        || resource_count (o,
                           "down",
                           &sum->node.down,
                           &sum->core.down,
                           &sum->gpu.down) < 0)
        fatal (0, "error decoding sched.resource-status RPC response");
    flux_future_destroy (f);
    draw_resource (sum);
}

static void stats_continuation (flux_future_t *f, void *arg)
{
    struct summary_pane *sum = arg;

    if (flux_rpc_get_unpack (f,
                             "{s:{s:i s:i s:i s:i s:i s:i s:i}}",
                             "job_states",
                               "depend", &sum->stats.depend,
                               "priority", &sum->stats.priority,
                               "sched", &sum->stats.sched,
                               "run", &sum->stats.run,
                               "cleanup", &sum->stats.cleanup,
                               "inactive", &sum->stats.inactive,
                               "total", &sum->stats.total))
        fatal (errno, "error decoding job-list.job-stats RPC response");
    flux_future_destroy (f);
    draw_stats (sum);
}

static void heartblink_cb (flux_reactor_t *r,
                           flux_watcher_t *w,
                           int revents,
                           void *arg)
{
    struct summary_pane *sum = arg;

    sum->heart_visible = false;
    draw_heartbeat (sum);
}

void summary_pane_heartbeat (struct summary_pane *sum)
{
    sum->heart_visible = true;
    flux_timer_watcher_reset (sum->heartblink, heartblink_duration, 0.);
    flux_watcher_start (sum->heartblink);
}

void summary_pane_query (struct summary_pane *sum)
{
    flux_future_t *f;
    flux_future_t *fstats;

    if (!(f = flux_rpc (sum->top->h, "sched.resource-status", NULL, 0, 0))
        || flux_future_then (f, -1, resource_continuation, sum) < 0) {
        flux_future_destroy (f);
    }
    if (!(fstats = flux_rpc (sum->top->h, "job-list.job-stats", "{}", 0, 0))
        || flux_future_then (fstats, -1, stats_continuation, sum) < 0) {
        flux_future_destroy (fstats);
    }
}

void summary_pane_draw (struct summary_pane *sum)
{
    werase (sum->win);
    draw_level (sum);
    draw_jobid (sum);
    draw_timeleft (sum);
    draw_resource (sum);
    draw_stats (sum);
    draw_heartbeat (sum);
}

void summary_pane_refresh (struct summary_pane *sum)
{
    wnoutrefresh (sum->win);
}

struct summary_pane *summary_pane_create (struct top *top)
{
    struct summary_pane *sum;
    flux_reactor_t *r = flux_get_reactor (top->h);

    if (!(sum = calloc (1, sizeof (*sum))))
        fatal (errno, "error creating context for summary pane");
    if (!(sum->heartblink = flux_timer_watcher_create (r,
                                                       heartblink_duration,
                                                       0.,
                                                       heartblink_cb,
                                                       sum)))
        fatal (errno, "error creating timer for heartbeat blink");
    if (!(sum->win = newwin (win_dim.y_length,
                             win_dim.x_length,
                             win_dim.y_begin,
                             win_dim.x_begin)))
        fatal (0, "error creating curses window for summary pane");
    sum->top = top;

    sum->expiration = get_expiration (top->h);
    sum->instance_level = get_instance_level (top->h);
    sum->jobid = get_jobid (top->h);

    summary_pane_query (sum);
    summary_pane_draw (sum);
    summary_pane_refresh (sum);
    return sum;
}

void summary_pane_destroy (struct summary_pane *sum)
{
    if (sum) {
        int saved_errno = errno;
        flux_watcher_destroy (sum->heartblink);
        delwin (sum->win);
        free (sum);
        errno = saved_errno;
    }
}

// vi:ts=4 sw=4 expandtab
