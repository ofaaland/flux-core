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
# include <config.h>
#endif
#include <jansson.h>
#include <flux/core.h>

#include "src/common/libccan/ccan/str/str.h"
#include "src/common/libutil/monotime.h"
#include "src/common/libutil/fsd.h"
#include "src/common/libhostlist/hostlist.h"
#include "src/common/librlist/rlist.h"

#include "builtin.h"

static const char *ansi_default = "\033[39m";
static const char *ansi_red = "\033[31m";
static const char *ansi_yellow = "\033[33m";
//static const char *ansi_green = "\033[32m";
static const char *ansi_dark_gray = "\033[90m";

static const char *vt100_mode_line = "\033(0";
static const char *vt100_mode_normal = "\033(B";

static struct optparse_option status_opts[] = {
    { .name = "rank", .key = 'r', .has_arg = 1, .arginfo = "NODEID",
      .usage = "Check health of subtree rooted at NODEID (default 0)",
    },
    { .name = "verbose", .key = 'v', .has_arg = 2, .arginfo = "[LEVEL]",
      .usage = "Increase reporting detail:"
               " 1=show time since current state was entered,"
               " 2=show round-trip RPC times."
    },
    { .name = "timeout", .key = 't', .has_arg = 1, .arginfo = "FSD",
      .usage = "Set RPC timeout (default none)",
    },
    { .name = "summary", .has_arg = 0,
      .usage = "Show only the root subtree status."
    },
    { .name = "down", .has_arg = 0,
      .usage = "Show only the partial/degraded subtrees."
    },
    { .name = "no-pretty", .has_arg = 0,
      .usage = "Do not indent entries and use line drawing characters"
               " to show overlay tree structure",
    },
    { .name = "no-ghost", .has_arg = 0,
      .usage = "Do not fill in presumed state of nodes that are"
               " inaccessible behind offline/lost overlay parents",
    },
    { .name = "no-color", .has_arg = 0,
      .usage = "Do not use color to highlight offline/lost nodes",
    },
    { .name = "wait", .key = 'w', .has_arg = 1, .arginfo = "STATE",
      .usage = "Wait until subtree enters STATE before reporting"
               " (full, partial, offline, degraded, lost)",
    },
    OPTPARSE_TABLE_END
};

static struct optparse_option disconnect_opts[] = {
    { .name = "parent", .key = 'r', .has_arg = 1, .arginfo = "NODEID",
      .usage = "Set parent rank to NODEID (default: determine from topology)",
    },
    OPTPARSE_TABLE_END
};

struct status {
    flux_t *h;
    int verbose;
    double timeout;
    optparse_t *opt;
    struct timespec start;
    const char *wait;
};

struct status_node {
    int rank;
    const char *status;
    double duration;
    bool ghost;
};

static json_t *overlay_topology;
static struct hostlist *overlay_hostmap;

typedef bool (*map_f)(struct status *ctx,
                      struct status_node *node,
                      bool parent,
                      int level);

static json_t *get_topology (flux_t *h)
{
    if (!overlay_topology) {
        flux_future_t *f;

        if (!(f = flux_rpc_pack (h,
                                 "overlay.topology",
                                 0,
                                 0,
                                 "{s:i}",
                                 "rank", 0))
            || flux_rpc_get_unpack (f, "O", &overlay_topology) < 0)
            log_err_exit ("error fetching overlay topology");

        flux_future_destroy (f);
    }
    return overlay_topology;
}

static struct hostlist *get_hostmap (flux_t *h)
{
    if (!overlay_hostmap) {
        const char *s;
        struct hostlist *hl;

        if (!(s = flux_attr_get (h, "hostlist"))
            || !(hl = hostlist_decode (s)))
            log_err_exit ("could not fetch/decode hostlist");
        overlay_hostmap = hl;
    }
    return overlay_hostmap;
}

static const char *status_duration (struct status *ctx, double since)
{
    char dbuf[128];
    static char buf[256];

    if (ctx->verbose < 1
        || since <= 0.
        || fsd_format_duration (dbuf, sizeof (dbuf), since) < 0)
        return "";
    snprintf (buf, sizeof (buf), " for %s", dbuf);
    return buf;
}

static const char *status_colorize (struct status *ctx,
                                    const char *status,
                                    bool ghost)
{
    static char buf[128];

    if (!optparse_hasopt (ctx->opt, "no-color")) {
        if (streq (status, "lost") && !ghost) {
            snprintf (buf, sizeof (buf), "%s%s%s",
                      ansi_red, status, ansi_default);
            status = buf;
        }
        else if (streq (status, "offline") && !ghost) {
            snprintf (buf, sizeof (buf), "%s%s%s",
                      ansi_yellow, status, ansi_default);
            status = buf;
        }
        else if (ghost) {
            snprintf (buf, sizeof (buf), "%s%s%s",
                      ansi_dark_gray, status, ansi_default);
            status = buf;
        }
    }
    return status;
}

static const char *status_indent (struct status *ctx, int n)
{
    static char buf[1024];
    if (optparse_hasopt (ctx->opt, "no-pretty") || n == 0)
        return "";
    snprintf (buf, sizeof (buf), "%*s%s%s%s", n - 1, "",
              vt100_mode_line,
              "m", // '|_'
              vt100_mode_normal);
    return buf;
}

/* Return string containing hostname and rank.
 */
static const char *status_getname (struct status *ctx, int rank)
{
    static char buf[128];

    snprintf (buf,
              sizeof (buf),
              "%d %s",
              rank,
              flux_get_hostbyrank (ctx->h, rank));
    return buf;
}

/* If --times, return string containing parenthesised elapsed
 * time since last RPC was started, with leading space.
 * Otherwise, return the empty string
 */
static const char *status_rpctime (struct status *ctx)
{
    static char buf[64];
    if (ctx->verbose < 2)
        return "";
    snprintf (buf, sizeof (buf), " (%.3f ms)", monotime_since (ctx->start));
    return buf;
}

static void status_print (struct status *ctx,
                          struct status_node *node,
                          bool parent,
                          int level)
{
    printf ("%s%s: %s%s%s\n",
            status_indent (ctx, level),
            status_getname (ctx, node->rank),
            status_colorize (ctx, node->status, node->ghost),
            status_duration (ctx, node->duration),
            parent ? status_rpctime (ctx) : "");
}

static void status_print_noname (struct status *ctx,
                                 struct status_node *node,
                                 bool parent,
                                 int level)
{
    printf ("%s%s%s%s\n",
            status_indent (ctx, level),
            status_colorize (ctx, node->status, node->ghost),
            status_duration (ctx, node->duration),
            parent ? status_rpctime (ctx) : "");
}

/* Look up topology of 'child_rank' within the subtree topology rooted
 * at 'parent_rank'. Caller must json_decref() the result.
 * Returns NULL if --ghost option was not provided, or the lookup fails.
 */
static json_t *topo_lookup (struct status *ctx,
                            int parent_rank,
                            int child_rank)
{
    flux_future_t *f;
    json_t *topo;

    if (optparse_hasopt (ctx->opt, "no-ghost"))
        return NULL;
    if (!(f = flux_rpc_pack (ctx->h,
                             "overlay.topology",
                             parent_rank,
                             0,
                             "{s:i}",
                             "rank", child_rank))
        || flux_future_wait_for (f, ctx->timeout) < 0
        || flux_rpc_get_unpack (f, "o", &topo) < 0) {
        flux_future_destroy (f);
        return NULL;
    }
    json_incref (topo);
    flux_future_destroy (f);
    return topo;
}

/* Walk a "ghost" subtree from the fixed topology.  Each node is assumed to
 * have the same 'status' as the offline/lost parent at the subtree root.
 * This augments healthwalk() to fill in nodes that would otherwise be missing
 * because they don't have a direct parent that is online for probing.
 * N.B. the starting point, the rank at the root of topo, is assumed to
 * have already been mapped/iterated over.
 */
static void status_ghostwalk (struct status *ctx,
                              json_t *topo,
                              int level,
                              const char *status,
                              map_f fun)
{
    json_t *children;
    size_t index;
    json_t *entry;
    struct status_node node = {
        .status = status,
        .duration = -1., // invalid - don't print
        .ghost = true,
    };

    if (json_unpack (topo, "{s:o}", "children", &children) < 0)
        return;
    json_array_foreach (children, index, entry) {
        if (json_unpack (entry, "{s:i}", "rank", &node.rank) < 0)
            return;
        if (fun (ctx, &node, false, level + 1))
            status_ghostwalk (ctx, entry, level + 1, status, fun);
    }
}

static double time_now (struct status *ctx)
{
    return flux_reactor_now (flux_get_reactor (ctx->h));
}

static flux_future_t *health_rpc (struct status *ctx,
                                  int rank,
                                  const char *wait,
                                  double timeout)
{
    flux_future_t *f;
    double start = time_now (ctx);
    const char *status;
    int rpc_flags = 0;

    if (wait)
        rpc_flags |= FLUX_RPC_STREAMING;

    if (!(f = flux_rpc (ctx->h,
                        "overlay.health",
                        NULL,
                        rank,
                        rpc_flags)))
        return NULL;

    do {
        if (flux_future_wait_for (f, timeout - (time_now (ctx) - start)) < 0
            || flux_rpc_get_unpack (f, "{s:s}", "status", &status) < 0) {
            flux_future_destroy (f);
            return NULL;
        }
        if (!wait || strcmp (wait, status) == 0)
            break;
        flux_future_reset (f);
    } while (1);

    return f;
}

/* Execute fun() for each online broker in subtree rooted at 'rank'.
 * If fun() returns true, follow tree to the broker's children.
 * If false, don't go down that path.
 */
static int status_healthwalk (struct status *ctx,
                              int rank,
                              int level,
                              map_f fun)
{
    struct status_node node = { .ghost = false };
    flux_future_t *f;
    json_t *children;
    int rc = 0;

    monotime (&ctx->start);

    if (!(f = health_rpc (ctx, rank, ctx->wait, ctx->timeout))
        || flux_rpc_get_unpack (f,
                                "{s:i s:s s:f s:o}",
                                "rank", &node.rank,
                                "status", &node.status,
                                "duration", &node.duration,
                                "children", &children) < 0) {
        /* RPC failed.
         * An error at level 0 should be fatal, e.g. unknown wait argument,
         * bad rank, timeout.  An error at level > 0 should return -1 so
         * ghostwalk() can be tried (parent hasn't noticed child crash?)
         * and sibling subtrees can be probed.
         */
        if (level == 0)
            log_msg_exit ("%s", future_strerror (f, errno));
        printf ("%s%s: %s%s\n",
                status_indent (ctx, level),
                status_getname (ctx, rank),
                future_strerror (f, errno),
                status_rpctime (ctx));
        rc = -1;
        goto done;
    }
    if (fun (ctx, &node, true, level)) {
        if (children) {
            size_t index;
            json_t *entry;
            struct status_node child = { .ghost = false };
            json_t *topo;

            json_array_foreach (children, index, entry) {
                if (json_unpack (entry,
                                 "{s:i s:s s:f}",
                                 "rank", &child.rank,
                                 "status", &child.status,
                                 "duration", &child.duration) < 0)
                    log_msg_exit ("error parsing child array entry");
                if (fun (ctx, &child, false, level + 1)) {
                    if (streq (child.status, "offline")
                        || streq (child.status, "lost")
                        || status_healthwalk (ctx, child.rank,
                                              level + 1, fun) < 0) {
                        topo = topo_lookup (ctx, node.rank, child.rank);
                        status_ghostwalk (ctx, topo, level + 1, child.status, fun);
                    }
                }
            }
        }
    }
done:
    flux_future_destroy (f);
    return rc;
}

/* map fun - print the first entry without adornment and stop the walk.
 */
static bool show_top (struct status *ctx,
                      struct status_node *node,
                      bool parent,
                      int level)
{
    status_print_noname (ctx, node, parent, level);
    return false;
}

/* map fun - only follow degraded/partial, but print all non-full nodes.
 */
static bool show_badtrees (struct status *ctx,
                           struct status_node *node,
                           bool parent,
                           int level)
{
    if (streq (node->status, "full"))
        return false;
    if (parent
        || streq (node->status, "lost")
        || streq (node->status, "offline"))
        status_print (ctx, node, parent, level);
    return true;
}

/* map fun - follow all live brokers and print everything
 */
static bool show_all (struct status *ctx,
                      struct status_node *node,
                      bool parent,
                      int level)
{
    if (parent
        || streq (node->status, "lost")
        || streq (node->status, "offline"))
        status_print (ctx, node, parent, level);
    return true;
}

static bool validate_wait (const char *wait)
{
    if (wait
        && strcmp (wait, "full") != 0
        && strcmp (wait, "partial") != 0
        && strcmp (wait, "degraded") != 0
        && strcmp (wait, "lost") != 0
        && strcmp (wait, "offline") != 0)
        return false;
    return true;
}

static int subcmd_status (optparse_t *p, int ac, char *av[])
{
    int rank = optparse_get_int (p, "rank", 0);
    struct status ctx;
    map_f fun;

    ctx.h = builtin_get_flux_handle (p);
    ctx.verbose = optparse_get_int (p, "verbose", 0);
    ctx.timeout = optparse_get_duration (p, "timeout", -1.0);
    ctx.opt = p;
    ctx.wait = optparse_get_str (p, "wait", NULL);
    if (!validate_wait (ctx.wait))
        log_msg_exit ("invalid --wait state");

    if (optparse_hasopt (p, "summary"))
        fun = show_top;
    else if (optparse_hasopt (p, "down"))
        fun = show_badtrees;
    else
        fun = show_all;

    status_healthwalk (&ctx, rank, 0, fun);

    return 0;
}

static int subcmd_gethostbyrank (optparse_t *p, int ac, char *av[])
{
    int optindex = optparse_option_index (p);
    flux_t *h = builtin_get_flux_handle (p);
    struct hostlist *hostmap = get_hostmap (h);
    struct hostlist *hosts;
    struct idset *ranks;
    unsigned int rank;
    char *s;

    if (optindex != ac - 1)
        log_msg_exit ("IDSET is required");
    if (!(ranks = idset_decode (av[optindex++])))
        log_err_exit ("IDSET could not be decoded");

    if (!(hosts = hostlist_create ()))
        log_err_exit ("failed to create hostlist");

    rank = idset_first (ranks);
    while (rank != IDSET_INVALID_ID) {
        const char *host;
        if (!(host = hostlist_nth (hostmap, rank)))
            log_msg_exit ("rank %u is not found in host map", rank);
        if (hostlist_append (hosts, host) < 0)
            log_err_exit ("error appending to hostlist");
        rank = idset_next (ranks, rank);
    }
    if (!(s = hostlist_encode (hosts)))
        log_err_exit ("error encoding hostlist");

    printf ("%s\n", s);

    free (s);
    hostlist_destroy (hosts);
    idset_destroy (ranks);

    return 0;
}

/* Recursively search 'topo' for the parent of 'rank'.
 * Return the parent of rank, or -1 on error.
 */
static int parentof (json_t *topo, int rank)
{
    int parent, child;
    json_t *children;
    size_t index;
    json_t *value;

    if (json_unpack (topo,
                     "{s:i s:o}",
                     "rank", &parent,
                     "children", &children) < 0)
        log_msg_exit ("error parsing topology");

    json_array_foreach (children, index, value) {
        if (json_unpack (value, "{s:i}", "rank", &child) < 0)
            log_msg_exit ("error parsing topology");
        if (child == rank)
            return parent;
    }
    json_array_foreach (children, index, value) {
        if ((parent = parentof (value, rank)) >= 0)
            return parent;
    }
    return -1;
}

/* Lookup instance topology from rank 0, then search for the parent of 'rank'.
 * Return parent or -1 on error.
 */
static int lookup_parentof (flux_t *h, int rank)
{
    json_t *topo = get_topology (h);
    int parent, size;

    /* Validate 'rank'.
     */
    if (json_unpack (topo,
                     "{s:i s:i}",
                     "rank", &parent,
                     "size", &size) < 0)
        log_msg_exit ("error parsing topology");
    if (rank < 0 || rank >= size)
        log_msg_exit ("%d is not a valid rank in this instance", rank);
    if (rank == 0)
        log_msg_exit ("%d has no parent", rank);

    return parentof (topo, rank);
}

static int subcmd_parentof (optparse_t *p, int ac, char *av[])
{
    int optindex = optparse_option_index (p);
    flux_t *h = builtin_get_flux_handle (p);
    int rank;

    if (optindex != ac - 1)
        log_msg_exit ("RANK is required");
    rank = strtoul (av[optindex++], NULL, 10);

    printf ("%d\n", lookup_parentof (h, rank));

    return 0;
}

static int subcmd_disconnect (optparse_t *p, int ac, char *av[])
{
    int optindex = optparse_option_index (p);
    flux_t *h = builtin_get_flux_handle (p);
    int parent = optparse_get_int (p, "parent", -1);
    int rank;
    flux_future_t *f;

    if (optindex != ac - 1)
        log_msg_exit ("RANK is required");
    rank = strtoul (av[optindex++], NULL, 10);
    if (parent == -1)
        parent = lookup_parentof (h, rank); // might return -1 (unlikely)

    log_msg ("asking rank %d to disconnect child rank %d", parent, rank);

    if (!(f = flux_rpc_pack (h,
                             "overlay.disconnect-subtree",
                             parent,
                             0,
                             "{s:i}",
                             "rank", rank)))
        log_err_exit ("overlay.disconnect-subtree");
    if (flux_rpc_get (f, NULL) < 0) {
        log_msg_exit ("overlay.disconnect-subtree: %s",
                      future_strerror (f, errno));
    }
    flux_future_destroy (f);

    return 0;
}
int cmd_overlay (optparse_t *p, int argc, char *argv[])
{
    log_init ("flux-overlay");

    if (optparse_run_subcommand (p, argc, argv) != OPTPARSE_SUCCESS)
        exit (1);

    hostlist_destroy (overlay_hostmap);

    return (0);
}

static struct optparse_subcommand overlay_subcmds[] = {
    { "status",
      "[OPTIONS]",
      "Display overlay subtree health status",
      subcmd_status,
      0,
      status_opts,
    },
    { "gethostbyrank",
      "[OPTIONS] IDSET",
      "lookup hostname(s) for rank(s), if available",
      subcmd_gethostbyrank,
      0,
      NULL,
    },
    { "parentof",
      "[OPTIONS] RANK",
      "show the parent of RANK",
      subcmd_parentof,
      0,
      NULL,
    },
    { "disconnect",
      "[OPTIONS] RANK",
      "disconnect a subtree rooted at RANK",
      subcmd_disconnect,
      0,
      disconnect_opts,
    },
    OPTPARSE_SUBCMD_END
};


int subcommand_overlay_register (optparse_t *p)
{
    optparse_err_t e;

    e = optparse_reg_subcommand (p,
        "overlay",
        cmd_overlay,
        NULL,
        "Manage overlay network",
        0,
        NULL);
    if (e != OPTPARSE_SUCCESS)
        return -1;

    e = optparse_reg_subcommands (optparse_get_subcommand (p, "overlay"),
                                  overlay_subcmds);
    return (e == OPTPARSE_SUCCESS ? 0 : -1);
}

/*
 * vi: ts=4 sw=4 expandtab
 */
