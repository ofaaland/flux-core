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
#include <assert.h>
#include <libgen.h>
#include <inttypes.h>
#include <sys/prctl.h>
#include <sys/resource.h>
#include <argz.h>
#include <flux/core.h>
#include <czmq.h>
#include <jansson.h>
#if HAVE_CALIPER
#include <caliper/cali.h>
#include <sys/syscall.h>
#endif
#if HAVE_VALGRIND
# if HAVE_VALGRIND_H
#  include <valgrind.h>
# elif HAVE_VALGRIND_VALGRIND_H
#  include <valgrind/valgrind.h>
# endif
#endif

#include "src/common/libczmqcontainers/czmq_containers.h"
#include "src/common/libutil/log.h"
#include "src/common/libutil/cleanup.h"
#include "src/common/libidset/idset.h"
#include "src/common/libutil/ipaddr.h"
#include "src/common/libutil/kary.h"
#include "src/common/libutil/monotime.h"
#include "src/common/libpmi/pmi.h"
#include "src/common/libpmi/pmi_strerror.h"
#include "src/common/libutil/fsd.h"
#include "src/common/libutil/errno_safe.h"

#include "module.h"
#include "brokercfg.h"
#include "groups.h"
#include "overlay.h"
#include "service.h"
#include "attr.h"
#include "log.h"
#include "content-cache.h"
#include "runat.h"
#include "heaptrace.h"
#include "exec.h"
#include "ping.h"
#include "rusage.h"
#include "boot_config.h"
#include "boot_pmi.h"
#include "publisher.h"
#include "state_machine.h"

#include "broker.h"


static int broker_event_sendmsg (broker_ctx_t *ctx, const flux_msg_t *msg);
static int broker_response_sendmsg (broker_ctx_t *ctx, const flux_msg_t *msg);
static void broker_request_sendmsg (broker_ctx_t *ctx, const flux_msg_t *msg);
static int broker_request_sendmsg_internal (broker_ctx_t *ctx,
                                            const flux_msg_t *msg);

static void overlay_recv_cb (const flux_msg_t *msg,
                             overlay_where_t where,
                             void *arg);
static void module_cb (module_t *p, void *arg);
static void module_status_cb (module_t *p, int prev_state, void *arg);
static void signal_cb (flux_reactor_t *r, flux_watcher_t *w,
                       int revents, void *arg);
static int broker_handle_signals (broker_ctx_t *ctx);

static flux_msg_handler_t **broker_add_services (broker_ctx_t *ctx);
static void broker_remove_services (flux_msg_handler_t *handlers[]);

static int load_module_byname (broker_ctx_t *ctx, const char *name,
                               const char *argz, size_t argz_len,
                               const flux_msg_t *request);
static int unload_module_byname (broker_ctx_t *ctx, const char *name,
                                 const flux_msg_t *request);

static void set_proctitle (uint32_t rank);

static int create_rundir (attr_t *attrs);

static int create_runat_phases (broker_ctx_t *ctx);

static int handle_event (broker_ctx_t *ctx, const flux_msg_t *msg);

static void init_attrs (attr_t *attrs, pid_t pid, struct flux_msg_cred *cred);

static int init_local_uri_attr (struct overlay *ov, attr_t *attrs);

static int set_uri_job_memo (attr_t *attrs);

static const struct flux_handle_ops broker_handle_ops;

static struct optparse_option opts[] = {
    { .name = "verbose",    .key = 'v', .has_arg = 2, .arginfo = "[LEVEL]",
      .usage = "Be annoyingly informative by degrees", },
    { .name = "setattr",    .key = 'S', .has_arg = 1, .arginfo = "ATTR=VAL",
      .usage = "Set broker attribute", },
    { .name = "config-path",.key = 'c', .has_arg = 1, .arginfo = "PATH",
      .usage = "Set broker config directory (default: none)", },
    OPTPARSE_TABLE_END,
};

void parse_command_line_arguments (int argc, char *argv[], broker_ctx_t *ctx)
{
    int optindex;
    const char *arg;

    if (!(ctx->opts = optparse_create ("flux-broker"))
        || optparse_add_option_table (ctx->opts, opts) != OPTPARSE_SUCCESS)
        log_msg_exit ("error setting up option parsing");
    if ((optindex = optparse_parse_args (ctx->opts, argc, argv)) < 0)
        exit (1);

    ctx->verbose = optparse_get_int (ctx->opts, "verbose", 0);

    optparse_get_str (ctx->opts, "config-path", NULL);

    while ((arg = optparse_getopt_next (ctx->opts, "setattr"))) {
        char *val, *attr;
        if (!(attr = strdup (arg)))
            log_err_exit ("out of memory duplicating optarg");
        if ((val = strchr (attr, '=')))
            *val++ = '\0';
        if (attr_add (ctx->attrs, attr, val, 0) < 0)
            if (attr_set (ctx->attrs, attr, val, true) < 0)
                log_err_exit ("setattr %s=%s", attr, val);
        free (attr);
    }

    if (optindex < argc) {
        int e;
        if ((e = argz_create (argv + optindex, &ctx->init_shell_cmd,
                              &ctx->init_shell_cmd_len)) != 0)
            log_errn_exit (e, "argz_create");
    }
}

static int setup_profiling (const char *program, int rank)
{
#if HAVE_CALIPER
    cali_begin_string_byname ("flux.type", "main");
    cali_begin_int_byname ("flux.tid", syscall (SYS_gettid));
    cali_begin_string_byname ("binary", program);
    cali_begin_int_byname ("flux.rank", rank);
    // TODO: this is a stopgap until we have better control over
    // instrumemtation in child processes. If we want to see what children
    // that load libflux are up to, this should be disabled
    unsetenv ("CALI_SERVICES_ENABLE");
    unsetenv ("CALI_CONFIG_PROFILE");
#endif
    return (0);
}

static int increase_rlimits (void)
{
    struct rlimit rlim;

    /*  Increase number of open files to max to prevent potential failures
     *   due to file descriptor exhaustion (e.g. failure to open /dev/urandom)
     */
    if (getrlimit (RLIMIT_NOFILE, &rlim) < 0) {
        log_err ("getrlimit");
        return -1;
    }
    rlim.rlim_cur = rlim.rlim_max;
    if (setrlimit (RLIMIT_NOFILE, &rlim) < 0) {
        log_err ("Failed to increase nofile limit");
        return -1;
    }
    return 0;
}

int main (int argc, char *argv[])
{
    broker_ctx_t ctx;
    sigset_t old_sigmask;
    struct sigaction old_sigact_int;
    struct sigaction old_sigact_term;
    flux_msg_handler_t **handlers = NULL;
    const flux_conf_t *conf;
    double boot_elapsed_sec;
    struct timespec boot_start_time;

    memset (&ctx, 0, sizeof (ctx));
    log_init (argv[0]);

    ctx.exit_rc = 1;

    if (!(ctx.sigwatchers = zlist_new ())
        || !(ctx.modhash = modhash_create ())
        || !(ctx.services = service_switch_create ())
        || !(ctx.attrs = attr_create ())
        || !(ctx.subscriptions = zlist_new ()))
        log_msg_exit ("Out of memory in early initialization");

    /* Record the instance owner: the effective uid of the broker. */
    ctx.cred.userid = getuid ();
    /* Set default rolemask for messages sent with flux_send()
     * on the broker's internal handle. */
    ctx.cred.rolemask = FLUX_ROLE_OWNER;

    init_attrs (ctx.attrs, getpid (), &ctx.cred);

    parse_command_line_arguments (argc, argv, &ctx);

    /* Block all signals, saving old mask and actions for SIGINT, SIGTERM.
     */
    sigset_t sigmask;
    sigfillset (&sigmask);
    if (sigprocmask (SIG_SETMASK, &sigmask, &old_sigmask) < 0
        || sigaction (SIGINT, NULL, &old_sigact_int) < 0
        || sigaction (SIGTERM, NULL, &old_sigact_term) < 0)
        log_err_exit ("error setting signal mask");

    /* Initialize libczmq zsys class.
     *
     * zsys_init() creates a global 0MQ context and starts the 0MQ I/O thread.
     * The context is implicitly shared by users of the zsock class within
     * the broker, including shmem connector, overlay.c, and module.c.
     * libczmq tracks 0MQ sockets created with zsock, and any left open are
     * closed by an atexit() handler to prevent zmq_ctx_term() from hanging.
     *
     * If something goes wrong, such as unclosed sockets in the atexit handler,
     * czmq sends messages to its log class, which we redirect to stderr here.
     *
     * Disable czmq's internal signal handlers for SIGINT and SIGTERM, since
     * the broker will install its own.
     */
    if (!zsys_init ()) {
        log_err ("zsys_init");
        goto cleanup;
    }
    zsys_set_logstream (stderr);
    zsys_set_logident ("flux-broker");
    zsys_handler_set (NULL);

    /* Set up the flux reactor with support for child watchers.
     * Associate an internal flux_t handle with the reactor.
     */
    if (!(ctx.reactor = flux_reactor_create (FLUX_REACTOR_SIGCHLD))
        || !(ctx.h = flux_handle_create (&ctx, &broker_handle_ops, 0))
        || flux_set_reactor (ctx.h, ctx.reactor) < 0) {
        log_err ("error setting up broker reactor/flux_t handle");
        goto cleanup;
    }

    /* Parse config.
     */
    if (!(ctx.config = brokercfg_create (ctx.h,
                                         optparse_get_str (ctx.opts,
                                                           "config-path",
                                                           NULL),
                                         ctx.attrs,
                                         ctx.modhash)))
        goto cleanup;
    conf = flux_get_conf (ctx.h);

    if (increase_rlimits () < 0)
        goto cleanup;

    /* Prepare signal handling
     */
    if (broker_handle_signals (&ctx) < 0) {
        log_err ("broker_handle_signals");
        goto cleanup;
    }

    if (!(ctx.overlay = overlay_create (ctx.h,
                                        ctx.attrs,
                                        overlay_recv_cb,
                                        &ctx))) {
        log_err ("overlay_create");
        goto cleanup;
    }

    /* Arrange for the publisher to route event messages.
     * handle_event - local subscribers (ctx.h)
     */
    if (!(ctx.publisher = publisher_create (ctx.h,
                                            (publisher_send_f)handle_event,
                                            &ctx))) {
        log_err ("error setting up event publishing service");
        goto cleanup;
    }

    if (create_rundir (ctx.attrs) < 0)
        goto cleanup;

    /* Execute broker network bootstrap.
     * Default method is pmi.
     * If [bootstrap] is defined in configuration, use static configuration.
     */
    monotime (&boot_start_time);
    if (flux_conf_unpack (conf, NULL, "{s:{}}", "bootstrap") == 0) {
        if (boot_config (ctx.h, ctx.overlay, ctx.attrs) < 0) {
            log_msg ("bootstrap failed");
            goto cleanup;
        }
    }
    else { // PMI
        if (boot_pmi (ctx.overlay, ctx.attrs) < 0) {
            log_msg ("bootstrap failed");
            goto cleanup;
        }
    }
    boot_elapsed_sec = monotime_since (boot_start_time) / 1000;

    ctx.rank = overlay_get_rank (ctx.overlay);
    ctx.size = overlay_get_size (ctx.overlay);

    assert (ctx.size > 0);

    /* Must be called after overlay setup */
    if (overlay_register_attrs (ctx.overlay) < 0) {
        log_err ("registering overlay attributes");
        goto cleanup;
    }

    if (ctx.verbose) {
        log_msg ("boot: rank=%d size=%d time %.3fs",
                  ctx.rank,
                  ctx.size,
                  boot_elapsed_sec);
    }

    // Setup profiling
    setup_profiling (argv[0], ctx.rank);

    /* Initialize logging.
     * OK to call flux_log*() after this.
     */
    logbuf_initialize (ctx.h, ctx.rank, ctx.attrs);

    /* Allow flux_get_rank() and flux_get_size() to work in the broker.
     */
    if (attr_cache_immutables (ctx.attrs, ctx.h) < 0) {
        log_err ("error priming broker attribute cache");
        goto cleanup;
    }

    if (!(ctx.groups = groups_create (&ctx))) {
        log_err ("groups_create");
        goto cleanup;
    }

    /* Create content cache.
     */
    if (!(ctx.cache = content_cache_create (ctx.h, ctx.attrs))) {
        log_err ("content_cache_create");
        goto cleanup;
    }

    if (ctx.verbose) {
        const char *parent = overlay_get_parent_uri (ctx.overlay);
        const char *child = overlay_get_bind_uri (ctx.overlay);
        log_msg ("parent: %s", parent ? parent : "none");
        log_msg ("child: %s", child ? child : "none");
    }

    set_proctitle (ctx.rank);

    if (init_local_uri_attr (ctx.overlay, ctx.attrs) < 0) // used by runat
        goto cleanup;

    if (ctx.rank == 0 && set_uri_job_memo (ctx.attrs) < 0)
        goto cleanup;

    if (create_runat_phases (&ctx) < 0)
        goto cleanup;

    /* If Flux was launched by Flux, now that PMI bootstrap and runat
     * initialization is complete, unset Flux job environment variables
     * so that they don't leak into the jobs other children of this instance.
     */
    unsetenv ("FLUX_JOB_ID");
    unsetenv ("FLUX_JOB_SIZE");
    unsetenv ("FLUX_JOB_NNODES");

    /* Wire up the overlay.
     */
    if (ctx.rank > 0) {
        if (ctx.verbose)
            log_msg ("initializing overlay connect");
        if (overlay_connect (ctx.overlay) < 0) {
            log_err ("overlay_connect");
            goto cleanup;
        }
    }

    /* Register internal services
     */
    if (attr_register_handlers (ctx.attrs, ctx.h) < 0) {
        log_err ("attr_register_handlers");
        goto cleanup;
    }
    if (heaptrace_initialize (ctx.h) < 0) {
        log_err ("heaptrace_initialize");
        goto cleanup;
    }
    if (exec_initialize (ctx.h, ctx.rank, ctx.attrs) < 0) {
        log_err ("exec_initialize");
        goto cleanup;
    }
    if (ping_initialize (ctx.h, "broker", overlay_get_uuid (ctx.overlay)) < 0) {
        log_err ("ping_initialize");
        goto cleanup;
    }
    if (rusage_initialize (ctx.h, "broker") < 0) {
        log_err ("rusage_initialize");
        goto cleanup;
    }

    if (!(handlers = broker_add_services (&ctx))) {
        log_err ("broker_add_services");
        goto cleanup;
    }

    /* Initialize module infrastructure.
     */
    if (ctx.verbose > 1)
        log_msg ("initializing modules");
    modhash_initialize (ctx.modhash,
                        ctx.h,
                        overlay_get_uuid (ctx.overlay),
                        ctx.attrs);

    /* Configure broker state machine
     */
    if (!(ctx.state_machine = state_machine_create (&ctx))) {
        log_err ("error creating broker state machine");
        goto cleanup;
    }
    state_machine_post (ctx.state_machine, "start");

    /* Load the local connector module.
     * Other modules will be loaded in rc1 using flux module,
     * which uses the local connector.
     * The shutdown protocol unloads it.
     */
    if (ctx.verbose > 1)
        log_msg ("loading connector-local");
    if (load_module_byname (&ctx, "connector-local", NULL, 0, NULL) < 0) {
        log_err ("load_module connector-local");
        goto cleanup;
    }

    /* Event loop
     */
    if (ctx.verbose > 1)
        log_msg ("entering event loop");
    /* Once we enter the reactor, default exit_rc is now 0 */
    ctx.exit_rc = 0;
    if (flux_reactor_run (ctx.reactor, 0) < 0)
        log_err ("flux_reactor_run");
    if (ctx.verbose > 1)
        log_msg ("exited event loop");

    /* inform all lingering subprocesses we are tearing down.  Do this
     * before any cleanup/teardown below, as this call will re-enter
     * the reactor.
     */
    exec_terminate_subprocesses (ctx.h);

cleanup:
    if (ctx.verbose > 1)
        log_msg ("cleaning up");

    /* Restore default sigmask and actions for SIGINT, SIGTERM
     */
    if (sigprocmask (SIG_SETMASK, &old_sigmask, NULL) < 0
        || sigaction (SIGINT, &old_sigact_int, NULL) < 0
        || sigaction (SIGTERM, &old_sigact_term, NULL) < 0)
        log_err ("error restoring signal mask");

    /* Unregister builtin services
     */
    attr_destroy (ctx.attrs);
    content_cache_destroy (ctx.cache);

    modhash_destroy (ctx.modhash);
    zlist_destroy (&ctx.sigwatchers);
    state_machine_destroy (ctx.state_machine);
    overlay_destroy (ctx.overlay);
    groups_destroy (ctx.groups);
    service_switch_destroy (ctx.services);
    broker_remove_services (handlers);
    publisher_destroy (ctx.publisher);
    brokercfg_destroy (ctx.config);
    runat_destroy (ctx.runat);
    flux_close (ctx.h);
    flux_reactor_destroy (ctx.reactor);
    zlist_destroy (&ctx.subscriptions);
    free (ctx.init_shell_cmd);
    optparse_destroy (ctx.opts);

    return ctx.exit_rc;
}

struct attrmap {
    const char *env;
    const char *attr;
    uint8_t required:1;
    uint8_t sanitize:1;
};

static struct attrmap attrmap[] = {
    { "FLUX_EXEC_PATH",         "conf.exec_path",           1, 0 },
    { "FLUX_CONNECTOR_PATH",    "conf.connector_path",      1, 0 },
    { "FLUX_MODULE_PATH",       "conf.module_path",         1, 0 },
    { "FLUX_PMI_LIBRARY_PATH",  "conf.pmi_library_path",    1, 0 },

    { "FLUX_URI",               "parent-uri",               0, 1 },
    { "FLUX_KVS_NAMESPACE",     "parent-kvs-namespace",     0, 1 },
    { NULL, NULL, 0, 0 },
};

static void init_attrs_from_environment (attr_t *attrs)
{
    struct attrmap *m;
    const char *val;
    int flags = 0;  // XXX possibly these should be immutable?

    for (m = &attrmap[0]; m->env != NULL; m++) {
        val = getenv (m->env);
        if (!val && m->required)
            log_msg_exit ("required environment variable %s is not set", m->env);
        if (attr_add (attrs, m->attr, val, flags) < 0)
            log_err_exit ("attr_add %s", m->attr);
        if (m->sanitize)
            unsetenv (m->env);
    }
}

static void init_attrs_broker_pid (attr_t *attrs, pid_t pid)
{
    char *attrname = "broker.pid";
    char pidval[32];

    snprintf (pidval, sizeof (pidval), "%u", pid);
    if (attr_add (attrs,
                  attrname,
                  pidval,
                  FLUX_ATTRFLAG_IMMUTABLE) < 0)
        log_err_exit ("attr_add %s", attrname);
}

static void init_attrs_rc_paths (attr_t *attrs)
{
    if (attr_add (attrs,
                  "broker.rc1_path",
                  flux_conf_builtin_get ("rc1_path", FLUX_CONF_AUTO),
                  0) < 0)
        log_err_exit ("attr_add rc1_path");

    if (attr_add (attrs,
                  "broker.rc3_path",
                  flux_conf_builtin_get ("rc3_path", FLUX_CONF_AUTO),
                  0) < 0)
        log_err_exit ("attr_add rc3_path");
}

static void init_attrs_shell_paths (attr_t *attrs)
{
    if (attr_add (attrs,
                  "conf.shell_pluginpath",
                  flux_conf_builtin_get ("shell_pluginpath", FLUX_CONF_AUTO),
                  0) < 0)
        log_err_exit ("attr_add conf.shell_pluginpath");
    if (attr_add (attrs,
                  "conf.shell_initrc",
                  flux_conf_builtin_get ("shell_initrc", FLUX_CONF_AUTO),
                  0) < 0)
        log_err_exit ("attr_add conf.shell_initrc");
}

static void init_attrs (attr_t *attrs, pid_t pid, struct flux_msg_cred *cred)
{
    /* Initialize config attrs from environment set up by flux(1)
     */
    init_attrs_from_environment (attrs);

    /* Initialize other miscellaneous attrs
     */
    init_attrs_broker_pid (attrs, pid);
    init_attrs_rc_paths (attrs);
    init_attrs_shell_paths (attrs);

    /* Allow version to be changed by instance owner for testing
     */
    if (attr_add (attrs, "version", FLUX_CORE_VERSION_STRING, 0) < 0)
        log_err_exit ("attr_add version");

    char tmp[32];
    snprintf (tmp, sizeof (tmp), "%ju", (uintmax_t)cred->userid);
    if (attr_add (attrs, "security.owner", tmp, FLUX_ATTRFLAG_IMMUTABLE) < 0)
        log_err_exit ("attr_add owner");
}

static void set_proctitle (uint32_t rank)
{
    static char proctitle[32];
    snprintf (proctitle, sizeof (proctitle), "flux-broker-%"PRIu32, rank);
    (void)prctl (PR_SET_NAME, proctitle, 0, 0, 0);
}

static int create_runat_rc2 (struct runat *r, const char *argz, size_t argz_len)
{
    if (argz == NULL) { // run interactive shell
        if (runat_push_shell (r, "rc2") < 0)
            return -1;
    }
    else if (argz_count (argz, argz_len) == 1) { // run shell -c "command"
        if (runat_push_shell_command (r, "rc2", argz, false) < 0)
            return -1;
    }
    else { // direct exec
        if (runat_push_command (r, "rc2", argz, argz_len, false) < 0)
            return -1;
    }
    return 0;
}

static int create_runat_phases (broker_ctx_t *ctx)
{
    const char *rc1, *rc3, *local_uri;
    bool rc2_none = false;

    if (attr_get (ctx->attrs, "local-uri", &local_uri, NULL) < 0) {
        log_err ("local-uri is not set");
        return -1;
    }
    if (attr_get (ctx->attrs, "broker.rc1_path", &rc1, NULL) < 0) {
        log_err ("broker.rc1_path is not set");
        return -1;
    }
    if (attr_get (ctx->attrs, "broker.rc3_path", &rc3, NULL) < 0) {
        log_err ("broker.rc3_path is not set");
        return -1;
    }
    if (attr_get (ctx->attrs, "broker.rc2_none", NULL, NULL) == 0)
        rc2_none = true;

    if (!(ctx->runat = runat_create (ctx->h, local_uri))) {
        log_err ("runat_create");
        return -1;
    }

    /* rc1 - initialization
     */
    if (rc1 && strlen (rc1) > 0) {
        if (runat_push_shell_command (ctx->runat, "rc1", rc1, true) < 0) {
            log_err ("runat_push_shell_command rc1");
            return -1;
        }
    }

    /* rc2 - initial program
     */
    if (ctx->rank == 0 && !rc2_none) {
        if (create_runat_rc2 (ctx->runat, ctx->init_shell_cmd,
                                          ctx->init_shell_cmd_len) < 0) {
            log_err ("create_runat_rc2");
            return -1;
        }
    }

    /* rc3 - finalization
     */
    if (rc3 && strlen (rc3) > 0) {
        if (runat_push_shell_command (ctx->runat, "rc3", rc3, true) < 0) {
            log_err ("runat_push_shell_command rc3");
            return -1;
        }
    }
    return 0;
}

static int checkdir (const char *name, const char *path)
{
    struct stat sb;

    if (stat (path, &sb) < 0) {
        log_err ("cannot stat %s %s", name, path);
        return -1;
    }
    if (!S_ISDIR (sb.st_mode)) {
        errno = ENOTDIR;
        log_err ("%s %s", name, path);
        return -1;
    }
    if ((sb.st_mode & S_IRWXU) != S_IRWXU) {
        log_msg ("%s %s does not have owner=rwx permissions", name, path);
        errno = EPERM;
        return -1;
    }
    return 0;
}


/*  Handle global rundir attribute.
 */
static int create_rundir (attr_t *attrs)
{
    const char *tmpdir;
    const char *run_dir = NULL;
    char path[1024];
    int len;
    bool do_cleanup = true;
    int rc = -1;

    /*  If rundir attribute isn't set, then create a temp directory
     *   and use that as rundir. If directory was set, try to create it if
     *   it doesn't exist. If directory was pre-existing, do not schedule
     *   the dir for auto-cleanup at broker exit.
     */
    if (attr_get (attrs, "rundir", &run_dir, NULL) < 0) {
        if (!(tmpdir = getenv ("TMPDIR")))
            tmpdir = "/tmp";
        len = snprintf (path, sizeof (path), "%s/flux-XXXXXX", tmpdir);
        if (len >= sizeof (path)) {
            log_msg ("rundir buffer overflow");
            goto done;
        }
        if (!(run_dir = mkdtemp (path))) {
            log_err ("cannot create directory in %s", tmpdir);
            goto done;
        }
        if (attr_add (attrs, "rundir", run_dir, 0) < 0) {
            log_err ("error setting rundir broker attribute");
            goto done;
        }
    }
    else if (mkdir (run_dir, 0700) < 0) {
        if (errno != EEXIST) {
            log_err ("error creating rundir %s ", run_dir);
            goto done;
        }
        /* Do not cleanup directory if we did not create it here
         */
        do_cleanup = false;
    }

    /*  Ensure created or existing directory is writeable:
     */
    if (checkdir ("rundir", run_dir) < 0)
        goto done;

    /*  Ensure that AF_UNIX sockets can be created in rundir - see #3925.
     */
    struct sockaddr_un sa;
    size_t path_limit = sizeof (sa.sun_path) - sizeof ("/local-9999");
    size_t path_length = strlen (run_dir);
    if (path_length > path_limit) {
        log_msg ("rundir length of %zu bytes exceeds max %zu"
                 " to allow for AF_UNIX socket creation.",
                 path_length,
                 path_limit);
        goto done;
    }

    /*  rundir is now fixed, so make the attribute immutable, and
     *   schedule the dir for cleanup at exit if we created it here.
     */
    if (attr_set_flags (attrs, "rundir", FLUX_ATTRFLAG_IMMUTABLE) < 0) {
        log_err ("error setting rundir broker attribute flags");
        goto done;
    }
    rc = 0;
done:
    if (do_cleanup && run_dir != NULL)
        cleanup_push_string (cleanup_directory_recursive, run_dir);
    return rc;
}

static int init_local_uri_attr (struct overlay *ov, attr_t *attrs)
{
    const char *uri;

    if (attr_get (attrs, "local-uri", &uri, NULL) < 0) {
        uint32_t rank = overlay_get_rank (ov);
        const char *rundir;
        char buf[1024];

        if (attr_get (attrs, "rundir", &rundir, NULL) < 0) {
            log_msg ("rundir attribute is not set");
            return -1;
        }
        if (snprintf (buf, sizeof (buf), "local://%s/local-%d",
                      rundir, rank) >= sizeof (buf)) {
            log_msg ("buffer overflow while building local-uri");
            return -1;
        }
        if (attr_add (attrs, "local-uri", buf, FLUX_ATTRFLAG_IMMUTABLE) < 0) {
            log_err ("setattr local-uri");
            return -1;
        }
    }
    else {
        char path[1024];

        if (strncmp (uri, "local://", 8) != 0) {
            log_msg ("local-uri is malformed");
            return -1;
        }
        if (snprintf (path, sizeof (path), "%s", uri + 8) >= sizeof (path)) {
            log_msg ("buffer overflow while checking local-uri");
            return -1;
        }
        if (checkdir ("local-uri directory", dirname (path)) < 0)
            return -1;

        /* see #3925 */
        struct sockaddr_un sa;
        size_t path_limit = sizeof (sa.sun_path) - 1;
        size_t path_length = strlen (uri + 8);
        if (path_length > path_limit) {
            log_msg ("local-uri length of %zu bytes exceeds max %zu"
                     " AF_UNIX socket path length",
                     path_length,
                     path_limit);
            return -1;
        }
    }
    return 0;
}

static int set_uri_job_memo (attr_t *attrs)
{
    const char *jobid = NULL;
    const char *parent_uri = NULL;
    const char *local_uri = NULL;
    const char *path;
    char uri [1024];
    char hostname [MAXHOSTNAMELEN + 1];
    flux_jobid_t id;
    flux_t *h;
    flux_future_t *f;
    int rc = -1;

    /* Skip if "jobid" or "parent-uri" not set, this is probably
     *  not a child of any Flux instance.
     */
    if (attr_get (attrs, "parent-uri", &parent_uri, NULL) < 0
        || parent_uri == NULL
        || attr_get (attrs, "jobid", &jobid, NULL) < 0
        || jobid == NULL)
        return 0;

    if (flux_job_id_parse (jobid, &id) < 0) {
        log_err ("Unable to parse jobid attribute '%s'", jobid);
        return -1;
    }
    if (attr_get (attrs, "local-uri", &local_uri, NULL) < 0) {
        log_err ("Unexpectedly unable to fetch local-uri attribute");
        return -1;
    }
    if (gethostname (hostname, sizeof (hostname)) < 0) {
        log_err ("gethostname failure");
        return -1;
    }
    path = local_uri + 8; /* forward past "local://" */
    if (snprintf (uri,
                 sizeof (uri),
                 "ssh://%s%s",
                 hostname, path) >= sizeof (uri)) {
        log_msg ("buffer overflow while checking local-uri");
        return -1;
    }

    /*  Open connection to parent instance and post "uri" user annotation
     */
    if (!(h = flux_open (parent_uri, 0))) {
        log_err ("flux_open to parent failed");
        return -1;
    }
    if (!(f = flux_rpc_pack (h,
                             "job-manager.memo",
                             FLUX_NODEID_ANY,
                             0,
                             "{s:I s:{s:s}}",
                             "id", id,
                             "memo",
                               "uri", uri))
        || flux_rpc_get (f, NULL) < 0) {
        log_err ("job-manager.memo uri");
        goto out;
    }
    rc = 0;
out:
    flux_future_destroy (f);
    flux_close (h);
    return rc;
}

static bool nodeset_member (const char *s, uint32_t rank)
{
    struct idset *ns = NULL;
    bool member = true;

    if (s) {
        if (!(ns = idset_decode (s)))
            log_msg_exit ("malformed nodeset: %s", s);
        member = idset_test (ns, rank);
        idset_destroy (ns);
    }
    return member;
}

static int mod_svc_cb (const flux_msg_t *msg, void *arg)
{
    module_t *p = arg;
    int rc = module_sendmsg (p, msg);
    return rc;
}

/* If a dlerror/dlsym error occurs during modfind/modname,
 * log it here.  Such messages can be helpful in diagnosing
 * dynamic binding problems for modules.
 */
static void module_dlerror (const char *errmsg, void *arg)
{
    flux_t *h = arg;
    flux_log (h, LOG_DEBUG, "flux_modname: %s", errmsg);
}


static int load_module_bypath (broker_ctx_t *ctx, const char *path,
                               const char *argz, size_t argz_len,
                               const flux_msg_t *request)
{
    module_t *p = NULL;
    char *name, *arg;

    if (!(name = flux_modname (path, module_dlerror, ctx->h))) {
        errno = ENOENT;
        goto error;
    }
    if (!(p = module_add (ctx->modhash, path)))
        goto error;
    if (service_add (ctx->services, module_get_name (p),
                                    module_get_uuid (p), mod_svc_cb, p) < 0)
        goto module_remove;
    arg = argz_next (argz, argz_len, NULL);
    while (arg) {
        module_add_arg (p, arg);
        arg = argz_next (argz, argz_len, arg);
    }
    module_set_poller_cb (p, module_cb, ctx);
    module_set_status_cb (p, module_status_cb, ctx);
    if (request && module_push_insmod (p, request) < 0) // response deferred
        goto service_remove;
    if (module_start (p) < 0)
        goto service_remove;
    flux_log (ctx->h, LOG_DEBUG, "insmod %s", name);
    free (name);
    return 0;
service_remove:
    service_remove_byuuid (ctx->services, module_get_uuid (p));
module_remove:
    module_remove (ctx->modhash, p);
error:
    free (name);
    return -1;
}

static int load_module_byname (broker_ctx_t *ctx, const char *name,
                               const char *argz, size_t argz_len,
                               const flux_msg_t *request)
{
    const char *modpath;
    char *path;

    if (attr_get (ctx->attrs, "conf.module_path", &modpath, NULL) < 0) {
        log_msg ("conf.module_path is not set");
        return -1;
    }
    if (!(path = flux_modfind (modpath, name, module_dlerror, ctx->h))) {
        log_msg ("%s: not found in module search path", name);
        return -1;
    }
    if (load_module_bypath (ctx, path, argz, argz_len, request) < 0) {
        free (path);
        return -1;
    }
    free (path);
    return 0;
}

static int unload_module_byname (broker_ctx_t *ctx, const char *name,
                                 const flux_msg_t *request)
{
    module_t *p;

    if (!(p = module_lookup_byname (ctx->modhash, name))) {
        errno = ENOENT;
        return -1;
    }
    if (module_stop (p) < 0)
        return -1;
    if (module_push_rmmod (p, request) < 0)
        return -1;
    flux_log (ctx->h, LOG_DEBUG, "rmmod %s", name);
    return 0;
}

static void broker_destroy_sigwatcher (void *data)
{
    flux_watcher_t *w = data;
    flux_watcher_stop (w);
    flux_watcher_destroy (w);
}

static int broker_handle_signals (broker_ctx_t *ctx)
{
    int i, sigs[] = { SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGSEGV, SIGFPE,
                      SIGALRM };
    flux_watcher_t *w;

    for (i = 0; i < sizeof (sigs) / sizeof (sigs[0]); i++) {
        w = flux_signal_watcher_create (ctx->reactor, sigs[i], signal_cb, ctx);
        if (!w) {
            log_err ("flux_signal_watcher_create");
            return -1;
        }
        if (zlist_push (ctx->sigwatchers, w) < 0) {
            log_errn (ENOMEM, "zlist_push");
            return -1;
        }
        zlist_freefn (ctx->sigwatchers, w, broker_destroy_sigwatcher, false);
        flux_watcher_start (w);
    }
    return 0;
}

/**
 ** Built-in services
 **/

/* Unload a module by name, asynchronously.
 * Message format is defined by RFC 5.
 * N.B. unload_module_byname() handles response, unless it fails early
 * and returns -1.
 */
static void broker_rmmod_cb (flux_t *h, flux_msg_handler_t *mh,
                             const flux_msg_t *msg, void *arg)
{
    broker_ctx_t *ctx = arg;
    const char *name;

    if (flux_request_unpack (msg, NULL, "{s:s}", "name", &name) < 0)
        goto error;
    if (unload_module_byname (ctx, name, msg) < 0)
        goto error;
    return;
error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

/* Load a module by name, asynchronously.
 * Message format is defined by RFC 5.
 * N.B. load_module_bypath() handles response, unless it returns -1.
 */
static void broker_insmod_cb (flux_t *h, flux_msg_handler_t *mh,
                              const flux_msg_t *msg, void *arg)
{
    broker_ctx_t *ctx = arg;
    const char *path;
    json_t *args;
    size_t index;
    json_t *value;
    char *argz = NULL;
    size_t argz_len = 0;
    error_t e;

    if (flux_request_unpack (msg, NULL, "{s:s s:o}", "path", &path,
                                                     "args", &args) < 0)
        goto error;
    if (!json_is_array (args))
        goto proto;
    json_array_foreach (args, index, value) {
        if (!json_is_string (value))
            goto proto;
        if ((e = argz_add (&argz, &argz_len, json_string_value (value)))) {
            errno = e;
            goto error;
        }
    }
    if (load_module_bypath (ctx, path, argz, argz_len, msg) < 0)
        goto error;
    free (argz);
    return;
proto:
    errno = EPROTO;
error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
    free (argz);
}

/* Load a module by name.
 * Message format is defined by RFC 5.
 */
static void broker_lsmod_cb (flux_t *h, flux_msg_handler_t *mh,
                             const flux_msg_t *msg, void *arg)
{
    broker_ctx_t *ctx = arg;
    json_t *mods = NULL;

    if (flux_request_decode (msg, NULL, NULL) < 0)
        goto error;
    if (!(mods = module_get_modlist (ctx->modhash, ctx->services)))
        goto error;
    if (flux_respond_pack (h, msg, "{s:O}", "mods", mods) < 0)
        flux_log_error (h, "%s: flux_respond_pack", __FUNCTION__);
    json_decref (mods);
    return;
error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

#if CODE_COVERAGE_ENABLED
void __gcov_flush (void);
#endif

static void broker_panic_cb (flux_t *h, flux_msg_handler_t *mh,
                             const flux_msg_t *msg, void *arg)
{
    const char *reason;
    int flags; // reserved

    if (flux_request_unpack (msg, NULL, "{s:s s:i}",
                             "reason", &reason,
                             "flags", &flags) < 0) {
        flux_log_error (h, "malformed broker.panic request");
        return;
    }
    fprintf (stderr, "PANIC: %s\n", reason);
#if CODE_COVERAGE_ENABLED
    __gcov_flush ();
#endif
    _exit (1);
    /*NOTREACHED*/
}

static void broker_disconnect_cb (flux_t *h, flux_msg_handler_t *mh,
                               const flux_msg_t *msg, void *arg)
{
    const char *sender;

    if ((sender = flux_msg_route_first (msg)))
        exec_terminate_subprocesses_by_uuid (h, sender);
    /* no response */
}

static void broker_sub_cb (flux_t *h, flux_msg_handler_t *mh,
                        const flux_msg_t *msg, void *arg)
{
    broker_ctx_t *ctx = arg;
    const char *uuid;
    const char *topic;

    if (flux_request_unpack (msg, NULL, "{ s:s }", "topic", &topic) < 0)
        goto error;
    if (!(uuid = flux_msg_route_first (msg))) {
        errno = EPROTO;
        goto error;
    }
    if (!uuid) {
        errno = EPROTO;
        goto error;
    }
    if (module_subscribe (ctx->modhash, uuid, topic) < 0)
        goto error;
    if (flux_respond (h, msg, NULL) < 0)
        flux_log_error (h, "%s: flux_respond", __FUNCTION__);
    return;
error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static void broker_unsub_cb (flux_t *h, flux_msg_handler_t *mh,
                          const flux_msg_t *msg, void *arg)
{
    broker_ctx_t *ctx = arg;
    const char *uuid;
    const char *topic;

    if (flux_request_unpack (msg, NULL, "{ s:s }", "topic", &topic) < 0)
        goto error;
    if (!(uuid = flux_msg_route_first (msg))) {
        errno = EPROTO;
        goto error;
    }
    if (!uuid) {
        errno = EPROTO;
        goto error;
    }
    if (module_unsubscribe (ctx->modhash, uuid, topic) < 0)
        goto error;
    if (flux_respond (h, msg, NULL) < 0)
        flux_log_error (h, "%s: flux_respond", __FUNCTION__);
    return;
error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "%s: flux_respond_error", __FUNCTION__);
}

static int route_to_handle (const flux_msg_t *msg, void *arg)
{
    broker_ctx_t *ctx = arg;
    if (flux_requeue (ctx->h, msg, FLUX_RQ_TAIL) < 0)
        flux_log_error (ctx->h, "%s: flux_requeue\n", __FUNCTION__);
    return 0;
}

/* Check whether requestor 'cred' is authorized to add/remove service 'name'.
 * Allow a guest control over a service IFF it is prefixed with "<userid>-".
 * Return 0 on success, -1 with errno set on failure.
 */
static int service_allow (struct flux_msg_cred cred, const char *name)
{
    char prefix[16];
    if ((cred.rolemask & FLUX_ROLE_OWNER))
        return 0;
    snprintf (prefix, sizeof (prefix), "%" PRIu32 "-", cred.userid);
    if (!strncmp (prefix, name, strlen (prefix)))
        return 0;
    errno = EPERM;
    return -1;
}

/* Dynamic service registration.
 * These handlers need to appear in broker.c so that they have
 *  access to broker internals like modhash
 */
static void service_add_cb (flux_t *h, flux_msg_handler_t *w,
                            const flux_msg_t *msg, void *arg)
{
    broker_ctx_t *ctx = arg;
    const char *name = NULL;
    const char *sender;
    module_t *p;
    struct flux_msg_cred cred;

    if (flux_request_unpack (msg, NULL, "{ s:s }", "service", &name) < 0
            || flux_msg_get_cred (msg, &cred) < 0)
        goto error;
    if (service_allow (cred, name) < 0)
        goto error;
    if (!(sender = flux_msg_route_first (msg))) {
        errno = EPROTO;
        goto error;
    }
    if (!(p = module_lookup (ctx->modhash, sender))) {
        errno = ENOENT;
        goto error;
    }
    if (service_add (ctx->services, name, sender, mod_svc_cb, p) < 0)
        goto error;
    if (flux_respond (h, msg, NULL) < 0)
        flux_log_error (h, "service_add: flux_respond");
    return;
error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "service_add: flux_respond_error");
}

static void service_remove_cb (flux_t *h, flux_msg_handler_t *w,
                               const flux_msg_t *msg, void *arg)
{
    broker_ctx_t *ctx = arg;
    const char *name;
    const char *uuid;
    const char *sender;
    struct flux_msg_cred cred;

    if (flux_request_unpack (msg, NULL, "{ s:s }", "service", &name) < 0
            || flux_msg_get_cred (msg, &cred) < 0)
        goto error;
    if (service_allow (cred, name) < 0)
        goto error;
    if (!(sender = flux_msg_route_first (msg))) {
        errno = EPROTO;
        goto error;
    }
    if (!(uuid = service_get_uuid (ctx->services, name))) {
        errno = ENOENT;
        goto error;
    }
    if (strcmp (uuid, sender) != 0) {
        errno = EINVAL;
        goto error;
    }
    service_remove (ctx->services, name);
    if (flux_respond (h, msg, NULL) < 0)
        flux_log_error (h, "service_remove: flux_respond");
    return;
error:
    if (flux_respond_error (h, msg, errno, NULL) < 0)
        flux_log_error (h, "service_remove: flux_respond_error");
}


static const struct flux_msg_handler_spec htab[] = {
    {
        FLUX_MSGTYPE_REQUEST,
        "broker.rmmod",
        broker_rmmod_cb,
        0
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "broker.insmod",
        broker_insmod_cb,
        0
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "broker.lsmod",
        broker_lsmod_cb,
        0
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "broker.panic",
        broker_panic_cb,
        0
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "broker.disconnect",
        broker_disconnect_cb,
        0
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "broker.sub",
        broker_sub_cb,
        0
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "broker.unsub",
        broker_unsub_cb,
        0
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "service.add",
        service_add_cb,
        FLUX_ROLE_USER,
    },
    {
        FLUX_MSGTYPE_REQUEST,
        "service.remove",
        service_remove_cb,
        FLUX_ROLE_USER,
    },
    FLUX_MSGHANDLER_TABLE_END,
};

struct internal_service {
    const char *name;
    const char *nodeset;
};

static struct internal_service services[] = {
    { "broker",             NULL }, // kind of a catch-all, slowly deprecating
    { "log",                NULL },
    { "content",            NULL },
    { "attr",               NULL },
    { "heaptrace",          NULL },
    { "event",              "[0]" },
    { "service",            NULL },
    { "overlay",            NULL },
    { "config",             NULL },
    { "runat",              NULL },
    { "state-machine",      NULL },
    { "groups",             NULL },
    { NULL, NULL, },
};

/* Register builtin services (sharing ctx->h and broker thread).
 * Register message handlers for some broker services.  Others are registered
 * in their own initialization functions.
 */
static flux_msg_handler_t **broker_add_services (broker_ctx_t *ctx)
{
    flux_msg_handler_t **handlers;
    struct internal_service *svc;
    for (svc = &services[0]; svc->name != NULL; svc++) {
        if (!nodeset_member (svc->nodeset, ctx->rank))
            continue;
        if (service_add (ctx->services, svc->name, NULL,
                         route_to_handle, ctx) < 0) {
            log_err ("error registering service for %s", svc->name);
            return NULL;
        }
    }

    if (flux_msg_handler_addvec (ctx->h, htab, ctx, &handlers) < 0) {
        log_err ("error registering message handlers");
        return NULL;
    }
    return handlers;
}

/* Unregister message handlers
 */
static void broker_remove_services (flux_msg_handler_t *handlers[])
{
    flux_msg_handler_delvec (handlers);
}

/**
 ** reactor callbacks
 **/

/* Handle messages received from overlay peers.
 */
static void overlay_recv_cb (const flux_msg_t *msg,
                             overlay_where_t where,
                             void *arg)
{
    broker_ctx_t *ctx = arg;
    int type;
    bool dropped = false;

    if (flux_msg_get_type (msg, &type) < 0)
        return;
    switch (type) {
        case FLUX_MSGTYPE_REQUEST:
            broker_request_sendmsg (ctx, msg); // handles errors internally
            break;
        case FLUX_MSGTYPE_RESPONSE:
            if (broker_response_sendmsg (ctx, msg) < 0)
                dropped = true;
            break;
        case FLUX_MSGTYPE_EVENT:
            /* If event originated from upstream peer, then it has already been
             * published and we are to continue its distribution.
             * Otherwise, take the next step to get the event published.
             */
            if (where == OVERLAY_UPSTREAM) {
                if (handle_event (ctx, msg) < 0)
                    dropped = true;
            }
            else {
                if (broker_event_sendmsg (ctx, msg) < 0)
                    dropped = true;
            }
            break;
        default:
            break;
    }
    /* Suppress logging if a response could not be sent due to ENOSYS,
     * which happens if sending module unloads before finishing all RPCs.
     */
    if (dropped && (type != FLUX_MSGTYPE_RESPONSE || errno != ENOSYS)) {
        const char *topic = "unknown";
        (void)flux_msg_get_topic (msg, &topic);
        flux_log_error (ctx->h, "DROP %s %s topic=%s",
                        where == OVERLAY_UPSTREAM ? "upstream" : "downstream",
                        flux_msg_typestr (type),
                        topic);
    }
}

/* Distribute events downstream, and to module and broker-resident subscribers.
 * On rank 0, publisher is wired to send events here also.
 */
static int handle_event (broker_ctx_t *ctx, const flux_msg_t *msg)
{
    uint32_t seq;
    const char *topic, *s;

    if (flux_msg_get_seq (msg, &seq) < 0
            || flux_msg_get_topic (msg, &topic) < 0) {
        flux_log (ctx->h, LOG_ERR, "dropping malformed event");
        return -1;
    }
    if (seq <= ctx->event_recv_seq) {
        //flux_log (ctx->h, LOG_DEBUG, "dropping duplicate event %d", seq);
        return -1;
    }
    if (ctx->event_recv_seq > 0) { /* don't log initial missed events */
        int first = ctx->event_recv_seq + 1;
        int count = seq - first;
        if (count > 1)
            flux_log (ctx->h, LOG_ERR, "lost events %d-%d", first, seq - 1);
        else if (count == 1)
            flux_log (ctx->h, LOG_ERR, "lost event %d", first);
    }
    ctx->event_recv_seq = seq;

    /* Forward to this rank's children.
     */
    overlay_sendmsg (ctx->overlay, msg, OVERLAY_DOWNSTREAM);

    /* Internal services may install message handlers for events.
     */
    s = zlist_first (ctx->subscriptions);
    while (s) {
        if (!strncmp (s, topic, strlen (s))) {
            if (flux_requeue (ctx->h, msg, FLUX_RQ_TAIL) < 0)
                flux_log_error (ctx->h, "%s: flux_requeue\n", __FUNCTION__);
            break;
        }
        s = zlist_next (ctx->subscriptions);
    }
    /* Finally, route to local module subscribers.
     */
    return module_event_mcast (ctx->modhash, msg);
}

/* Callback to send disconnect messages on behalf of unloading module.
 */
void disconnect_send_cb (const flux_msg_t *msg, void *arg)
{
    broker_ctx_t *ctx = arg;
    broker_request_sendmsg (ctx, msg);
}

/* Handle messages on the service socket of a module.
 */
static void module_cb (module_t *p, void *arg)
{
    broker_ctx_t *ctx = arg;
    flux_msg_t *msg = module_recvmsg (p);
    int type;
    int ka_errnum, ka_status;
    int count;

    if (!msg)
        goto done;
    if (flux_msg_get_type (msg, &type) < 0)
        goto done;
    switch (type) {
        case FLUX_MSGTYPE_RESPONSE:
            (void)broker_response_sendmsg (ctx, msg);
            break;
        case FLUX_MSGTYPE_REQUEST:
            count = flux_msg_route_count (msg);
            /* Requests originated by the broker module will have a route
             * count of 1.  Ensure that, when the module is unloaded, a
             * disconnect message is sent to all services used by broker module.
             */
            if (count == 1) {
                if (module_disconnect_arm (p, msg, disconnect_send_cb, ctx) < 0)
                    flux_log_error (ctx->h, "error arming module disconnect");
            }
            /* Requests sent by the module on behalf of _its_ peers, e.g.
             * connector-local module with connected clients, will have a
             * route count greater than one here.  If this broker is not
             * "online" (entered INIT state), politely rebuff these requests.
             * Possible scenario for this message: user submitting a job on
             * a login node before cluster reboot is complete.
             */
            else if (count > 1 && !ctx->online) {
                const char *errmsg = "Upstream Flux broker is offline."
                                     " Try again later.";

                if (flux_respond_error (ctx->h, msg, EAGAIN, errmsg) < 0)
                    flux_log_error (ctx->h, "send offline response message");
                break;
            }
            broker_request_sendmsg (ctx, msg);
            break;
        case FLUX_MSGTYPE_EVENT:
            if (broker_event_sendmsg (ctx, msg) < 0) {
                flux_log_error (ctx->h, "%s(%s): broker_event_sendmsg %s",
                                __FUNCTION__, module_get_name (p),
                                flux_msg_typestr (type));
            }
            break;
        case FLUX_MSGTYPE_KEEPALIVE:
            if (flux_keepalive_decode (msg, &ka_errnum, &ka_status) < 0) {
                flux_log_error (ctx->h, "%s: flux_keepalive_decode",
                                module_get_name (p));
                break;
            }
            if (ka_status == FLUX_MODSTATE_FINALIZING) {
                /* Module is finalizing and doesn't want any more messages.
                 * mute the module and respond with the same keepalive
                 * message for synchronization (module waits to proceed)
                 */
                module_mute (p);
                if (module_sendmsg (p, msg) < 0)
                    flux_log_error (ctx->h,
                                    "%s: reply to finalizing: module_sendmsg",
                                    module_get_name (p));
            }
            if (ka_status == FLUX_MODSTATE_EXITED)
                module_set_errnum (p, ka_errnum);
            module_set_status (p, ka_status);
            break;
        default:
            flux_log (ctx->h, LOG_ERR, "%s(%s): unexpected %s",
                      __FUNCTION__, module_get_name (p),
                      flux_msg_typestr (type));
            break;
    }
done:
    flux_msg_destroy (msg);
}

static int module_insmod_respond (flux_t *h, module_t *p)
{
    int rc;
    int errnum = 0;
    int status = module_get_status (p);
    flux_msg_t *msg = module_pop_insmod (p);

    if (msg == NULL)
        return 0;

    /* If the module is EXITED, return error to insmod if mod_main() < 0
     */
    if (status == FLUX_MODSTATE_EXITED)
        errnum = module_get_errnum (p);
    if (errnum == 0)
        rc = flux_respond (h, msg, NULL);
    else
        rc = flux_respond_error (h, msg, errnum, NULL);

    flux_msg_destroy (msg);
    return rc;
}

static int module_rmmod_respond (flux_t *h, module_t *p)
{
    flux_msg_t *msg;
    int rc = 0;
    while ((msg = module_pop_rmmod (p))) {
        if (flux_respond (h, msg, NULL) < 0)
            rc = -1;
        flux_msg_destroy (msg);
    }
    return rc;
}

static void module_status_cb (module_t *p, int prev_status, void *arg)
{
    broker_ctx_t *ctx = arg;
    int status = module_get_status (p);
    const char *name = module_get_name (p);

    /* Transition from INIT
     * If module started normally, i.e. INIT->RUNNING, then
     * respond to insmod requests now. O/w, delay responses until
     * EXITED, when any errnum is available.
     */
    if (prev_status == FLUX_MODSTATE_INIT
        && status == FLUX_MODSTATE_RUNNING) {
        if (module_insmod_respond (ctx->h, p) < 0)
            flux_log_error (ctx->h, "flux_respond to insmod %s", name);
    }

    /* Transition to EXITED
     * Remove service routes, respond to insmod & rmmod request(s), if any,
     * and remove the module (which calls pthread_join).
     */
    if (status == FLUX_MODSTATE_EXITED) {
        flux_log (ctx->h, LOG_DEBUG, "module %s exited", name);
        service_remove_byuuid (ctx->services, module_get_uuid (p));

        if (module_insmod_respond (ctx->h, p) < 0)
            flux_log_error (ctx->h, "flux_respond to insmod %s", name);

        if (module_rmmod_respond (ctx->h, p) < 0)
            flux_log_error (ctx->h, "flux_respond to rmmod %s", name);

        module_remove (ctx->modhash, p);
    }
}

static void signal_cb (flux_reactor_t *r, flux_watcher_t *w,
                         int revents, void *arg)
{
    broker_ctx_t *ctx = arg;
    int signum = flux_signal_watcher_get_signum (w);

    flux_log (ctx->h, LOG_INFO, "signal %d", signum);
    state_machine_kill (ctx->state_machine, signum);
}

/* Route request.
 * On success, return 0.  On failure, return -1 with errno set.
 */
static int broker_request_sendmsg_internal (broker_ctx_t *ctx,
                                            const flux_msg_t *msg)
{
    uint32_t nodeid;
    uint8_t flags;

    if (flux_msg_get_nodeid (msg, &nodeid) < 0)
        return -1;
    if (flux_msg_get_flags (msg, &flags) < 0)
        return -1;
    /* Route up TBON if destination if upstream of this broker.
     */
    if ((flags & FLUX_MSGFLAG_UPSTREAM) && nodeid == ctx->rank) {
        if (overlay_sendmsg (ctx->overlay, msg, OVERLAY_UPSTREAM) < 0)
            return -1;
    }
    /* Deliver to local service if destination *could* be this broker.
     * If there is no such service locally (ENOSYS), route up TBON.
     */
    else if (((flags & FLUX_MSGFLAG_UPSTREAM) && nodeid != ctx->rank)
                                              || nodeid == FLUX_NODEID_ANY) {
        if (service_send (ctx->services, msg) < 0) {
            if (errno != ENOSYS)
                return -1;
            if (overlay_sendmsg (ctx->overlay, msg, OVERLAY_UPSTREAM) < 0) {
                if (errno == EHOSTUNREACH)
                    errno = ENOSYS;
                return -1;
            }
        }
    }
    /* Deliver to local service if this broker is the addressed rank.
     */
    else if (nodeid == ctx->rank) {
        if (service_send (ctx->services, msg) < 0)
            return -1;
    }
    /* Send the request up or down TBON as addressed.
     */
    else {
        if (overlay_sendmsg (ctx->overlay, msg, OVERLAY_ANY) < 0)
            return -1;
    }
    return 0;
}

/* Route request.  If there is an error routing the request,
 * generate an error response.  Make an extra effort to return a useful
 * error message if ENOSYS indicates an unmatched service name.
 */
static void broker_request_sendmsg (broker_ctx_t *ctx, const flux_msg_t *msg)
{
    if (broker_request_sendmsg_internal (ctx, msg) < 0) {
        const char *topic;
        char errbuf[64];
        const char *errstr = NULL;

        if (errno == ENOSYS && flux_msg_get_topic (msg, &topic) == 0) {
            snprintf (errbuf,
                      sizeof (errbuf),
                      "No service matching %s is registered", topic);
            errstr = errbuf;
        }
        if (flux_respond_error (ctx->h, msg, errno, errstr) < 0)
            flux_log_error (ctx->h, "flux_respond");
    }
}

/* Route a response message, determining next hop from route stack.
 * If there is no next hop, routing is complete to broker-resident service.
 * If the next hop is an overlay peer, route up or down the TBON.
 * If not a peer, look up a module by uuid.
 */
static int broker_response_sendmsg (broker_ctx_t *ctx, const flux_msg_t *msg)
{
    int rc;
    const char *uuid;

    if (!(uuid = flux_msg_route_last (msg)))
        rc = flux_requeue (ctx->h, msg, FLUX_RQ_TAIL);
    else if (overlay_uuid_is_parent (ctx->overlay, uuid))
        rc = overlay_sendmsg (ctx->overlay, msg, OVERLAY_UPSTREAM);
    else if (overlay_uuid_is_child (ctx->overlay, uuid))
        rc = overlay_sendmsg (ctx->overlay, msg, OVERLAY_DOWNSTREAM);
    else
        rc = module_response_sendmsg (ctx->modhash, msg);
    return rc;
}

/* Events are forwarded up the TBON to rank 0, then published per RFC 3.
 * An alternate publishing mechanism that allows the event sequence number
 * to be obtained is to send an RPC to event.pub.
 */
static int broker_event_sendmsg (broker_ctx_t *ctx, const flux_msg_t *msg)
{
    int rc;

    if (ctx->rank > 0)
        rc = overlay_sendmsg (ctx->overlay, msg, OVERLAY_UPSTREAM);
    else
        rc = publisher_send (ctx->publisher, msg);
    return rc;
}

/**
 ** Broker's internal flux_t implementation
 ** N.B. recv() method is missing because messages are "received"
 ** when routing logic calls flux_requeue().
 **/

static int broker_send (void *impl, const flux_msg_t *msg, int flags)
{
    broker_ctx_t *ctx = impl;
    int type;
    struct flux_msg_cred cred;
    flux_msg_t *cpy = NULL;
    int rc = -1;

    if (!(cpy = flux_msg_copy (msg, true)))
        goto done;
    if (flux_msg_get_type (cpy, &type) < 0)
        goto done;
    if (flux_msg_get_cred (cpy, &cred) < 0)
        goto done;
    if (cred.userid == FLUX_USERID_UNKNOWN)
        cred.userid = ctx->cred.userid;
    if (cred.rolemask == FLUX_ROLE_NONE)
        cred.rolemask = ctx->cred.rolemask;
    if (flux_msg_set_cred (cpy, cred) < 0)
        goto done;

    switch (type) {
        case FLUX_MSGTYPE_REQUEST:
            rc = broker_request_sendmsg_internal (ctx, cpy);
            break;
        case FLUX_MSGTYPE_RESPONSE:
            rc = broker_response_sendmsg (ctx, cpy);
            break;
        case FLUX_MSGTYPE_EVENT:
            rc = broker_event_sendmsg (ctx, cpy);
            break;
        default:
            errno = EINVAL;
            break;
    }
done:
    flux_msg_destroy (cpy);
    return rc;
}

static int broker_subscribe (void *impl, const char *topic)
{
    broker_ctx_t *ctx = impl;
    char *cpy = NULL;

    if (!(cpy = strdup (topic)))
        goto nomem;
    if (zlist_append (ctx->subscriptions, cpy) < 0)
        goto nomem;
    zlist_freefn (ctx->subscriptions, cpy, free, true);
    return 0;
nomem:
    free (cpy);
    errno = ENOMEM;
    return -1;
}

static int broker_unsubscribe (void *impl, const char *topic)
{
    broker_ctx_t *ctx = impl;
    char *s = zlist_first (ctx->subscriptions);
    while (s) {
        if (!strcmp (s, topic)) {
            zlist_remove (ctx->subscriptions, s);
            break;
        }
        s = zlist_next (ctx->subscriptions);
    }
    return 0;
}

static const struct flux_handle_ops broker_handle_ops = {
    .send = broker_send,
    .event_subscribe = broker_subscribe,
    .event_unsubscribe = broker_unsubscribe,
};


#if HAVE_VALGRIND
/* Disable dlclose() during valgrind operation
 */
void I_WRAP_SONAME_FNNAME_ZZ(Za,dlclose)(void *dso) {}
#endif

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
