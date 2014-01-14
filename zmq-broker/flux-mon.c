/* flux-mon.c - flux mon subcommand */

#define _GNU_SOURCE
#include <getopt.h>
#include <json/json.h>
#include <assert.h>
#include <libgen.h>

#include "cmb.h"
#include "util.h"
#include "log.h"

#define OPTIONS "hqa:d:"
static const struct option longopts[] = {
    {"help",       no_argument,        0, 'h'},
    {"query",      no_argument,        0, 'q'},
    {"add",        required_argument,  0, 'a'},
    {"delete",     required_argument,  0, 'd'},
    { 0, 0, 0, 0 },
};

static void mon_list (flux_t h, int argc, char *argv[]);
static void mon_add (flux_t h, int argc, char *argv[]);
static void mon_del (flux_t h, int argc, char *argv[]);
static void mon_commit (flux_t h, int argc, char *argv[]);
static void mon_set (flux_t h, int argc, char *argv[]);
static void mon_get (flux_t h, int argc, char *argv[]);

void usage (void)
{
    fprintf (stderr, 
"Usage: flux-mon list\n"
"       flux-mon add <name> rpc <request tag>\n"
"       flux-mon del <name>\n"
"       flux-mon set commit-type=always|onrequest|ondel\n"
"       flux-mon get commit-type\n"
"       flux-mon commit\n"
);
    exit (1);
}

int main (int argc, char *argv[])
{
    flux_t h;
    int ch;
    char *cmd = NULL;

    log_init ("flux-mon");

    while ((ch = getopt_long (argc, argv, OPTIONS, longopts, NULL)) != -1) {
        switch (ch) {
            case 'h': /* --help */
                usage ();
                break;
            default:
                usage ();
                break;
        }
    }
    if (optind == argc)
        usage ();
    cmd = argv[optind++];

    if (!(h = cmb_init ()))
        err_exit ("cmb_init");

    if (!strcmp (cmd, "list"))
        mon_list (h, argc - optind, argv + optind);
    else if (!strcmp (cmd, "add"))
        mon_add (h, argc - optind, argv + optind);
    else if (!strcmp (cmd, "del"))
        mon_del (h, argc - optind, argv + optind);
    else if (!strcmp (cmd, "commit"))
        mon_commit (h, argc - optind, argv + optind);
    else if (!strcmp (cmd, "set"))
        mon_set (h, argc - optind, argv + optind);
    else if (!strcmp (cmd, "get"))
        mon_get (h, argc - optind, argv + optind);
    else
        usage ();

    flux_handle_destroy (&h);
    log_fini ();
    return 0;
}

static void mon_del (flux_t h, int argc, char *argv[])
{
    char *key;

    if (argc < 1)
        usage ();
    if (asprintf (&key, "conf.mon.source.%s", argv[0]) < 0)
        oom ();
    if (kvs_get (h, key, NULL) < 0 && errno == ENOENT)
        err_exit ("%s", key);
    if (kvs_unlink (h, key) < 0)
        err_exit ("%s", key);
    if (kvs_commit (h) < 0)
        err_exit ("kvs_commit");
    free (key);
}

static void mon_add (flux_t h, int argc, char *argv[])
{
    char *name, *type, *key;
    json_object *o;

    if (argc < 2)
        usage ();
    name = argv[0];
    type = argv[1];

    o = util_json_object_new_object ();
    util_json_object_add_string (o, "name", name);
    if (!strcmp (type, "rpc")) {
        if (argc != 3)
            usage ();
        util_json_object_add_string (o, "type", "rpc");
        util_json_object_add_string (o, "tag", argv[2]);
    } else {
        usage ();
    }
    if (asprintf (&key, "conf.mon.source.%s", name) < 0)
        oom ();
    if (kvs_put (h, key, o) < 0)
        err_exit ("kvs_put %s", key);
    if (kvs_commit (h) < 0)
        err_exit ("kvs_commit");
    free (key);
    json_object_put (o);
}

static void mon_list (flux_t h, int argc, char *argv[])
{
    json_object *o;
    const char *name, *s;
    kvsdir_t dir;
    kvsitr_t itr;

    if (argc != 0)
        usage ();

    if (kvs_get_dir (h, &dir, "conf.mon.source") < 0) {
        if (errno == ENOENT)
            return;
        err_exit ("conf.mon.source");
    }
    itr = kvsitr_create (dir);
    while ((name = kvsitr_next (itr))) {
        if ((kvsdir_get (dir, name, &o) == 0)) {
            s = json_object_to_json_string_ext (o, JSON_C_TO_STRING_PLAIN);
            printf ("%s:  %s\n", name, s);
            json_object_put (o);
        }
    }
    kvsitr_destroy (itr);
    kvsdir_destroy (dir);
}

static void mon_commit (flux_t h, int argc, char *argv[])
{
    char *name;
    int nprocs;
    json_object *event;

    if (argc != 0)
        usage ();
    name = uuid_generate_str ();
    nprocs = flux_size (h) + 1;
    event = util_json_object_new_object ();

    util_json_object_add_string (event, "name", name);    
    util_json_object_add_int (event, "nprocs", nprocs);
    if (flux_event_send (h, event, "event.mon.commit") < 0)
        err_exit ("flux_event_send"); 
    if (kvs_fence (h, name, nprocs) < 0)
        err_exit ("kvs_fence");
    free (name);
}

static void mon_set (flux_t h, int argc, char *argv[])
{
    char *key;

    if (argc != 2)
        usage ();
    if (!strcmp (argv[0], "commit-type")) {
        if (!(!strcmp (argv[1], "always") || !strcmp (argv[1], "onrequest")
                                          || !strcmp (argv[1], "ondel")))
            usage ();
        if (asprintf (&key, "conf.mon.%s", argv[0]) < 0)
            oom ();
        if (kvs_put_string (h, key, argv[1]) < 0)
            err_exit ("%s", key);
        if (kvs_commit (h) < 0)
            err_exit ("kvs_commit");
        free (key);
    } else {
        usage ();
    }
}

static void mon_get (flux_t h, int argc, char *argv[])
{
    char *key;
    char *val = NULL;

    if (argc != 1)
        usage ();
    if (!strcmp (argv[0], "commit-type")) {
        if (asprintf (&key, "conf.mon.%s", argv[0]) < 0)
            oom ();
        if (kvs_get_string (h, key, &val) < 0 && errno == ESRCH)
            err_exit ("%s", key);
        printf ("%s: %s\n", argv[0], val ? val : "default");
        free (key);
        if (val)
            free (val);
    }
}

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
