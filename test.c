#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <pwd.h>
#include <assert.h>
#include <locale.h>
#include <jansson.h>
#include <argz.h>
#include <flux/core.h>
#include <flux/optparse.h>

int log_msg_exit (char *s)
{
    printf("%s\n", s);
}

int log_err_exit (char *s)
{
    printf("error: %s\n", s);
}

struct info_ctx {
    flux_jobid_t id;
    json_t *keys;
};

void info_output (flux_future_t *f, const char *suffix, flux_jobid_t id)
{
    const char *s;

    if (flux_rpc_get_unpack (f, "{s:s}", suffix, &s) < 0) {
        if (errno == ENOENT) {
            flux_future_destroy (f);
            log_msg_exit ("job id or key not found");
        }
        else
            log_err_exit ("flux_rpc_get_unpack");
    }

    /* XXX - prettier output later */
    printf ("%s\n", s);
}
void info_continuation (flux_future_t *f, void *arg)
{
    struct info_ctx *ctx = arg;
    size_t index;
    json_t *key;

    json_array_foreach (ctx->keys, index, key) {
        const char *s = json_string_value (key);
        info_output (f, s, ctx->id);
    }

    flux_future_destroy (f);
}

void info_lookup (flux_t *h,
                  flux_jobid_t id)
{
    const char *topic = "job-info.lookup";
    flux_future_t *f;
    struct info_ctx ctx = {0};
    char *info_string = "R";
    json_t *s;

    ctx.id = id;
    if (!(ctx.keys = json_array ()))
        log_msg_exit ("json_array");

    if (!(s = json_string (info_string)))
        log_msg_exit ("json_string");

    if (json_array_append_new (ctx.keys, s) < 0)
        log_msg_exit ("json_array_append");

    if (!(f = flux_rpc_pack (h, topic, FLUX_NODEID_ANY, 0,
                             "{s:I s:O s:i}",
                             "id", ctx.id,
                             "keys", ctx.keys,
                             "flags", 0)))
        log_err_exit ("flux_rpc_pack");
    if (flux_future_then (f, -1., info_continuation, &ctx) < 0)
        log_err_exit ("flux_future_then");
    if (flux_reactor_run (flux_get_reactor (h), 0) < 0)
        log_err_exit ("flux_reactor_run");

    json_decref (ctx.keys);
}

int cmd_info (flux_jobid_t id)
{
    flux_t *h;

    if (!(h = flux_open (NULL, 0)))
        log_err_exit ("flux_open");

    info_lookup (h, id);

    flux_close (h);
    return (0);
}

int main(int argc, char **argv)
{
	flux_jobid_t id;
	char *flux_jobid_s;

	if (argc > 2) {
		printf("%s: too many arguments.\n", argv[0]);
		printf("   with 0 arguments: use contents of environment variable FLUX_JOB_ID.\n");
		printf("   with 1 argument:  use that argument as the flux job id.\n");
		return(1);
	} else if (argc == 2) {
		flux_jobid_s = argv[1];
	} else {
		flux_jobid_s = getenv("FLUX_JOB_ID");
		if (flux_jobid_s == NULL) {
			printf("%s: no argument specifying Job ID and no environment variable FLUX_JOB_ID exists\n", argv[0]);
			return(1);
		}
		printf("%s: using FLUX_JOB_ID %s\n", argv[0], flux_jobid_s);
	}

	if (flux_job_id_parse(flux_jobid_s, &id) < 0) {
		printf("%s: unable to parse Job ID %s\n", argv[0], flux_jobid_s);
		return(2);
	}

	cmd_info (id);

	return(0);
}
