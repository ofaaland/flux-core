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

int cmd_list (flux_jobid_t id)
{
    int max_entries = 5;
    flux_t *h = NULL;
    flux_t *child_handle = NULL;
    flux_future_t *f;
    json_t *jobs;
    size_t index;
    json_t *value;
    const char *okey;
    json_t *ovalue;
    const char *attrs_json_str = "[\"expiration\"]";
    const char *uri = NULL;

    if (!(h = flux_open (NULL, 0)))
        log_err_exit ("flux_open");

    /*
     * Whether to ask our parent or not
     * See https://github.com/flux-framework/flux-core/issues/3817
     */

	if (!getenv("FLUX_KVS_NAMESPACE")) {
        uri = flux_attr_get (h, "parent-uri");
        if (!uri) {
		    printf("no FLUX_KVS_NAMESPACE and flux_attr_get for parent-uri failed\n");
            flux_close (h);
            return (-1);
        }

        child_handle = h;
        h = flux_open (uri, 0);
        if (!h) {
		    printf("flux_open with parent-uri %s failed\n", uri);
            flux_close (child_handle);
            return (-1);
        }
    }

    if (!(f = flux_job_list (h, max_entries, attrs_json_str,
        FLUX_USERID_UNKNOWN, FLUX_JOB_STATE_RUNNING)))
        log_err_exit ("flux_job_list");
    if (flux_rpc_get_unpack (f, "{s:o}", "jobs", &jobs) < 0)
        log_err_exit ("flux_job_list");

    value = json_array_get(jobs, 0);
    if (value) {
        ovalue = json_object_get(value, "expiration");
        if (!ovalue) {
            printf("json_object_get(expiration) returned NULL\n");
            log_err_exit ("flux_job_list");
        }

        printf("expiration is %f\n", json_real_value(ovalue));
    }
    flux_future_destroy (f);
    flux_close (h);
    flux_close (child_handle);

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

	if (cmd_list (id) < 0) {
		printf("%s: unable to look up expiration %s\n", argv[0]);
		return(3);
	}

	return(0);
}

/*
 * vim: tabstop=4 shiftwidth=4 expandtab smartindent:
 */
