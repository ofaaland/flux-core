#!/bin/sh
#

test_description='Verify subtree health transitions and tools

Ensure that the overlay subtree health status transitions
appropriately as brokers are taken offline or lost, and also
put flux overlay status tool and related RPCs and subcommands
through their paces.
'

. `dirname $0`/sharness.sh

export TEST_UNDER_FLUX_FANOUT=2

test_under_flux 15 system

startctl="flux python ${SHARNESS_TEST_SRCDIR}/scripts/startctl.py"

overlay_connected_children() {
	rank=$1
        flux python -c "import flux; print(flux.Flux().rpc(\"overlay.stats.get\",nodeid=${rank}).get_str())" | jq -r '.["child-connected"]'
}

# Usage: wait_connected rank count tries delay
wait_connected() {
	local rank=$1
	local count=$2
	local tries=$3
	local delay=$4

	while test $tries -gt 0; do
		local n=$(overlay_connected_children $rank)
		echo $n children
		test $n -eq $count && return 0
		sleep $delay
		tries=$(($tries-1))
	done
	return 1
}

bad_topo_request() {
        flux python -c "import flux; print(flux.Flux().rpc(\"overlay.topology\",nodeid=0).get_str())"
}
bad_topo_request_rank99() {
        flux python -c "import flux; print(flux.Flux().rpc(\"overlay.topology\",{\"rank\":99},nodeid=0).get_str())"
}

test_expect_success 'overlay.topology RPC with no payload fails' '
	test_must_fail bad_topo_request
'
test_expect_success 'overlay.topology RPC with bad rank fails' '
	test_must_fail bad_topo_request_rank99
'

test_expect_success 'flux overlay status fails on bad rank' '
	test_must_fail flux overlay status --summary --rank 99
'

test_expect_success 'flux overlay fails on bad subcommand' '
	test_must_fail flux overlay notcommand
'

test_expect_success 'overlay status is full' '
	test "$(flux overlay status --summary)" = "full"
'

test_expect_success 'stop broker 3 with children 7,8' '
	$startctl kill 3 15
'

test_expect_success 'wait for rank 0 overlay status to be partial' '
	run_timeout 10 flux overlay status --rank 0 --summary --wait=partial
'

# Just because rank 0 is partial doesn't mean rank 3 is offline yet
# (shutdown starts at the leaves, and rank 3 will turn partial as
# soon as one of its children goes offline)
test_expect_success HAVE_JQ 'wait for rank 1 to lose connection with rank 3' '
	wait_connected 1 1 10 0.2
'

test_expect_success 'flux overlay status -vv works' '
	flux overlay status -vv
'

test_expect_success 'flux overlay status shows rank 3 offline' '
	echo "3 fake3: offline" >health.exp &&
	flux overlay status --no-pretty --no-color | grep fake3 >health.out &&
	test_cmp health.exp health.out
'

test_expect_success 'flux overlay status --summary' '
	flux overlay status --summary
'

test_expect_success 'flux overlay status --down' '
	flux overlay status --down
'

test_expect_success 'flux overlay status -vv' '
	flux overlay status -vv
'

test_expect_success 'flux overlay status: 0,1:partial, 3:offline' '
	flux overlay status --no-color --no-pretty  >health2.out &&
	grep "0 fake0: partial" health2.out &&
	grep "1 fake1: partial" health2.out &&
	grep "3 fake3: offline" health2.out
'

test_expect_success 'flux overlay status: 0-1:partial, 3,7-8:offline' '
	flux overlay status --no-color --no-pretty >health3.out &&
	grep "0 fake0: partial" health3.out &&
	grep "1 fake1: partial" health3.out &&
	grep "3 fake3: offline" health3.out &&
	grep "7 fake7: offline" health3.out &&
	grep "8 fake8: offline" health3.out
'

test_expect_success 'flux overlay status: 0,1:partial, 3,7-8:offline, rest:full' '
	flux overlay status --no-color --no-pretty >health4.out &&
	grep "0 fake0: partial" health4.out &&
	grep "1 fake1: partial" health4.out &&
	grep "3 fake3: offline" health4.out &&
	grep "7 fake7: offline" health4.out &&
	grep "8 fake8: offline" health4.out &&
	grep "4 fake4: full" health4.out &&
	grep "9 fake9: full" health4.out &&
	grep "10 fake10: full" health4.out &&
	grep "2 fake2: full" health4.out &&
	grep "5 fake5: full" health4.out &&
	grep "11 fake11: full" health4.out &&
	grep "6 fake6: full" health4.out &&
	grep "13 fake13: full" health4.out &&
	grep "14 fake14: full" health4.out
'

test_expect_success 'kill broker 14' '
	$startctl kill 14 9
'

# Ensure an EHOSTUNREACH is encountered to trigger connected state change.
test_expect_success 'ping to rank 14 fails with EHOSTUNREACH' '
	echo "flux-ping: 14!broker.ping: No route to host" >ping.exp &&
	test_must_fail flux ping 14 2>ping.err &&
	test_cmp ping.exp ping.err
'

test_expect_success 'wait for rank 0 subtree to be degraded' '
	run_timeout 10 flux overlay status --summary --wait=degraded
'

test_expect_success 'wait for unknown status fails' '
	test_must_fail flux overlay status --wait=foo
'

test_expect_success 'wait timeout works' '
	test_must_fail flux overlay status --wait=full --summary --timeout=0.1s
'

test_expect_success 'flux overlay status -vv' '
	flux overlay status -vv
'
test_expect_success 'flux overlay status -v' '
	flux overlay status -v
'

test_expect_success 'flux overlay gethostbyrank with no rank fails' '
	test_must_fail flux overlay gethostbyrank
'

test_expect_success 'flux overlay gethostbyrank 0 works' '
	echo fake0 >host.0.exp &&
	flux overlay gethostbyrank 0 >host.0.out &&
	test_cmp host.0.exp host.0.out
'

test_expect_success 'flux overlay gethostbyrank 0-14 works' '
	echo "fake[0-14]" >host.0-14.exp &&
	flux overlay gethostbyrank 0-14 >host.0-14.out &&
	test_cmp host.0-14.exp host.0-14.out
'

test_expect_success 'flux overlay gethostbyrank fails on invalid idset' '
	test_must_fail flux overlay gethostbyrank -- -1
'

test_expect_success 'flux overlay gethostbyrank fails on out of range rank' '
	test_must_fail flux overlay gethostbyrank 100
'

test_done
