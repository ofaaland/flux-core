#!/bin/sh -e
#
#  Ensure a job exception is raised when broker rank disconnects
#
#  1. Submit job and ensure it has started
#  2. Kill a leaf broker
#  3. Ensure job exception is raised and includes appropriate error note
#
export startctl="flux python ${SHARNESS_TEST_SRCDIR}/scripts/startctl.py"
SHELL=/bin/sh flux start -s 4 -o,-Stbon.fanout=4 --test-exit-mode=leader '\
   id=$(flux mini submit -n4 -N4 sleep inf) \
&& flux job wait-event $id start \
&& $startctl kill 3 9 \
&& flux job wait-event $id exception >t3906.output 2>&1'

cat t3906.output

grep 'lost contact with job shell on broker.*rank 3' t3906.output
