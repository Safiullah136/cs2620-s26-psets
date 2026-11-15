#!/usr/bin/env bash
#
# Efficiently search for a "bad seed" that causes a ctconsensus-type program
# (e.g. ./ctstubborn) to fail. Each invocation of the program with
# `-R COUNT -q` tries COUNT fresh random seeds and exits (with a non-zero
# status) on the first failure, reporting the offending seed.
#
# Because each run picks its own unbiased fresh batch of seeds, we just keep
# re-running it until one batch contains a failing seed.
#
# Usage: ./find_bad_seed.sh [COUNT]
#   COUNT   Number of random seeds to try per run (default: 100000)

set -euo pipefail

BIN="${BIN:-./build/ctfail}"
COUNT="${1:-100000}"

if [[ ! -x "$BIN" ]]; then
    echo "error: '$BIN' is not executable" >&2
    exit 2
fi

echo "searching for a bad seed ($COUNT seeds per run)" >&2

# Re-run until a batch fails. A successful batch (all seeds pass) exits 0 and
# just starts the next one; a failing batch exits non-zero, so we stop.
for ((run = 1; ; run++)); do
    echo "  run $run: trying $COUNT seeds..." >&2
    if "$BIN" -n 5 -R "$COUNT" -q; then
        continue
    fi
    # The program printed "*** FAILURE on seed ..." and exited non-zero.
    exit $?
done