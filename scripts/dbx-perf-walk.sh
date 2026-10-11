#!/usr/bin/env bash
#
# Measure the metadata walk with `perf`.
#
# The walk is the issue-#58 pipeline, one process per level:
#
#   metastore ls | ratelimit --rate 15 | catalog ls | ratelimit --rate 15
#     | schema ls | ratelimit --rate 15 | credentials check | credentials get
#     | table info | table ls | partition ls | bytemass
#
# The catalog levels fan out — one Unity Catalog request per record — and
# Databricks rate-limits REST calls (the buckets are per endpoint; ~15/s is the
# SDK's own ceiling and is safe — decisions/0006-catalog-rate-limit.md), so a
# `ratelimit --rate` stage paces the refs ahead of each fan-out.
# `credentials check`/`credentials get` are one-to-one and the storage levels
# (`table info` down) read object storage, not the catalog, so neither is paced.
#
# The catalog levels span the whole Unity Catalog; the table levels descend the
# one catalog this principal may read outside compute (PQB_WALK_CATALOG,
# default `pqbench_ext`, the external fixture — see scripts/dbx-ext/ and
# experiments/exp33_databricks_credential_vending.md). Unity refuses
# `EXTERNAL USE SCHEMA` on the sample and managed-default catalogs (`samples`
# is owned by `System user`; `dbx_samples` tables are `TABLE_DB_STORAGE`), and
# `credentials check` filters by eligibility, not by grant, so the walk narrows
# to the readable catalog before the table levels.
#
# A live run shares the endpoint and can be throttled or hit a transient
# transport error, so `main` retries the walk at half the rate (the pattern
# `docs/cli.md` gives: "pace the walk and retry it at a lower rate in the
# shell") before it fails.
#
# The walk runs three ways:
#
#   shape   one pass with `tee` after each level, so every level's record count
#           is visible (the "jobs are limited by the number of tables" claim);
#   stat    `perf stat` over the whole pipeline, repeated for the spread;
#   record  `perf record -g` then `perf report` for the hot symbols.
#
# Auth: either PQB_ENDPOINT / PQB_TOKEN, or DBX_HOST plus a bearer source — the
# GitHub Actions OIDC token (`id-token: write`; set DATABRICKS_CLIENT_ID and
# DATABRICKS_ACCOUNT_ID, and the service principal needs a GitHub federation
# policy), or the e2e service principal's client secret
# (DBX_SAMPLES_SP_CLIENT_ID / DBX_SAMPLES_SP_CLIENT_SECRET). Run by the
# `dbx-perf-walk` CI job; the same script runs locally.
#
# Usage:
#
#   scripts/dbx-perf-walk.sh                    # shape + stat + record
#   PQB_PERF_MODE=stat scripts/dbx-perf-walk.sh
#   PQB_RATE=5 PQB_PERF_REPEAT=5 scripts/dbx-perf-walk.sh
#
# Tunables (environment): PQB, PQB_RATE, PQB_PERF_MODE, PQB_PERF_EVENTS,
# PQB_PERF_REPEAT, PQB_PERF_TOP, PQB_PERF_RETRIES, PQB_WALK_CATALOG,
# PQB_ENDPOINT, PQB_TOKEN, AWS_REGION.

set -euo pipefail

PQB="${PQB:-target/release/pqbench}"
RATE="${PQB_RATE:-15}"
MODE="${PQB_PERF_MODE:-all}"
EVENTS="${PQB_PERF_EVENTS:-cycles,instructions,duration_time}"
REPEAT="${PQB_PERF_REPEAT:-3}"
TOP="${PQB_PERF_TOP:-30}"
RETRIES="${PQB_PERF_RETRIES:-3}"
CATALOG="${PQB_WALK_CATALOG:-pqbench_ext}"
REGION="${AWS_REGION:-us-east-2}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Keep only the table-refs of the catalog the principal may read outside
# compute. The levels above still listed the whole Unity Catalog; this narrows
# the descent so `credentials get` is not refused for the rest.
keep_readable_catalog() {
    jq -c --arg catalog "$CATALOG" \
        'select(.kind == "pqbench.table-ref" and (.id | startswith($catalog + ".")))'
}

# The walk, as `docs/performance.md` writes it: one pipeline, every level
# streaming refs to the next. `--run` is the internal entry `perf` re-execs.
walk() {
    set -o pipefail
    "$PQB" metastore ls --format json |
        "$PQB" ratelimit --rate "$RATE" |
        "$PQB" catalog ls --format json |
        "$PQB" ratelimit --rate "$RATE" |
        "$PQB" schema ls --format json |
        "$PQB" ratelimit --rate "$RATE" |
        keep_readable_catalog |
        "$PQB" credentials check --format json |
        "$PQB" credentials get --format json |
        "$PQB" table info --format json |
        "$PQB" table ls --format json |
        "$PQB" partition ls --format json |
        "$PQB" bytemass --format json
}

if [ "${1:-}" = "--run" ]; then
    walk
    exit 0
fi

require() {
    [ -n "${!1:-}" ] || {
        echo "$1 is not set" >&2
        exit 1
    }
}

# Mint the walk's bearer. On GitHub Actions with `id-token: write` and the
# service principal's application ID (DATABRICKS_CLIENT_ID) and the OIDC
# audience (DATABRICKS_ACCOUNT_ID), exchange the runner's OIDC token for a
# Databricks OAuth token (workload identity federation — no stored secret; the
# SP needs a GitHub federation policy). Fall back to the service principal's
# client secret otherwise, then report.
mint_token() {
    if [ -n "${ACTIONS_ID_TOKEN_REQUEST_URL:-}" ] &&
        [ -n "${DATABRICKS_CLIENT_ID:-}" ] &&
        [ -n "${DATABRICKS_ACCOUNT_ID:-}" ]; then
        local id_token token
        id_token="$(curl -fsS \
            -H "Authorization: bearer ${ACTIONS_ID_TOKEN_REQUEST_TOKEN:?}" \
            "${ACTIONS_ID_TOKEN_REQUEST_URL}&audience=${DATABRICKS_ACCOUNT_ID}" |
            jq -r .value)" || id_token=""
        if [ -n "$id_token" ]; then
            token="$(curl -fsS -X POST "$DBX_HOST/oidc/v1/token" \
                --data-urlencode "client_id=$DATABRICKS_CLIENT_ID" \
                --data-urlencode "subject_token=$id_token" \
                --data-urlencode "subject_token_type=urn:ietf:params:oauth:token-type:jwt" \
                --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:token-exchange" \
                --data-urlencode 'scope=all-apis' |
                jq -r .access_token)" || token=""
            if [ -n "$token" ] && [ "$token" != "null" ]; then
                printf '%s\n' "$token"
                return
            fi
            echo "the OIDC token exchange failed; falling back to the client secret" >&2
        else
            echo "the OIDC token request failed; falling back to the client secret" >&2
        fi
    fi
    require DBX_SAMPLES_SP_CLIENT_ID
    require DBX_SAMPLES_SP_CLIENT_SECRET
    curl -fsS -u "$DBX_SAMPLES_SP_CLIENT_ID:$DBX_SAMPLES_SP_CLIENT_SECRET" \
        -X POST "$DBX_HOST/oidc/v1/token" \
        --data 'grant_type=client_credentials&scope=all-apis' |
        jq -r .access_token
}

run_shape() {
    local dir="$WORK/shape" name status
    mkdir -p "$dir"
    echo "== shape =="
    set -o pipefail
    "$PQB" metastore ls --format json | tee "$dir/catalogs" |
        "$PQB" ratelimit --rate "$RATE" |
        "$PQB" catalog ls --format json | tee "$dir/schemas" |
        "$PQB" ratelimit --rate "$RATE" |
        "$PQB" schema ls --format json | tee "$dir/refs" |
        "$PQB" ratelimit --rate "$RATE" |
        keep_readable_catalog | tee "$dir/readable" |
        "$PQB" credentials check --format json | tee "$dir/checked" |
        "$PQB" credentials get --format json | tee "$dir/vended" |
        "$PQB" table info --format json | tee "$dir/tables" |
        "$PQB" table ls --format json | tee "$dir/partitions" |
        "$PQB" partition ls --format json | tee "$dir/files" |
        "$PQB" bytemass --format json >"$dir/bytemass"
    # `run_mode` runs in a condition, where the shell ignores `set -e`; return
    # the walk's status so the retry in `main` sees a failed walk.
    status=$?
    [ "$status" -eq 0 ] || return "$status"
    for name in catalogs schemas refs readable checked vended tables partitions files bytemass; do
        printf '  %-12s %8s records\n' "$name" "$(wc -l <"$dir/$name")"
    done
}

run_stat() {
    if ! command -v perf >/dev/null 2>&1; then
        echo "== perf stat (perf not found; timing one run) ==" >&2
        time "$0" --run >/dev/null
        return
    fi
    echo "== perf stat ($EVENTS, -r $REPEAT) =="
    perf stat -e "$EVENTS" -r "$REPEAT" -- "$0" --run >/dev/null
}

run_record() {
    if ! command -v perf >/dev/null 2>&1; then
        echo "== perf record (perf not found; skipping) ==" >&2
        return
    fi
    echo "== perf record =="
    perf record -g -o "$WORK/perf.data" -- "$0" --run >/dev/null || return $?
    perf report --stdio -i "$WORK/perf.data" --no-children >"$WORK/report.txt" 2>/dev/null || true
    head -n "$TOP" "$WORK/report.txt"
}

run_mode() {
    case "$MODE" in
    shape) run_shape ;;
    stat) run_stat ;;
    record) run_record ;;
    all)
        run_shape || return $?
        run_stat || return $?
        run_record || return $?
        ;;
    *)
        echo "PQB_PERF_MODE must be shape|stat|record|all, got $MODE" >&2
        return 2
        ;;
    esac
}

main() {
    if [ -n "${PQB_ENDPOINT:-}" ] && [ -n "${PQB_TOKEN:-}" ]; then
        :
    else
        require DBX_HOST
        PQB_ENDPOINT="$DBX_HOST/api/2.1/unity-catalog"
        PQB_TOKEN="$(mint_token)"
    fi
    export PQB_ENDPOINT PQB_TOKEN AWS_REGION="$REGION"

    # A live walk shares the endpoint; retry at half the rate before giving up
    # (docs/cli.md: "pace the walk and retry it at a lower rate in the shell").
    local attempt=1
    until PQB_RATE="$RATE" run_mode; do
        if [ "$attempt" -ge "$RETRIES" ] || [ "$RATE" -le 1 ]; then
            echo "the walk failed at rate $RATE after $attempt attempt(s)" >&2
            return 1
        fi
        attempt=$((attempt + 1))
        RATE=$((RATE / 2))
        echo "retrying the walk at rate $RATE" >&2
    done
}

main "$@"
