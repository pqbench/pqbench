#!/usr/bin/env bash
#
# Walk the metadata walk once against the live endpoint.
#
# The walk is the issue-#58 pipeline, one process per level:
#
#   list catalogs | ratelimit --rate 15 | list schemas | ratelimit --rate 15
#     | list tables | ratelimit --rate 15 | credentials check | credentials get
#     | get table | list files | list columns | summarize columns | format --html
#
# The catalog levels fan out — one Unity Catalog request per record — and
# Databricks rate-limits REST calls (the buckets are per endpoint; ~15/s is the
# SDK's own ceiling and is safe — decisions/0006-catalog-rate-limit.md), so a
# `ratelimit --rate` stage paces the refs ahead of each fan-out: `list schemas`
# (one request per catalog), `list tables` (one per schema), and the table stages
# from `credentials check` on (one catalog request per table, including
# `credentials get`'s record read and `get table`'s). The storage levels
# (`list files` down) read object storage, not the catalog, so they take no
# stage of their own. The last two stages fold the measured columns into the
# sample-weighted rows and draw the treemap page, so one pass covers the whole
# `list columns | summarize columns | format --html` pipeline.
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
# `docs/reference/cli.md` gives: "pace the walk and retry it at a lower rate in the
# shell") before it fails.
#
# One pass, with `tee` after each level, so every level's record count is
# visible (the "jobs are limited by the number of tables" claim). Profile it
# with `perf` by hand — `docs/guides/performance.md` has the recipe.
#
# Auth: either PQB_ENDPOINT / PQB_TOKEN, or DBX_HOST plus a bearer source — the
# GitHub Actions OIDC token (`id-token: write`; set DATABRICKS_CLIENT_ID and
# DATABRICKS_ACCOUNT_ID, and the service principal needs a GitHub federation
# policy), or the e2e service principal's client secret
# (DBX_SAMPLES_SP_CLIENT_ID / DBX_SAMPLES_SP_CLIENT_SECRET). Run by the
# `dbx-walk` CI job; the same script runs locally.
#
# Usage:
#
#   scripts/dbx-walk.sh
#   PQB_RATE=5 scripts/dbx-walk.sh
#
# Tunables (environment): PQB, PQB_RATE, PQB_WALK_RETRIES, PQB_WALK_CATALOG,
# PQB_WALK_PAGE, PQB_ENDPOINT, PQB_TOKEN, AWS_REGION.

set -euo pipefail

PQB="${PQB:-target/debug/pqbench}"
RATE="${PQB_RATE:-15}"
RETRIES="${PQB_WALK_RETRIES:-3}"
CATALOG="${PQB_WALK_CATALOG:-pqbench_ext}"
REGION="${AWS_REGION:-us-east-2}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Keep only the table-refs of the catalog the principal may read outside
# compute. The levels above still listed the whole Unity Catalog; this narrows
# the descent so `credentials get` is not refused for the rest.
keep_readable_catalog() {
    jq -c --arg catalog "$CATALOG" \
        'select((.name | startswith("catalogs/\($catalog)/")) and (.name | contains("/tables/")))'
}

# The walk, as `docs/guides/performance.md` writes it: one pipeline, every level
# streaming refs to the next, `tee`d so each level's records can be counted.
walk() {
    local dir="$WORK/walk" name status
    mkdir -p "$dir"
    "$PQB" list catalogs | tee "$dir/catalogs" |
        "$PQB" ratelimit --rate "$RATE" |
        "$PQB" list schemas | tee "$dir/schemas" |
        "$PQB" ratelimit --rate "$RATE" |
        "$PQB" list tables | tee "$dir/refs" |
        "$PQB" ratelimit --rate "$RATE" |
        keep_readable_catalog | tee "$dir/readable" |
        "$PQB" credentials check | tee "$dir/checked" |
        "$PQB" credentials get | tee "$dir/vended" |
        "$PQB" get table | tee "$dir/tables" |
        "$PQB" list files | tee "$dir/files" |
        "$PQB" list columns | tee "$dir/columns" |
        "$PQB" summarize columns | tee "$dir/summary" |
        "$PQB" format --html >"$dir/page.html"
    # `walk` runs in a condition, where the shell ignores `set -e`; return the
    # walk's status so the retry in `main` sees a failed walk.
    status=$?
    [ "$status" -eq 0 ] || return "$status"
    grep -q '^<!DOCTYPE html>' "$dir/page.html" || {
        echo "the walk wrote no HTML page" >&2
        return 1
    }
    for name in catalogs schemas refs readable checked vended tables files columns summary; do
        printf '  %-12s %8s records\n' "$name" "$(wc -l <"$dir/$name")"
    done
    printf '  %-12s %8s bytes\n' page "$(wc -c <"$dir/page.html")"
    if [ -n "${PQB_WALK_PAGE:-}" ]; then
        cp "$dir/page.html" "$PQB_WALK_PAGE"
        echo "page: $PQB_WALK_PAGE"
    fi
}

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
    # (docs/reference/cli.md: "pace the walk and retry it at a lower rate in the shell").
    local attempt=1
    until walk; do
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
