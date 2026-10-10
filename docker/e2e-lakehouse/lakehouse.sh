#!/usr/bin/env bash
# The local stand behind `make lakehouse`: object storage holding Delta and
# Iceberg tables, Unity Catalog for Delta, and Iceberg REST for Iceberg. Each
# verb is also useful on its own; see docker/e2e-lakehouse/README.md.
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
CARGO=${CARGO:-cargo}
run_compose() { docker compose -f "$root/docker/e2e-lakehouse/compose.yaml" "$@"; }
run_aws_cli() { run_compose run --rm -T aws-cli "$@"; }
unity_catalog="http://localhost:${UNITY_CATALOG_PORT:-8080}/api/2.1/unity-catalog"
iceberg_rest="http://localhost:${ICEBERG_REST_PORT:-8181}"
s3_endpoint="http://localhost:${RUSTFS_PORT:-9000}"
table_location="s3://lakehouse/unity/events"
iceberg_location="s3://lakehouse/iceberg"
vended_env="local/lakehouse/vended.env"
iceberg_meta="$root/docker/e2e-lakehouse/iceberg/metadata-location"
pqbench_bin="${CARGO_TARGET_DIR:-$root/target}/debug/pqbench"

ensure_pqbench() {
    [ -x "$pqbench_bin" ] || $CARGO build -p pqbench-cli --features delta-s3,iceberg-s3
}

# Create, or accept that a previous run already did.
register() {
    local resource=$1 record=$2 response
    response=$(curl -sS -X POST "$unity_catalog/$resource" \
        -H 'Content-Type: application/json' -d "$record")
    case "$(jq -r '.error_code // empty' <<< "$response")" in
        "" | *_ALREADY_EXISTS) ;;
        *) echo "$response" >&2; exit 1 ;;
    esac
}

# Unity cannot send AssumeRole to an S3-compatible endpoint
# (unitycatalog/unitycatalog#43), so the stand mints the session itself and
# configures Unity to vend exactly this one. Reuse it while it is valid: Unity's
# environment then stays put and Compose has no reason to recreate it.
mint_credential() {
    # Reuse the session for 11 of its 12 hours, then mint a fresh one.
    local credential_lifetime_seconds=43200 credential_reuse_minutes=660
    [ -n "$(find "$vended_env" -mmin "-$credential_reuse_minutes" 2> /dev/null)" ] && return
    local key secret token
    read -r key secret token < <(run_aws_cli sts assume-role \
        --role-arn arn:aws:iam::000000000000:role/pqbench-read \
        --role-session-name pqbench-stand \
        --duration-seconds "$credential_lifetime_seconds" \
        --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text)
    mkdir -p "$(dirname "$vended_env")"
    cat > "$vended_env" <<EOF
VENDED_ACCESS_KEY_ID=$key
VENDED_SECRET_ACCESS_KEY=$secret
VENDED_SESSION_TOKEN=$token
EOF
    chmod 0600 "$vended_env"
}

# A started JVM is not an available API.
wait_http() {
    local url=$1 label=$2
    local attempts=60 poll_interval_seconds=2 attempt
    for attempt in $(seq "$attempts"); do
        curl -fsS "$url" -o /dev/null 2> /dev/null && return
        [ "$attempt" -lt "$attempts" ] || break
        sleep "$poll_interval_seconds"
    done
    echo "$label did not answer at $url" >&2
    exit 1
}

# Set the walk's environment: the catalog endpoint and dialect (PQB_*) and the
# object-store options (AWS_*), all read from the environment. An empty
# endpoint, dialect, or key is unset, so a stage that only talks to the catalog
# (`credentials get`) carries no storage key.
walk_env() {
    local endpoint=$1 dialect=$2 key=$3 secret=$4 token=${5:-}
    if [ -n "$endpoint" ]; then export PQB_ENDPOINT="$endpoint"; else unset PQB_ENDPOINT; fi
    if [ -n "$dialect" ]; then export PQB_TABLE_FORMAT="$dialect"; else unset PQB_TABLE_FORMAT; fi
    export AWS_REGION=us-east-1
    export AWS_ENDPOINT="$s3_endpoint" AWS_ENDPOINT_URL="$s3_endpoint"
    export AWS_ALLOW_HTTP=true AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
    if [ -n "$key" ]; then export AWS_ACCESS_KEY_ID="$key"; else unset AWS_ACCESS_KEY_ID; fi
    if [ -n "$secret" ]; then export AWS_SECRET_ACCESS_KEY="$secret"; else unset AWS_SECRET_ACCESS_KEY; fi
    if [ -n "$token" ]; then export AWS_SESSION_TOKEN="$token"; else unset AWS_SESSION_TOKEN; fi
}

up() {
    # Storage first: Unity starts with a credential rustfs has to mint.
    run_compose up -d --wait rustfs
    mint_credential
    set -a
    . "$vended_env"
    set +a
    run_compose up -d --wait unity-catalog iceberg-rest
    wait_http "$unity_catalog/catalogs" "Unity Catalog"
    wait_http "$iceberg_rest/v1/config" "Iceberg REST"
}

seed_s3() {
    run_aws_cli s3api head-bucket --bucket lakehouse 2> /dev/null ||
        run_aws_cli s3api create-bucket --bucket lakehouse > /dev/null
    run_aws_cli s3 sync --delete /table "$table_location" > /dev/null
}

seed_unity() {
    register catalogs '{"name": "pqbench"}'
    register schemas '{"catalog_name": "pqbench", "name": "demo"}'
    # Unity cannot migrate a table definition, so replace it. The table is
    # EXTERNAL: dropping it leaves the objects alone.
    delete_status=$(curl -sS -o /dev/null -w '%{http_code}' -X DELETE "$unity_catalog/tables/pqbench.demo.events")
    case "$delete_status" in
        200 | 204 | 404) ;;
        *) echo "DELETE tables/pqbench.demo.events failed: HTTP $delete_status" >&2; exit 1 ;;
    esac
    register tables "$(jq -nc --arg location "$table_location" '{
        catalog_name: "pqbench", schema_name: "demo", name: "events",
        table_type: "EXTERNAL", data_source_format: "DELTA",
        storage_location: $location,
        columns: [
            {name: "id", type_name: "LONG", type_text: "long", position: 0,
             nullable: true,
             type_json: ({name: "id", type: "long", nullable: true, metadata: {}} | tojson)},
            {name: "label", type_name: "STRING", type_text: "string", position: 1,
             nullable: true,
             type_json: ({name: "label", type: "string", nullable: true, metadata: {}} | tojson)}]}')"
}

# Accept 409 (already exists) and 404 (nothing to delete). Any other status
# is a down or 5xx catalog and must fail.
iceberg_http() {
    local method=$1 path=$2 body=${3:-}
    local response code
    if [ -n "$body" ]; then
        response=$(curl -sS -w '\n%{http_code}' -X "$method" "$iceberg_rest$path" \
            -H 'Content-Type: application/json' -d "$body")
    else
        response=$(curl -sS -w '\n%{http_code}' -X "$method" "$iceberg_rest$path")
    fi
    code=${response##*$'\n'}
    response=${response%$'\n'*}
    case "$method:$code" in
        POST:200 | POST:201 | POST:409 | DELETE:200 | DELETE:204 | DELETE:404)
            printf '%s' "$response"
            ;;
        *)
            echo "$method $path failed: HTTP $code $response" >&2
            exit 1
            ;;
    esac
}

seed_iceberg() {
    run_aws_cli s3 sync --delete /iceberg/demo "$iceberg_location/demo" > /dev/null
    iceberg_http POST /v1/namespaces '{"namespace":["demo"]}' > /dev/null
    iceberg_http DELETE /v1/namespaces/demo/tables/events > /dev/null
    local metadata response
    metadata=$(tr -d '\n' < "$iceberg_meta")
    response=$(iceberg_http POST /v1/namespaces/demo/register \
        "{\"name\":\"events\",\"metadata-location\":\"$metadata\"}")
    if ! jq -e '."metadata-location" // .metadata.location' <<< "$response" > /dev/null; then
        echo "$response" >&2
        exit 1
    fi
}

# The README's shape: rows, file count, and column names from the bytemass stream.
measurement_shape() {
    jq -rs '
        ([.[] | select(.kind == "pqbench.bytemass-file")] | length) as $files
        | ([.[] | select(.kind == "pqbench.bytemass-row") | .row_count] | first) as $rows
        | ([.[] | select(.kind == "pqbench.bytemass-row") | .column] | sort) as $columns
        | "\($rows) rows, \($files) file(s), columns [\($columns | join(", "))]"'
}

expect_events() {
    local label=$1 measurement=$2
    local expected="3 rows, 1 file(s), columns [id, label]"
    local measured
    measured=$(measurement_shape <<< "$measurement")
    [ "$measured" = "$expected" ] || {
        echo "check failed ($label): expected $expected; measured $measured" >&2
        exit 1
    }
    echo "$measured"
}

check_unity() {
    ensure_pqbench
    set -a
    . "$vended_env"
    set +a
    local measurement measured

    # Unity vends a temporary credential; the walk reads the table's storage
    # under it, with no catalog endpoint (the table is named by URI).
    local vended_key vended_secret vended_token
    read -r vended_key vended_secret vended_token < <(
        curl -sS -X POST "$unity_catalog/temporary-table-credentials" \
            -H 'Content-Type: application/json' \
            -d "$(curl -sS "$unity_catalog/tables/pqbench.demo.events" |
                jq -c '{table_id, operation: "READ"}')" |
        jq -r '.aws_temp_credentials | .access_key_id, .secret_access_key, .session_token')
    walk_env "" "" "$vended_key" "$vended_secret" "$vended_token"
    measurement=$("$pqbench_bin" table ls "$table_location" |
        "$pqbench_bin" partition ls |
        "$pqbench_bin" bytemass --json) || {
        echo "check failed: the table pipe produced no measurement" >&2
        exit 1
    }
    measured=$(expect_events "unity table" "$measurement")

    # `schema ls` lists the same table from Unity; the walk reads it under the
    # lease the environment carries, and measures the files the window added.
    walk_env "$unity_catalog" "" "$VENDED_ACCESS_KEY_ID" "$VENDED_SECRET_ACCESS_KEY" "$VENDED_SESSION_TOKEN"
    measurement=$("$pqbench_bin" schema ls pqbench.demo --format json |
        "$pqbench_bin" table info |
        "$pqbench_bin" table ls |
        "$pqbench_bin" partition ls |
        "$pqbench_bin" bytemass --json) || {
        echo "check failed: the lake pipe produced no measurement" >&2
        exit 1
    }
    measured=$(expect_events "unity lake" "$measurement")

    # `catalog ls` lists Unity's schemas for the same catalog.
    local schemas
    schemas=$("$pqbench_bin" catalog ls pqbench --format json |
        jq -r 'select(.kind == "pqbench.schema") | .name') || {
        echo "check failed: catalog ls produced no schemas" >&2
        exit 1
    }
    [ "$schemas" = "demo" ] || {
        echo "check failed (unity catalog ls): expected demo; measured ${schemas:-nothing}" >&2
        exit 1
    }

    # `schema ls` lists Unity's tables for the same schema.
    local tables
    tables=$("$pqbench_bin" schema ls pqbench.demo --format json |
        jq -r 'select(.kind == "pqbench.table-ref") | .id + " " + (.storage_path // "-")') || {
        echo "check failed: schema ls produced no tables" >&2
        exit 1
    }
    [ "$tables" = "pqbench.demo.events $table_location" ] || {
        echo "check failed (unity schema ls): expected pqbench.demo.events $table_location; measured ${tables:-nothing}" >&2
        exit 1
    }

    # `table info` reads the record without files: the Unity record plus the
    # Delta snapshot metadata, as an enriched `pqbench.table-ref` v2.
    local table_info
    table_info=$("$pqbench_bin" table info pqbench.demo.events --format json |
        jq -r 'select(.kind == "pqbench.table-ref") | "\(.id) \(.format) snapshot=\(.snapshot_version) columns=[\([.columns[].name] | join(","))]"') || {
        echo "check failed: table info produced no table" >&2
        exit 1
    }
    [ "$table_info" = "pqbench.demo.events delta snapshot=0 columns=[id,label]" ] || {
        echo "check failed (unity table info): expected pqbench.demo.events delta snapshot=0 columns=[id,label]; measured ${table_info:-nothing}" >&2
        exit 1
    }

    # `credentials get` materializes the vended session on each ref; with no
    # key in the environment it must come from the catalog. The ref carries the
    # lease (keys + token) only — the endpoint stays in the environment.
    walk_env "$unity_catalog" "" "" ""
    local vended_ref
    vended_ref=$("$pqbench_bin" schema ls pqbench.demo --format json |
        "$pqbench_bin" credentials get --format json |
        jq -r 'select(.kind == "pqbench.table-ref") | "\(.id) key=\(.env.AWS_ACCESS_KEY_ID // "-") session=\(if .env.AWS_SESSION_TOKEN then "set" else "unset" end) endpoint=\(.env.AWS_ENDPOINT // "-")"') || {
        echo "check failed: credentials get produced no ref" >&2
        exit 1
    }
    [ "$vended_ref" = "pqbench.demo.events key=$VENDED_ACCESS_KEY_ID session=set endpoint=-" ] || {
        echo "check failed (unity credentials get): expected pqbench.demo.events key=$VENDED_ACCESS_KEY_ID session=set endpoint=-; measured ${vended_ref:-nothing}" >&2
        exit 1
    }

    local vended_info
    vended_info=$("$pqbench_bin" schema ls pqbench.demo --format json |
        "$pqbench_bin" credentials get --format json |
        "$pqbench_bin" table info --format json |
        jq -r 'select(.kind == "pqbench.table-ref") | "\(.id) \(.format) snapshot=\(.snapshot_version) columns=[\([.columns[].name] | join(","))] env=\(if .env then "set" else "none" end)"') || {
        echo "check failed: the vended table loop produced no record" >&2
        exit 1
    }
    [ "$vended_info" = "pqbench.demo.events delta snapshot=0 columns=[id,label] env=set" ] || {
        echo "check failed (unity vended table info): expected pqbench.demo.events delta snapshot=0 columns=[id,label] env=set; measured ${vended_info:-nothing}" >&2
        exit 1
    }

    echo "Unity Catalog ready: $unity_catalog/tables/pqbench.demo.events (storage $s3_endpoint): $measured; catalog ls pqbench: $schemas; schema ls pqbench.demo: $tables; table info: $table_info; vended table info: $vended_info"
}

check_iceberg() {
    ensure_pqbench
    local measurement measured
    # `schema ls` lists the Iceberg REST namespace; `table info` reads
    # loadTable's inline metadata and fills the storage path (the metadata
    # JSON), then the walk measures the files the window added.
    walk_env "$iceberg_rest/v1" "iceberg" "test" "test"
    measurement=$("$pqbench_bin" schema ls pqbench.demo --format json |
        "$pqbench_bin" table info |
        "$pqbench_bin" table ls |
        "$pqbench_bin" partition ls |
        "$pqbench_bin" bytemass --json) || {
        echo "check failed: the Iceberg lake pipe produced no measurement" >&2
        exit 1
    }
    measured=$(expect_events "iceberg lake" "$measurement")

    # `table info` reads loadTable's inline metadata, without files.
    local table_info
    table_info=$("$pqbench_bin" table info pqbench.demo.events --format json |
        jq -r 'select(.kind == "pqbench.table-ref") | "\(.id) \(.format) columns=[\([.columns[].name] | join(","))] snapshot=\(if .snapshot_version > 0 then "set" else "unset" end)"') || {
        echo "check failed: table info produced no Iceberg table" >&2
        exit 1
    }
    [ "$table_info" = "pqbench.demo.events iceberg columns=[id,label] snapshot=set" ] || {
        echo "check failed (iceberg table info): expected pqbench.demo.events iceberg columns=[id,label] snapshot=set; measured ${table_info:-nothing}" >&2
        exit 1
    }
    echo "Iceberg REST ready: $iceberg_rest/v1/namespaces/demo/tables/events: $measured; table info: $table_info"
}

check() {
    check_unity
    check_iceberg
}

case "${1:-}" in
    up) up ;;
    seed-s3) seed_s3 ;;
    seed-unity) seed_unity ;;
    seed-iceberg) seed_iceberg ;;
    check) check ;;
    check-unity) check_unity ;;
    check-iceberg) check_iceberg ;;
    *) echo "usage: ${0##*/} up|seed-s3|seed-unity|seed-iceberg|check|check-unity|check-iceberg" >&2; exit 64 ;;
esac
