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
    [ -x "$pqbench_bin" ] || $CARGO build -p pqbench-cli --features delta-s3,unity,iceberg-s3
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

storage_env() {
    jq -n --arg s3 "$s3_endpoint" '{
        AWS_ACCESS_KEY_ID: "test", AWS_SECRET_ACCESS_KEY: "test",
        AWS_REGION: "us-east-1", AWS_ENDPOINT: $s3, AWS_ENDPOINT_URL: $s3,
        AWS_ALLOW_HTTP: "true", AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false"}'
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
    local tmp code
    tmp=$(mktemp)
    if [ -n "$body" ]; then
        code=$(curl -sS -o "$tmp" -w '%{http_code}' -X "$method" "$iceberg_rest$path" \
            -H 'Content-Type: application/json' -d "$body")
    else
        code=$(curl -sS -o "$tmp" -w '%{http_code}' -X "$method" "$iceberg_rest$path")
    fi
    case "$method:$code" in
        POST:200 | POST:201 | POST:409 | DELETE:200 | DELETE:204 | DELETE:404)
            cat "$tmp"
            rm -f "$tmp"
            ;;
        *)
            echo "$method $path failed: HTTP $code $(cat "$tmp")" >&2
            rm -f "$tmp"
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

# The README's shape: rows, file count, and column names from the bytemass end.
measurement_shape() {
    jq -rs '
        (map(select(.event == "end")) | first) as $end
        | ([.[] | select(.kind == "pqbench.bytemass-row") | .column] | sort) as $columns
        | "\($end.row_count) rows, \($end.file_count) file(s), columns [\($columns | join(", "))]"'
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

    # Unity vends a temporary credential as a pqbench.remote-source.
    measurement=$(curl -sS -X POST "$unity_catalog/temporary-table-credentials" \
            -H 'Content-Type: application/json' \
            -d "$(curl -sS "$unity_catalog/tables/pqbench.demo.events" |
                jq -c '{table_id, operation: "READ"}')" |
        jq -c --arg s3 "$s3_endpoint" --arg table "$table_location" \
            '{kind: "pqbench.remote-source", version: 1, inputs: [$table],
            env: (.aws_temp_credentials | {AWS_ACCESS_KEY_ID: .access_key_id,
                AWS_SECRET_ACCESS_KEY: .secret_access_key,
                AWS_SESSION_TOKEN: .session_token, AWS_REGION: "us-east-1",
                AWS_ENDPOINT: $s3, AWS_ENDPOINT_URL: $s3, AWS_ALLOW_HTTP: "true",
                AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false"})}' |
        "$pqbench_bin" table |
        "$pqbench_bin" bytemass --json) || {
        echo "check failed: the table pipe produced no measurement" >&2
        exit 1
    }
    measured=$(expect_events "unity table" "$measurement")

    # `lake` lists the same table from Unity, then the same table | bytemass pipe.
    local lake_source="local/lakehouse/lake-source.json"
    jq -nc --arg endpoint "$unity_catalog" --arg s3 "$s3_endpoint" \
        --arg key "$VENDED_ACCESS_KEY_ID" --arg secret "$VENDED_SECRET_ACCESS_KEY" \
        --arg token "$VENDED_SESSION_TOKEN" \
        '{kind: "pqbench.lake-source", version: 1, endpoint: $endpoint,
        env: {AWS_ACCESS_KEY_ID: $key, AWS_SECRET_ACCESS_KEY: $secret,
            AWS_SESSION_TOKEN: $token, AWS_REGION: "us-east-1",
            AWS_ENDPOINT: $s3, AWS_ENDPOINT_URL: $s3, AWS_ALLOW_HTTP: "true",
            AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false"}}' > "$lake_source"
    measurement=$("$pqbench_bin" lake "$lake_source" --include pqbench.demo.events |
        "$pqbench_bin" table |
        "$pqbench_bin" bytemass --json) || {
        echo "check failed: the lake pipe produced no measurement" >&2
        exit 1
    }
    measured=$(expect_events "unity lake" "$measurement")

    echo "Unity Catalog ready: $unity_catalog/tables/pqbench.demo.events (storage $s3_endpoint): $measured"
}

check_iceberg() {
    ensure_pqbench
    local metadata measurement measured
    metadata=$(curl -sS "$iceberg_rest/v1/namespaces/demo/tables/events" |
        jq -er '."metadata-location" // .metadata."metadata-location"')
    measurement=$(jq -c -n --arg metadata "$metadata" --argjson env "$(storage_env)" \
        '{kind: "pqbench.remote-source", version: 1, inputs: [$metadata], env: $env}' |
        "$pqbench_bin" table |
        "$pqbench_bin" bytemass --json) || {
        echo "check failed: the Iceberg table pipe produced no measurement" >&2
        exit 1
    }
    measured=$(expect_events "iceberg table" "$measurement")
    echo "Iceberg REST ready: $iceberg_rest/v1/namespaces/demo/tables/events: $measured"
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
    *) echo "usage: ${0##*/} up|seed-s3|seed-unity|seed-iceberg|check" >&2; exit 64 ;;
esac
