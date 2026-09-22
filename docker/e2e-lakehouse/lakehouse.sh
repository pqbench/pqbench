#!/usr/bin/env bash
# The local stand behind `make lakehouse`: object storage holding a Delta table,
# and a Unity Catalog that vends expiring credentials for it. Each verb is also
# useful on its own; see docker/e2e-lakehouse/README.md.
set -euo pipefail
root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$root"
CARGO=${CARGO:-cargo}
compose() { docker compose -f "$root/docker/e2e-lakehouse/compose.yaml" "$@"; }
aws_cli() { compose run --rm -T aws "$@"; }
unity="http://localhost:${UNITY_CATALOG_PORT:-8080}/api/2.1/unity-catalog"
storage="http://localhost:${RUSTFS_PORT:-9000}"
location="s3://lakehouse/unity/events"
vended="local/lakehouse/vended.env"

# Create, or accept that a previous run already did.
register() {
    local response
    response=$(curl -sS -X POST "$unity/$1" -H 'Content-Type: application/json' -d "$2")
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
    [ -n "$(find "$vended" -mmin -660 2> /dev/null)" ] && return
    local key secret token
    read -r key secret token < <(aws_cli sts assume-role \
        --role-arn arn:aws:iam::000000000000:role/pqbench-read \
        --role-session-name pqbench-stand --duration-seconds 43200 \
        --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' --output text)
    mkdir -p "$(dirname "$vended")"
    cat > "$vended" <<EOF
VENDED_ACCESS_KEY_ID=$key
VENDED_SECRET_ACCESS_KEY=$secret
VENDED_SESSION_TOKEN=$token
EOF
}

up() {
    $CARGO build -p pqbench-cli --features delta-s3
    # Storage first: Unity starts with a credential rustfs has to mint.
    compose up -d --wait rustfs
    mint_credential
    set -a
    . "$vended"
    set +a
    compose up -d --wait unity-catalog
    # A started JVM is not an available API.
    for attempt in $(seq 60); do
        curl -fsS "$unity/catalogs" -o /dev/null 2> /dev/null && return
        [ "$attempt" -lt 60 ] || break
        sleep 2
    done
    echo "Unity Catalog did not answer at $unity" >&2
    exit 1
}

seed_s3() {
    aws_cli s3api head-bucket --bucket lakehouse 2> /dev/null ||
        aws_cli s3api create-bucket --bucket lakehouse > /dev/null
    aws_cli s3 sync --delete /table "$location" > /dev/null
}

seed_unity() {
    register catalogs '{"name": "pqbench"}'
    register schemas '{"catalog_name": "pqbench", "name": "demo"}'
    # Unity cannot migrate a table definition, so replace it. The table is
    # EXTERNAL: dropping it leaves the objects alone.
    curl -sS -X DELETE "$unity/tables/pqbench.demo.events" -o /dev/null
    register tables '{
        "catalog_name": "pqbench", "schema_name": "demo", "name": "events",
        "table_type": "EXTERNAL", "data_source_format": "DELTA",
        "storage_location": "'"$location"'",
        "columns": [
            {"name": "id", "type_name": "LONG", "type_text": "long", "position": 0,
             "nullable": true,
             "type_json": "{\"name\": \"id\", \"type\": \"long\", \"nullable\": true, \"metadata\": {}}"},
            {"name": "label", "type_name": "STRING", "type_text": "string", "position": 1,
             "nullable": true,
             "type_json": "{\"name\": \"label\", \"type\": \"string\", \"nullable\": true, \"metadata\": {}}"}]}'
}

# The README's pipe, so the stand is seen to answer the question it exists for.
check() {
    curl -sS -X POST "$unity/temporary-table-credentials" \
            -H 'Content-Type: application/json' \
            -d "$(curl -sS "$unity/tables/pqbench.demo.events" |
                jq -c '{table_id, operation: "READ"}')" |
        jq -c --arg s3 "$storage" '{kind: "pqbench.remote-source", version: 1,
            inputs: ["'"$location"'"],
            env: (.aws_temp_credentials | {AWS_ACCESS_KEY_ID: .access_key_id,
                AWS_SECRET_ACCESS_KEY: .secret_access_key,
                AWS_SESSION_TOKEN: .session_token, AWS_REGION: "us-east-1",
                AWS_ENDPOINT: $s3, AWS_ENDPOINT_URL: $s3, AWS_ALLOW_HTTP: "true",
                AWS_VIRTUAL_HOSTED_STYLE_REQUEST: "false"})}' |
        target/debug/pqbench table |
        target/debug/pqbench bytemass --json |
        jq -e '.num_rows == 3 and .file_count == 1
            and ([.columns[].path] | sort) == ["id", "label"]' > /dev/null
    echo "Unity Catalog ready: $unity/tables/pqbench.demo.events (storage $storage)"
}

case "${1:-}" in
    up) up ;;
    seed-s3) seed_s3 ;;
    seed-unity) seed_unity ;;
    check) check ;;
    *) echo "usage: ${0##*/} up|seed-s3|seed-unity|check" >&2; exit 64 ;;
esac
