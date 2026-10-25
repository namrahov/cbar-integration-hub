#!/usr/bin/env bash
# Creates the Kafka UI groups and accounts in lldap. Idempotent.
#   kafka-admins  : admin           (full rights in Kafka UI)
#   kafka-viewers : viewer          (read-only demo user)
#   lldap_strict_readonly : kafka-ui-bind (service account Kafka UI uses to look up groups)
# Usage: bootstrap-lldap.sh <http url> <file with ADMIN_PASSWORD, BIND_PASSWORD, VIEWER_PASSWORD>
set -euo pipefail

URL="$1"
SECRETS="$2"
sed -i 's/\r$//' "$SECRETS"
# shellcheck disable=SC1090
source "$SECRETS"
rm -f "$SECRETS"

TOKEN="$(curl -sf "$URL/auth/simple/login" -H 'Content-Type: application/json' \
  -d "$(jq -n --arg p "$ADMIN_PASSWORD" '{username: "admin", password: $p}')" | jq -r .token)"

# gql <query> [variables json]
gql() {
  local vars="${2:-null}"
  local out
  out="$(curl -sf "$URL/api/graphql" -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "$(jq -n --arg q "$1" --argjson v "$vars" '{query: $q, variables: $v}')")"
  if jq -e '.errors' <<<"$out" >/dev/null; then
    echo "GraphQL error: $(jq -c .errors <<<"$out")" >&2
    return 1
  fi
  echo "$out"
}

group_id() {
  gql 'query { groups { id displayName } }' | jq -r --arg n "$1" '.data.groups[] | select(.displayName == $n) | .id'
}

ensure_group() {
  if [ -z "$(group_id "$1")" ]; then
    gql 'mutation($n: String!) { createGroup(name: $n) { id } }' "$(jq -n --arg n "$1" '{n: $n}')" >/dev/null
    echo "created group $1"
  fi
}

ensure_user() {  # id display_name
  local exists
  exists="$(gql 'query { users { id } }' | jq -r --arg u "$1" '.data.users[] | select(.id == $u) | .id')"
  if [ -z "$exists" ]; then
    gql 'mutation($u: CreateUserInput!) { createUser(user: $u) { id } }' \
      "$(jq -n --arg id "$1" --arg dn "$2" '{u: {id: $id, email: ($id + "@cbar.lab"), displayName: $dn}}')" >/dev/null
    echo "created user $1"
  fi
}

ensure_member() {  # user group
  local gid members
  gid="$(group_id "$2")"
  members="$(gql 'query($g: Int!) { group(groupId: $g) { users { id } } }' "$(jq -n --argjson g "$gid" '{g: $g}')" \
    | jq -r '.data.group.users[].id')"
  if ! grep -qx "$1" <<<"$members"; then
    gql 'mutation($u: String!, $g: Int!) { addUserToGroup(userId: $u, groupId: $g) { ok } }' \
      "$(jq -n --arg u "$1" --argjson g "$gid" '{u: $u, g: $g}')" >/dev/null
    echo "added $1 to $2"
  fi
}

set_password() {  # user password
  /opt/lldap/lldap_set_password --base-url "$URL" --token "$TOKEN" --username "$1" --password "$2" >/dev/null
}

ensure_group kafka-admins
ensure_group kafka-viewers

ensure_user kafka-ui-bind "Kafka UI service account"
ensure_user viewer "Read-only viewer"

ensure_member admin kafka-admins
ensure_member viewer kafka-viewers
ensure_member kafka-ui-bind lldap_strict_readonly

set_password kafka-ui-bind "$BIND_PASSWORD"
set_password viewer "$VIEWER_PASSWORD"

echo "== groups"
gql 'query { groups { displayName users { id } } }' \
  | jq -r '.data.groups[] | "\(.displayName): \([.users[].id] | join(", "))"'
