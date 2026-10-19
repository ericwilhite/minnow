#!/usr/bin/env bash
# Retry interrupted dependency downloads without retrying tests or deterministic install errors.
set -eu

log=$(mktemp)
trap 'rm -f "$log"' EXIT

for attempt in 1 2 3; do
  if npm ci >"$log" 2>&1; then
    cat "$log"
    exit 0
  else
    status=$?
  fi
  cat "$log"
  if [ "$attempt" -eq 3 ] || ! grep -Eq 'npm (error|ERR!) code (ECONNRESET|ETIMEDOUT|EAI_AGAIN|EPIPE|ECONNREFUSED|ENETUNREACH|EHOSTUNREACH|ENOTFOUND|E502|E503|E504)([[:space:]]|$)' "$log"; then
    exit "$status"
  fi
  echo "Dependency download interrupted; retrying npm ci (attempt $((attempt + 1))/3)."
  sleep "$((attempt * 5))"
done
