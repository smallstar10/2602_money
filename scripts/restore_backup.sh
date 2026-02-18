#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <backup-tar.gz>"
  exit 1
fi

BACKUP_FILE="$1"
HOME_DIR="${HOME:-/home/hyeonbin}"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

tar -xzf "$BACKUP_FILE" -C "$TMP_DIR"

for rel in \
  "2602_money/data/money.db" \
  "hotdeal_bot/data/hotdeal.db" \
  "blog_bot/reports/stats.csv" \
  "blog_bot/data/daily_completion_state.json"
do
  src="$TMP_DIR/$rel"
  dst="$HOME_DIR/$rel"
  if [[ -f "$src" ]]; then
    mkdir -p "$(dirname "$dst")"
    cp "$src" "$dst"
    echo "restored: $dst"
  fi
done

echo "restore complete."
