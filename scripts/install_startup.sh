#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$HOME/.config/systemd/user"
cp "$ROOT"/systemd/* "$HOME/.config/systemd/user/"
systemctl --user daemon-reload
systemctl --user enable --now 2602-money-hourly.timer
systemctl --user enable --now 2602-money-nightly.timer
systemctl --user enable --now 2602-money-watchdog.timer
systemctl --user enable --now 2602-money-chatcmd.timer
systemctl --user enable --now 2602-money-morning.timer
systemctl --user enable --now 2602-money-evening.timer
systemctl --user enable --now 2602-money-backup.timer
systemctl --user enable --now 2602-money-intraday-status.timer
systemctl --user restart 2602-money-hourly.timer
systemctl --user restart 2602-money-nightly.timer
systemctl --user restart 2602-money-watchdog.timer
systemctl --user restart 2602-money-chatcmd.timer
systemctl --user restart 2602-money-morning.timer
systemctl --user restart 2602-money-evening.timer
systemctl --user restart 2602-money-backup.timer
systemctl --user restart 2602-money-intraday-status.timer
systemctl --user status 2602-money-hourly.timer --no-pager -n 3 || true
systemctl --user status 2602-money-nightly.timer --no-pager -n 3 || true
systemctl --user status 2602-money-watchdog.timer --no-pager -n 3 || true
systemctl --user status 2602-money-chatcmd.timer --no-pager -n 3 || true
systemctl --user status 2602-money-morning.timer --no-pager -n 3 || true
systemctl --user status 2602-money-evening.timer --no-pager -n 3 || true
systemctl --user status 2602-money-backup.timer --no-pager -n 3 || true
systemctl --user status 2602-money-intraday-status.timer --no-pager -n 3 || true
