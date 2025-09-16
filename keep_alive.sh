#!/usr/bin/env bash
set -euo pipefail
while true; do
  python3 ratp_status.py --server --archive || true
  sleep 2
done
