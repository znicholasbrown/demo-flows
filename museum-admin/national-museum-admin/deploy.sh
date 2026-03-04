#!/usr/bin/env bash
set -euo pipefail

PREFECT_PROFILE=national-museum-admin prefect deploy --all
