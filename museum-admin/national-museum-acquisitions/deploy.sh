#!/usr/bin/env bash
set -euo pipefail

PREFECT_PROFILE=national-museum-acquisitions prefect deploy --all
