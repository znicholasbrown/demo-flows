#!/usr/bin/env bash
set -euo pipefail

PREFECT_PROFILE=national-museum-acquisitions python museum_acquisitions_flow.py
