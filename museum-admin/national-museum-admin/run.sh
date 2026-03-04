#!/usr/bin/env bash
set -euo pipefail

PREFECT_PROFILE=national-museum-admin python museum_operations_flow.py
