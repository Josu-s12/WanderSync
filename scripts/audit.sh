#!/bin/sh
# Auditoría de dependencias (pip-audit) en un contenedor limpio - Linux / macOS
# Uso (desde la raíz del proyecto):  ./scripts/audit.sh
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
docker run --rm -v "$ROOT:/src" -w /src python:3.12-slim \
  sh -c "pip install -q --root-user-action=ignore pip-audit==2.10.1 && python scripts/run_audit.py"
