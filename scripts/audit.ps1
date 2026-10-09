# Auditoría de dependencias (pip-audit) en un contenedor limpio - Windows / PowerShell
# Uso (desde la raíz del proyecto):  .\scripts\audit.ps1
$root = Split-Path -Parent $PSScriptRoot
docker run --rm -v "${root}:/src" -w /src python:3.12-slim sh -c "pip install -q --root-user-action=ignore pip-audit==2.10.1 && python scripts/run_audit.py"
