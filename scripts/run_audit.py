"""Auditoría de la cadena de suministro (Supply Chain Security) con pip-audit.

Se ejecuta dentro de un contenedor limpio (ver scripts/audit.ps1 o scripts/audit.sh).
Para cada servicio resuelve TODAS sus dependencias (directas y transitivas), las cruza
contra la base de vulnerabilidades de PyPI/OSV y genera:
  - docs/security/pip-audit/<servicio>.json   (salida cruda de pip-audit)
  - docs/security/REPORTE_AUDITORIA_DEPENDENCIAS.md   (resumen formal)
"""
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "docs" / "security" / "pip-audit"
SERVICES = {
    "flights": "services/flights/requirements.txt",
    "hotels": "services/hotels/requirements.txt",
    "cars": "services/cars/requirements.txt",
    "orders": "services/orders/requirements.txt",
    "gateway": "gateway/requirements.txt",
    "mock-source": "mock-source/requirements.txt",
    "ingestion": "ingestion/requirements.txt",
}
# Control positivo: una versión antigua y vulnerable para demostrar que la herramienta detecta fallos
CONTROL = "requests==2.25.0"


def audit(requirements: Path, output: Path) -> dict:
    subprocess.run(["pip-audit", "-r", str(requirements), "--format", "json", "--output", str(output),
                    "--progress-spinner", "off"], check=False)
    data = json.loads(output.read_text())
    # pip-audit puede listar el mismo paquete/vulnerabilidad más de una vez: se deduplica
    unique = {}
    for dep in data["dependencies"]:
        entry = unique.setdefault((dep["name"].lower(), dep.get("version")), {**dep, "vulns": []})
        seen = {v["id"] for v in entry["vulns"]}
        for vuln in dep.get("vulns", []):
            if vuln["id"] not in seen:
                seen.add(vuln["id"])
                entry["vulns"].append(vuln)
    data["dependencies"] = list(unique.values())
    return data


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    version = subprocess.run(["pip-audit", "--version"], capture_output=True, text=True).stdout.strip()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    results = {}
    for name, req in SERVICES.items():
        print(f"Auditando {name} ({req})...", flush=True)
        results[name] = audit(ROOT / req, OUT / f"{name}.json")

    control_req = OUT / "control-positivo-requirements.txt"
    control_req.write_text(CONTROL + "\n")
    control = audit(control_req, OUT / "control-positivo.json")

    total_vulns = 0
    lines = [
        "# Reporte de auditoría de dependencias (Supply Chain Security)",
        "",
        f"- **Herramienta:** `{version}` (base de datos de vulnerabilidades de PyPI / OSV)",
        f"- **Fecha de ejecución:** {now}",
        "- **Alcance:** dependencias directas **y transitivas** de cada microservicio, resueltas desde su `requirements.txt`",
        "- **Cómo reproducir:** `./scripts/audit.ps1` (Windows) o `./scripts/audit.sh` (Linux/macOS)",
        "",
        "## Resumen por servicio",
        "",
        "| Servicio | Archivo | Paquetes auditados | Vulnerabilidades | Resultado |",
        "|---|---|---|---|---|",
    ]
    for name, data in results.items():
        deps = data["dependencies"]
        vulns = sum(len(d.get("vulns", [])) for d in deps)
        total_vulns += vulns
        verdict = "✅ Sin vulnerabilidades conocidas" if vulns == 0 else "❌ Requiere actualización"
        lines.append(f"| {name} | `{SERVICES[name]}` | {len(deps)} | {vulns} | {verdict} |")

    lines += ["", f"**Total de vulnerabilidades en el proyecto: {total_vulns}**", ""]

    vulnerable = [(n, d, v) for n, data in results.items() for d in data["dependencies"] for v in d.get("vulns", [])]
    if vulnerable:
        lines += ["## Hallazgos", "", "| Servicio | Paquete | Versión | ID | Corregido en |", "|---|---|---|---|---|"]
        for n, d, v in vulnerable:
            lines.append(f"| {n} | {d['name']} | {d['version']} | {v['id']} | {', '.join(v.get('fix_versions', [])) or '—'} |")
        lines.append("")

    control_vulns = [(d, v) for d in control["dependencies"] for v in d.get("vulns", [])]
    lines += [
        "## Control positivo (verificación de la herramienta)",
        "",
        f"Para demostrar que la auditoría realmente detecta riesgos, se auditó a propósito `{CONTROL}`, "
        f"una versión antigua. Resultado: **{len(control_vulns)} vulnerabilidad(es) detectada(s)**.",
        "",
        "| Paquete | Versión | ID | Alias | Corregido en |",
        "|---|---|---|---|---|",
    ]
    for d, v in control_vulns:
        lines.append(f"| {d['name']} | {d['version']} | {v['id']} | {', '.join(v.get('aliases', [])) or '—'} | "
                     f"{', '.join(v.get('fix_versions', [])) or '—'} |")

    lines += [
        "",
        "## Medidas de seguridad de la cadena de suministro aplicadas",
        "",
        "1. **Versiones fijadas (`==`)** en todos los `requirements.txt`: builds reproducibles, sin actualizaciones silenciosas.",
        "2. **Auditoría automatizada** con `pip-audit` sobre el árbol completo de dependencias (incluye transitivas).",
        "3. **Imágenes base oficiales y mínimas** (`python:3.12-slim`, `nginx-unprivileged:alpine`) con etiqueta fija.",
        "4. **Contenedores sin root** (`USER appuser` / nginx-unprivileged): limita el impacto de una RCE en una librería.",
        "5. **Superficie mínima de red**: solo el frontend, Prefect UI, el dashboard de Dask y la fuente mock se publican al host.",
        "6. **Procedimiento ante hallazgos**: subir a la versión indicada en *Corregido en*, reconstruir (`docker compose up -d --build`) "
        "y volver a ejecutar la auditoría hasta obtener 0 vulnerabilidades.",
        "",
        "## Detalle de paquetes auditados",
        "",
    ]
    for name, data in results.items():
        lines += [f"<details><summary><b>{name}</b> ({len(data['dependencies'])} paquetes)</summary>", "",
                  "| Paquete | Versión | Vulnerabilidades |", "|---|---|---|"]
        for d in sorted(data["dependencies"], key=lambda x: x["name"].lower()):
            lines.append(f"| {d['name']} | {d['version']} | {len(d.get('vulns', []))} |")
        lines += ["", "</details>", ""]

    report = ROOT / "docs" / "security" / "REPORTE_AUDITORIA_DEPENDENCIAS.md"
    report.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nReporte generado: {report.relative_to(ROOT)}")
    print(f"Vulnerabilidades en el proyecto: {total_vulns} | Control positivo: {len(control_vulns)} detectada(s)")
    return 1 if total_vulns else 0


if __name__ == "__main__":
    sys.exit(main())
