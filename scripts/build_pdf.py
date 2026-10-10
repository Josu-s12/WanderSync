"""Genera docs/Documento_Tecnico_WanderSync.pdf a partir de docs/ARQUITECTURA.md.

Renderiza el Markdown (marked) y los diagramas (Mermaid) en un navegador Chromium headless
(Edge o Chrome) y lo imprime a PDF. Solo usa la librería estándar de Python.

Uso:  python scripts/build_pdf.py   [--keep-html para conservar el HTML intermedio]
"""
import json
import shutil
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
SOURCE = DOCS / "ARQUITECTURA.md"
OUTPUT = DOCS / "Documento_Tecnico_WanderSync.pdf"
TEMP_HTML = DOCS / "_documento_print.html"

AUTHOR = "Josue Sarmiento"
REPO = "https://github.com/Josu-s12/WanderSync"
MONTHS = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
          "septiembre", "octubre", "noviembre", "diciembre"]

BROWSERS = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    "msedge", "google-chrome", "chromium", "chromium-browser",
]

TEMPLATE = """<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<title>Documento Técnico de Arquitectura - WanderSync</title>
<script src="https://cdn.jsdelivr.net/npm/marked@12.0.2/marked.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10.9.1/dist/mermaid.min.js"></script>
<style>
  @page { size: A4; margin: 18mm 16mm 18mm 16mm; }
  * { box-sizing: border-box; }
  body { font-family: "Segoe UI", Roboto, Arial, sans-serif; font-size: 10.5pt; line-height: 1.5; color: #1c2430; margin: 0; }
  .cover { height: 255mm; display: flex; flex-direction: column; justify-content: center; text-align: center; page-break-after: always; }
  .cover .badge { display: inline-block; margin: 0 auto 18mm; width: 26mm; height: 26mm; border-radius: 6mm; background: #0f766e; color: #fff; font-size: 30pt; line-height: 26mm; }
  .cover h1 { font-size: 26pt; margin: 0 0 4mm; color: #0f766e; border: none; }
  .cover h2 { font-size: 15pt; font-weight: 500; margin: 0 0 22mm; color: #344054; border: none; page-break-before: avoid; }
  .cover table { margin: 0 auto; border-collapse: collapse; font-size: 11pt; }
  .cover td { padding: 2.2mm 5mm; border-bottom: 1px solid #e3e7ec; text-align: left; }
  .cover td:first-child { font-weight: 600; color: #667085; }
  h1 { font-size: 18pt; color: #0f766e; }
  h2 { font-size: 15pt; color: #0f766e; border-bottom: 2px solid #0f766e; padding-bottom: 1.5mm; margin-top: 0; page-break-before: always; }
  h3 { font-size: 12pt; color: #1c2430; margin-top: 6mm; page-break-after: avoid; }
  h2 + *, h3 + * { page-break-before: avoid; }
  table { border-collapse: collapse; width: 100%; margin: 3mm 0 5mm; font-size: 9pt; page-break-inside: auto; }
  tr { page-break-inside: avoid; }
  th { background: #f0f5f4; text-align: left; }
  th, td { border: 1px solid #d0d5dd; padding: 1.6mm 2.2mm; vertical-align: top; }
  code { font-family: Consolas, "Courier New", monospace; font-size: 8.8pt; background: #f2f4f7; padding: 0.3mm 1mm; border-radius: 1mm; }
  pre { background: #f6f8fa; border: 1px solid #e3e7ec; border-radius: 2mm; padding: 3mm; white-space: pre-wrap; word-break: break-word; page-break-inside: avoid; }
  pre code { background: none; padding: 0; }
  pre.mermaid { background: #fff; border: none; text-align: center; padding: 0; margin: 4mm 0; }
  pre.mermaid svg { max-width: 100% !important; height: auto; }
  img { display: block; max-width: 100%; max-height: 120mm; margin: 2mm auto 6mm; border: 1px solid #d0d5dd; border-radius: 2mm; page-break-inside: avoid; }
  blockquote { margin: 3mm 0; padding: 2mm 4mm; border-left: 3px solid #0f766e; background: #f0f5f4; }
  a { color: #0f766e; text-decoration: none; }
  hr { display: none; }
  .toc-skip { display: none; }
</style>
</head>
<body>
<section class="cover">
  <div class="badge">&#9992;</div>
  <h1>Documento Técnico de Arquitectura</h1>
  <h2>WanderSync Travel Solutions<br>Plataforma de Empaquetamiento Turístico Dinámico</h2>
  <table>
    <tr><td>Autor</td><td>__AUTHOR__</td></tr>
    <tr><td>Asignatura</td><td>Patrones Arquitectónicos Avanzados</td></tr>
    <tr><td>Evaluación</td><td>Parcial práctico del segundo corte</td></tr>
    <tr><td>Repositorio</td><td>__REPO__</td></tr>
    <tr><td>Fecha</td><td>__DATE__</td></tr>
  </table>
</section>
<main id="content"></main>
<script>
  const source = __MARKDOWN__;
  document.getElementById("content").innerHTML = marked.parse(source);
  document.querySelectorAll("pre > code.language-mermaid").forEach((code) => {
    const pre = document.createElement("pre");
    pre.className = "mermaid";
    pre.textContent = code.textContent;
    code.parentElement.replaceWith(pre);
  });
  mermaid.initialize({ startOnLoad: false, theme: "neutral", securityLevel: "strict",
                       flowchart: { useMaxWidth: true }, sequence: { useMaxWidth: true } });
  mermaid.run({ querySelector: "pre.mermaid" }).then(() => { document.body.dataset.ready = "1"; });
</script>
</body>
</html>
"""


def find_browser() -> str:
    for candidate in BROWSERS:
        path = shutil.which(candidate) or (candidate if Path(candidate).exists() else None)
        if path:
            return path
    sys.exit("No se encontró Edge ni Chrome para generar el PDF")


def main() -> None:
    markdown = SOURCE.read_text(encoding="utf-8")
    # La portada reemplaza el título y los datos iniciales del Markdown
    markdown = markdown[markdown.index("## Contenido"):]
    today = date.today()
    html = (TEMPLATE
            .replace("__AUTHOR__", AUTHOR)
            .replace("__REPO__", REPO)
            .replace("__DATE__", f"{MONTHS[today.month - 1].capitalize()} de {today.year}")
            .replace("__MARKDOWN__", json.dumps(markdown)))
    TEMP_HTML.write_text(html, encoding="utf-8")
    # Perfil aislado: si el navegador ya está abierto, no le delega el trabajo a esa ventana
    profile = Path(tempfile.mkdtemp(prefix="wandersync-pdf-"))
    before = OUTPUT.stat().st_mtime if OUTPUT.exists() else 0
    try:
        subprocess.run([
            find_browser(), "--headless=new", "--disable-gpu", "--no-pdf-header-footer",
            f"--user-data-dir={profile}",
            "--virtual-time-budget=30000", "--run-all-compositor-stages-before-draw",
            f"--print-to-pdf={OUTPUT}", TEMP_HTML.as_uri(),
        ], check=True, timeout=180)
    finally:
        if "--keep-html" not in sys.argv:
            TEMP_HTML.unlink(missing_ok=True)
        shutil.rmtree(profile, ignore_errors=True)
    if not OUTPUT.exists() or OUTPUT.stat().st_mtime == before:
        sys.exit("El navegador no generó el PDF. En Windows ejecútalo desde PowerShell o CMD (no desde Git Bash).")
    print(f"PDF generado: {OUTPUT.relative_to(ROOT)} ({OUTPUT.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
