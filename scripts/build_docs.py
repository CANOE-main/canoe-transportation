"""Explicit documentation asset setup, offline rendering, preview and PDF checks."""

from __future__ import annotations

import argparse
import base64
import functools
import hashlib
import io
import json
import logging
import subprocess
import sys
import tarfile
import threading
from contextlib import contextmanager
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import PurePosixPath
from urllib.parse import unquote, urlsplit

from mkdocs.config import load_config

if __package__:
    from .docs_hooks import ROOT, artifact_path
else:
    from docs_hooks import ROOT, artifact_path

LOGGER = logging.getLogger("documentation")


def configuration():
    return load_config(config_file=str(ROOT / "mkdocs.yml"))


def prepare_assets():
    """The only documentation command that acquires renderer packages."""
    import requests

    vendor = artifact_path("documentation_vendor")
    vendor.mkdir(parents=True, exist_ok=True)
    packages = configuration().extra["renderer_packages"]
    manifest = {"packages": packages, "files": {}}
    for name, spec in packages.items():
        LOGGER.info("Preparing pinned documentation renderer: %s", name)
        response = requests.get(spec["url"], timeout=90)
        response.raise_for_status()
        data = response.content
        actual = "sha512-" + base64.b64encode(hashlib.sha512(data).digest()).decode("ascii")
        if actual != spec["integrity"]:
            raise ValueError(f"Renderer archive integrity mismatch: {name}")
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
            for member in archive:
                if not member.isfile():
                    continue
                license_file = PurePosixPath(member.name).name.lower().startswith("license")
                if member.name.startswith(spec["prefix"]):
                    relative = member.name.removeprefix(spec["prefix"])
                    # Preserve self-contained ESM chunks and MathJax extension modules;
                    # source maps/types/source trees are not publication assets.
                    if PurePosixPath(relative).suffix not in {".js", ".mjs", ".css", ".woff", ".woff2"}:
                        continue
                elif license_file:
                    relative = PurePosixPath(member.name).name
                else:
                    continue
                target = (vendor / name / relative).resolve()
                if not target.is_relative_to(vendor):
                    raise ValueError("Unsafe renderer archive member")
                contents = archive.extractfile(member).read()
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(contents)
                manifest["files"][target.relative_to(vendor).as_posix()] = hashlib.sha256(contents).hexdigest()
    (vendor / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    LOGGER.info("Prepared %d hashed renderer files in %s", len(manifest["files"]), vendor)


def build():
    subprocess.run([sys.executable, "-m", "mkdocs", "build", "--strict"], cwd=ROOT, check=True)


@contextmanager
def preview_server(port=0):
    config = configuration()
    prefix = urlsplit(config.site_url).path.rstrip("/")
    site = artifact_path("documentation_site")

    class Handler(SimpleHTTPRequestHandler):
        def do_GET(self):
            if not self.path.startswith(prefix + "/"):
                self.send_error(404)
                return
            self.path = self.path[len(prefix):]
            super().do_GET()

        def log_message(self, format, *args):
            if args and str(args[1]) not in {"200", "304"}:
                LOGGER.warning(format, *args)

    server = ThreadingHTTPServer(("127.0.0.1", port), functools.partial(Handler, directory=str(site)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}{prefix}/"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def check_site_links():
    """Check the actual published file set, local assets, and generated anchors."""
    from bs4 import BeautifulSoup

    site = artifact_path("documentation_site")
    prefix = urlsplit(configuration().site_url).path
    failures = []
    html_files = list(site.rglob("*.html"))
    anchors = {}
    for path in html_files:
        soup = BeautifulSoup(path.read_text(encoding="utf-8"), "html.parser")
        anchors[path] = {tag["id"] for tag in soup.find_all(id=True)}
    for path in html_files:
        soup = BeautifulSoup(path.read_text(encoding="utf-8"), "html.parser")
        for tag in soup.find_all(["a", "img", "script", "link"]):
            target = tag.get("href") or tag.get("src")
            if not target:
                continue
            link = urlsplit(target)
            if link.scheme or link.netloc:
                continue
            if link.path.startswith("/"):
                if not link.path.startswith(prefix):
                    failures.append(f"{path.relative_to(site)}: outside project subpath: {target}")
                    continue
                destination = site / unquote(link.path.removeprefix(prefix))
            else:
                destination = path.parent / unquote(link.path) if link.path else path
            destination = destination.resolve()
            if destination.is_dir():
                destination /= "index.html"
            if not destination.is_file():
                failures.append(f"{path.relative_to(site)}: missing {target}")
            elif link.fragment and destination in anchors and unquote(link.fragment) not in anchors[destination]:
                failures.append(f"{path.relative_to(site)}: missing anchor {target}")
    report = {"html_files": len(html_files), "failures": sorted(set(failures))}
    directory = artifact_path("documentation_render_validation")
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "links.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    if failures:
        raise ValueError("Published link check failed:\n" + "\n".join(report["failures"][:30]))
    LOGGER.info("Checked local links and assets in %d HTML files", len(html_files))


def pdf(channel=None):
    from playwright.sync_api import sync_playwright

    check_site_links()
    identity = json.loads((artifact_path("documentation_render_validation") / "build_identity.json").read_text(encoding="utf-8"))
    for page_uri, source_path in identity["source_paths"].items():
        if hashlib.sha256((ROOT / source_path).read_bytes()).hexdigest() != identity["source_sha256"][page_uri]:
            raise ValueError(f"Documentation changed after site build: {source_path}; run build again")
    supporting_inputs = {
        "mkdocs.yml": identity["configuration_sha256"],
        "uv.lock": identity["dependency_lock_sha256"],
        **identity["figure_sha256"],
        **identity["publication_support_sha256"],
    }
    for source_path, expected in supporting_inputs.items():
        if hashlib.sha256((ROOT / source_path).read_bytes()).hexdigest() != expected:
            raise ValueError(f"Publication input changed after site build: {source_path}; run build again")
    output = artifact_path("documentation_report")
    output.parent.mkdir(parents=True, exist_ok=True)
    failures, remote_requests = [], []
    with preview_server() as base, sync_playwright() as playwright:
        options = {"headless": True}
        if channel:
            options["channel"] = channel
        LOGGER.info("Launching headless browser for %sprint_page/", base)
        browser = playwright.chromium.launch(**options)
        browser_version = browser.version
        page = browser.new_page(viewport={"width": 1440, "height": 1000})

        def local_only(route):
            if urlsplit(route.request.url).hostname not in {"127.0.0.1", "localhost"}:
                remote_requests.append(route.request.url)
                route.abort()
            else:
                route.continue_()

        page.route("**/*", local_only)
        def page_error(error):
            failures.append(str(error))
            LOGGER.error("Browser rendering: %s", error)

        page.on("pageerror", page_error)
        page.on("response", lambda response: failures.append(f"HTTP {response.status}: {response.url}") if response.status >= 400 else None)
        page.goto(base + "print_page/", wait_until="networkidle", timeout=120000)
        LOGGER.info("Print page loaded; waiting for equations and diagrams")
        page.wait_for_function("window.canoeRender && (window.canoeRender.ready || window.canoeRender.errors.length)", timeout=180000)
        state = page.evaluate("window.canoeRender")
        failures.extend(state["errors"])
        page.evaluate("async () => { await document.fonts.ready; document.querySelectorAll('details').forEach(el => el.open = true); }")
        broken = page.evaluate("[...document.images].filter(x => !x.complete || !x.naturalWidth).map(x => x.src)")
        failures.extend(broken)
        if failures or remote_requests:
            browser.close()
            raise ValueError(f"Incomplete offline rendering: errors={failures}; external requests={remote_requests}")
        page.emulate_media(media="print")
        page.pdf(path=str(output), print_background=True, prefer_css_page_size=True,
                 display_header_footer=True,
                 header_template="<span></span>",
                 footer_template='<div style="font:8px Arial;width:100%;text-align:center;color:#667">CANOE transportation · <span class="pageNumber"></span> / <span class="totalPages"></span></div>')
        browser.close()
    state.update({"pdf": output.relative_to(ROOT).as_posix(), "external_requests": remote_requests,
                  "site_revision": identity["revision"], "site_built_at": identity["built_at"],
                  "browser_version": browser_version, "browser_channel": channel or "playwright-chromium",
                  "sha256": hashlib.sha256(output.read_bytes()).hexdigest()})
    directory = artifact_path("documentation_render_validation")
    (directory / "render.json").write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
    render_pdf_pages(output, directory)
    LOGGER.info("Complete report: %s", output)


def render_pdf_pages(output, directory):
    import pymupdf

    pages = directory / "pdf_pages"
    pages.mkdir(parents=True, exist_ok=True)
    report = []
    sheets = []
    with pymupdf.open(output) as document:
        for index, page in enumerate(document):
            # Review images are regenerated alongside their exact PDF identity.
            zoom = min(1.7, 1600 / page.rect.width)
            page.get_pixmap(matrix=pymupdf.Matrix(zoom, zoom), alpha=False).save(pages / f"page-{index+1:03}.png")
            outside = sum(not page.rect.contains(pymupdf.Rect(block[:4]))
                          for block in page.get_text("blocks") if block[6] == 0)
            report.append({"page": index+1, "width_pt": page.rect.width, "height_pt": page.rect.height,
                           "text_characters": len(page.get_text().strip()), "outside_page_text_blocks": outside})
        # Small, reproducible overview sheets supplement the full-resolution pages;
        # they are review inputs, not an automated claim of visual correctness.
        for first in range(0, len(document), 16):
            with pymupdf.open() as overview:
                sheet = overview.new_page(width=1200, height=1600)
                for position, source in enumerate(range(first, min(first + 16, len(document)))):
                    column, row = position % 4, position // 4
                    cell = pymupdf.Rect(column*300+8, row*400+8, (column+1)*300-8, (row+1)*400-28)
                    sheet.show_pdf_page(cell, document, source)
                    sheet.insert_text((column*300+12, (row+1)*400-10), f"Page {source+1}", fontsize=12)
                name = f"contact-{first//16+1:02}.png"
                sheet.get_pixmap(alpha=False).save(directory / name)
                sheets.append(name)
    expected_pages = {f"page-{index+1:03}.png" for index in range(len(report))}
    for stale in pages.glob("page-*.png"):
        if stale.name not in expected_pages:
            stale.unlink()
    for stale in directory.glob("contact-*.png"):
        if stale.name not in sheets:
            stale.unlink()
    (directory / "pdf_pages.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    LOGGER.info("Rendered %d PDF pages for visual review at %s", len(report), pages)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["prepare-assets", "build", "serve", "pdf", "check"])
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--browser-channel", choices=["chrome", "msedge"], help="Use an installed browser instead of Playwright's explicitly installed Chromium")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if args.command == "prepare-assets":
        prepare_assets()
    elif args.command == "build":
        build()
        check_site_links()
    elif args.command == "check":
        check_site_links()
    elif args.command == "pdf":
        pdf(args.browser_channel)
    else:
        with preview_server(args.port) as url:
            LOGGER.info("Previewing prepared documentation at %s", url)
            try:
                threading.Event().wait()
            except KeyboardInterrupt:
                pass


if __name__ == "__main__":
    main()
