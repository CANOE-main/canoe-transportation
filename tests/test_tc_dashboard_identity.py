"""TC presentation tokens must not invalidate unchanged published observations."""

from hashlib import sha256
from pathlib import Path

import pandas as pd
from pydantic import ValidationError
import pytest

from fetching import assorted_sources as adapter
from utils import load_config_bundle


ROOT = Path(__file__).resolve().parents[1]
SOURCE = "transport_canada_ev_dashboard"
NAVIGATION_TOKEN = b"js-view-dom-id-" + b"a" * 64
ROTATED_NAVIGATION_TOKEN = b"js-view-dom-id-" + b"b" * 64
HTML = (
    '<!DOCTYPE html>\r\n<html lang="en"><head>\r\n'
    '<!-- <script src="/comment.js?abc123"></script> -->\r\n'
    '<link data-note=\'href="/shadow.css?abc123"\' REL="alternate StyleSheet" '
    "href = '/styles/main.css?abc123' />\r\n"
    '<script data-src="/shadow.js?abc123" src="/scripts/main.js?abc123"></script>\r\n'
    '<script>const shadow = \'<script src="/inline.js?abc123">\';</script>\r\n'
    '<link rel="preload" href="/preload.css?abc123">\r\n'
    '<script src="/versioned.js?v=abc123"></script>\r\n'
    '<img src="/image.js?abc123"></head>\r\n'
    f'<body><div class="view-navigation-buttons view-id-navigation_buttons js-view-dom-id-{"a" * 64}">Next</div>'
    '<h2>Électricité: June 2026</h2><p>Total chargers: <strong>40,164</strong></p>'
    '</body></html>\r\n'
).encode("utf-8")
ROTATED = HTML.replace(b"/styles/main.css?abc123", b"/styles/main.css?to37kg").replace(
    b"/scripts/main.js?abc123", b"/scripts/main.js?Z0a9B8"
).replace(NAVIGATION_TOKEN, ROTATED_NAVIGATION_TOKEN)


def request(tmp_path, **changes):
    return adapter.ArtifactRequest.model_validate({
        "source_id": SOURCE,
        "component_id": "public_light_duty_chargers",
        "source_version": "June 2026 reviewed snapshot",
        "url": "https://tc.canada.ca/en/dashboard",
        "cache_path": (tmp_path / "dashboard.html").resolve(),
        "file_type": "html",
        "expected_sha256": sha256(HTML).hexdigest(),
        "expected_bytes": len(HTML),
        "expected_content_sha256": adapter.tc_dashboard_content_sha256(HTML),
        **changes,
    })


class Session:
    def __init__(self, content):
        self.content = content
        self.calls = []

    def get(self, url, **options):
        self.calls.append((url, options))
        content = self.content

        class Response:
            def raise_for_status(self):
                pass

            def iter_content(self, chunk_size):
                assert chunk_size > 0
                yield content[:43]
                yield content[43:]

        return Response()


def test_canonical_identity_removes_only_bounded_asset_queries_and_navigation_suffix():
    canonical = HTML.replace(b"/styles/main.css?abc123", b"/styles/main.css").replace(
        b"/scripts/main.js?abc123", b"/scripts/main.js"
    ).replace(NAVIGATION_TOKEN, b"js-view-dom-id-")
    expected = sha256(canonical).hexdigest()
    assert adapter.tc_dashboard_content_sha256(HTML) == expected
    assert adapter.tc_dashboard_content_sha256(HTML.decode("utf-8")) == expected
    assert adapter.tc_dashboard_content_sha256(ROTATED) == expected


@pytest.mark.parametrize("tag,canonical", [
    (b'<SCRIPT SRC="/a.JS?Ab123Z"></SCRIPT>', b'<SCRIPT SRC="/a.JS"></SCRIPT>'),
    (b"<script\n src='/a.js?Ab123Z'></script>", b"<script\n src='/a.js'></script>"),
    (b'<script src=/a.js?Ab123Z></script>', b'<script src=/a.js></script>'),
    (b'<link href=/a.css?Ab123Z rel=stylesheet>', b'<link href=/a.css rel=stylesheet>'),
    (b'<link href="/a.css?Ab123Z" rel="STYLESHEET"/>', b'<link href="/a.css" rel="STYLESHEET"/>'),
])
def test_attribute_quotes_case_and_line_offsets_preserve_every_other_byte(tag, canonical):
    prefix = "<!DOCTYPE html>\r\n<!-- Électricité -->\r\n".encode("utf-8")
    assert adapter.tc_dashboard_content_sha256(prefix + tag) == sha256(prefix + canonical).hexdigest()


@pytest.mark.parametrize("tag", [
    b'<script src="/a.js?abcdefg"></script>',
    b'<script src="/a.js?abc12"></script>',
    b'<script src="/a.js?abc-12"></script>',
    b'<script src="/a.js?v=abc123"></script>',
    b'<script src="/a.js?abc123&x=1"></script>',
    b'<script src="/a.js?abc123#anchor"></script>',
    b'<script src="/a.js;v=1?abc123"></script>',
    b'<script src="/page?redirect=a.js?abc123"></script>',
    b'<script src="/a.json?abc123"></script>',
    b'<script src="data:text/javascript,a.js?abc123"></script>',
    b'<script src="javascript:a.js?abc123"></script>',
    b'<script src="/a.js?abc123" src="/b.js?abc123"></script>',
    b'<script data-src="/a.js?abc123"></script>',
    b'<link rel="preload" href="/a.css?abc123">',
    b'<link href="/a.css?abc123">',
    b'<img src="/a.js?abc123">',
    b'<!-- <script src="/a.js?abc123"></script> -->',
    b'<script>const text = \'<script src="/a.js?abc123">\';</script>',
])
def test_arbitrary_queries_comments_inline_code_and_non_asset_tags_remain_exact(tag):
    assert adapter.tc_dashboard_content_sha256(tag) == sha256(tag).hexdigest()


@pytest.mark.parametrize("container", ["textarea", "title", "xmp", "iframe", "noembed", "noframes", "noscript"])
def test_literal_asset_examples_in_rcdata_and_rawtext_are_not_real_asset_tags(container):
    html = f'<{container}><script src="/example.js?abc123"></script></{container}>'.encode()
    assert adapter.tc_dashboard_content_sha256(html) == sha256(html).hexdigest()


@pytest.mark.parametrize("classes", [
    b"view-navigation-buttons view-id-navigation_buttons " + NAVIGATION_TOKEN,
    NAVIGATION_TOKEN + b" view-id-navigation_buttons view-navigation-buttons",
    b"view-navigation-buttons\t" + NAVIGATION_TOKEN + b"\nview-id-navigation_buttons other-class",
])
def test_navigation_fingerprint_ignores_only_its_suffix_and_preserves_surrounding_class_bytes(classes):
    html = b'<DIV CLASS="' + classes + b'" data-id="unchanged">Next</DIV>'
    expected = html.replace(NAVIGATION_TOKEN, b"js-view-dom-id-")
    assert adapter.tc_dashboard_content_sha256(html) == sha256(expected).hexdigest()
    assert adapter.tc_dashboard_content_sha256(html.replace(NAVIGATION_TOKEN, ROTATED_NAVIGATION_TOKEN)) == sha256(expected).hexdigest()


@pytest.mark.parametrize("html", [
    b'<span class="view-navigation-buttons view-id-navigation_buttons ' + NAVIGATION_TOKEN + b'">Next</span>',
    b'<div class="view-navigation-buttons ' + NAVIGATION_TOKEN + b'">Next</div>',
    b'<div class="view-id-navigation_buttons ' + NAVIGATION_TOKEN + b'">Next</div>',
    b'<div class="view-navigation-buttonsX view-id-navigation_buttons ' + NAVIGATION_TOKEN + b'">Next</div>',
    b'<div class="view-navigation-buttons view-id-navigation_buttons js-view-dom-id-' + b"a" * 63 + b'">Next</div>',
    b'<div class="view-navigation-buttons view-id-navigation_buttons js-view-dom-id-' + b"a" * 65 + b'">Next</div>',
    b'<div class="view-navigation-buttons view-id-navigation_buttons js-view-dom-id-' + b"A" * 64 + b'">Next</div>',
    b'<div class="view-navigation-buttons view-id-navigation_buttons prefix-' + NAVIGATION_TOKEN + b'">Next</div>',
    b'<div class="view-navigation-buttons view-id-navigation_buttons ' + NAVIGATION_TOKEN + b'-suffix">Next</div>',
    b'<div class="view-navigation-buttons view-id-navigation_buttons" data-token="' + NAVIGATION_TOKEN + b'">Next</div>',
    b'<div class="view-navigation-buttons view-id-navigation_buttons" class="' + NAVIGATION_TOKEN + b'">Next</div>',
    b'<div class="view-navigation-buttons view-id-navigation_buttons ' + NAVIGATION_TOKEN + b" " + ROTATED_NAVIGATION_TOKEN + b'">Next</div>',
])
def test_navigation_fingerprint_normalization_is_restricted_to_exact_nav_div_class_token(html):
    assert adapter.tc_dashboard_content_sha256(html) == sha256(html).hexdigest()


@pytest.mark.parametrize("container", ["comment", "script", "style", "textarea", "title"])
def test_navigation_markup_in_inert_examples_remains_exact(container):
    example = b'<div class="view-navigation-buttons view-id-navigation_buttons ' + NAVIGATION_TOKEN + b'">Next</div>'
    html = b"<!-- " + example + b" -->" if container == "comment" else f"<{container}>".encode() + example + f"</{container}>".encode()
    assert adapter.tc_dashboard_content_sha256(html) == sha256(html).hexdigest()


def test_clean_download_accepts_token_rotation_and_preserves_actual_bytes_and_provenance(tmp_path, caplog):
    selected = request(tmp_path)
    session = Session(ROTATED)
    assert not selected.cache_path.exists()
    with caplog.at_level("INFO"):
        assert adapter.ensure_cached(selected, download=True, session=session) == "downloaded"
    assert selected.cache_path.read_bytes() == ROTATED
    actual = sha256(ROTATED).hexdigest()
    assert adapter.validate_cached_identity(selected) == actual
    provenance = adapter._provenance(selected, actual)
    assert provenance["cache_sha256"] == actual
    assert provenance["preferred_raw_sha256"] == sha256(HTML).hexdigest()
    assert provenance["expected_content_sha256"] == adapter.tc_dashboard_content_sha256(HTML)
    assert provenance["identity_basis"] == "presentation_tokens_only"
    assert provenance["cache_bytes"] == len(ROTATED)
    manifest = adapter._manifest_row(
        selected, checksum=actual, cache_status="downloaded",
        stats=adapter.SelectionStats(1, 1, 0), selection="fixture", output_files=[], warning_count=0,
    )
    assert manifest["sha256"] == actual
    assert manifest["bytes"] == len(ROTATED)
    for key in ("preferred_raw_sha256", "expected_content_sha256", "identity_basis"):
        assert manifest[key] == provenance[key]
    output = tmp_path / "normalized"
    adapter.write_outputs(
        outputs={}, manifest_rows=[manifest], warnings=[], output_dir=output,
        rules={"manifest_file": "manifest.csv", "warnings_file": "warnings.jsonl"},
    )
    persisted = pd.read_csv(output / "manifest.csv").iloc[0]
    assert persisted["sha256"] == actual
    assert persisted["bytes"] == len(ROTATED)
    assert persisted["identity_basis"] == "presentation_tokens_only"
    assert persisted["preferred_raw_sha256"] == sha256(HTML).hexdigest()
    assert persisted["expected_content_sha256"] == adapter.tc_dashboard_content_sha256(HTML)
    assert len(session.calls) == 1
    assert "presentation-token rotation" in caplog.text
    assert not selected.cache_path.with_suffix(".html.part").exists()


@pytest.mark.parametrize("before,after", [
    (b"40,164", b"40,165"),
    (b"June 2026", b"July 2026"),
    (b"/inline.js?abc123", b"/inline.js?ZZ1234"),
    (b"/comment.js?abc123", b"/comment.js?ZZ1234"),
    (b"/preload.css?abc123", b"/preload.css?ZZ1234"),
    (b"/shadow.css?abc123", b"/shadow.css?ZZ1234"),
    (b"/shadow.js?abc123", b"/shadow.js?ZZ1234"),
    (b"/image.js?abc123", b"/image.js?ZZ1234"),
    (b"/versioned.js?v=abc123", b"/versioned.js?v=ZZ1234"),
    (b"<strong>", b"<strong >"),
    (b"view-navigation-buttons", b"view-navigation-buttons-changed"),
    (b"Next</div>", b"Previous</div>"),
    (b"\r\n", b"\n"),
])
def test_changed_values_labels_code_formatting_and_arbitrary_queries_are_rejected(tmp_path, before, after):
    selected = request(tmp_path)
    changed = ROTATED.replace(before, after)
    with pytest.raises(adapter.AssortedSourcesError, match="content SHA-256 changed"):
        adapter.fetch_to_cache(selected, session=Session(changed))
    assert not selected.cache_path.exists()
    assert not selected.cache_path.with_suffix(".html.part").exists()


def test_exact_raw_pin_requires_consistent_content_identity_and_records_exact_provenance(tmp_path):
    selected = request(tmp_path)
    selected.cache_path.write_bytes(HTML)
    actual = adapter.validate_cached_identity(selected)
    assert adapter._provenance(selected, actual)["identity_basis"] == "exact_raw"
    invalid = request(tmp_path, expected_content_sha256="0" * 64)
    with pytest.raises(adapter.AssortedSourcesError, match="content SHA-256 changed"):
        adapter.validate_cached_identity(invalid)


def test_optional_policy_is_loaded_from_source_adapter():
    bundle = load_config_bundle("config/scenarios/default.yaml", repo_root=ROOT)
    bundle.sources.sources[SOURCE].adapter["expected_content_sha256"] = "f" * 64
    selected = adapter._top_level_request(
        bundle, source_id=SOURCE, component_id="public_light_duty_chargers", file_type="html",
    )
    assert selected.expected_content_sha256 == "f" * 64


@pytest.mark.parametrize("changes", [
    {"source_id": "other_source"},
    {"file_type": "csv", "cache_path": Path.cwd() / "fixture.csv"},
    {"expected_content_sha256": "not-a-digest"},
])
def test_content_policy_is_typed_and_restricted_to_tc_html(tmp_path, changes):
    with pytest.raises(ValidationError, match="content SHA-256|Content SHA-256"):
        request(tmp_path, **changes)


def test_non_opted_in_tc_retains_strict_raw_identity(tmp_path):
    selected = request(tmp_path, expected_content_sha256=None)
    with pytest.raises(adapter.AssortedSourcesError, match="SHA-256 changed"):
        adapter.fetch_to_cache(selected, session=Session(ROTATED))


def test_other_sources_keep_their_existing_manifest_schema(tmp_path):
    selected = request(tmp_path, source_id="other_source", expected_content_sha256=None)
    selected.cache_path.write_bytes(HTML)
    manifest = adapter._manifest_row(
        selected, checksum=sha256(HTML).hexdigest(), cache_status="cached",
        stats=adapter.SelectionStats(1, 1, 0), selection="fixture", output_files=[], warning_count=0,
    )
    assert set(manifest) == set(adapter.MANIFEST_COLUMNS)
    output = tmp_path / "normalized"
    adapter.write_outputs(
        outputs={}, manifest_rows=[manifest], warnings=[], output_dir=output,
        rules={"manifest_file": "manifest.csv", "warnings_file": "warnings.jsonl"},
    )
    assert list(pd.read_csv(output / "manifest.csv").columns) == adapter.MANIFEST_COLUMNS


def test_non_utf8_html_is_rejected_without_guessing_an_encoding():
    with pytest.raises(adapter.AssortedSourcesError, match="not UTF-8 HTML"):
        adapter.tc_dashboard_content_sha256(b"<html>\xff</html>")
