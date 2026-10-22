import json

import pytest

from signaldesk.exports import (
    ExportUnavailable,
    attachment_filename,
    export_json,
    export_markdown,
)
from signaldesk.schemas import RunRead


def complete_run():
    return RunRead(
        id="run-123",
        project_id="project-1",
        mode="demo",
        demo_revision=2,
        status="completed",
        stage="completed",
        created_at="2026-10-22T00:00:00+00:00",
        updated_at="2026-10-22T00:01:00+00:00",
        report={
            "title": "Pricing comparison",
            "summary": "The price increased.",
            "mode": "demo",
            "findings": [
                {
                    "title": "Pro price changed",
                    "detail": "$25 → $35",
                    "category": "pricing",
                    "impact": "medium",
                    "citations": [
                        {"evidence_id": "old", "quote": "Pro costs $25."},
                        {"evidence_id": "new", "quote": "Pro costs $35."},
                    ],
                }
            ],
            "recommendations": ["Review the pricing page."],
            "limitations": ["A snapshot, not a release date."],
            "evidence": [
                {
                    "id": "new",
                    "source_name": "Current source",
                    "url": "https://example.com/pricing",
                    "title": "Pricing",
                    "text": "Pro costs $35.",
                    "change_type": "changed",
                    "previous_id": "old",
                    "diff": "-Pro costs $25.\n+Pro costs $35.",
                    "observed_at": "2026-10-22T00:00:00Z",
                    "is_previous": False,
                },
                {
                    "id": "old",
                    "source_name": "Previous source",
                    "url": "https://example.com/pricing",
                    "title": "Old pricing",
                    "text": "Pro costs $25.",
                    "change_type": "baseline",
                    "observed_at": "2026-10-21T00:00:00Z",
                    "is_previous": True,
                },
            ],
        },
    )


def test_markdown_contains_provenance_history_diff_and_metadata():
    markdown = export_markdown(complete_run())
    for expected in (
        "Pricing comparison",
        "演示（demo）",
        "2026",
        "摘要",
        "建议",
        "局限",
        "原文差异",
        "历史快照",
        "$25",
        "$35",
        "https://example.com/pricing",
        "new",
        "old",
    ):
        assert expected in markdown
    assert "[Current source](https://example.com/pricing)" in markdown


def test_external_html_markdown_and_unsafe_urls_are_inert():
    run = complete_run()
    malicious = "<img src=x onerror=alert(1)> [click](javascript:alert(1)) ![pixel](https://evil.example/pixel)"
    run.report["title"] = malicious
    run.report["summary"] = malicious
    run.report["recommendations"] = [malicious]
    run.report["limitations"] = [malicious]
    run.report["findings"][0]["title"] = malicious
    run.report["findings"][0]["detail"] = malicious
    run.report["evidence"][0]["source_name"] = malicious
    run.report["evidence"][0]["url"] = 'javascript:alert("x")'
    run.report["evidence"][0]["diff"] = malicious
    run.report["evidence"][0]["text"] = "Pro costs $35. " + malicious
    run.report["findings"][0]["citations"][1]["quote"] = malicious
    markdown = export_markdown(run)
    assert "<img" not in markdown
    assert "[click](javascript:" not in markdown
    assert "![pixel](https:" not in markdown
    assert "&lt;img" in markdown
    assert "已省略无效或非 HTTP(S) 链接" in markdown


def test_http_links_encode_markdown_delimiters_and_reject_credentials():
    run = complete_run()
    run.report["evidence"][0]["url"] = 'https://example.com/a(b)?q=<img>&x="quote"'
    run.report["evidence"][1]["url"] = "https://user:password@example.com/private"
    markdown = export_markdown(run)
    assert "https://example.com/a%28b%29?q=%3Cimg%3E&x=%22quote%22" in markdown
    assert "user:password" not in markdown


def test_json_preserves_report_and_filename_never_uses_external_titles():
    run = complete_run()
    run.report["title"] = '<script>alert("x")</script>'
    assert json.loads(export_json(run)) == run.model_dump(mode="json")
    assert (
        attachment_filename('../../unsafe\r\nX-Evil: true"', "markdown")
        == "signaldesk-unsafeX-Eviltrue.md"
    )
    assert attachment_filename("", "json") == "signaldesk-report.json"


@pytest.mark.parametrize("status", ["queued", "running", "failed", "interrupted", "cancelled"])
def test_only_completed_runs_can_be_exported(status):
    run = complete_run().model_copy(update={"status": status})
    with pytest.raises(ExportUnavailable):
        export_markdown(run)
    with pytest.raises(ExportUnavailable):
        export_json(run)


def test_missing_or_inconsistent_reports_cannot_be_exported():
    run = complete_run().model_copy(update={"report": {"title": "Incomplete"}})
    with pytest.raises(ExportUnavailable):
        export_json(run)
    run = complete_run()
    run.report["findings"][0]["citations"][0]["evidence_id"] = "missing"
    with pytest.raises(ExportUnavailable):
        export_markdown(run)
