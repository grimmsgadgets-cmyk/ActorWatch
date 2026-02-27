from datetime import datetime, timezone

import services.recent_activity_service as recent_activity_service


def _parse_iso(value: str):
    raw = str(value or "").strip()
    if not raw:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def test_recent_activity_synthesis_filters_generic_target_placeholders():
    highlights = [
        {
            "category": "ransomware",
            "target_text": "this threat, high-impact sectors such as healthcare",
            "text": "Victims observed in healthcare environments.",
            "date": "2026-02-10T00:00:00Z",
            "source_url": "https://example.com/report-1",
        }
    ]

    rows = recent_activity_service.build_recent_activity_synthesis_core(
        highlights,
        deps={
            "extract_target_from_activity_text": lambda _text: "",
            "parse_published_datetime": _parse_iso,
        },
    )

    who = next(row for row in rows if row["label"] == "Who is affected")
    assert "this threat" not in who["text"].lower()
    assert "healthcare" in who["text"].lower()


def test_recent_activity_synthesis_falls_back_when_only_generic_target_text():
    highlights = [
        {
            "category": "phishing",
            "target_text": "this threat",
            "text": "General threat activity.",
            "date": "2026-02-10T00:00:00Z",
            "source_url": "https://example.com/report-2",
        }
    ]

    rows = recent_activity_service.build_recent_activity_synthesis_core(
        highlights,
        deps={
            "extract_target_from_activity_text": lambda _text: "",
            "parse_published_datetime": _parse_iso,
        },
    )

    who = next(row for row in rows if row["label"] == "Who is affected")
    assert "not explicitly named" in who["text"].lower()


def test_recent_activity_synthesis_infers_sector_from_source_text_when_target_missing():
    highlights = [
        {
            "category": "ransomware",
            "target_text": "",
            "text": "The campaign impacted multiple hospitals and regional healthcare providers.",
            "date": "2026-02-10T00:00:00Z",
            "source_url": "https://example.com/report-3",
        }
    ]

    rows = recent_activity_service.build_recent_activity_synthesis_core(
        highlights,
        deps={
            "extract_target_from_activity_text": lambda _text: "",
            "parse_published_datetime": _parse_iso,
        },
    )

    who = next(row for row in rows if row["label"] == "Who is affected")
    assert "healthcare" in who["text"].lower()


def test_recent_activity_synthesis_filters_vendor_boilerplate_rows():
    highlights = [
        {
            "category": "impact",
            "target_text": "",
            "text": "Check Point Harmony Endpoint provides protection against this threat.",
            "date": "2026-02-10T00:00:00Z",
            "source_url": "https://vendor.example/protection",
        },
        {
            "category": "initial access",
            "target_text": "Telecommunications",
            "text": "Qilin shifted access pattern and impacted telecom entities.",
            "date": "2026-02-11T00:00:00Z",
            "source_url": "https://intel.example/qilin-telecom",
        },
    ]

    rows = recent_activity_service.build_recent_activity_synthesis_core(
        highlights,
        deps={
            "extract_target_from_activity_text": lambda _text: "",
            "parse_published_datetime": _parse_iso,
        },
    )

    assert rows
    who = next(row for row in rows if row["label"] == "Who is affected")
    assert "telecommunications" in who["text"].lower()
