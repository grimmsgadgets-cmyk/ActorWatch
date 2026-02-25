from datetime import datetime, timedelta, timezone


def _parse_iso_datetime(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    normalized = raw.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def window_bounds_core(*, now: datetime | None = None, window_days: int = 30) -> tuple[datetime, datetime]:
    safe_days = max(1, int(window_days))
    end = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    start = end - timedelta(days=safe_days)
    return start, end


def is_in_window_core(value: str, *, window_start: datetime, window_end: datetime) -> bool:
    parsed = _parse_iso_datetime(value)
    if parsed is None:
        return False
    return window_start <= parsed <= window_end


def apply_no_evidence_rule_core(check: dict[str, object]) -> dict[str, object]:
    has_evidence = bool(check.get("has_evidence"))
    if has_evidence:
        return check
    where_to_start = str(check.get("first_step") or "").strip()
    if where_to_start and not where_to_start.lower().startswith("data gap:"):
        check["first_step"] = f"Data gap: {where_to_start}"
    elif not where_to_start:
        check["first_step"] = (
            "Data gap: insufficient thread-linked 30-day evidence to assert actor-specific behavior. "
            "Run baseline hunt and collect evidence."
        )
    check["evidence_used"] = ["No thread-linked evidence in last 30 days."]
    return check


def rank_quick_checks_core(checks: list[dict[str, object]]) -> list[dict[str, object]]:
    severity_rank = {"high": 3, "medium": 2, "low": 1}
    tier_rank = {"A": 4, "B": 3, "C": 2, "D": 1}

    def _rank_key(check: dict[str, object]) -> tuple[int, int, float]:
        severity = severity_rank.get(str(check.get("priority") or "").strip().lower(), 0)
        evidence_tier = tier_rank.get(str(check.get("evidence_tier") or "").strip().upper(), 0)
        has_evidence = 1 if bool(check.get("has_evidence")) else 0
        last_seen = _parse_iso_datetime(str(check.get("last_seen_evidence_at") or ""))
        recency = last_seen.timestamp() if last_seen is not None else 0.0
        return severity, evidence_tier, has_evidence, recency

    return sorted(checks, key=_rank_key, reverse=True)


def filter_iocs_for_check_core(
    ioc_items: list[dict[str, object]],
    *,
    relevant_types: set[str] | None = None,
    relevant_values: set[str] | None = None,
) -> list[dict[str, object]]:
    type_filter = {str(value or "").strip().lower() for value in (relevant_types or set()) if str(value or "").strip()}
    value_filter = {str(value or "").strip().lower() for value in (relevant_values or set()) if str(value or "").strip()}
    if not type_filter and not value_filter:
        return list(ioc_items)

    filtered: list[dict[str, object]] = []
    for ioc in ioc_items:
        if not isinstance(ioc, dict):
            continue
        ioc_type = str(ioc.get("ioc_type") or "").strip().lower()
        ioc_value = str(ioc.get("ioc_value") or "").strip().lower()
        if type_filter and ioc_type not in type_filter:
            continue
        if value_filter and ioc_value not in value_filter:
            continue
        filtered.append(ioc)
    return filtered
