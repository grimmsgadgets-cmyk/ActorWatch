import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import quote_plus


def parse_feed_entries_core(xml_text: str) -> list[dict[str, str | None]]:
    entries: list[dict[str, str | None]] = []
    root = ET.fromstring(xml_text)

    # RSS
    for item in root.findall('.//item'):
        title = (item.findtext('title') or '').strip() or None
        link = (item.findtext('link') or '').strip() or None
        pub = (item.findtext('pubDate') or '').strip() or None
        if link:
            entries.append({'title': title, 'link': link, 'published_at': pub})

    # Atom
    namespace = {'atom': 'http://www.w3.org/2005/Atom'}
    for entry in root.findall('.//atom:entry', namespace):
        title = (entry.findtext('atom:title', default='', namespaces=namespace) or '').strip() or None
        updated = (entry.findtext('atom:updated', default='', namespaces=namespace) or '').strip() or None
        link_el = entry.find('atom:link[@rel="alternate"]', namespace) or entry.find('atom:link', namespace)
        link = link_el.get('href').strip() if link_el is not None and link_el.get('href') else None
        if link:
            entries.append({'title': title, 'link': link, 'published_at': updated})

    deduped: list[dict[str, str | None]] = []
    seen: set[str] = set()
    for entry in entries:
        link = entry.get('link')
        if link and link not in seen:
            deduped.append(entry)
            seen.add(link)
    return deduped


def parse_published_datetime_core(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def within_lookback_core(*, published_at: str | None, lookback_days: int, now_utc: datetime | None = None) -> bool:
    dt = parse_published_datetime_core(published_at)
    if dt is None:
        return True
    reference_now = now_utc or datetime.now(timezone.utc)
    cutoff = reference_now - timedelta(days=lookback_days)
    return dt >= cutoff


def import_ransomware_live_actor_activity_core(
    *,
    connection,
    actor_id: str,
    actor_terms: list[str],
    deps: dict[str, object],
) -> int:
    _http_get = deps['http_get']
    _now_iso = deps['now_iso']
    _upsert_source_for_actor = deps['upsert_source_for_actor']

    imported = 0
    seen_groups: set[str] = set()

    for term in actor_terms:
        group = term.strip().lower().replace(' ', '')
        if len(group) < 3 or group in seen_groups:
            continue
        seen_groups.add(group)
        endpoint = f'https://api.ransomware.live/v2/groupvictims/{quote_plus(group)}'
        try:
            response = _http_get(endpoint, timeout=20.0, follow_redirects=True)
            if response.status_code != 200:
                continue
            data = response.json()
            if not isinstance(data, list) or not data:
                continue
        except Exception:
            continue

        lines: list[str] = []
        country_counts: dict[str, int] = {}
        recent_90 = 0
        latest_attack_dt: datetime | None = None
        latest_attack_label = ''
        cutoff_90 = datetime.now(timezone.utc) - timedelta(days=90)
        for victim in data[:20]:
            if not isinstance(victim, dict):
                continue
            victim_name = str(victim.get('victim') or victim.get('name') or '').strip()
            attack_date = str(victim.get('attackdate') or victim.get('discovery_date') or '').strip()
            country = str(victim.get('country') or '').strip()
            if not victim_name:
                continue
            entry = f'{attack_date or "unknown-date"} - {victim_name}'
            if country:
                entry += f' ({country})'
                country_counts[country] = country_counts.get(country, 0) + 1
            parsed_date = parse_published_datetime_core(attack_date)
            if parsed_date and parsed_date >= cutoff_90:
                recent_90 += 1
            if parsed_date and (latest_attack_dt is None or parsed_date > latest_attack_dt):
                latest_attack_dt = parsed_date
                latest_attack_label = parsed_date.date().isoformat()
            lines.append(entry)
            if len(lines) >= 15:
                break

        if not lines:
            continue

        top_countries = sorted(country_counts.items(), key=lambda item: item[1], reverse=True)[:3]
        countries_text = ', '.join([f'{country} ({count})' for country, count in top_countries]) if top_countries else 'not specified'
        examples = '; '.join(lines[:3])
        tempo_text = f'Latest listed activity: {latest_attack_label}. ' if latest_attack_label else ''
        summary = (
            f'Ransomware.live trend for {group}: {len(data)} total public victim disclosures, '
            f'{recent_90} in the last 90 days. '
            f'{tempo_text}'
            f'Most frequent victim geographies in the current sample: {countries_text}. '
            f'Recently observed targets include: {examples}.'
        )
        _upsert_source_for_actor(
            connection,
            actor_id,
            'Ransomware.live',
            endpoint,
            _now_iso(),
            summary,
            trigger_excerpt=f'{group} ransomware activity synthesis (tempo, geography, and target examples) from ransomware.live.',
        )
        imported += 1

    return imported


def parse_ioc_values_core(raw: str) -> list[str]:
    parts = re.split(r'[\n,]+', raw)
    values: list[str] = []
    for part in parts:
        candidate = part.strip()
        if not candidate:
            continue
        if candidate not in values:
            values.append(candidate)
    return values
