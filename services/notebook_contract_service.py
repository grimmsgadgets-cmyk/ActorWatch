import re
from datetime import datetime, timezone


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _plain_text(value: object) -> str:
    return ' '.join(str(value or '').strip().split())


def _simplify_text(text: object) -> str:
    simplified = _plain_text(text)
    if not simplified:
        return ''
    replacements = (
        ('command and control', 'remote control traffic'),
        ('command-and-control', 'remote control traffic'),
        ('c2', 'remote control traffic'),
        ('initial access', 'first way in'),
        ('lateral movement', 'movement between systems'),
        ('exfiltration', 'data theft'),
        ('tradecraft', 'behavior pattern'),
        ('ttps', 'common attacker behaviors'),
        ('ttp', 'common attacker behavior'),
        ('telemetry', 'security logs'),
    )
    lowered = simplified.lower()
    for src, dst in replacements:
        lowered = re.sub(rf'\b{re.escape(src)}\b', dst, lowered, flags=re.IGNORECASE)
    lowered = lowered[:420]
    return lowered[0].upper() + lowered[1:] if lowered else ''


def _default_priority_cards(*, actor_name: str, now_iso: str) -> list[dict[str, object]]:
    cards: list[dict[str, object]] = []
    templates = (
        (
            'Check for suspicious sign-in patterns',
            'Look for repeated login failures or unusual login locations in the last 24 hours.',
            'Start with identity and VPN logs for high-value users.',
        ),
        (
            'Check for suspicious endpoint commands',
            'Look for encoded scripts, unusual admin tools, or repeated suspicious command lines.',
            'Start with endpoint process logs and filter to critical hosts first.',
        ),
        (
            'Check for unusual outbound network traffic',
            'Look for repeated outbound traffic to rare domains or IPs from the same hosts.',
            'Start with DNS and proxy logs, then pivot to the endpoint timeline.',
        ),
    )
    for idx, (title, watch, start) in enumerate(templates, start=1):
        cards.append(
            {
                'id': f'baseline-{idx}',
                'title': title,
                'quick_check_title': title,
                'question_text': f'What should we validate now for {actor_name or "this actor"}?',
                'priority': 'Medium',
                'severity': 'Medium',
                'behavior_to_hunt': watch,
                'where_to_start': start,
                'first_step': start,
                'what_to_watch': watch,
                'what_to_look_for': watch,
                'required_data': 'Identity logs | Endpoint process logs | DNS/proxy logs',
                'decision_rule': 'Escalate if repeated suspicious behavior is confirmed on the same host or user.',
                'analyst_output': 'List affected host/user, time window, and recommended containment action.',
                'evidence_used': ['Baseline guidance: waiting for actor-linked evidence.'],
                'has_evidence': False,
                'evidence_status': 'baseline_guidance',
                'window_start': now_iso,
                'window_end': now_iso,
            }
        )
    return cards


def _ensure_list(payload: dict[str, object], key: str) -> list[dict[str, object]]:
    value = payload.get(key)
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def finalize_notebook_contract_core(payload: dict[str, object]) -> dict[str, object]:
    notebook = payload if isinstance(payload, dict) else {}
    actor = notebook.get('actor') if isinstance(notebook.get('actor'), dict) else {}
    actor_name = _plain_text(actor.get('display_name') or actor.get('id') or 'this actor')
    now_iso = _utc_now_iso()

    if not isinstance(notebook.get('counts'), dict):
        notebook['counts'] = {'sources': 0, 'timeline_events': 0, 'open_questions': 0}

    cards = _ensure_list(notebook, 'priority_questions')
    if not cards:
        cards = _default_priority_cards(actor_name=actor_name, now_iso=now_iso)
    for card in cards:
        title = _plain_text(card.get('title') or card.get('quick_check_title') or card.get('question_text'))
        if not title:
            title = 'Priority quick check'
        where_to_start = _simplify_text(card.get('where_to_start') or card.get('first_step'))
        if not where_to_start:
            where_to_start = 'Start with identity, endpoint, and network logs for the last 24 hours.'
        what_to_watch = _simplify_text(card.get('what_to_watch') or card.get('what_to_look_for') or card.get('behavior_to_hunt'))
        if not what_to_watch:
            what_to_watch = 'Look for repeated suspicious behavior on the same host or user.'
        decision_rule = _simplify_text(card.get('decision_rule') or card.get('success_condition'))
        if not decision_rule:
            decision_rule = 'Escalate when the same suspicious pattern repeats and affects critical systems.'
        analyst_output = _simplify_text(card.get('analyst_output') or card.get('expected_output'))
        if not analyst_output:
            analyst_output = 'Output host, user, timestamp, key evidence, and next action.'
        evidence_used = card.get('evidence_used')
        if not isinstance(evidence_used, list) or not any(_plain_text(item) for item in evidence_used):
            evidence_used = ['Baseline guidance: waiting for actor-linked evidence.']
        card['title'] = title
        card['quick_check_title'] = title
        card['question_text'] = _simplify_text(card.get('question_text') or f'What should we validate now for {actor_name}?')
        card['behavior_to_hunt'] = what_to_watch
        card['where_to_start'] = where_to_start
        card['first_step'] = where_to_start
        card['what_to_watch'] = what_to_watch
        card['what_to_look_for'] = what_to_watch
        card['required_data'] = _simplify_text(card.get('required_data') or 'Identity logs, endpoint logs, and network logs.')
        card['decision_rule'] = decision_rule
        card['analyst_output'] = analyst_output
        card['evidence_used'] = [_simplify_text(item) for item in evidence_used if _plain_text(item)]
        card['severity'] = _plain_text(card.get('severity') or card.get('priority') or 'Medium').title()
        card['priority'] = _plain_text(card.get('priority') or card.get('severity') or 'Medium').title()
        card['evidence_status'] = 'evidence_backed' if bool(card.get('has_evidence')) else 'baseline_guidance'
        card['window_start'] = _plain_text(card.get('window_start') or now_iso)
        card['window_end'] = _plain_text(card.get('window_end') or now_iso)
    notebook['priority_questions'] = cards
    notebook['priority_phase_groups'] = [{'phase': 'Operational Signal', 'cards': cards}]

    top_change_signals = _ensure_list(notebook, 'top_change_signals')
    if not top_change_signals:
        top_change_signals = [
            {
                'change_summary': f'No fully validated new behavior for {actor_name} yet.',
                'change_why_new': 'Current output is baseline guidance while new actor-linked evidence is collected.',
                'change_confidence': 'medium',
                'change_window_days': '30',
                'validated_source_count': '0',
                'validated_sources': [],
            }
        ]
    for item in top_change_signals:
        item['change_summary'] = _simplify_text(item.get('change_summary') or f'No fully validated new behavior for {actor_name} yet.')
        item['change_why_new'] = _simplify_text(item.get('change_why_new') or 'Using baseline monitoring until enough corroborated evidence is available.')
        item['change_confidence'] = _plain_text(item.get('change_confidence') or 'medium').lower()
        item['change_window_days'] = _plain_text(item.get('change_window_days') or '30')
        item['validated_source_count'] = _plain_text(item.get('validated_source_count') or '0')
        if not isinstance(item.get('validated_sources'), list):
            item['validated_sources'] = []
    notebook['top_change_signals'] = top_change_signals

    synthesis = _ensure_list(notebook, 'recent_activity_synthesis')
    if not synthesis:
        synthesis = [
            {
                'label': 'What changed',
                'text': _simplify_text(f'No confirmed new behavior for {actor_name} yet.'),
                'confidence': 'medium',
                'lineage': 'baseline',
            },
            {
                'label': 'Who is affected',
                'text': 'Focus on high-value users, admin endpoints, and internet-facing systems first.',
                'confidence': 'medium',
                'lineage': 'baseline',
            },
            {
                'label': 'What to do first',
                'text': 'Run the quick checks now and escalate if repeated suspicious activity is confirmed.',
                'confidence': 'medium',
                'lineage': 'baseline',
            },
        ]
    for item in synthesis:
        item['label'] = _plain_text(item.get('label') or 'Summary')
        item['text'] = _simplify_text(item.get('text') or 'Baseline summary for junior analysts.')
        item['confidence'] = _plain_text(item.get('confidence') or 'medium').lower()
        item['lineage'] = _plain_text(item.get('lineage') or 'baseline')
    notebook['recent_activity_synthesis'] = synthesis

    highlights = _ensure_list(notebook, 'recent_activity_highlights')
    if not highlights:
        highlights = [
            {
                'text': 'Baseline watch: monitor identity, endpoint, and network logs for repeated suspicious behavior.',
                'source_url': '',
                'source_name': 'Baseline',
                'freshness_label': 'baseline',
                'freshness_class': 'badge',
                'date': now_iso.split('T', 1)[0],
            }
        ]
    notebook['recent_activity_highlights'] = highlights

    environment_checks = _ensure_list(notebook, 'environment_checks')
    if not environment_checks:
        environment_checks = [
            {'title': 'Identity checks', 'detail': 'Review failed logins, unusual login locations, and admin account changes.'},
            {'title': 'Endpoint checks', 'detail': 'Review suspicious scripts, new services, and unusual scheduled tasks.'},
            {'title': 'Network checks', 'detail': 'Review rare outbound domains/IPs and repeated beacon-like traffic.'},
        ]
    for check in environment_checks:
        check['title'] = _plain_text(check.get('title') or 'Environment check')
        check['detail'] = _simplify_text(check.get('detail') or check.get('text') or 'Run baseline monitoring checks.')
    notebook['environment_checks'] = environment_checks

    if not isinstance(notebook.get('source_quality_filters'), dict):
        notebook['source_quality_filters'] = {}
    filters = notebook['source_quality_filters']
    filters['contract_enforced'] = '1'
    filters['language_mode'] = 'junior_analyst'
    notebook['source_quality_filters'] = filters

    notebook['contract_status'] = {
        'cards_complete': True,
        'themes_complete': True,
        'language_complete': True,
    }
    return notebook
