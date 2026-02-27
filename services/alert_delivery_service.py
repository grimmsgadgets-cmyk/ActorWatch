import sqlite3
import uuid
from datetime import datetime, timezone


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_subscription_target(raw_target: str) -> tuple[str, str]:
    target = str(raw_target or '').strip()
    if not target:
        return ('in_app', '')
    lowered = target.lower()
    if lowered in {'in_app', 'ui', 'app'}:
        return ('in_app', target)
    if lowered.startswith('log'):
        return ('log', target)
    if lowered.startswith('webhook:'):
        return ('webhook', target.split(':', 1)[1].strip())
    if lowered.startswith('http://') or lowered.startswith('https://'):
        return ('webhook', target)
    if lowered.startswith('ticket:'):
        return ('ticket', target.split(':', 1)[1].strip())
    return ('unsupported', target)


def dispatch_alert_deliveries_core(
    *,
    actor_id: str,
    alert_id: str,
    title: str,
    detail: str,
    severity: str,
    subscriptions: list[str],
    db_path: str,
    http_post,
) -> dict[str, int]:
    now_iso = _utc_now_iso()
    delivered = 0
    failed = 0
    queued = 0
    unsupported = 0
    targets = subscriptions if isinstance(subscriptions, list) and subscriptions else ['in_app']
    with sqlite3.connect(db_path) as connection:
        for raw_target in targets:
            channel, target = _parse_subscription_target(str(raw_target or ''))
            status = 'queued'
            response_summary = ''
            error_detail = ''
            if channel in {'in_app', 'log'}:
                status = 'delivered'
                response_summary = 'stored'
                delivered += 1
            elif channel == 'ticket':
                status = 'queued'
                response_summary = 'ticket workflow pending integration'
                queued += 1
            elif channel == 'webhook':
                if not target:
                    status = 'failed'
                    error_detail = 'empty_webhook_target'
                    failed += 1
                else:
                    try:
                        response = http_post(
                            target,
                            json={
                                'actor_id': actor_id,
                                'alert_id': alert_id,
                                'title': str(title or ''),
                                'detail': str(detail or ''),
                                'severity': str(severity or 'medium'),
                                'created_at': now_iso,
                            },
                            timeout=5.0,
                        )
                        status_code = int(getattr(response, 'status_code', 0) or 0)
                        if 200 <= status_code < 300:
                            status = 'delivered'
                            response_summary = f'http_{status_code}'
                            delivered += 1
                        else:
                            status = 'failed'
                            response_summary = f'http_{status_code}'
                            failed += 1
                    except Exception as exc:
                        status = 'failed'
                        error_detail = str(exc)[:500]
                        failed += 1
            else:
                status = 'unsupported'
                response_summary = 'unsupported_subscription_target'
                unsupported += 1
            connection.execute(
                '''
                INSERT INTO actor_alert_delivery_events (
                    id, actor_id, alert_id, channel, target, status,
                    response_summary, error_detail, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    str(uuid.uuid4()),
                    actor_id,
                    alert_id,
                    channel,
                    str(target or '')[:500],
                    status,
                    str(response_summary or '')[:240],
                    str(error_detail or '')[:500],
                    now_iso,
                ),
            )
        connection.commit()
    return {
        'delivered': delivered,
        'failed': failed,
        'queued': queued,
        'unsupported': unsupported,
    }
