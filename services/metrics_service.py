from datetime import datetime, timezone
from threading import Lock
import re


_METRICS_LOCK = Lock()
_COUNTERS: dict[str, int] = {}
_REQUESTS_BY_ROUTE: dict[str, int] = {}
_REQUESTS_BY_STATUS: dict[str, int] = {}


def _inc_counter(name: str, amount: int = 1) -> None:
    _COUNTERS[name] = int(_COUNTERS.get(name, 0)) + int(amount)


def normalize_path_core(path: str) -> str:
    normalized = str(path or '').strip() or '/'
    normalized = re.sub(
        r'\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b',
        ':id',
        normalized,
    )
    normalized = re.sub(r'/\d+', '/:id', normalized)
    normalized = re.sub(r'/[A-Za-z0-9_-]{20,}', '/:id', normalized)
    return normalized


def record_request_core(*, method: str, path: str, status_code: int) -> None:
    route_key = f"{str(method or '').upper()} {normalize_path_core(path)}"
    status_bucket = f'{int(status_code) // 100}xx'
    with _METRICS_LOCK:
        _inc_counter('requests_total')
        _REQUESTS_BY_ROUTE[route_key] = int(_REQUESTS_BY_ROUTE.get(route_key, 0)) + 1
        _REQUESTS_BY_STATUS[status_bucket] = int(_REQUESTS_BY_STATUS.get(status_bucket, 0)) + 1


def record_refresh_queue_core(queued_count: int) -> None:
    with _METRICS_LOCK:
        _inc_counter('auto_refresh_runs_total')
        _inc_counter('auto_refresh_queued_total', max(0, int(queued_count)))


def record_generation_core(*, success: bool) -> None:
    with _METRICS_LOCK:
        _inc_counter('generation_runs_total')
        _inc_counter('generation_success_total' if success else 'generation_failed_total')


def record_feed_import_core(*, imported_count: int, success: bool) -> None:
    with _METRICS_LOCK:
        _inc_counter('feed_import_runs_total')
        _inc_counter('feed_import_items_total', max(0, int(imported_count)))
        _inc_counter('feed_import_success_total' if success else 'feed_import_failed_total')


def snapshot_metrics_core() -> dict[str, object]:
    with _METRICS_LOCK:
        return {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'counters': dict(_COUNTERS),
            'requests_by_route': dict(_REQUESTS_BY_ROUTE),
            'requests_by_status': dict(_REQUESTS_BY_STATUS),
        }


def reset_metrics_core() -> None:
    with _METRICS_LOCK:
        _COUNTERS.clear()
        _REQUESTS_BY_ROUTE.clear()
        _REQUESTS_BY_STATUS.clear()
