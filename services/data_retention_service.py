# Allowlist for tables managed by data retention.
# Maps table_name -> (timestamp_column, result_key).
# These are compile-time constants — never derived from user input.
_RETENTION_TABLES: tuple[tuple[str, str, str], ...] = (
    ('question_updates', 'created_at', 'question_updates_deleted'),
    ('analyst_observation_history', 'updated_at', 'observation_history_deleted'),
    ('ioc_history', 'created_at', 'ioc_history_deleted'),
    ('analyst_feedback_events', 'created_at', 'feedback_events_deleted'),
)
_ALLOWED_TABLE_NAMES: frozenset[str] = frozenset(t[0] for t in _RETENTION_TABLES)
_ALLOWED_TS_COLS: frozenset[str] = frozenset({'created_at', 'updated_at'})


def prune_data_core(
    connection,
    *,
    retention_days: int,
    keep_min_rows_per_table: int = 500,
) -> dict[str, int]:
    safe_days = max(1, int(retention_days))
    keep_rows = max(0, int(keep_min_rows_per_table))
    results: dict[str, int] = {}

    # Keep most recent source rows, prune old rows by retrieved timestamp.
    source_cutoff = connection.execute(
        '''
        SELECT retrieved_at
        FROM sources
        ORDER BY COALESCE(retrieved_at, published_at) DESC
        LIMIT 1 OFFSET ?
        ''',
        (keep_rows,),
    ).fetchone()
    if source_cutoff is not None and str(source_cutoff[0] or '').strip():
        before = connection.total_changes
        connection.execute(
            '''
            DELETE FROM sources
            WHERE COALESCE(retrieved_at, published_at) < datetime('now', ?)
              AND COALESCE(retrieved_at, published_at) < ?
            ''',
            (f'-{safe_days} days', str(source_cutoff[0])),
        )
        results['sources_deleted'] = int(connection.total_changes - before)
    else:
        results['sources_deleted'] = 0

    # Prune old history/events while keeping recent minimum rows.
    # table and ts_col come from _RETENTION_TABLES — a compile-time allowlist, never user input.
    for table, ts_col, key in _RETENTION_TABLES:
        # Defensive guard: skip anything not in the explicit allowlist.
        if table not in _ALLOWED_TABLE_NAMES or ts_col not in _ALLOWED_TS_COLS:
            continue
        cutoff = connection.execute(  # nosec B608
            f'SELECT {ts_col} FROM {table} ORDER BY {ts_col} DESC LIMIT 1 OFFSET ?',
            (keep_rows,),
        ).fetchone()
        if cutoff is None or not str(cutoff[0] or '').strip():
            results[key] = 0
            continue
        before = connection.total_changes
        connection.execute(  # nosec B608
            f"DELETE FROM {table} WHERE {ts_col} < datetime('now', ?) AND {ts_col} < ?",
            (f'-{safe_days} days', str(cutoff[0])),
        )
        results[key] = int(connection.total_changes - before)

    return results
