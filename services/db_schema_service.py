def ensure_schema(connection) -> None:
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_profiles (
            id TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            scope_statement TEXT,
            created_at TEXT NOT NULL
        )
        '''
    )
    actor_cols = connection.execute('PRAGMA table_info(actor_profiles)').fetchall()
    if not any(col[1] == 'is_tracked' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN is_tracked INTEGER NOT NULL DEFAULT 0"
        )
    if not any(col[1] == 'notebook_status' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN notebook_status TEXT NOT NULL DEFAULT 'idle'"
        )
    if not any(col[1] == 'notebook_message' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN notebook_message TEXT NOT NULL DEFAULT 'Waiting for tracking action.'"
        )
    if not any(col[1] == 'notebook_updated_at' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN notebook_updated_at TEXT"
        )
    if not any(col[1] == 'last_refresh_duration_ms' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN last_refresh_duration_ms INTEGER"
        )
    if not any(col[1] == 'last_refresh_sources_processed' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN last_refresh_sources_processed INTEGER"
        )

    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_state (
            actor_id TEXT PRIMARY KEY,
            capability_grid_json TEXT NOT NULL,
            behavioral_model_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS observation_records (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_ref TEXT,
            source_date TEXT,
            ttp_json TEXT NOT NULL,
            tools_json TEXT NOT NULL,
            infra_json TEXT NOT NULL,
            targets_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS delta_proposals (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            observation_id TEXT NOT NULL,
            delta_type TEXT NOT NULL,
            affected_category TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        '''
    )
    delta_columns = connection.execute('PRAGMA table_info(delta_proposals)').fetchall()
    if not any(column[1] == 'validation_template_json' for column in delta_columns):
        connection.execute(
            "ALTER TABLE delta_proposals ADD COLUMN validation_template_json TEXT NOT NULL DEFAULT '{}'"
        )

    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS state_transition_log (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            delta_id TEXT NOT NULL,
            previous_state_json TEXT NOT NULL,
            new_state_json TEXT NOT NULL,
            action TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        '''
    )

    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS sources (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            url TEXT NOT NULL,
            published_at TEXT,
            retrieved_at TEXT NOT NULL,
            pasted_text TEXT NOT NULL,
            source_fingerprint TEXT,
            source_tier TEXT,
            confidence_weight INTEGER
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_feed_state (
            actor_id TEXT NOT NULL,
            feed_name TEXT NOT NULL,
            feed_url TEXT NOT NULL,
            last_checked_at TEXT,
            last_success_at TEXT,
            last_success_published_at TEXT,
            last_imported_count INTEGER NOT NULL DEFAULT 0,
            total_imported INTEGER NOT NULL DEFAULT 0,
            consecutive_failures INTEGER NOT NULL DEFAULT 0,
            total_failures INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            PRIMARY KEY (actor_id, feed_name, feed_url)
        )
        '''
    )
    source_cols = connection.execute('PRAGMA table_info(sources)').fetchall()
    if not any(col[1] == 'source_fingerprint' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN source_fingerprint TEXT")
    if not any(col[1] == 'title' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN title TEXT")
    if not any(col[1] == 'headline' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN headline TEXT")
    if not any(col[1] == 'og_title' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN og_title TEXT")
    if not any(col[1] == 'html_title' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN html_title TEXT")
    if not any(col[1] == 'publisher' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN publisher TEXT")
    if not any(col[1] == 'site_name' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN site_name TEXT")
    if not any(col[1] == 'source_tier' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN source_tier TEXT")
    if not any(col[1] == 'confidence_weight' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN confidence_weight INTEGER")
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_sources_actor_fingerprint
        ON sources(actor_id, source_fingerprint)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS timeline_events (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            occurred_at TEXT NOT NULL,
            category TEXT NOT NULL,
            title TEXT NOT NULL,
            summary TEXT NOT NULL,
            source_id TEXT,
            target_text TEXT NOT NULL DEFAULT '',
            ttp_ids_json TEXT NOT NULL DEFAULT '[]'
        )
        '''
    )
    timeline_cols = connection.execute('PRAGMA table_info(timeline_events)').fetchall()
    if not any(col[1] == 'target_text' for col in timeline_cols):
        connection.execute("ALTER TABLE timeline_events ADD COLUMN target_text TEXT NOT NULL DEFAULT ''")
    if not any(col[1] == 'ttp_ids_json' for col in timeline_cols):
        connection.execute("ALTER TABLE timeline_events ADD COLUMN ttp_ids_json TEXT NOT NULL DEFAULT '[]'")
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS question_threads (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            question_text TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS question_updates (
            id TEXT PRIMARY KEY,
            thread_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            trigger_excerpt TEXT NOT NULL,
            update_note TEXT,
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS environment_guidance (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            thread_id TEXT,
            platform TEXT NOT NULL,
            what_to_look_for TEXT NOT NULL,
            where_to_look TEXT NOT NULL,
            query_hint TEXT,
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS ioc_items (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            ioc_type TEXT NOT NULL,
            ioc_value TEXT NOT NULL,
            source_ref TEXT,
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS requirement_context (
            actor_id TEXT PRIMARY KEY,
            org_context TEXT NOT NULL DEFAULT '',
            priority_mode TEXT NOT NULL DEFAULT 'Operational',
            updated_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS requirement_items (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            req_type TEXT NOT NULL,
            requirement_text TEXT NOT NULL,
            rationale_text TEXT NOT NULL,
            source_name TEXT,
            source_url TEXT,
            source_published_at TEXT,
            validation_score INTEGER NOT NULL DEFAULT 0,
            validation_notes TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL
        )
        '''
    )
    requirement_cols = connection.execute('PRAGMA table_info(requirement_items)').fetchall()
    if not any(col[1] == 'validation_score' for col in requirement_cols):
        connection.execute("ALTER TABLE requirement_items ADD COLUMN validation_score INTEGER NOT NULL DEFAULT 0")
    if not any(col[1] == 'validation_notes' for col in requirement_cols):
        connection.execute("ALTER TABLE requirement_items ADD COLUMN validation_notes TEXT NOT NULL DEFAULT ''")
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS analyst_observations (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            item_type TEXT NOT NULL,
            item_key TEXT NOT NULL,
            note TEXT NOT NULL DEFAULT '',
            source_ref TEXT NOT NULL DEFAULT '',
            confidence TEXT NOT NULL DEFAULT 'moderate',
            source_reliability TEXT NOT NULL DEFAULT '',
            information_credibility TEXT NOT NULL DEFAULT '',
            updated_by TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_observations_actor_item
        ON analyst_observations(actor_id, item_type, item_key)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS analyst_observation_history (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            item_type TEXT NOT NULL,
            item_key TEXT NOT NULL,
            note TEXT NOT NULL DEFAULT '',
            source_ref TEXT NOT NULL DEFAULT '',
            confidence TEXT NOT NULL DEFAULT 'moderate',
            source_reliability TEXT NOT NULL DEFAULT '',
            information_credibility TEXT NOT NULL DEFAULT '',
            updated_by TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_observation_history_actor_item_updated
        ON analyst_observation_history(actor_id, item_type, item_key, updated_at DESC)
        '''
    )
    connection.commit()


def initialize_sqlite_core(*, deps: dict[str, object]) -> str:
    _resolve_startup_db_path = deps['resolve_startup_db_path']
    _configure_mitre_store = deps['configure_mitre_store']
    _clear_mitre_store_cache = deps['clear_mitre_store_cache']
    _reset_app_mitre_caches = deps['reset_app_mitre_caches']
    _ensure_mitre_attack_dataset = deps['ensure_mitre_attack_dataset']
    _sqlite_connect = deps['sqlite_connect']

    db_path = _resolve_startup_db_path()
    _configure_mitre_store(db_path)
    _clear_mitre_store_cache()
    _reset_app_mitre_caches()
    _ensure_mitre_attack_dataset()
    with _sqlite_connect(db_path) as connection:
        ensure_schema(connection)
    return db_path
