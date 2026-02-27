def ensure_schema(connection) -> None:
    schema_version = '2026-02-27.3'
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        INSERT INTO schema_meta (key, value, updated_at)
        VALUES ('schema_version', ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = excluded.updated_at
        ''',
        (schema_version,),
    )
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
    if not any(col[1] == 'canonical_name' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN canonical_name TEXT"
        )
    if not any(col[1] == 'aliases_csv' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN aliases_csv TEXT NOT NULL DEFAULT ''"
        )
    connection.execute(
        '''
        UPDATE actor_profiles
        SET canonical_name = LOWER(TRIM(display_name))
        WHERE COALESCE(TRIM(canonical_name), '') = ''
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_actor_profiles_canonical_name
        ON actor_profiles(canonical_name)
        '''
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
    if not any(col[1] == 'auto_refresh_last_run_at' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN auto_refresh_last_run_at TEXT"
        )
    if not any(col[1] == 'auto_refresh_last_status' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN auto_refresh_last_status TEXT"
        )
    if not any(col[1] == 'last_confirmed_at' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN last_confirmed_at TEXT"
        )
    if not any(col[1] == 'last_confirmed_by' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN last_confirmed_by TEXT"
        )
    if not any(col[1] == 'last_confirmed_note' for col in actor_cols):
        connection.execute(
            "ALTER TABLE actor_profiles ADD COLUMN last_confirmed_note TEXT"
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
        CREATE TABLE IF NOT EXISTS tracking_intent_register (
            actor_id TEXT PRIMARY KEY,
            why_track TEXT NOT NULL DEFAULT '',
            mission_impact TEXT NOT NULL DEFAULT '',
            intelligence_focus TEXT NOT NULL DEFAULT '',
            key_questions_json TEXT NOT NULL DEFAULT '[]',
            priority TEXT NOT NULL DEFAULT 'medium',
            impact TEXT NOT NULL DEFAULT 'medium',
            review_cadence_days INTEGER NOT NULL DEFAULT 30,
            confirmation_min_sources INTEGER NOT NULL DEFAULT 2,
            confirmation_max_age_days INTEGER NOT NULL DEFAULT 90,
            confirmation_criteria TEXT NOT NULL DEFAULT '',
            updated_by TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_collection_plans (
            actor_id TEXT PRIMARY KEY,
            monitored_sources_json TEXT NOT NULL DEFAULT '[]',
            monitor_frequency TEXT NOT NULL DEFAULT 'daily',
            trigger_conditions_json TEXT NOT NULL DEFAULT '[]',
            alert_subscriptions_json TEXT NOT NULL DEFAULT '[]',
            alert_notifications_enabled INTEGER NOT NULL DEFAULT 1,
            updated_by TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        '''
    )
    collection_plan_cols = connection.execute('PRAGMA table_info(actor_collection_plans)').fetchall()
    if not any(col[1] == 'alert_notifications_enabled' for col in collection_plan_cols):
        connection.execute(
            "ALTER TABLE actor_collection_plans ADD COLUMN alert_notifications_enabled INTEGER NOT NULL DEFAULT 1"
        )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_relationship_edges (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            src_entity_type TEXT NOT NULL,
            src_entity_key TEXT NOT NULL,
            relationship_type TEXT NOT NULL,
            dst_entity_type TEXT NOT NULL,
            dst_entity_key TEXT NOT NULL,
            source_ref TEXT NOT NULL DEFAULT '',
            observed_on TEXT NOT NULL DEFAULT '',
            confidence TEXT NOT NULL DEFAULT 'moderate',
            analyst TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_relationship_edges_actor
        ON actor_relationship_edges(actor_id, relationship_type, updated_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_change_items (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            change_summary TEXT NOT NULL,
            change_type TEXT NOT NULL DEFAULT 'other',
            ttp_tag INTEGER NOT NULL DEFAULT 0,
            infra_tag INTEGER NOT NULL DEFAULT 0,
            tooling_tag INTEGER NOT NULL DEFAULT 0,
            targeting_tag INTEGER NOT NULL DEFAULT 0,
            timing_tag INTEGER NOT NULL DEFAULT 0,
            access_vector_tag INTEGER NOT NULL DEFAULT 0,
            confidence TEXT NOT NULL DEFAULT 'moderate',
            source_ref TEXT NOT NULL DEFAULT '',
            observed_on TEXT NOT NULL DEFAULT '',
            created_by TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_change_items_actor_created
        ON actor_change_items(actor_id, created_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_change_conflicts (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            conflict_topic TEXT NOT NULL,
            source_a_ref TEXT NOT NULL,
            source_b_ref TEXT NOT NULL,
            arbitration_outcome TEXT NOT NULL,
            confidence TEXT NOT NULL DEFAULT 'moderate',
            analyst TEXT NOT NULL DEFAULT '',
            resolved_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_change_conflicts_actor_resolved
        ON actor_change_conflicts(actor_id, resolved_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_technique_coverage (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            technique_id TEXT NOT NULL,
            technique_name TEXT NOT NULL DEFAULT '',
            detection_name TEXT NOT NULL DEFAULT '',
            control_name TEXT NOT NULL DEFAULT '',
            coverage_status TEXT NOT NULL DEFAULT 'unknown',
            validation_status TEXT NOT NULL DEFAULT 'unknown',
            validation_evidence TEXT NOT NULL DEFAULT '',
            updated_by TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_alert_events (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            alert_type TEXT NOT NULL DEFAULT 'change_detection',
            severity TEXT NOT NULL DEFAULT 'medium',
            title TEXT NOT NULL,
            detail TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'open',
            source_ref TEXT NOT NULL DEFAULT '',
            channel_targets_json TEXT NOT NULL DEFAULT '[]',
            change_item_id TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            acknowledged_at TEXT NOT NULL DEFAULT '',
            acknowledged_by TEXT NOT NULL DEFAULT ''
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_actor_alert_events_actor_status_created
        ON actor_alert_events(actor_id, status, created_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_alert_delivery_events (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            alert_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            target TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'queued',
            response_summary TEXT NOT NULL DEFAULT '',
            error_detail TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_report_preferences (
            actor_id TEXT PRIMARY KEY,
            delta_brief_enabled INTEGER NOT NULL DEFAULT 1,
            delta_brief_period TEXT NOT NULL DEFAULT 'weekly',
            delta_brief_window_days INTEGER NOT NULL DEFAULT 7,
            updated_by TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_alert_delivery_actor_alert_created
        ON actor_alert_delivery_events(actor_id, alert_id, created_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_technique_coverage_actor_technique
        ON actor_technique_coverage(actor_id, technique_id)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_tasks (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            title TEXT NOT NULL,
            details TEXT NOT NULL DEFAULT '',
            priority TEXT NOT NULL DEFAULT 'medium',
            status TEXT NOT NULL DEFAULT 'open',
            owner TEXT NOT NULL DEFAULT '',
            due_date TEXT NOT NULL DEFAULT '',
            linked_type TEXT NOT NULL DEFAULT '',
            linked_key TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_actor_tasks_actor_status_due
        ON actor_tasks(actor_id, status, due_date, updated_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_operational_outcomes (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            outcome_type TEXT NOT NULL,
            summary TEXT NOT NULL,
            result TEXT NOT NULL DEFAULT '',
            linked_task_id TEXT NOT NULL DEFAULT '',
            linked_technique_id TEXT NOT NULL DEFAULT '',
            evidence_ref TEXT NOT NULL DEFAULT '',
            created_by TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_actor_outcomes_actor_created
        ON actor_operational_outcomes(actor_id, created_at DESC)
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
            ingested_at TEXT,
            source_date_type TEXT,
            retrieved_at TEXT NOT NULL,
            pasted_text TEXT NOT NULL,
            source_fingerprint TEXT,
            source_type TEXT,
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
    if not any(col[1] == 'source_type' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN source_type TEXT")
    if not any(col[1] == 'confidence_weight' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN confidence_weight INTEGER")
    if not any(col[1] == 'ingested_at' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN ingested_at TEXT")
    if not any(col[1] == 'source_date_type' for col in source_cols):
        connection.execute("ALTER TABLE sources ADD COLUMN source_date_type TEXT")
    connection.execute(
        '''
        UPDATE sources
        SET ingested_at = COALESCE(NULLIF(ingested_at, ''), retrieved_at)
        '''
    )
    connection.execute(
        '''
        UPDATE sources
        SET source_date_type = CASE
            WHEN COALESCE(TRIM(published_at), '') <> '' THEN 'published'
            ELSE 'ingested'
        END
        WHERE COALESCE(TRIM(source_date_type), '') = ''
        '''
    )
    connection.execute(
        '''
        UPDATE sources
        SET source_type = COALESCE(NULLIF(TRIM(source_type), ''), 'manual')
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_sources_actor_fingerprint
        ON sources(actor_id, source_fingerprint)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS source_documents (
            id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            raw_text TEXT NOT NULL DEFAULT '',
            html_text TEXT NOT NULL DEFAULT '',
            fetched_at TEXT NOT NULL,
            http_status INTEGER,
            content_type TEXT NOT NULL DEFAULT '',
            parse_status TEXT NOT NULL DEFAULT 'unknown',
            parse_error TEXT NOT NULL DEFAULT ''
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_source_documents_source_fetched
        ON source_documents(source_id, fetched_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS source_entities (
            id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_value TEXT NOT NULL,
            normalized_value TEXT NOT NULL DEFAULT '',
            confidence REAL NOT NULL DEFAULT 0.0,
            extractor TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_source_entities_source_type
        ON source_entities(source_id, entity_type)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_resolution (
            id TEXT PRIMARY KEY,
            source_id TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            match_type TEXT NOT NULL DEFAULT '',
            matched_term TEXT NOT NULL DEFAULT '',
            confidence REAL NOT NULL DEFAULT 0.0,
            explanation_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_actor_resolution_actor_source
        ON actor_resolution(actor_id, source_id)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS source_scoring (
            source_id TEXT PRIMARY KEY,
            relevance_score REAL NOT NULL DEFAULT 0.0,
            trust_score REAL NOT NULL DEFAULT 0.0,
            recency_score REAL NOT NULL DEFAULT 0.0,
            novelty_score REAL NOT NULL DEFAULT 0.0,
            final_score REAL NOT NULL DEFAULT 0.0,
            scored_at TEXT NOT NULL,
            features_json TEXT NOT NULL DEFAULT '{}'
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_source_scoring_final
        ON source_scoring(final_score DESC, scored_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS ingest_decisions (
            id TEXT PRIMARY KEY,
            source_id TEXT,
            actor_id TEXT NOT NULL,
            stage TEXT NOT NULL,
            decision TEXT NOT NULL,
            reason_code TEXT NOT NULL DEFAULT '',
            details_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_ingest_decisions_actor_stage_created
        ON ingest_decisions(actor_id, stage, created_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS web_backfill_cache (
            actor_id TEXT PRIMARY KEY,
            queried_at TEXT NOT NULL,
            result_urls_json TEXT NOT NULL DEFAULT '[]',
            inserted_count INTEGER NOT NULL DEFAULT 0
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS backfill_runs (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            mode TEXT NOT NULL DEFAULT '',
            queries_attempted INTEGER NOT NULL DEFAULT 0,
            candidates_found INTEGER NOT NULL DEFAULT 0,
            pages_fetched INTEGER NOT NULL DEFAULT 0,
            pages_parsed_ok INTEGER NOT NULL DEFAULT 0,
            sources_inserted INTEGER NOT NULL DEFAULT 0,
            error_summary_json TEXT NOT NULL DEFAULT '{}'
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_backfill_runs_actor_started
        ON backfill_runs(actor_id, started_at DESC)
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
        CREATE TABLE IF NOT EXISTS quick_check_overrides (
            actor_id TEXT NOT NULL,
            thread_id TEXT NOT NULL,
            first_step TEXT NOT NULL DEFAULT '',
            what_to_look_for TEXT NOT NULL DEFAULT '',
            expected_output TEXT NOT NULL DEFAULT '',
            generated_at TEXT NOT NULL,
            PRIMARY KEY (actor_id, thread_id)
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
            normalized_value TEXT,
            validation_status TEXT NOT NULL DEFAULT 'unvalidated',
            validation_reason TEXT NOT NULL DEFAULT '',
            confidence_score INTEGER NOT NULL DEFAULT 0,
            source_id TEXT,
            source_ref TEXT,
            extraction_method TEXT NOT NULL DEFAULT 'manual',
            lifecycle_status TEXT NOT NULL DEFAULT 'active',
            handling_tlp TEXT NOT NULL DEFAULT 'TLP:CLEAR',
            valid_from TEXT,
            valid_until TEXT,
            revoked INTEGER NOT NULL DEFAULT 0,
            revoked_at TEXT,
            first_seen_at TEXT,
            last_seen_at TEXT,
            seen_count INTEGER NOT NULL DEFAULT 1,
            is_active INTEGER NOT NULL DEFAULT 1,
            updated_at TEXT,
            created_at TEXT NOT NULL
        )
        '''
    )
    ioc_cols = connection.execute('PRAGMA table_info(ioc_items)').fetchall()
    if not any(col[1] == 'normalized_value' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN normalized_value TEXT")
    if not any(col[1] == 'validation_status' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN validation_status TEXT NOT NULL DEFAULT 'unvalidated'")
    if not any(col[1] == 'validation_reason' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN validation_reason TEXT NOT NULL DEFAULT ''")
    if not any(col[1] == 'confidence_score' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN confidence_score INTEGER NOT NULL DEFAULT 0")
    if not any(col[1] == 'source_id' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN source_id TEXT")
    if not any(col[1] == 'extraction_method' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN extraction_method TEXT NOT NULL DEFAULT 'manual'")
    if not any(col[1] == 'lifecycle_status' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN lifecycle_status TEXT NOT NULL DEFAULT 'active'")
    if not any(col[1] == 'handling_tlp' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN handling_tlp TEXT NOT NULL DEFAULT 'TLP:CLEAR'")
    if not any(col[1] == 'valid_from' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN valid_from TEXT")
    if not any(col[1] == 'valid_until' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN valid_until TEXT")
    if not any(col[1] == 'revoked' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN revoked INTEGER NOT NULL DEFAULT 0")
    if not any(col[1] == 'revoked_at' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN revoked_at TEXT")
    if not any(col[1] == 'first_seen_at' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN first_seen_at TEXT")
    if not any(col[1] == 'last_seen_at' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN last_seen_at TEXT")
    if not any(col[1] == 'seen_count' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN seen_count INTEGER NOT NULL DEFAULT 1")
    if not any(col[1] == 'is_active' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    if not any(col[1] == 'updated_at' for col in ioc_cols):
        connection.execute("ALTER TABLE ioc_items ADD COLUMN updated_at TEXT")
    connection.execute(
        '''
        UPDATE ioc_items
        SET
            ioc_type = LOWER(TRIM(ioc_type)),
            normalized_value = COALESCE(NULLIF(normalized_value, ''), LOWER(TRIM(ioc_value))),
            validation_status = CASE
                WHEN TRIM(COALESCE(validation_status, '')) = '' THEN 'unvalidated'
                ELSE validation_status
            END,
            first_seen_at = COALESCE(first_seen_at, created_at),
            last_seen_at = COALESCE(last_seen_at, created_at),
            updated_at = COALESCE(updated_at, created_at),
            seen_count = CASE WHEN seen_count IS NULL OR seen_count < 1 THEN 1 ELSE seen_count END,
            lifecycle_status = CASE
                WHEN LOWER(TRIM(COALESCE(lifecycle_status, ''))) IN ('active', 'monitor', 'superseded', 'revoked', 'false_positive')
                    THEN LOWER(TRIM(lifecycle_status))
                ELSE 'active'
            END,
            handling_tlp = CASE
                WHEN UPPER(TRIM(COALESCE(handling_tlp, ''))) IN ('TLP:CLEAR', 'TLP:GREEN', 'TLP:AMBER', 'TLP:AMBER+STRICT', 'TLP:RED')
                    THEN UPPER(TRIM(handling_tlp))
                ELSE 'TLP:CLEAR'
            END,
            valid_from = COALESCE(valid_from, first_seen_at, created_at),
            revoked = CASE
                WHEN LOWER(TRIM(COALESCE(lifecycle_status, ''))) IN ('revoked', 'false_positive') THEN 1
                WHEN revoked IS NULL THEN 0
                ELSE revoked
            END,
            revoked_at = CASE
                WHEN LOWER(TRIM(COALESCE(lifecycle_status, ''))) IN ('revoked', 'false_positive') AND COALESCE(revoked_at, '') = ''
                    THEN COALESCE(updated_at, created_at)
                ELSE revoked_at
            END
        '''
    )
    connection.execute(
        '''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ioc_items_actor_type_normalized
        ON ioc_items(actor_id, ioc_type, normalized_value)
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_ioc_items_actor_last_seen
        ON ioc_items(actor_id, last_seen_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS ioc_history (
            id TEXT PRIMARY KEY,
            ioc_item_id TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            ioc_type TEXT NOT NULL,
            ioc_value TEXT NOT NULL,
            normalized_value TEXT NOT NULL,
            validation_status TEXT NOT NULL,
            validation_reason TEXT NOT NULL DEFAULT '',
            confidence_score INTEGER NOT NULL DEFAULT 0,
            source_id TEXT,
            source_ref TEXT,
            extraction_method TEXT NOT NULL,
            lifecycle_status TEXT NOT NULL DEFAULT 'active',
            handling_tlp TEXT NOT NULL DEFAULT 'TLP:CLEAR',
            valid_from TEXT,
            valid_until TEXT,
            revoked INTEGER NOT NULL DEFAULT 0,
            revoked_at TEXT,
            created_at TEXT NOT NULL
        )
        '''
    )
    ioc_history_cols = connection.execute('PRAGMA table_info(ioc_history)').fetchall()
    if not any(col[1] == 'lifecycle_status' for col in ioc_history_cols):
        connection.execute("ALTER TABLE ioc_history ADD COLUMN lifecycle_status TEXT NOT NULL DEFAULT 'active'")
    if not any(col[1] == 'handling_tlp' for col in ioc_history_cols):
        connection.execute("ALTER TABLE ioc_history ADD COLUMN handling_tlp TEXT NOT NULL DEFAULT 'TLP:CLEAR'")
    if not any(col[1] == 'valid_from' for col in ioc_history_cols):
        connection.execute("ALTER TABLE ioc_history ADD COLUMN valid_from TEXT")
    if not any(col[1] == 'valid_until' for col in ioc_history_cols):
        connection.execute("ALTER TABLE ioc_history ADD COLUMN valid_until TEXT")
    if not any(col[1] == 'revoked' for col in ioc_history_cols):
        connection.execute("ALTER TABLE ioc_history ADD COLUMN revoked INTEGER NOT NULL DEFAULT 0")
    if not any(col[1] == 'revoked_at' for col in ioc_history_cols):
        connection.execute("ALTER TABLE ioc_history ADD COLUMN revoked_at TEXT")
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_ioc_history_actor_created
        ON ioc_history(actor_id, created_at DESC)
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
            claim_type TEXT NOT NULL DEFAULT 'assessment',
            citation_url TEXT NOT NULL DEFAULT '',
            observed_on TEXT NOT NULL DEFAULT '',
            updated_by TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        '''
    )
    obs_cols = connection.execute('PRAGMA table_info(analyst_observations)').fetchall()
    if not any(col[1] == 'claim_type' for col in obs_cols):
        connection.execute("ALTER TABLE analyst_observations ADD COLUMN claim_type TEXT NOT NULL DEFAULT 'assessment'")
    if not any(col[1] == 'citation_url' for col in obs_cols):
        connection.execute("ALTER TABLE analyst_observations ADD COLUMN citation_url TEXT NOT NULL DEFAULT ''")
    if not any(col[1] == 'observed_on' for col in obs_cols):
        connection.execute("ALTER TABLE analyst_observations ADD COLUMN observed_on TEXT NOT NULL DEFAULT ''")
    connection.execute(
        '''
        UPDATE analyst_observations
        SET claim_type = CASE
            WHEN LOWER(TRIM(COALESCE(claim_type, ''))) IN ('evidence', 'assessment')
                THEN LOWER(TRIM(claim_type))
            ELSE 'assessment'
        END
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
            claim_type TEXT NOT NULL DEFAULT 'assessment',
            citation_url TEXT NOT NULL DEFAULT '',
            observed_on TEXT NOT NULL DEFAULT '',
            updated_by TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL
        )
        '''
    )
    obs_hist_cols = connection.execute('PRAGMA table_info(analyst_observation_history)').fetchall()
    if not any(col[1] == 'claim_type' for col in obs_hist_cols):
        connection.execute("ALTER TABLE analyst_observation_history ADD COLUMN claim_type TEXT NOT NULL DEFAULT 'assessment'")
    if not any(col[1] == 'citation_url' for col in obs_hist_cols):
        connection.execute("ALTER TABLE analyst_observation_history ADD COLUMN citation_url TEXT NOT NULL DEFAULT ''")
    if not any(col[1] == 'observed_on' for col in obs_hist_cols):
        connection.execute("ALTER TABLE analyst_observation_history ADD COLUMN observed_on TEXT NOT NULL DEFAULT ''")
    connection.execute(
        '''
        UPDATE analyst_observation_history
        SET claim_type = CASE
            WHEN LOWER(TRIM(COALESCE(claim_type, ''))) IN ('evidence', 'assessment')
                THEN LOWER(TRIM(claim_type))
            ELSE 'assessment'
        END
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_observation_history_actor_item_updated
        ON analyst_observation_history(actor_id, item_type, item_key, updated_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS analyst_feedback_events (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            item_type TEXT NOT NULL,
            item_id TEXT NOT NULL,
            feedback_label TEXT NOT NULL,
            rating_score INTEGER NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            source_id TEXT,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_feedback_actor_type_item_created
        ON analyst_feedback_events(actor_id, item_type, item_id, created_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS actor_environment_profiles (
            actor_id TEXT PRIMARY KEY,
            query_dialect TEXT NOT NULL DEFAULT 'generic',
            field_mapping_json TEXT NOT NULL DEFAULT '{}',
            default_time_window_hours INTEGER NOT NULL DEFAULT 24,
            updated_at TEXT NOT NULL
        )
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS notebook_cache (
            actor_id TEXT NOT NULL,
            cache_key TEXT NOT NULL,
            data_fingerprint TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (actor_id, cache_key)
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_notebook_cache_actor_updated
        ON notebook_cache(actor_id, updated_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS source_reliability (
            actor_id TEXT NOT NULL,
            domain TEXT NOT NULL,
            helpful_count INTEGER NOT NULL DEFAULT 0,
            unhelpful_count INTEGER NOT NULL DEFAULT 0,
            reliability_score REAL NOT NULL DEFAULT 0.5,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (actor_id, domain)
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_source_reliability_actor_score
        ON source_reliability(actor_id, reliability_score DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS notebook_generation_jobs (
            id TEXT PRIMARY KEY,
            actor_id TEXT NOT NULL,
            trigger_type TEXT NOT NULL DEFAULT 'manual_refresh',
            status TEXT NOT NULL DEFAULT 'queued',
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            duration_ms INTEGER,
            imported_sources INTEGER NOT NULL DEFAULT 0,
            final_message TEXT NOT NULL DEFAULT '',
            error_message TEXT NOT NULL DEFAULT ''
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_generation_jobs_actor_created
        ON notebook_generation_jobs(actor_id, created_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS notebook_generation_phases (
            id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            phase_key TEXT NOT NULL,
            phase_label TEXT NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 1,
            status TEXT NOT NULL,
            message TEXT NOT NULL DEFAULT '',
            error_detail TEXT NOT NULL DEFAULT '',
            started_at TEXT NOT NULL,
            finished_at TEXT,
            duration_ms INTEGER
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_generation_phases_job_started
        ON notebook_generation_phases(job_id, started_at ASC)
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_generation_phases_actor_started
        ON notebook_generation_phases(actor_id, started_at DESC)
        '''
    )
    connection.execute(
        '''
        CREATE TABLE IF NOT EXISTS llm_synthesis_cache (
            actor_key TEXT NOT NULL,
            cache_kind TEXT NOT NULL,
            input_fingerprint TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            estimated_cost_ms INTEGER NOT NULL DEFAULT 0,
            hit_count INTEGER NOT NULL DEFAULT 0,
            saved_ms_total INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (actor_key, cache_kind, input_fingerprint)
        )
        '''
    )
    connection.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_llm_cache_actor_updated
        ON llm_synthesis_cache(actor_key, updated_at DESC)
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
