def generation_journal_deps_core(*, db_path, new_id, utc_now_iso) -> dict[str, object]:
    return {
        'db_path': db_path,
        'new_id': new_id,
        'utc_now_iso': utc_now_iso,
    }


def create_generation_job_core(
    *,
    actor_id: str,
    trigger_type: str,
    initial_status: str,
    generation_journal_service,
    generation_journal_deps,
) -> str:
    return generation_journal_service.create_generation_job_core(
        actor_id=actor_id,
        trigger_type=trigger_type,
        initial_status=initial_status,
        deps=generation_journal_deps,
    )


def mark_generation_job_started_core(
    *,
    job_id: str,
    generation_journal_service,
    generation_journal_deps,
) -> None:
    generation_journal_service.mark_generation_job_started_core(
        job_id=job_id,
        deps=generation_journal_deps,
    )


def finalize_generation_job_core(
    *,
    job_id: str,
    status: str,
    imported_sources: int,
    duration_ms: int,
    final_message: str,
    error_message: str,
    generation_journal_service,
    generation_journal_deps,
) -> None:
    generation_journal_service.finalize_generation_job_core(
        job_id=job_id,
        status=status,
        imported_sources=imported_sources,
        duration_ms=duration_ms,
        final_message=final_message,
        error_message=error_message,
        deps=generation_journal_deps,
    )


def start_generation_phase_core(
    *,
    actor_id: str,
    job_id: str,
    phase_key: str,
    phase_label: str,
    attempt: int,
    message: str,
    generation_journal_service,
    generation_journal_deps,
) -> str:
    return generation_journal_service.start_generation_phase_core(
        job_id=job_id,
        actor_id=actor_id,
        phase_key=phase_key,
        phase_label=phase_label,
        attempt=attempt,
        message=message,
        deps=generation_journal_deps,
    )


def finish_generation_phase_core(
    *,
    phase_id: str,
    status: str,
    message: str,
    error_detail: str,
    duration_ms: int | None,
    generation_journal_service,
    generation_journal_deps,
) -> None:
    generation_journal_service.finish_generation_phase_core(
        phase_id=phase_id,
        status=status,
        message=message,
        error_detail=error_detail,
        duration_ms=duration_ms,
        deps=generation_journal_deps,
    )
