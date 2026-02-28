from pathlib import Path


def mitre_dataset_path_core(
    *,
    configure_mitre_store,
    env_path: str,
    db_path: str,
) -> Path:
    configure_mitre_store()
    path = Path(env_path.strip()) if env_path.strip() else None
    if path is not None:
        return path
    return Path(db_path).resolve().parent / 'mitre_enterprise_attack.json'


def ensure_mitre_attack_dataset_core(*, with_mitre_store_sync, mitre_store) -> bool:
    return with_mitre_store_sync(lambda: mitre_store.ensure_mitre_attack_dataset())


def load_mitre_groups_core(*, with_mitre_store_sync, mitre_store):
    return with_mitre_store_sync(lambda: mitre_store.load_mitre_groups())


def load_mitre_dataset_core(*, with_mitre_store_sync, mitre_store):
    return with_mitre_store_sync(lambda: mitre_store.load_mitre_dataset())


def mitre_campaign_link_index_core(*, with_mitre_store_sync, mitre_store):
    return with_mitre_store_sync(lambda: mitre_store.mitre_campaign_link_index())


def normalize_technique_id_core(value: str, *, mitre_store) -> str:
    return mitre_store.normalize_technique_id(value)


def mitre_technique_index_core(*, with_mitre_store_sync, mitre_store):
    return with_mitre_store_sync(lambda: mitre_store.mitre_technique_index())


def mitre_valid_technique_ids_core(*, with_mitre_store_sync, mitre_store):
    return with_mitre_store_sync(lambda: mitre_store.mitre_valid_technique_ids())


def mitre_technique_phase_index_core(*, with_mitre_store_sync, mitre_store):
    return with_mitre_store_sync(lambda: mitre_store.mitre_technique_phase_index())


def capability_category_from_technique_id_core(
    ttp_id: str,
    *,
    with_mitre_store_sync,
    mitre_store,
    attack_tactic_to_capability_map: dict[str, str],
    capability_grid_keys: list[str],
) -> str | None:
    return with_mitre_store_sync(
        lambda: mitre_store.capability_category_from_technique_id(
            ttp_id,
            attack_tactic_to_capability_map=attack_tactic_to_capability_map,
            capability_grid_keys=capability_grid_keys,
        )
    )


def match_mitre_group_core(actor_name: str, *, with_mitre_store_sync, mitre_store):
    return with_mitre_store_sync(lambda: mitre_store.match_mitre_group(actor_name))


def load_mitre_software_core(*, with_mitre_store_sync, mitre_store):
    return with_mitre_store_sync(lambda: mitre_store.load_mitre_software())


def match_mitre_software_core(name: str, *, with_mitre_store_sync, mitre_store):
    return with_mitre_store_sync(lambda: mitre_store.match_mitre_software(name))


def build_actor_profile_from_mitre_core(
    actor_name: str,
    *,
    with_mitre_store_sync,
    mitre_store,
    first_sentences,
):
    return with_mitre_store_sync(
        lambda: mitre_store.build_actor_profile_from_mitre(
            actor_name,
            first_sentences=first_sentences,
        )
    )


def group_top_techniques_core(group_stix_id: str, *, limit: int, with_mitre_store_sync, mitre_store):
    return with_mitre_store_sync(lambda: mitre_store.group_top_techniques(group_stix_id, limit=limit))


def known_technique_ids_for_entity_core(entity_stix_id: str, *, with_mitre_store_sync, mitre_store):
    return with_mitre_store_sync(lambda: mitre_store.known_technique_ids_for_entity(entity_stix_id))


def favorite_attack_vectors_core(
    techniques: list[dict[str, str]],
    *,
    limit: int,
    configure_mitre_store,
    mitre_store,
) -> list[str]:
    configure_mitre_store()
    return mitre_store.favorite_attack_vectors(techniques, limit=limit)
