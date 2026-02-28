"""
Domain allowlists, confidence tiers, and keyword catalogs for ActorWatch.

All domain lists are defined in their complete final state (no post-hoc mutations).
"""

# --- Actor search and trust domains ---

ACTOR_SEARCH_DOMAINS: list[str] = [
    # Government / CERT
    'cisa.gov',
    'fbi.gov',
    'ncsc.gov.uk',
    'jpcert.or.jp',
    'jvn.jp',
    'cert.ssi.gouv.fr',
    # News / media
    'bleepingcomputer.com',
    'thehackernews.com',
    'therecord.media',
    'securityweek.com',
    'darkreading.com',
    'krebsonsecurity.com',
    'cyberscoop.com',
    'isc.sans.edu',
    # Primary CTI vendors
    'mandiant.com',
    'crowdstrike.com',
    'sentinelone.com',
    'talosintelligence.com',
    'unit42.paloaltonetworks.com',
    'microsoft.com',
    'securelist.com',
    'cloud.google.com',
    'proofpoint.com',
    'redcanary.com',
    'huntress.com',
    'arcticwolf.com',
    'rapid7.com',
    'sophos.com',
    'trendmicro.com',
    'welivesecurity.com',
    'eset.com',
    # DFIR and research
    'thedfirreport.com',
    'recordedfuture.com',
    'intel471.com',
    'sygnia.co',
    'checkpoint.com',
    'malwarebytes.com',
    'zerodayinitiative.com',
    'netwitness.com',
    'corelight.com',
    'eclecticiq.com',
    'levelblue.com',
    # Vendors / advisories
    'cisco.com',
    'fortinet.com',
    'ivanti.com',
    # Live threat intel
    'ransomware.live',
]

TRUSTED_ACTIVITY_DOMAINS: set[str] = set(ACTOR_SEARCH_DOMAINS + ['attack.mitre.org'])

HIGH_CONFIDENCE_SOURCE_DOMAINS: set[str] = {
    'cisa.gov',
    'fbi.gov',
    'ncsc.gov.uk',
    'attack.mitre.org',
    'jpcert.or.jp',
    'jvn.jp',
    'cert.ssi.gouv.fr',
}

MEDIUM_CONFIDENCE_SOURCE_DOMAINS: set[str] = {
    'mandiant.com',
    'crowdstrike.com',
    'sentinelone.com',
    'talosintelligence.com',
    'unit42.paloaltonetworks.com',
    'microsoft.com',
    'securelist.com',
    'cloud.google.com',
    'proofpoint.com',
    'redcanary.com',
    'huntress.com',
    'arcticwolf.com',
    'rapid7.com',
    'sophos.com',
    'trendmicro.com',
    'welivesecurity.com',
    'eset.com',
    'cisco.com',
    'fortinet.com',
    'ivanti.com',
    'thedfirreport.com',
    'recordedfuture.com',
    'intel471.com',
    'sygnia.co',
    'checkpoint.com',
    'malwarebytes.com',
    'zerodayinitiative.com',
    'netwitness.com',
    'corelight.com',
    'eclecticiq.com',
    'levelblue.com',
}

SECONDARY_CONTEXT_DOMAINS: set[str] = {
    'bleepingcomputer.com',
    'thehackernews.com',
    'therecord.media',
    'darkreading.com',
    'krebsonsecurity.com',
    'isc.sans.edu',
    'securityweek.com',
    'cyberscoop.com',
    'ncsc.gov.uk',
}

# --- NLP / analysis config ---

QUESTION_SEED_KEYWORDS: list[str] = [
    'should review',
    'should detect',
    'organizations should',
    'mitigate',
    'look for',
    'monitor',
    'search for',
    'hunt for',
    'indicator',
    'ioc',
    'cve-',
    'ttp',
    'phish',
    'powershell',
    'wmi',
    'dns',
    'beacon',
    'exploit',
]

# --- MITRE ATT&CK capability mapping ---

CAPABILITY_GRID_KEYS: list[str] = [
    'initial_access',
    'persistence',
    'execution',
    'privilege_escalation',
    'defense_evasion',
    'command_and_control',
    'lateral_movement',
    'exfiltration',
    'impact',
    'tooling',
    'infrastructure',
    'targeting',
    'tempo',
]

BEHAVIORAL_MODEL_KEYS: list[str] = [
    'access_strategy',
    'tool_acquisition',
    'persistence_philosophy',
    'targeting_logic',
    'adaptation_pattern',
    'operational_tempo',
]

ATTACK_TACTIC_TO_CAPABILITY_MAP: dict[str, str] = {
    'reconnaissance': 'targeting',
    'resource_development': 'infrastructure',
    'initial_access': 'initial_access',
    'execution': 'execution',
    'persistence': 'persistence',
    'privilege_escalation': 'privilege_escalation',
    'defense_evasion': 'defense_evasion',
    'credential_access': 'privilege_escalation',
    'discovery': 'lateral_movement',
    'lateral_movement': 'lateral_movement',
    'collection': 'exfiltration',
    'command_and_control': 'command_and_control',
    'exfiltration': 'exfiltration',
    'impact': 'impact',
}
