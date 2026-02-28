import re
from datetime import datetime
from typing import Callable


def _behavior_id_from_context(context_text: str, primary_category: str) -> str:
    text = f'{primary_category} {context_text}'.lower()
    if any(token in text for token in ('phish', 'email', 'malspam', 'inbox', 'attachment')):
        return 'phishing'
    if any(token in text for token in ('ransom', 'encrypt', 'impact', 'vssadmin', 'wbadmin', 'bcdedit')):
        return 'impact'
    if any(token in text for token in ('exfil', 'data theft', 'archive', '7zip', 'rar', 'upload')):
        return 'exfiltration'
    if any(token in text for token in ('lateral', 'rdp', 'smb', 'wmic', 'psexec', 'pass the hash')):
        return 'lateral_movement'
    if any(token in text for token in ('powershell', 'script', 'cmd.exe', 'scheduled task', '4698', '4688', '4104')):
        return 'execution'
    if any(token in text for token in ('beacon', 'c2', 'dns', 'proxy', 'callback', 'command and control')):
        return 'command_and_control'
    return 'general_activity'


def _extract_behavior_observables(text: str) -> dict[str, list[str]]:
    normalized = str(text or '').lower()
    event_ids: list[str] = []
    for token in re.findall(r'\b(?:1[0-9]{3}|2[0-9]{3}|3[0-9]{3}|4[0-9]{3}|5[0-9]{3})\b', normalized):
        if token not in event_ids:
            event_ids.append(token)

    command_patterns: list[tuple[str, str]] = [
        (r'\bvssadmin\b', 'vssadmin'),
        (r'\bwbadmin\b', 'wbadmin'),
        (r'\bbcdedit\b', 'bcdedit'),
        (r'\bwmic\b.*\bshadowcopy\b.*\bdelete\b', 'wmic shadowcopy delete'),
        (r'\bnet\s+stop\b', 'net stop'),
        (r'\bfrombase64string\b', 'frombase64string'),
        (r'\b-enc\b', '-enc'),
        (r'\biex\b', 'iex'),
        (r'\bschtasks\b', 'schtasks'),
        (r'\bpsexec\b', 'psexec'),
        (r'\brdp\b', 'rdp'),
        (r'\bsmb\b', 'smb'),
        (r'\bpowershell\b', 'powershell'),
    ]
    commands: list[str] = []
    for pattern, label in command_patterns:
        if re.search(pattern, normalized):
            commands.append(label)

    behavior_markers: list[str] = []
    marker_patterns: list[tuple[str, str]] = [
        ('recovery inhibition', r'\brecovery\s+inhibit\w*\b|\bvssadmin\b|\bwbadmin\b|\bbcdedit\b'),
        ('service stop burst', r'\bnet\s+stop\b|\bservice\s+stop\b'),
        ('encoded execution', r'\b-enc\b|\bfrombase64string\b|\bencoded\b|\bpowershell\b'),
        ('remote logon pivot', r'\bremote\s+logon\b|\blateral\b|\brdp\b|\bpsexec\b|\bwmic\b'),
        ('beacon recurrence', r'\bbeacon(?:ing)?\b|\bcallback\b|\bc2\b|\bcommand\s+and\s+control\b'),
        ('archive staging', r'\barchive\b|\b7zip\b|\brar\b|\bstag(?:e|ing)\b'),
        ('outbound upload', r'\bupload\b|\bexfil\w*\b|\boutbound\b'),
    ]
    for label, pattern in marker_patterns:
        if re.search(pattern, normalized):
            behavior_markers.append(label)

    return {
        'event_ids': event_ids[:8],
        'commands': commands[:10],
        'markers': behavior_markers[:6],
    }


QUICK_CHECK_TEMPLATE_HINTS: dict[str, dict[str, list[str]]] = {
    'impact': {
        'event_ids': ['4688', '4104', '4698'],
        'log_sources': ['Windows Security', 'PowerShell Script Block'],
    },
    'execution': {
        'event_ids': ['4104', '4688', '4698'],
        'log_sources': ['Windows Security', 'PowerShell Script Block'],
    },
    'lateral_movement': {
        'event_ids': ['4624', '4648', '4672'],
        'log_sources': ['Windows Security'],
    },
    'command_and_control': {
        'event_ids': ['5156'],
        'log_sources': ['Windows Security', 'DNS/Proxy'],
    },
    'exfiltration': {
        'event_ids': ['4688'],
        'log_sources': ['Windows Security', 'Proxy'],
    },
    'phishing': {
        'event_ids': [],
        'log_sources': ['Email Gateway', 'Identity Sign-in'],
    },
    'general_activity': {
        'event_ids': [],
        'log_sources': ['Windows Security'],
    },
}


def _format_evidence_ref_core(*, title: str, date_value: str, url: str) -> str:
    clean_title = str(title or '').strip() or 'Untitled report'
    clean_url = str(url or '').strip()
    clean_date = str(date_value or '').strip()
    if clean_date and 'T' in clean_date:
        clean_date = clean_date.split('T', 1)[0]
    if clean_url:
        return f'{clean_title} | {clean_date or "unknown date"} | {clean_url}'
    return f'{clean_title} | {clean_date or "unknown date"}'


def _quick_check_is_evidence_backed_core(
    *,
    evidence_refs: list[dict[str, str]],
    observables: dict[str, list[str]],
) -> bool:
    has_refs = len(evidence_refs) >= 1
    observable_count = sum(len(observables.get(key, [])) for key in ('event_ids', 'commands', 'markers'))
    has_observables = observable_count >= 1
    # Keep cards populated when multiple corroborating sources exist but explicit
    # host-observable tokens are not extracted yet.
    return has_refs and (has_observables or len(evidence_refs) >= 2)


def _quick_check_update_effective_dt(
    update: dict[str, object],
    *,
    parse_published_datetime: Callable[[str], datetime | None],
) -> datetime | None:
    for key in ('source_published_at', 'source_ingested_at', 'source_retrieved_at', 'created_at'):
        parsed = parse_published_datetime(str(update.get(key) or ''))
        if parsed is not None:
            return parsed
    return None


def _select_event_ids_for_where_to_start_core(
    *,
    evidence_event_ids: list[str],
    template_hint_event_ids: list[str],
) -> dict[str, object]:
    evidence_ids = [str(item).strip() for item in evidence_event_ids if str(item).strip()]
    hint_ids = [str(item).strip() for item in template_hint_event_ids if str(item).strip()]
    if evidence_ids:
        return {
            'mode': 'evidence',
            'event_ids': evidence_ids[:6],
            'line': f'Event IDs {", ".join(evidence_ids[:6])}',
        }
    if hint_ids:
        return {
            'mode': 'baseline',
            'event_ids': hint_ids[:6],
            'line': f'Baseline suggestion: Event IDs {", ".join(hint_ids[:6])} (not evidence-linked)',
        }
    return {
        'mode': 'data_gap',
        'event_ids': [],
        'line': 'Data gap: no evidence-derived event IDs in last 30 days. Validate logging coverage.',
    }


def _behavior_query_pack(
    *,
    behavior_id: str,
    ioc_values: list[str],
    event_ids: list[str],
) -> list[dict[str, str]]:
    ioc_clause = ' or '.join([value for value in ioc_values if value]) if ioc_values else ''
    event_id_clause = ', '.join(event_ids[:6]) if event_ids else '4104, 4688, 4624, 4698'

    if behavior_id == 'impact':
        return [
            {
                'platform': 'Splunk',
                'why_this_query': 'Find likely impact preparation commands and cluster repeats by host/user.',
                'query': (
                    'index=* sourcetype=WinEventLog:Security EventCode=4688 '
                    '| eval cmd=lower(coalesce(CommandLine, Process_Command_Line, NewProcessName)) '
                    '| search cmd="*vssadmin*" OR cmd="*wbadmin*" OR cmd="*bcdedit*" OR cmd="*net stop*" '
                    'OR cmd="*wmic*shadowcopy*delete*" '
                    '| stats count min(_time) as first_seen max(_time) as last_seen values(cmd) as commands by host user '
                    '| convert ctime(first_seen) ctime(last_seen) | sort - count'
                ),
            },
            {
                'platform': 'Sentinel (KQL)',
                'why_this_query': 'Detect impact-aligned process execution with explicit recovery-inhibit tooling.',
                'query': (
                    'SecurityEvent | where TimeGenerated >= ago(24h) | where EventID == 4688 '
                    '| extend cmd = tolower(coalesce(ProcessCommandLine, CommandLine, NewProcessName)) '
                    '| where cmd has "vssadmin" or cmd has "wbadmin" or cmd has "bcdedit" '
                    'or cmd has "net stop" or (cmd has "wmic" and cmd has "shadowcopy" and cmd has "delete") '
                    '| summarize repeat_count_24h=count(), first_seen_24h=min(TimeGenerated), '
                    'last_seen_24h=max(TimeGenerated), sample_cmds=make_set(cmd, 20) by Computer, Account '
                    '| order by repeat_count_24h desc'
                ),
            },
            {
                'platform': 'Elastic',
                'why_this_query': 'Fast filter for impact tooling commands in process creation telemetry.',
                'query': (
                    'event.code:4688 and process.command_line:(*vssadmin* or *wbadmin* or *bcdedit* '
                    'or *"net stop"* or (*wmic* and *shadowcopy* and *delete*))'
                ),
            },
        ]

    if behavior_id == 'phishing':
        return [
            {
                'platform': 'Splunk',
                'why_this_query': 'Cluster suspicious sender/domain activity to identify campaign repetition.',
                'query': (
                    'index=* (sourcetype=o365:management:activity OR sourcetype=ms:defender:email) '
                    '| eval sender=lower(coalesce(SenderFromAddress, SenderMailFromAddress, sender, user)) '
                    '| eval subject=coalesce(Subject, subject, MessageSubject) '
                    '| search sender="*@*" '
                    '| stats count values(subject) as subjects values(RecipientEmailAddress) as recipients by sender '
                    '| sort - count'
                ),
            },
            {
                'platform': 'Sentinel (KQL)',
                'why_this_query': 'Detect repeated suspicious sender + subject patterns over 24h.',
                'query': (
                    'EmailEvents | where TimeGenerated >= ago(24h) '
                    '| extend sender=tolower(SenderFromAddress), subject=tostring(Subject) '
                    '| summarize repeat_count_24h=count(), subjects=make_set(subject, 20), '
                    'first_seen_24h=min(TimeGenerated), last_seen_24h=max(TimeGenerated) '
                    'by sender, RecipientEmailAddress '
                    '| order by repeat_count_24h desc'
                ),
            },
            {
                'platform': 'Elastic',
                'why_this_query': 'Quick filter for suspicious inbound sender and phishing markers.',
                'query': (
                    '(email.from.address:*@* and email.subject:(*invoice* or *urgent* or *payment* or *password*))'
                ),
            },
        ]

    if behavior_id == 'lateral_movement':
        return [
            {
                'platform': 'Splunk',
                'why_this_query': 'Identify repeated remote logon pivots by host/user pairs.',
                'query': (
                    'index=* sourcetype=WinEventLog:Security (EventCode=4624 OR EventCode=4648 OR EventCode=4672) '
                    '| eval logon_type=coalesce(Logon_Type, LogonType) '
                    '| search logon_type=3 OR logon_type=10 '
                    '| stats count min(_time) as first_seen max(_time) as last_seen values(logon_type) as logon_types by host user src_ip '
                    '| convert ctime(first_seen) ctime(last_seen) | sort - count'
                ),
            },
            {
                'platform': 'Sentinel (KQL)',
                'why_this_query': 'Surface unusual privileged remote logons and host-to-host pivots.',
                'query': (
                    'SecurityEvent | where TimeGenerated >= ago(24h) '
                    '| where EventID in (4624, 4648, 4672) '
                    '| extend logonType=tostring(LogonType) '
                    '| where logonType in ("3","10") '
                    '| summarize repeat_count_24h=count(), first_seen_24h=min(TimeGenerated), '
                    'last_seen_24h=max(TimeGenerated) by Computer, Account, IpAddress, EventID '
                    '| order by repeat_count_24h desc'
                ),
            },
            {
                'platform': 'Elastic',
                'why_this_query': 'Filter remote logon events tied to likely lateral movement.',
                'query': 'event.code:(4624 or 4648 or 4672) and winlog.event_data.LogonType:(3 or 10)',
            },
        ]

    if behavior_id == 'exfiltration':
        return [
            {
                'platform': 'Splunk',
                'why_this_query': 'Find suspicious archiving plus outbound transfer spikes.',
                'query': (
                    'index=* (sourcetype=WinEventLog:Security EventCode=4688 OR sourcetype=proxy*) '
                    '| eval cmd=lower(coalesce(CommandLine, Process_Command_Line, NewProcessName)) '
                    '| eval host_key=coalesce(host, ComputerName, dvc) '
                    '| search cmd="*7z*" OR cmd="*rar*" OR cmd="*winrar*" OR cmd="*tar*" OR uri_path="*upload*" '
                    '| stats count min(_time) as first_seen max(_time) as last_seen values(cmd) as indicators by host_key user dest '
                    '| convert ctime(first_seen) ctime(last_seen) | sort - count'
                ),
            },
            {
                'platform': 'Sentinel (KQL)',
                'why_this_query': 'Correlate archive tooling with network egress indicators in 24h.',
                'query': (
                    'union isfuzzy=true SecurityEvent, DeviceNetworkEvents '
                    '| where TimeGenerated >= ago(24h) '
                    '| extend cmd=tolower(tostring(ProcessCommandLine)), dest=tostring(RemoteUrl) '
                    '| where cmd has "7z" or cmd has "rar" or cmd has "winrar" or cmd has "tar" or dest has "upload" '
                    '| summarize repeat_count_24h=count(), first_seen_24h=min(TimeGenerated), '
                    'last_seen_24h=max(TimeGenerated), indicators=make_set(coalesce(cmd,dest), 30) by Computer, Account '
                    '| order by repeat_count_24h desc'
                ),
            },
            {
                'platform': 'Elastic',
                'why_this_query': 'Quick filter for compression tooling and likely upload behavior.',
                'query': 'process.command_line:(*7z* or *rar* or *winrar* or *tar*) or url.path:*upload*',
            },
        ]

    if behavior_id == 'execution':
        return [
            {
                'platform': 'Splunk',
                'why_this_query': 'Detect encoded/scripted execution and suspicious task creation patterns.',
                'query': (
                    'index=* sourcetype=WinEventLog:Security (EventCode=4104 OR EventCode=4688 OR EventCode=4698) '
                    '| eval cmd=lower(coalesce(CommandLine, ScriptBlockText, Process_Command_Line, NewProcessName)) '
                    '| search cmd="*-enc*" OR cmd="*frombase64string*" OR cmd="*iex*" OR cmd="*schtasks*" '
                    '| stats count min(_time) as first_seen max(_time) as last_seen values(cmd) as commands by host user EventCode '
                    '| convert ctime(first_seen) ctime(last_seen) | sort - count'
                ),
            },
            {
                'platform': 'Sentinel (KQL)',
                'why_this_query': 'Find encoded PowerShell and scheduled task abuse linked to execution.',
                'query': (
                    'SecurityEvent | where TimeGenerated >= ago(24h) | where EventID in (4104, 4688, 4698) '
                    '| extend cmd=tolower(coalesce(ProcessCommandLine, CommandLine, NewProcessName, ScriptBlockText)) '
                    '| where cmd has "-enc" or cmd has "frombase64string" or cmd has "iex" or cmd has "schtasks" '
                    '| summarize repeat_count_24h=count(), first_seen_24h=min(TimeGenerated), '
                    'last_seen_24h=max(TimeGenerated), sample_cmds=make_set(cmd, 20) by Computer, Account, EventID '
                    '| order by repeat_count_24h desc'
                ),
            },
            {
                'platform': 'Elastic',
                'why_this_query': 'Filter encoded/script-host execution for analyst review.',
                'query': (
                    'event.code:(4104 or 4688 or 4698) and process.command_line:(*-enc* or *frombase64string* or *iex* or *schtasks*)'
                ),
            },
        ]

    query_hint = f'Event ID scope: {event_id_clause}.'
    if ioc_clause:
        query_hint += f' IOC pivots: {ioc_clause}.'
    return [
        {
            'platform': 'Splunk',
            'why_this_query': 'Baseline suspicious repeated host/user activity in Windows logs.',
            'query': (
                'index=* sourcetype=WinEventLog:Security '
                f'({" OR ".join([f"EventCode={event_id}" for event_id in (event_ids or ["4104", "4688", "4624", "4698"])[:4]])}) '
                '| stats count min(_time) as first_seen max(_time) as last_seen by host user EventCode '
                '| convert ctime(first_seen) ctime(last_seen) | sort - count'
            ),
        },
        {
            'platform': 'Sentinel (KQL)',
            'why_this_query': 'Baseline repeated account/host patterns across key event IDs.',
            'query': (
                'SecurityEvent | where TimeGenerated >= ago(24h) '
                f'| where EventID in ({", ".join((event_ids or ["4104", "4688", "4624", "4698"])[:4])}) '
                '| summarize repeat_count_24h=count(), first_seen_24h=min(TimeGenerated), '
                'last_seen_24h=max(TimeGenerated) by Computer, Account, EventID '
                '| order by repeat_count_24h desc'
            ),
        },
        {
            'platform': 'Elastic',
            'why_this_query': query_hint,
            'query': (
                'event.code:('
                + ' or '.join((event_ids or ['4104', '4688', '4624', '4698'])[:4])
                + ')'
            ),
        },
    ]
