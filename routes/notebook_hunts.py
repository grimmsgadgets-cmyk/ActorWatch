import csv
import io
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

import route_paths
import services.quick_checks_view_service as quick_checks_view_service
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response


def register_notebook_hunts_routes(*, router: APIRouter, deps: dict[str, object]) -> None:
    _fetch_actor_notebook = deps['fetch_actor_notebook']
    _db_path = deps['db_path']
    _load_environment_profile = deps['load_environment_profile']
    _generate_ioc_hunt_queries = deps['generate_ioc_hunt_queries']
    _get_ollama_status = deps['get_ollama_status']
    _templates = deps['templates']
    _ioc_value_is_hunt_relevant = deps['ioc_value_is_hunt_relevant']

    @router.get(route_paths.ACTOR_IOC_HUNT_QUERIES, response_class=HTMLResponse)
    def actor_ioc_hunt_queries(
        request: Request,
        actor_id: str,
        quick_check_id: str | None = None,
        check_template_id: str | None = None,
        thread_id: str | None = None,
        window_days: int = 30,
        window_start: str | None = None,
        window_end: str | None = None,
        ioc_type: str | None = None,
        confidence: str | None = None,
        source_count: str | None = None,
        freshness: str | None = None,
        export: str | None = None,
        source_tier: str | None = None,
        min_confidence_weight: str | None = None,
        source_days: str | None = None,
        query_lookback_hours: str | None = None,
    ) -> Response:
        selected_check_id = str(quick_check_id or check_template_id or thread_id or '').strip()
        try:
            safe_window_days = max(1, min(365, int(window_days or 30)))
        except Exception:
            safe_window_days = 30
        now_utc = datetime.now(timezone.utc)
        parsed_start: datetime | None = None
        parsed_end: datetime | None = None
        try:
            if str(window_start or '').strip():
                parsed_start = datetime.fromisoformat(str(window_start).strip().replace('Z', '+00:00'))
                if parsed_start.tzinfo is None:
                    parsed_start = parsed_start.replace(tzinfo=timezone.utc)
                parsed_start = parsed_start.astimezone(timezone.utc)
        except Exception:
            parsed_start = None
        try:
            if str(window_end or '').strip():
                parsed_end = datetime.fromisoformat(str(window_end).strip().replace('Z', '+00:00'))
                if parsed_end.tzinfo is None:
                    parsed_end = parsed_end.replace(tzinfo=timezone.utc)
                parsed_end = parsed_end.astimezone(timezone.utc)
        except Exception:
            parsed_end = None
        if parsed_start is not None and parsed_end is not None and parsed_start <= parsed_end:
            window_start_dt = parsed_start
            window_end_dt = parsed_end
        else:
            window_start_dt, window_end_dt = quick_checks_view_service.window_bounds_core(
                now=now_utc,
                window_days=safe_window_days,
            )
        window_start_iso = window_start_dt.isoformat()
        window_end_iso = window_end_dt.isoformat()
        notebook = _fetch_actor_notebook(
            actor_id,
            source_tier=source_tier,
            min_confidence_weight=min_confidence_weight,
            source_days=source_days or '30',
            build_on_cache_miss=False,
            allow_stale_cache=True,
        )
        actor_meta = notebook.get('actor', {}) if isinstance(notebook, dict) else {}
        actor_name = str(actor_meta.get('display_name') or actor_id)
        cards_raw = notebook.get('priority_questions', []) if isinstance(notebook, dict) else []
        cards_list = cards_raw if isinstance(cards_raw, list) else []
        if selected_check_id:
            cards_list = [card for card in cards_list if str(card.get('id') or '').strip() == selected_check_id]

        cards_by_id = {
            str(card.get('id') or '').strip(): card
            for card in cards_list
            if isinstance(card, dict) and str(card.get('id') or '').strip()
        }
        selected_card = cards_by_id.get(selected_check_id) if selected_check_id else None
        selected_card_text = (
            ' '.join(
                [
                    str(selected_card.get('quick_check_title') or '') if isinstance(selected_card, dict) else '',
                    str(selected_card.get('question_text') or '') if isinstance(selected_card, dict) else '',
                    str(selected_card.get('behavior_to_hunt') or '') if isinstance(selected_card, dict) else '',
                    str(selected_card.get('what_to_watch') or '') if isinstance(selected_card, dict) else '',
                ]
            ).strip().lower()
        )
        relevant_types = {
            str(ioc.get('ioc_type') or '').strip().lower()
            for ioc in (selected_card.get('related_iocs') if isinstance(selected_card, dict) and isinstance(selected_card.get('related_iocs'), list) else [])
            if isinstance(ioc, dict) and str(ioc.get('ioc_type') or '').strip()
        }
        relevant_values = {
            str(ioc.get('ioc_value') or '').strip().lower()
            for ioc in (selected_card.get('related_iocs') if isinstance(selected_card, dict) and isinstance(selected_card.get('related_iocs'), list) else [])
            if isinstance(ioc, dict) and str(ioc.get('ioc_value') or '').strip()
        }

        cards_for_hunts: list[dict[str, object]] = []
        card_behavior_queries: dict[str, list[dict[str, str]]] = {}
        environment_profile: dict[str, object] = {}
        with sqlite3.connect(_db_path()) as connection:
            environment_profile = _load_environment_profile(connection, actor_id=actor_id)
            for card in cards_list:
                if not isinstance(card, dict):
                    continue
                card_id = str(card.get('id') or '').strip()
                if not card_id:
                    continue
                behavior_queries_raw = card.get('behavior_queries')
                behavior_queries_list = behavior_queries_raw if isinstance(behavior_queries_raw, list) else []
                clean_behavior_queries: list[dict[str, str]] = []
                for query_item in behavior_queries_list:
                    if not isinstance(query_item, dict):
                        continue
                    platform = str(query_item.get('platform') or '').strip()
                    query = str(query_item.get('query') or '').strip()
                    why_this_query = str(query_item.get('why_this_query') or '').strip()
                    if not platform or not query:
                        continue
                    clean_behavior_queries.append(
                        {
                            'platform': platform[:80],
                            'query': query[:1200],
                            'why_this_query': why_this_query[:220],
                        }
                    )
                if clean_behavior_queries:
                    card_behavior_queries[card_id] = clean_behavior_queries[:6]
                related_iocs_raw = card.get('related_iocs')
                related_iocs = related_iocs_raw if isinstance(related_iocs_raw, list) else []
                evidence_rows = connection.execute(
                    '''
                    SELECT qu.source_id, qu.trigger_excerpt, s.url, s.title, s.headline, s.og_title, s.html_title, s.published_at
                    FROM question_updates qu
                    JOIN sources s ON s.id = qu.source_id
                    WHERE qu.thread_id = ?
                      AND s.actor_id = ?
                      AND COALESCE(s.published_at, s.ingested_at, s.retrieved_at, '') >= ?
                    ORDER BY qu.created_at DESC
                    LIMIT 8
                    ''',
                    (card_id, actor_id, window_start_iso),
                ).fetchall()
                evidence_items: list[dict[str, str]] = []
                seen_evidence_ids: set[str] = set()
                for row in evidence_rows:
                    evidence_id = str(row[0] or '').strip()
                    source_url = str(row[2] or '').strip()
                    if not evidence_id or not source_url or evidence_id in seen_evidence_ids:
                        continue
                    seen_evidence_ids.add(evidence_id)
                    evidence_items.append(
                        {
                            'id': evidence_id,
                            'source_url': source_url,
                            'source_title': str(row[3] or row[4] or row[5] or row[6] or source_url),
                            'source_date': str(row[7] or ''),
                            'excerpt': str(row[1] or '')[:320],
                        }
                    )

                if not related_iocs or not evidence_items:
                    continue

                cards_for_hunts.append(
                    {
                        'id': card_id,
                        'check_template_id': card_id,
                        'quick_check_title': str(card.get('quick_check_title') or card.get('question_text') or ''),
                        'question_text': str(card.get('question_text') or ''),
                        'related_iocs': related_iocs[:8],
                        'evidence': evidence_items[:10],
                    }
                )

        hunt_payload = _generate_ioc_hunt_queries(
            actor_name,
            cards_for_hunts,
            environment_profile=environment_profile,
        )
        hunt_by_card = hunt_payload.get('items_by_card', {}) if isinstance(hunt_payload, dict) else {}
        reason = str(hunt_payload.get('reason') or '') if isinstance(hunt_payload, dict) else ''
        ollama_status = _get_ollama_status()
        evidence_map_by_card: dict[str, list[dict[str, object]]] = {
            str(card.get('id') or ''): (card.get('evidence') if isinstance(card.get('evidence'), list) else [])
            for card in cards_for_hunts
            if isinstance(card, dict) and str(card.get('id') or '').strip()
        }
        ioc_map_by_card: dict[str, list[dict[str, object]]] = {
            str(card.get('id') or ''): (card.get('related_iocs') if isinstance(card.get('related_iocs'), list) else [])
            for card in cards_for_hunts
            if isinstance(card, dict) and str(card.get('id') or '').strip()
        }
        used_ioc_pairs = {
            (
                str(ioc.get('ioc_type') or '').strip().lower(),
                str(ioc.get('ioc_value') or '').strip().lower(),
            )
            for iocs in ioc_map_by_card.values()
            for ioc in (iocs if isinstance(iocs, list) else [])
            if isinstance(ioc, dict)
        }
        ioc_items_raw = notebook.get('ioc_items', []) if isinstance(notebook, dict) else []
        ioc_items = ioc_items_raw if isinstance(ioc_items_raw, list) else []
        filtered_iocs = quick_checks_view_service.filter_iocs_for_check_core(
            ioc_items,
            relevant_types=relevant_types,
            relevant_values=relevant_values,
        )
        iocs_in_window: list[dict[str, object]] = []
        for ioc in filtered_iocs:
            if not isinstance(ioc, dict):
                continue
            ioc_type_value = str(ioc.get('ioc_type') or '').strip().lower()
            ioc_value = str(ioc.get('ioc_value') or '').strip()
            if not ioc_type_value or not ioc_value:
                continue
            last_seen_raw = str(ioc.get('last_seen_at') or ioc.get('created_at') or '')
            if not quick_checks_view_service.is_in_window_core(
                last_seen_raw,
                window_start=window_start_dt,
                window_end=window_end_dt,
            ):
                continue
            iocs_in_window.append(ioc)

        ioc_buckets: dict[str, list[str]] = {
            'domain': [],
            'url': [],
            'ip': [],
            'hash': [],
            'email': [],
        }
        for ioc in iocs_in_window:
            ioc_type_value = str(ioc.get('ioc_type') or '').strip().lower()
            ioc_value = str(ioc.get('ioc_value') or '').strip()
            if ioc_type_value in ioc_buckets and ioc_value:
                if ioc_value not in ioc_buckets[ioc_type_value]:
                    ioc_buckets[ioc_type_value].append(ioc_value)
        for key in ioc_buckets:
            ioc_buckets[key] = ioc_buckets[key][:25]

        profile_default_lookback_hours = 24
        try:
            profile_default_lookback_hours = max(
                1,
                min(24 * 30, int(environment_profile.get('default_time_window_hours') or 24)),
            )
        except Exception:
            profile_default_lookback_hours = 24
        default_lookback_hours = profile_default_lookback_hours
        try:
            requested_lookback = int(str(query_lookback_hours or '').strip())
            if requested_lookback in {24, 24 * 7, 24 * 14, 24 * 30}:
                default_lookback_hours = requested_lookback
        except Exception:
            pass
        lookback_presets = [
            {'hours': 24, 'label': '24h', 'active': default_lookback_hours == 24},
            {'hours': 24 * 7, 'label': '7d', 'active': default_lookback_hours == 24 * 7},
            {'hours': 24 * 14, 'label': '14d', 'active': default_lookback_hours == 24 * 14},
            {'hours': 24 * 30, 'label': '30d', 'active': default_lookback_hours == 24 * 30},
        ]

        with sqlite3.connect(_db_path()) as connection:
            feedback_rows = connection.execute(
                '''
                SELECT item_id, COUNT(*), SUM(rating_score)
                FROM analyst_feedback_events
                WHERE actor_id = ? AND item_type = 'hunt_query'
                GROUP BY item_id
                ''',
                (actor_id,),
            ).fetchall()
        query_feedback_map: dict[str, dict[str, object]] = {
            str(row[0]): {'votes': int(row[1] or 0), 'score': int(row[2] or 0)}
            for row in feedback_rows
        }

        section_templates = [
            {
                'id': 'dns_proxy_web',
                'label': 'DNS/Proxy/Web',
                'required_data': 'DNS query logs, web proxy logs, HTTP gateway telemetry',
                'returns': 'Potential outbound C2/beaconing and suspicious web destination matches',
            },
            {
                'id': 'network',
                'label': 'Network',
                'required_data': 'Firewall flows, NetFlow/Zeek, IDS/IPS alerts',
                'returns': 'Network connections, destination patterns, and recurrent communication paths',
            },
            {
                'id': 'endpoint_edr',
                'label': 'Endpoint/EDR',
                'required_data': 'EDR process telemetry, command-line logs, host events',
                'returns': 'Host/process execution tied to IOC or behavior patterns',
            },
            {
                'id': 'identity',
                'label': 'Identity',
                'required_data': 'Identity provider sign-in logs, auth events, directory audit logs',
                'returns': 'Suspicious account auth activity and anomalous access behavior',
            },
        ]
        platform_tabs = [
            {'key': 'generic', 'label': 'Generic (Vendor-neutral)'},
            {'key': 'sentinel', 'label': 'Sentinel KQL'},
            {'key': 'splunk', 'label': 'Splunk SPL'},
            {'key': 'elastic', 'label': 'Elastic KQL/ES|QL'},
        ]
        platform_keys = [str(item['key']) for item in platform_tabs]
        section_views: list[dict[str, object]] = []
        for section in section_templates:
            section_views.append(
                {
                    **section,
                    'platforms': {key: [] for key in platform_keys},
                }
            )
        section_by_id = {str(section['id']): section for section in section_views}

        def _platform_key(value: str) -> str:
            lowered = str(value or '').strip().lower()
            if any(token in lowered for token in ('generic', 'vendor-neutral', 'pseudocode')):
                return 'generic'
            if 'kql' in lowered or 'sentinel' in lowered:
                return 'sentinel'
            if 'splunk' in lowered or 'spl' in lowered:
                return 'splunk'
            if 'elastic' in lowered or 'es|ql' in lowered:
                return 'elastic'
            return 'generic'

        def _section_id_for_query(*, card: dict[str, object], query_item: dict[str, object]) -> str:
            text_blob = ' '.join(
                [
                    str(card.get('quick_check_title') or card.get('question_text') or ''),
                    str(query_item.get('why_this_query') or ''),
                    str(query_item.get('query') or ''),
                    str(query_item.get('ioc_value') or ''),
                ]
            ).lower()
            if any(token in text_blob for token in ('dns', 'proxy', 'domain', 'url', 'web')):
                return 'dns_proxy_web'
            if any(token in text_blob for token in ('sign-in', 'signin', 'identity', 'account', 'auth', 'logon')):
                return 'identity'
            if any(token in text_blob for token in ('process', 'powershell', 'edr', 'endpoint', 'commandline', 'eventid')):
                return 'endpoint_edr'
            return 'network'

        def _kql_dynamic(values: list[str]) -> str:
            escaped = [str(value).replace("\\", "\\\\").replace("'", "\\'") for value in values if str(value).strip()]
            if not escaped:
                return 'dynamic([])'
            return "dynamic([" + ', '.join([f"'{value}'" for value in escaped]) + "])"

        def _splunk_or(field: str, values: list[str]) -> str:
            escaped = [str(value).replace('"', '\\"') for value in values if str(value).strip()]
            if not escaped:
                return 'false()'
            return '(' + ' OR '.join([f'{field}="{value}"' for value in escaped]) + ')'

        def _es_values(values: list[str]) -> str:
            escaped = [str(value).replace('\\', '\\\\').replace('"', '\\"') for value in values if str(value).strip()]
            if not escaped:
                return '"__no_ioc__"'
            return ', '.join([f'"{value}"' for value in escaped])

        def _section_required_data(section_id: str, platform: str) -> str:
            mapping = {
                ('dns_proxy_web', 'generic'): 'DNS + proxy + web logs with source, destination, and URL/domain fields',
                ('dns_proxy_web', 'sentinel'): 'DnsEvents, DeviceNetworkEvents, CommonSecurityLog (proxy/web gateway)',
                ('dns_proxy_web', 'splunk'): 'DNS logs (bind/infoblox/sysmon), proxy logs (zscaler/bluecoat), web gateway indexes',
                ('dns_proxy_web', 'elastic'): 'logs-dns*, logs-proxy*, logs-web* data streams',
                ('network', 'generic'): 'Firewall/flow telemetry with src/dst IP, port, protocol, and device context',
                ('network', 'sentinel'): 'CommonSecurityLog (firewall), DeviceNetworkEvents, VMConnection/Zeek if available',
                ('network', 'splunk'): 'Firewall/NetFlow/Zeek indexes, IDS logs',
                ('network', 'elastic'): 'logs-network*, logs-firewall*, logs-zeek*',
                ('endpoint_edr', 'generic'): 'EDR/endpoint process and file telemetry with command line, hash, and user/host context',
                ('endpoint_edr', 'sentinel'): 'DeviceProcessEvents, DeviceFileEvents, SecurityEvent (4688/4104)',
                ('endpoint_edr', 'splunk'): 'EDR process/file telemetry, WinEventLog:Security, PowerShell logs',
                ('endpoint_edr', 'elastic'): 'logs-endpoint*, logs-windows*, logs-powershell*',
                ('identity', 'generic'): 'Identity/authentication logs with account, source IP, result, and app/resource context',
                ('identity', 'sentinel'): 'SigninLogs, IdentityLogonEvents, AuditLogs, SecurityEvent(4624/4625)',
                ('identity', 'splunk'): 'IdP sign-in indexes (AAD/Okta), Windows auth logs, directory audit logs',
                ('identity', 'elastic'): 'logs-identity*, logs-auth*, logs-audit*',
            }
            return mapping.get((section_id, platform), '')

        def _baseline_query_item(section_id: str, platform: str) -> dict[str, str]:
            domains = ioc_buckets.get('domain', [])
            urls = ioc_buckets.get('url', [])
            ips = ioc_buckets.get('ip', [])
            hashes = ioc_buckets.get('hash', [])
            emails = ioc_buckets.get('email', [])
            lookback = f'{default_lookback_hours}h'
            if platform == 'sentinel':
                if section_id == 'dns_proxy_web':
                    query = (
                        f"let lookback = {lookback};\n"
                        f"let domains = {_kql_dynamic(domains)};\n"
                        f"let urls = {_kql_dynamic(urls)};\n"
                        "union isfuzzy=true DnsEvents, DeviceNetworkEvents, CommonSecurityLog\n"
                        "| where TimeGenerated >= ago(lookback)\n"
                        "| extend match_domain=tostring(coalesce(Name, QueryName, DestinationHostName, RequestURL, RemoteUrl)), "
                        "match_url=tostring(coalesce(RemoteUrl, RequestURL, UrlOriginal))\n"
                        "| where (array_length(domains) > 0 and match_domain in~ (domains)) or (array_length(urls) > 0 and match_url has_any (urls))\n"
                        "| summarize hits=count(), first_seen=min(TimeGenerated), last_seen=max(TimeGenerated) by DeviceName, SourceIP, DestinationIP, match_domain, match_url\n"
                        "| sort by hits desc"
                    )
                    returns = 'Matches DNS/proxy/web telemetry to known domains/URLs and summarizes recurring destinations.'
                elif section_id == 'network':
                    query = (
                        f"let lookback = {lookback};\n"
                        f"let ips = {_kql_dynamic(ips)};\n"
                        f"let domains = {_kql_dynamic(domains)};\n"
                        "union isfuzzy=true CommonSecurityLog, DeviceNetworkEvents\n"
                        "| where TimeGenerated >= ago(lookback)\n"
                        "| extend dst_ip=tostring(coalesce(DestinationIP, RemoteIP, DestinationIP_s)), dst_host=tostring(coalesce(DestinationHostName, RemoteUrl, DestinationHostName_s))\n"
                        "| where (array_length(ips) > 0 and dst_ip in (ips)) or (array_length(domains) > 0 and dst_host has_any (domains))\n"
                        "| summarize conn_count=count(), first_seen=min(TimeGenerated), last_seen=max(TimeGenerated) by DeviceName, SourceIP, dst_ip, dst_host, Protocol\n"
                        "| sort by conn_count desc"
                    )
                    returns = 'Finds network flow and firewall connections to IOC destinations with recurrence and host pivots.'
                elif section_id == 'endpoint_edr':
                    query = (
                        f"let lookback = {lookback};\n"
                        f"let domains = {_kql_dynamic(domains)};\n"
                        f"let hashes = {_kql_dynamic(hashes)};\n"
                        "union isfuzzy=true DeviceProcessEvents, DeviceFileEvents, SecurityEvent\n"
                        "| where TimeGenerated >= ago(lookback)\n"
                        "| extend cmd=tostring(coalesce(ProcessCommandLine, CommandLine, NewProcessName)), file_hash=tostring(coalesce(SHA256, SHA1, MD5)), net_hint=tostring(coalesce(RemoteUrl, RemoteIP, DestinationHostName))\n"
                        "| where (array_length(hashes) > 0 and file_hash in (hashes)) or (array_length(domains) > 0 and net_hint has_any (domains))\n"
                        "| summarize hits=count(), first_seen=min(TimeGenerated), last_seen=max(TimeGenerated), commands=make_set(cmd,5) by DeviceName, InitiatingProcessAccountName, file_hash, net_hint\n"
                        "| sort by hits desc"
                    )
                    returns = 'Correlates endpoint process/file activity with IOC hashes/domains and highlights repeated host activity.'
                else:
                    query = (
                        f"let lookback = {lookback};\n"
                        f"let ips = {_kql_dynamic(ips)};\n"
                        f"let users = {_kql_dynamic(emails)};\n"
                        "union isfuzzy=true SigninLogs, IdentityLogonEvents, SecurityEvent\n"
                        "| where TimeGenerated >= ago(lookback)\n"
                        "| extend user=tostring(coalesce(UserPrincipalName, Account, TargetUserName)), src_ip=tostring(coalesce(IPAddress, SourceIP, IpAddress))\n"
                        "| where (array_length(users) > 0 and user in~ (users)) or (array_length(ips) > 0 and src_ip in (ips))\n"
                        "| summarize attempts=count(), failures=countif(ResultType !in ('0','Success')), first_seen=min(TimeGenerated), last_seen=max(TimeGenerated) by user, src_ip, AppDisplayName\n"
                        "| sort by failures desc, attempts desc"
                    )
                    returns = 'Surfaces suspicious identity sign-ins tied to IOC IPs/accounts and prioritizes failed-auth anomalies.'
            elif platform == 'splunk':
                if section_id == 'dns_proxy_web':
                    query = (
                        f"index=* earliest=-{default_lookback_hours}h "
                        f"({ _splunk_or('query', domains) } OR { _splunk_or('dest_domain', domains) } OR { _splunk_or('url', urls) }) "
                        "| eval match=coalesce(query,dest_domain,url,uri) "
                        "| stats count as hits min(_time) as first_seen max(_time) as last_seen values(src) as src values(dest) as dest by host user match "
                        "| convert ctime(first_seen) ctime(last_seen) | sort - hits"
                    )
                    returns = 'Finds DNS/proxy/web IOC matches and recurring source-to-destination patterns.'
                elif section_id == 'network':
                    query = (
                        f"index=* earliest=-{default_lookback_hours}h "
                        f"({ _splunk_or('dest_ip', ips) } OR { _splunk_or('dest', domains) }) "
                        "| stats count as conn_count min(_time) as first_seen max(_time) as last_seen by src_ip dest_ip dest_port transport app "
                        "| convert ctime(first_seen) ctime(last_seen) | sort - conn_count"
                    )
                    returns = 'Tracks firewall/flow connections to IOC endpoints and recurrent communication paths.'
                elif section_id == 'endpoint_edr':
                    query = (
                        f"index=* earliest=-{default_lookback_hours}h "
                        f"({ _splunk_or('process_hash', hashes) } OR { _splunk_or('CommandLine', domains) } OR { _splunk_or('Processes.process', domains) }) "
                        "| eval cmd=coalesce(CommandLine,process,Processes.process,NewProcessName) "
                        "| stats count as hits min(_time) as first_seen max(_time) as last_seen values(cmd) as commands by host user process_hash "
                        "| convert ctime(first_seen) ctime(last_seen) | sort - hits"
                    )
                    returns = 'Maps EDR/endpoint process activity to IOC hashes/domains and returns host-user command pivots.'
                else:
                    query = (
                        f"index=* earliest=-{default_lookback_hours}h "
                        f"({ _splunk_or('src_ip', ips) } OR { _splunk_or('user', emails) } OR { _splunk_or('user_principal_name', emails) }) "
                        "| eval actor_user=coalesce(user,user_principal_name,Account_Name) "
                        "| stats count as attempts count(eval(action=\"failure\" OR result=\"failure\")) as failures min(_time) as first_seen max(_time) as last_seen by actor_user src_ip app result "
                        "| convert ctime(first_seen) ctime(last_seen) | sort - failures - attempts"
                    )
                    returns = 'Highlights identity authentication anomalies associated with IOC IPs/accounts.'
            elif platform == 'elastic':
                if section_id == 'dns_proxy_web':
                    query = (
                        "FROM logs-dns*, logs-proxy*, logs-web* \n"
                        f"| WHERE @timestamp >= NOW() - INTERVAL {default_lookback_hours} HOURS\n"
                        f"| WHERE dns.question.name IN ({_es_values(domains)}) OR url.full IN ({_es_values(urls)}) OR destination.domain IN ({_es_values(domains)})\n"
                        "| STATS hits = COUNT(*), first_seen = MIN(@timestamp), last_seen = MAX(@timestamp) BY host.name, user.name, source.ip, destination.ip, destination.domain, url.full\n"
                        "| SORT hits DESC"
                    )
                    returns = 'Filters DNS/proxy/web logs for IOC domains/URLs and aggregates recurring destination activity.'
                elif section_id == 'network':
                    query = (
                        "FROM logs-network*, logs-firewall*, logs-zeek* \n"
                        f"| WHERE @timestamp >= NOW() - INTERVAL {default_lookback_hours} HOURS\n"
                        f"| WHERE destination.ip IN ({_es_values(ips)}) OR destination.domain IN ({_es_values(domains)})\n"
                        "| STATS conn_count = COUNT(*), first_seen = MIN(@timestamp), last_seen = MAX(@timestamp) BY source.ip, destination.ip, destination.port, network.transport, host.name\n"
                        "| SORT conn_count DESC"
                    )
                    returns = 'Finds network flow hits for IOC destinations and surfaces high-frequency connection paths.'
                elif section_id == 'endpoint_edr':
                    domain_probe = domains[0] if domains else '__no_ioc__'
                    query = (
                        "FROM logs-endpoint*, logs-windows*, logs-powershell* \n"
                        f"| WHERE @timestamp >= NOW() - INTERVAL {default_lookback_hours} HOURS\n"
                        f"| WHERE file.hash.sha256 IN ({_es_values(hashes)}) OR process.command_line LIKE \"%{domain_probe}%\"\n"
                        "| STATS hits = COUNT(*), first_seen = MIN(@timestamp), last_seen = MAX(@timestamp), cmds = VALUES(process.command_line) BY host.name, user.name, process.hash.sha256\n"
                        "| SORT hits DESC"
                    )
                    returns = 'Links endpoint process/file activity to IOC hashes/domains with host-user command context.'
                else:
                    query = (
                        "FROM logs-identity*, logs-auth*, logs-audit* \n"
                        f"| WHERE @timestamp >= NOW() - INTERVAL {default_lookback_hours} HOURS\n"
                        f"| WHERE source.ip IN ({_es_values(ips)}) OR user.email IN ({_es_values(emails)}) OR user.name IN ({_es_values(emails)})\n"
                        "| STATS attempts = COUNT(*), failures = COUNT_IF(event.outcome == \"failure\"), first_seen = MIN(@timestamp), last_seen = MAX(@timestamp) BY user.name, user.email, source.ip, event.dataset\n"
                        "| SORT failures DESC, attempts DESC"
                    )
                    returns = 'Prioritizes suspicious identity auth events tied to IOC IPs and user identities.'
            else:
                if section_id == 'dns_proxy_web':
                    query = (
                        f"Time window: last {default_lookback_hours}h\n"
                        "Data sources: DNS + Proxy + Web logs\n"
                        f"Filter: domain in {domains[:10]} OR url in {urls[:10]}\n"
                        "Group by: src_host/src_user, destination_domain/url\n"
                        "Return: repeat_count, first_seen, last_seen, top destinations\n"
                        "Pivot: from repeated destinations into endpoint process and identity sign-ins"
                    )
                    returns = 'Vendor-neutral workflow for IOC matching in DNS/proxy/web telemetry.'
                elif section_id == 'network':
                    query = (
                        f"Time window: last {default_lookback_hours}h\n"
                        "Data sources: firewall/netflow/network sensor logs\n"
                        f"Filter: destination_ip in {ips[:10]} OR destination_domain in {domains[:10]}\n"
                        "Group by: src_ip, dst_ip, dst_port, protocol, asset\n"
                        "Return: connection_count, first_seen, last_seen, recurrent paths\n"
                        "Pivot: correlate same src assets with endpoint/identity events"
                    )
                    returns = 'Vendor-neutral network hunt for IOC destination reachability and recurrence.'
                elif section_id == 'endpoint_edr':
                    query = (
                        f"Time window: last {default_lookback_hours}h\n"
                        "Data sources: EDR process + file + script logs\n"
                        f"Filter: process/file hash in {hashes[:10]} OR command line contains {domains[:10]}\n"
                        "Group by: host, user, process, hash\n"
                        "Return: execution_count, command_lines, first_seen, last_seen\n"
                        "Pivot: connect matching executions to network callbacks and auth anomalies"
                    )
                    returns = 'Vendor-neutral endpoint/EDR hunt for IOC-linked execution and artifacts.'
                else:
                    query = (
                        f"Time window: last {default_lookback_hours}h\n"
                        "Data sources: IdP sign-in + directory audit + auth logs\n"
                        f"Filter: source_ip in {ips[:10]} OR account/email in {emails[:10]}\n"
                        "Group by: account, source_ip, app/resource, auth result\n"
                        "Return: attempts, failures, impossible travel or unusual source patterns\n"
                        "Pivot: tie suspicious accounts back to endpoint and network indicators"
                    )
                    returns = 'Vendor-neutral identity hunt for IOC-associated authentication anomalies.'
            return {
                'required_data': _section_required_data(section_id, platform),
                'returns': returns,
                'query': query,
            }

        seen_queries: dict[tuple[str, str], set[str]] = {}

        for card_id, card in cards_by_id.items():
            evidence_items_raw = evidence_map_by_card.get(card_id, [])
            evidence_items = evidence_items_raw if isinstance(evidence_items_raw, list) else []
            evidence_lookup = {
                str(item.get('id') or ''): item
                for item in evidence_items
                if isinstance(item, dict) and str(item.get('id') or '').strip()
            }
            query_items_raw = hunt_by_card.get(card_id, []) if isinstance(hunt_by_card, dict) else []
            query_items = list(query_items_raw) if isinstance(query_items_raw, list) else []
            behavior_query_items_raw = card_behavior_queries.get(card_id, [])
            behavior_query_items = behavior_query_items_raw if isinstance(behavior_query_items_raw, list) else []
            for behavior_query in behavior_query_items:
                if not isinstance(behavior_query, dict):
                    continue
                query_items.append(
                    {
                        'platform': str(behavior_query.get('platform') or 'SIEM'),
                        'ioc_value': '',
                        'query': str(behavior_query.get('query') or ''),
                        'why_this_query': str(behavior_query.get('why_this_query') or ''),
                        'evidence_source_ids': [],
                        'evidence_sources': [],
                    }
                )
            for query_item in query_items:
                if not isinstance(query_item, dict):
                    continue
                query_value = str(query_item.get('query') or '').strip()
                if not query_value:
                    continue
                query_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{card_id}:{query_value}"))
                query_feedback = query_feedback_map.get(query_id, {'votes': 0, 'score': 0})
                refs_raw = query_item.get('evidence_source_ids')
                refs = refs_raw if isinstance(refs_raw, list) else []
                evidence_sources = [
                    evidence_lookup.get(
                        str(ref_id),
                        {'id': str(ref_id), 'source_title': str(ref_id), 'source_url': '', 'source_date': ''},
                    )
                    for ref_id in refs
                ]
                platform = _platform_key(str(query_item.get('platform') or ''))
                section_id = _section_id_for_query(card=card, query_item=query_item)
                section = section_by_id.get(section_id)
                if section is None:
                    continue
                dedupe_key = (section_id, platform)
                section_seen = seen_queries.setdefault(dedupe_key, set())
                if query_value in section_seen:
                    continue
                section_seen.add(query_value)
                section['platforms'][platform].append(
                    {
                        'card_id': card_id,
                        'card_title': str(card.get('quick_check_title') or card.get('question_text') or card_id),
                        'required_data': _section_required_data(section_id, platform) or str(section.get('required_data') or ''),
                        'returns': str(query_item.get('why_this_query') or section.get('returns') or ''),
                        'query': query_value,
                        'query_id': query_id,
                        'feedback_votes': int(query_feedback.get('votes') or 0),
                        'feedback_score': int(query_feedback.get('score') or 0),
                        'evidence_sources': evidence_sources,
                    }
                )
        has_generated_queries = any(
            isinstance(items, list) and len(items) > 0
            for items in (hunt_by_card.values() if isinstance(hunt_by_card, dict) else [])
        )

        for section in section_views:
            section_id = str(section.get('id') or '').strip()
            for platform in platform_keys:
                baseline_item = _baseline_query_item(section_id, platform)
                if not baseline_item:
                    continue
                existing_items = section['platforms'].get(platform, [])
                baseline_query = str(baseline_item.get('query') or '').strip()
                if not baseline_query:
                    continue
                if any(str(item.get('query') or '').strip() == baseline_query for item in existing_items):
                    continue
                section['platforms'][platform] = [
                    {
                        'card_id': selected_check_id or '',
                        'card_title': 'Actor-scoped baseline IOC hunt',
                        'required_data': str(baseline_item.get('required_data') or section.get('required_data') or ''),
                        'returns': str(baseline_item.get('returns') or section.get('returns') or ''),
                        'query': baseline_query,
                        'query_id': str(uuid.uuid5(uuid.NAMESPACE_URL, f"{actor_id}:{section_id}:{platform}:baseline")),
                        'feedback_votes': 0,
                        'feedback_score': 0,
                        'evidence_sources': [],
                    }
                ] + list(existing_items)
        has_primary_queries = any(
            bool(section.get('platforms', {}).get(platform))
            for section in section_views
            for platform in platform_keys
        )

        misc_ioc_rows: list[dict[str, object]] = []
        for ioc in iocs_in_window:
            if not isinstance(ioc, dict):
                continue
            ioc_type_value = str(ioc.get('ioc_type') or '').strip().lower()
            ioc_value = str(ioc.get('ioc_value') or '').strip()
            if not ioc_type_value or not ioc_value:
                continue
            if not _ioc_value_is_hunt_relevant(ioc_type_value, ioc_value):
                continue
            last_seen_raw = str(ioc.get('last_seen_at') or ioc.get('created_at') or '')
            key = (ioc_type_value, ioc_value.lower())
            if key in used_ioc_pairs:
                continue
            row = {
                'ioc_type': ioc_type_value,
                'ioc_value': ioc_value,
                'first_seen': str(ioc.get('created_at') or ''),
                'last_seen': last_seen_raw,
                'sources_count': int(ioc.get('seen_count') or 1),
                'confidence': int(ioc.get('confidence_score') or 0),
                'source_ref': str(ioc.get('source_ref') or ''),
            }
            misc_ioc_rows.append(row)

        if selected_check_id:
            actor_name_lc = actor_name.lower()

            def _misc_matches_selected_check(row: dict[str, object]) -> bool:
                ioc_value_lc = str(row.get('ioc_value') or '').strip().lower()
                source_ref_lc = str(row.get('source_ref') or '').strip().lower()
                if relevant_values and ioc_value_lc in relevant_values:
                    return True
                if relevant_types and str(row.get('ioc_type') or '').strip().lower() in relevant_types:
                    if not relevant_values:
                        return True
                if selected_card_text and (ioc_value_lc in selected_card_text or (source_ref_lc and source_ref_lc in selected_card_text)):
                    return True
                if actor_name_lc and source_ref_lc and actor_name_lc in source_ref_lc:
                    return True
                return False

            misc_ioc_rows = [row for row in misc_ioc_rows if _misc_matches_selected_check(row)]

        normalized_type_filter = str(ioc_type or '').strip().lower()
        if normalized_type_filter:
            misc_ioc_rows = [row for row in misc_ioc_rows if str(row.get('ioc_type') or '').strip().lower() == normalized_type_filter]
        try:
            min_confidence = int(confidence) if confidence is not None and str(confidence).strip() != '' else None
        except Exception:
            min_confidence = None
        if min_confidence is not None:
            misc_ioc_rows = [row for row in misc_ioc_rows if int(row.get('confidence') or 0) >= min_confidence]
        try:
            min_source_count = int(source_count) if source_count is not None and str(source_count).strip() != '' else None
        except Exception:
            min_source_count = None
        if min_source_count is not None:
            misc_ioc_rows = [row for row in misc_ioc_rows if int(row.get('sources_count') or 0) >= min_source_count]
        freshness_value = str(freshness or '').strip().lower()
        if freshness_value in {'24h', '7d', '30d'}:
            cutoff_hours = 24 if freshness_value == '24h' else (24 * 7 if freshness_value == '7d' else 24 * 30)
            freshness_cutoff = now_utc - timedelta(hours=cutoff_hours)
            misc_ioc_rows = [
                row
                for row in misc_ioc_rows
                if quick_checks_view_service.is_in_window_core(
                    str(row.get('last_seen') or ''),
                    window_start=freshness_cutoff,
                    window_end=now_utc,
                )
            ]
        misc_ioc_rows.sort(
            key=lambda row: (str(row.get('last_seen') or ''), int(row.get('confidence') or 0)),
            reverse=True,
        )

        if str(export or '').strip().lower() == 'json':
            return JSONResponse(
                {
                    'actor_id': actor_id,
                    'window_start': window_start_iso,
                    'window_end': window_end_iso,
                    'quick_check_id': selected_check_id,
                    'count': len(misc_ioc_rows),
                    'items': misc_ioc_rows,
                }
            )
        if str(export or '').strip().lower() == 'csv':
            buffer = io.StringIO()
            writer = csv.writer(buffer)
            writer.writerow(['type', 'value', 'first_seen', 'last_seen', 'sources_count', 'confidence'])
            for row in misc_ioc_rows:
                writer.writerow(
                    [
                        row.get('ioc_type', ''),
                        row.get('ioc_value', ''),
                        row.get('first_seen', ''),
                        row.get('last_seen', ''),
                        row.get('sources_count', ''),
                        row.get('confidence', ''),
                    ]
                )
            return Response(
                content=buffer.getvalue(),
                media_type='text/csv',
                headers={'Content-Disposition': f'attachment; filename="{actor_id}-misc-iocs.csv"'},
            )

        return _templates.TemplateResponse(
            request,
            'ioc_hunts.html',
            {
                'actor_id': actor_id,
                'actor_name': actor_name,
                'check_template_id': selected_check_id,
                'quick_check_id': selected_check_id,
                'reason': reason,
                'has_generated_queries': has_generated_queries,
                'has_primary_queries': has_primary_queries,
                'ollama_status': ollama_status,
                'environment_profile': environment_profile,
                'query_lookback_hours': default_lookback_hours,
                'lookback_presets': lookback_presets,
                'window_start': window_start_iso,
                'window_end': window_end_iso,
                'platform_tabs': platform_tabs,
                'sections': section_views,
                'misc_iocs': misc_ioc_rows,
                'filters': {
                    'ioc_type': ioc_type or '',
                    'confidence': confidence or '',
                    'source_count': source_count or '',
                    'freshness': freshness or '',
                },
            },
        )

