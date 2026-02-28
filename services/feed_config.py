"""
Feed catalog and derived feed lists for ActorWatch.

Single source of truth for all RSS/CTI feed definitions.
Organized by use-case to simplify targeted pull strategies.
"""

FEED_CATALOG: dict[str, list[tuple[str, str]]] = {
    'ioc': [
        ('CISA Alerts', 'https://www.cisa.gov/cybersecurity-advisories/all.xml'),
        ('NCSC UK', 'https://www.ncsc.gov.uk/api/1/services/v1/report-rss-feed.xml'),
        ('Cisco Talos', 'https://blog.talosintelligence.com/rss/'),
        ('Palo Alto Unit 42', 'https://unit42.paloaltonetworks.com/feed/'),
        ('Mandiant Blog', 'https://www.mandiant.com/resources/blog/rss.xml'),
        ('SentinelOne Labs', 'https://www.sentinelone.com/labs/feed/'),
        ('CrowdStrike Blog', 'https://www.crowdstrike.com/en-us/blog/feed/'),
        ('Securelist', 'https://securelist.com/feed/'),
    ],
    'research': [
        ('Microsoft Security', 'https://www.microsoft.com/en-us/security/blog/feed/'),
        ('Google Cloud Threat Intelligence', 'https://cloud.google.com/blog/topics/threat-intelligence/rss/'),
        ('Proofpoint Blog', 'https://www.proofpoint.com/us/blog/rss.xml'),
        ('Red Canary Blog', 'https://redcanary.com/blog/feed/'),
        ('Huntress Blog', 'https://www.huntress.com/blog/rss.xml'),
        ('Arctic Wolf Labs', 'https://arcticwolf.com/resources/blog/feed/'),
        ('Rapid7 Blog', 'https://www.rapid7.com/blog/rss/'),
        ('Sophos News', 'https://news.sophos.com/en-us/feed/'),
        ('Trend Micro Research', 'https://www.trendmicro.com/en_us/research.html/rss.xml'),
        ('ESET WeLiveSecurity', 'https://www.welivesecurity.com/en/rss/feed'),
        ('CISA News', 'https://www.cisa.gov/news.xml'),
        ('The DFIR Report', 'https://thedfirreport.com/feed/'),
        ('Recorded Future Blog', 'https://www.recordedfuture.com/feed'),
        ('Intel 471 Blog', 'https://www.intel471.com/blog/feed'),
        ('Sygnia Blog', 'https://www.sygnia.co/blog/feed/'),
        ('Check Point Research', 'https://research.checkpoint.com/feed/'),
        ('Malwarebytes Labs', 'https://www.malwarebytes.com/blog/feed/index.xml'),
        ('Zero Day Initiative Blog', 'https://www.zerodayinitiative.com/blog?format=rss'),
        ('NetWitness Blog', 'https://www.netwitness.com/en-us/blog/feed/'),
        ('Corelight Labs', 'https://corelight.com/blog/rss.xml'),
        ('EclecticIQ Blog', 'https://blog.eclecticiq.com/rss.xml'),
        ('LevelBlue SpiderLabs Blog', 'https://www.levelblue.com/en-us/resources/blogs/spiderlabs-blog/rss.xml'),
        ('CERT-FR', 'https://www.cert.ssi.gouv.fr/feed/'),
    ],
    'advisory': [
        ('Cisco PSIRT', 'https://sec.cloudapps.cisco.com/security/center/psirtrss20/CiscoSecurityAdvisory.xml'),
        ('Fortinet PSIRT', 'https://filestore.fortinet.com/fortiguard/rss/ir.xml'),
        ('Ivanti Security Advisory', 'https://www.ivanti.com/blog/topics/security-advisory/rss'),
        ('JPCERT Alerts', 'https://www.jpcert.or.jp/rss/jpcert.rdf'),
        ('JVN Vulnerability', 'https://jvn.jp/rss/jvn.rdf'),
    ],
    'context': [
        ('BleepingComputer', 'https://www.bleepingcomputer.com/feed/'),
        ('The Hacker News', 'https://feeds.feedburner.com/TheHackersNews'),
        ('Krebs on Security', 'https://krebsonsecurity.com/feed/'),
        ('The Record', 'https://therecord.media/feed'),
        ('Dark Reading', 'https://www.darkreading.com/rss.xml'),
        ('SANS Internet Storm Center', 'https://isc.sans.edu/rssfeed_full.xml'),
    ],
}

IOC_INTELLIGENCE_FEEDS = FEED_CATALOG['ioc']
PRIMARY_CTI_FEEDS = FEED_CATALOG['ioc'] + FEED_CATALOG['research']
EXPANDED_PRIMARY_ADVISORY_FEEDS = FEED_CATALOG['advisory']
SECONDARY_CONTEXT_FEEDS = FEED_CATALOG['context']
DEFAULT_CTI_FEEDS = PRIMARY_CTI_FEEDS + EXPANDED_PRIMARY_ADVISORY_FEEDS + SECONDARY_CONTEXT_FEEDS
