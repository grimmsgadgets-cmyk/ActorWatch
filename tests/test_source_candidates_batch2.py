from urllib.parse import urlparse

import app as app_module
import services.web_backfill_service as web_backfill_service


def test_batch2_feeds_are_in_default_feeds_and_outbound_allowlist():
    expected_feeds = {
        ('Malwarebytes Labs', 'https://www.malwarebytes.com/blog/feed/index.xml'),
        ('Zero Day Initiative Blog', 'https://www.zerodayinitiative.com/blog?format=rss'),
        ('NetWitness Blog', 'https://www.netwitness.com/en-us/blog/feed/'),
        ('Corelight Labs', 'https://corelight.com/blog/rss.xml'),
        ('EclecticIQ Blog', 'https://blog.eclecticiq.com/rss.xml'),
        ('LevelBlue SpiderLabs Blog', 'https://www.levelblue.com/en-us/resources/blogs/spiderlabs-blog/rss.xml'),
        ('CERT-FR', 'https://www.cert.ssi.gouv.fr/feed/'),
    }
    assert expected_feeds.issubset(set(app_module.DEFAULT_CTI_FEEDS))

    outbound_allowed = set(app_module.OUTBOUND_ALLOWED_DOMAINS)
    for _name, feed_url in expected_feeds:
        host = (urlparse(feed_url).hostname or '').strip('.').lower()
        assert host
        assert host in outbound_allowed


def test_backfill_allowlist_includes_batch2_registrables():
    expected = {
        'malwarebytes.com',
        'zerodayinitiative.com',
        'netwitness.com',
        'corelight.com',
        'eclecticiq.com',
        'levelblue.com',
        'gouv.fr',
    }
    assert expected.issubset(set(web_backfill_service.PRIMARY_ALLOWLIST_REGISTRABLE))
