from urllib.parse import urlparse

import app as app_module


def test_batch1_feeds_are_in_default_feeds_and_outbound_allowlist():
    expected_feeds = {
        ('The DFIR Report', 'https://thedfirreport.com/feed/'),
        ('Recorded Future Blog', 'https://www.recordedfuture.com/feed'),
        ('Intel 471 Blog', 'https://www.intel471.com/blog/feed'),
        ('Sygnia Blog', 'https://www.sygnia.co/blog/feed/'),
        ('Check Point Research', 'https://research.checkpoint.com/feed/'),
    }
    default_feed_set = set(app_module.DEFAULT_CTI_FEEDS)
    assert expected_feeds.issubset(default_feed_set)

    outbound_allowed = set(app_module.OUTBOUND_ALLOWED_DOMAINS)
    for _name, feed_url in expected_feeds:
        host = (urlparse(feed_url).hostname or '').strip('.').lower()
        assert host
        assert host in outbound_allowed
