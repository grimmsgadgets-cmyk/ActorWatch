from pipelines.source_derivation import derive_source_from_url_core


class _Resp:
    def __init__(self, *, url: str, text: str):
        self.url = url
        self.text = text
        self.status_code = 200
        self.headers = {'content-type': 'text/html'}

    def raise_for_status(self) -> None:
        return None


def test_derive_source_uses_structured_parser_for_mitre():
    html = '''
    <html>
      <head><title>APT Group</title></head>
      <body>
        <main>
          <h1>APT Group G1234</h1>
          <p>This group uses T1059 command execution and spearphishing techniques in campaigns.</p>
          <p>Infrastructure includes domain bad.example and malware hash 44d88612fea8a8f36de82e1278abb02f.</p>
        </main>
      </body>
    </html>
    '''

    result = derive_source_from_url_core(
        'https://attack.mitre.org/groups/G1234/',
        deps={
            'safe_http_get': lambda _url, timeout=20.0: _Resp(url='https://attack.mitre.org/groups/G1234/', text=html),
            'extract_question_sentences': lambda text: [text.split('.')[0]],
            'first_sentences': lambda text, count=1: text[:120],
        },
    )

    assert str(result.get('parse_status')) == 'parsed_structured_mitre'
    assert 'APT Group G1234' in str(result.get('pasted_text') or '')


def test_derive_source_uses_structured_parser_for_cisa():
    html = '''
    <html>
      <head><title>CISA Advisory</title></head>
      <body>
        <main>
          <h1>AA26-001A Advisory</h1>
          <p>Threat actors are exploiting edge devices and deploying ransomware at scale.</p>
          <li>Patch internet-facing appliances immediately.</li>
        </main>
      </body>
    </html>
    '''

    result = derive_source_from_url_core(
        'https://www.cisa.gov/news-events/cybersecurity-advisories/aa26-001a',
        deps={
            'safe_http_get': lambda _url, timeout=20.0: _Resp(url='https://www.cisa.gov/news-events/cybersecurity-advisories/aa26-001a', text=html),
            'extract_question_sentences': lambda text: [text.split('.')[0]],
            'first_sentences': lambda text, count=1: text[:120],
        },
    )

    assert str(result.get('parse_status')) == 'parsed_structured_cisa'
    assert 'AA26-001A Advisory' in str(result.get('pasted_text') or '')
