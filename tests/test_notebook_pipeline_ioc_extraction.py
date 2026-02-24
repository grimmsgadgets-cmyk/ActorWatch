from pipelines.notebook_pipeline import _extract_ioc_candidates_from_text


def test_extract_ioc_candidates_skips_software_like_domain_tokens():
    text = (
        "The malware operator dashboard uses Next.js for the web UI framework. "
        "This section is implementation detail and not an IOC.\n\n"
        "Observed indicators include domain c2.bad-example.net and callback IP 185.88.1.45."
    )

    extracted = _extract_ioc_candidates_from_text(text)
    pairs = {(ioc_type, ioc_value) for ioc_type, ioc_value in extracted}

    assert ('domain', 'next.js') not in pairs
    assert ('domain', 'c2.bad-example.net') in pairs
    assert ('ip', '185.88.1.45') in pairs


def test_extract_ioc_candidates_requires_domain_ioc_context():
    spacer = " ".join(["release-note"] * 60)
    text = (
        "Product note: migrate frontend from legacy.bundle to modern.stack soon.\n"
        f"{spacer}\n"
        "Indicators: suspicious DNS domain bad-control.example and malware hash "
        "9f86d081884c7d659a2feaa0c55ad015."
    )

    extracted = _extract_ioc_candidates_from_text(text)
    pairs = {(ioc_type, ioc_value) for ioc_type, ioc_value in extracted}

    assert ('domain', 'legacy.bundle') not in pairs
    assert ('domain', 'modern.stack') not in pairs
    assert ('domain', 'bad-control.example') in pairs
