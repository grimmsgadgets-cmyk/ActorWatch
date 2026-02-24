import services.ioc_validation_service as ioc_validation_service


def test_validate_ioc_candidate_accepts_public_ip():
    result = ioc_validation_service.validate_ioc_candidate_core(
        raw_value='8.8.8.8',
        raw_type='ip',
        extraction_method='manual',
    )
    assert result['valid'] is True
    assert result['ioc_type'] == 'ip'
    assert result['validation_status'] == 'valid'
    assert result['is_active'] == 1


def test_validate_ioc_candidate_suppresses_private_ip():
    result = ioc_validation_service.validate_ioc_candidate_core(
        raw_value='10.1.2.3',
        raw_type='ip',
        extraction_method='auto_source_regex',
    )
    assert result['valid'] is True
    assert result['validation_status'] == 'suppressed_benign'
    assert result['is_active'] == 0


def test_validate_ioc_candidate_rejects_invalid_hash():
    result = ioc_validation_service.validate_ioc_candidate_core(
        raw_value='zzzz-not-hash',
        raw_type='hash',
        extraction_method='manual',
    )
    assert result['valid'] is False
    assert result['validation_status'] == 'invalid'
