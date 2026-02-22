def ollama_available_core(*, deps: dict[str, object]) -> bool:
    _get_env = deps['get_env']
    _http_get = deps['http_get']

    base_url = _get_env('OLLAMA_BASE_URL', 'http://localhost:11434').rstrip('/')
    try:
        response = _http_get(f'{base_url}/api/tags', timeout=2.5)
        return response.status_code == 200
    except Exception:
        return False


def get_ollama_status_core(*, deps: dict[str, object]) -> dict[str, str | bool]:
    _get_env = deps['get_env']
    _http_get = deps['http_get']

    base_url = _get_env('OLLAMA_BASE_URL', 'http://localhost:11434').rstrip('/')
    model = _get_env('OLLAMA_MODEL', 'llama3.1:8b')
    try:
        response = _http_get(f'{base_url}/api/tags', timeout=2.5)
        if response.status_code != 200:
            return {
                'available': False,
                'base_url': base_url,
                'model': model,
                'message': f'Ollama check failed (HTTP {response.status_code}).',
            }
        data = response.json()
        models = data.get('models', []) if isinstance(data, dict) else []
        model_names = {
            str(item.get('name'))
            for item in models
            if isinstance(item, dict) and item.get('name')
        }
        has_model = model in model_names or model.split(':')[0] in {m.split(':')[0] for m in model_names}
        if has_model:
            return {
                'available': True,
                'base_url': base_url,
                'model': model,
                'message': 'Local LLM is reachable and model is available.',
            }
        return {
            'available': True,
            'base_url': base_url,
            'model': model,
            'message': 'Ollama is reachable, but configured model was not found in tags.',
        }
    except Exception as exc:
        return {
            'available': False,
            'base_url': base_url,
            'model': model,
            'message': f'Ollama is not reachable: {exc}',
        }


def format_duration_ms_core(milliseconds: int | None) -> str:
    if milliseconds is None or milliseconds <= 0:
        return 'n/a'
    if milliseconds < 1000:
        return f'{milliseconds}ms'
    seconds = milliseconds / 1000.0
    if seconds < 60:
        return f'{seconds:.1f}s'
    minutes = int(seconds // 60)
    remaining = int(round(seconds % 60))
    return f'{minutes}m {remaining}s'
