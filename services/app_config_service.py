"""
Application configuration loaded from environment variables.

Allows a host platform (e.g. ThreatSpire) to rebrand and configure
ActorWatch without touching source code. All values have safe defaults
for standalone community-edition deployments.
"""
import os

# Branding
APP_NAME: str = os.environ.get('ACTORWATCH_APP_NAME', 'ActorWatch')
LOGO_PATH: str = os.environ.get('ACTORWATCH_LOGO_PATH', '')
PRIMARY_COLOR: str = os.environ.get('ACTORWATCH_PRIMARY_COLOR', '#35527f')
THEME: str = os.environ.get('ACTORWATCH_THEME', 'dark')  # 'dark' | 'light'

# Routing
# Set this when ActorWatch is mounted at a sub-path, e.g. /modules/actorwatch.
# In standalone mode leave empty or unset.  All generated URLs will be
# prefixed with this value.  Must NOT have a trailing slash.
BASE_PATH: str = os.environ.get('ACTORWATCH_BASE_PATH', '').rstrip('/')

# Auth
# Community edition: AUTH_ENABLED = False (passthrough).
# When embedded in a host that manages identity, the host sets this to True
# and supplies its own auth middleware — the app core does not change.
AUTH_ENABLED: bool = os.environ.get('ACTORWATCH_AUTH_ENABLED', '').lower() in (
    '1', 'true', 'yes',
)
