# ActorWatch

A lightweight threat actor lookup tool that turns reporting into a clear
starting point for defensive review.

![App Screenshot](docs/screenshots/updated2.png)

------------------------------------------------------------------------

## Overview

ActorWatch is a small web application that helps you:

-   Look up a known threat actor
-   See a concise summary of who they are
-   Review recent reporting highlights
-   Get a short list of practical defensive checks to start with

It is intended to provide an at-a-glance reference, not a full CTI
platform.

------------------------------------------------------------------------

## Architecture

See `docs/architecture.md` for the contributor map:

- request/route modules
- service layer responsibilities
- pipeline responsibilities
- database ownership and refresh flow

This is the best starting point for community contributors.

For operator onboarding, screen guidance, and common admin workflows, see:

- `docs/community_guide.md`

------------------------------------------------------------------------

## Requirements

Before starting, ensure you have:

-   A computer running Windows, macOS, or Linux
-   **Docker Desktop** installed and running\
    https://www.docker.com/products/docker-desktop
-   Internet access on first run so Docker can pull base images

No external gateway or Codex-specific network setup is required.

To verify Docker is installed:

    docker --version

If a version number appears, Docker is ready.

------------------------------------------------------------------------

## Setup Instructions

### 1. Clone the Repository

Using Git:

    git clone https://github.com/grimmsgadgets-cmyk/ActorWatch.git
    cd ActorWatch

Or download the ZIP from GitHub and open a terminal inside the extracted
folder.

------------------------------------------------------------------------

### 2. Build and Start the Application

From inside the project folder:

    docker compose up --build

This will:

-   Build the Docker image
-   Install dependencies inside the container
-   Start the web application and local Ollama service

The first run may take a few minutes.

Security defaults:

-   The app is published on `127.0.0.1:8000` (local machine only)
-   Reverse-proxy headers are not trusted by default (`TRUST_PROXY_HEADERS=0`)
-   Outbound URL fetches use a built-in domain allowlist unless you set
    `OUTBOUND_ALLOWED_DOMAINS`
-   Cross-site browser write requests are blocked (Origin/Referer validation)

------------------------------------------------------------------------

### 3. Access the Application

Once the container is running, open your browser and go to:

    http://localhost:8000

If that does not load, check the terminal output for the correct port
number.

------------------------------------------------------------------------

## Community Ops (Quick)

Useful endpoints:

- `GET /health`
- `GET /actors`
- `POST /actors`
- `POST /actors/{target_actor_id}/merge` (form or JSON with `source_actor_id`)
- `GET /actors/{actor_id}/refresh/stats`

Auto-refresh defaults:

- `AUTO_REFRESH_ENABLED=1`
- `AUTO_REFRESH_MIN_INTERVAL_HOURS=24`
- `AUTO_REFRESH_LOOP_SECONDS=300`
- `AUTO_REFRESH_BATCH_SIZE=3`
- `AUTO_MERGE_DUPLICATE_ACTORS=1`

------------------------------------------------------------------------

## Stopping the Application

Press:

    CTRL + C

Then optionally clean up containers:

    docker compose down

------------------------------------------------------------------------

## Backup and Restore

The SQLite database lives in Docker volume `actortracker_db`.

Backup:

```bash
docker run --rm -v actortracker_actortracker_db:/from -v "$PWD":/to alpine \
  sh -c "cp /from/app.db /to/app.db.backup"
```

Restore:

```bash
docker run --rm -v actortracker_actortracker_db:/to -v "$PWD":/from alpine \
  sh -c "cp /from/app.db.backup /to/app.db"
```

## Minimal Troubleshooting Checklist

If something does not work, check the following:

-   **Docker running?**\
    Ensure Docker Desktop is open and running.

-   **Port already in use?**\
    If you see a port error, another application may be using port 8000.

-   **Changes not appearing?**\
    Rebuild the container: docker compose down docker compose up --build

-   **Build failed?**\
    Scroll up in the terminal to read the exact error message.

------------------------------------------------------------------------

## Project Status

Early-stage and evolving.\
Expect iterative improvements and UI changes.
