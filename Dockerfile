# NAIA 2.0 — headless Remote Web runtime.
#
# Optional release infrastructure only. The default product path stays
# `python NAIA_web_headless.py` with requirements-headless.txt; nothing here is
# required for a normal clone run (PROJECT_LAYOUT_POLICY.md, "Two Track Boundary").
#
#   docker compose up -d --build
#
# Everything the app writes lives under /data, which compose mounts from the
# host: config (API tokens), data (the ~1.4GB tag archive), output, extensions,
# wildcards, save, logs, cache, downloads.

# Caddy ships as a single static binary; take it from the official image rather
# than adding an apt source. See docker/Caddyfile for what it is doing here.
FROM caddy:2-alpine AS caddy

# NAIA supports Python 3.10-3.12; 3.13+ is not supported yet (run_NAIA_web.command:97).
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    NAIA_USER_DATA_DIR=/data \
    NAIA_HEADLESS_OPEN_BROWSER=0 \
    NAIA_APP_PORT=17243 \
    NAIA_PROXY_PORT=7243 \
    HOME=/data/.home

COPY --from=caddy /usr/bin/caddy /usr/local/bin/caddy

# uid 1000 matches the first human account on most Linux hosts, so a bind mount
# usually just works. Override with `user:` in compose when yours differs.
RUN set -eux; \
    useradd --create-home --uid 1000 --shell /usr/sbin/nologin naia; \
    mkdir -p /app /data; \
    chown naia:naia /app /data

WORKDIR /app

# Dependencies first so source edits do not invalidate the (slow) install layer.
# Every dependency resolves to a manylinux wheel, so no compiler is needed.
COPY requirements-headless.txt ./
RUN pip install --no-cache-dir -r requirements-headless.txt

COPY --chown=naia:naia . /app
COPY --chown=naia:naia docker/Caddyfile /etc/caddy/Caddyfile
COPY --chown=naia:naia docker/entrypoint.sh /usr/local/bin/naia-entrypoint
RUN chmod +x /usr/local/bin/naia-entrypoint

USER naia
EXPOSE 7243

# /api/status is ungated, so this says nothing about whether a token is set —
# only that the proxy is up and the backend behind it is answering.
HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD ["python", "-c", "import os,urllib.request;urllib.request.urlopen('http://127.0.0.1:'+os.environ.get('NAIA_PROXY_PORT','7243')+'/api/status',timeout=8)"]

ENTRYPOINT ["/usr/local/bin/naia-entrypoint"]
