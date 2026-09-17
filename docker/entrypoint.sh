#!/usr/bin/env bash
# Container entrypoint: the NAIA headless backend bound to loopback, with the
# in-container proxy (docker/Caddyfile) published in front of it. If either
# process exits the container exits too, so the restart policy can act.
set -uo pipefail

: "${NAIA_USER_DATA_DIR:=/data}"
: "${NAIA_APP_PORT:=17243}"
: "${NAIA_PROXY_PORT:=7243}"
: "${NAIA_LOG_LEVEL:=warning}"
: "${NAIA_SEED_WILDCARDS:=1}"
: "${HOME:=${NAIA_USER_DATA_DIR}/.home}"
export NAIA_USER_DATA_DIR NAIA_APP_PORT NAIA_PROXY_PORT HOME

mkdir -p "$NAIA_USER_DATA_DIR" 2>/dev/null || true
if [ ! -w "$NAIA_USER_DATA_DIR" ]; then
	echo "[naia] $NAIA_USER_DATA_DIR is not writable by uid $(id -u):$(id -g)." >&2
	echo "[naia] The mounted directory belongs to another user. Either chown it," >&2
	echo "[naia] or set NAIA_UID / NAIA_GID in .env to your own id (id -u / id -g)." >&2
	exit 1
fi

# Path.home() backs the cloudflared working directory and the Grok auth file;
# keep it inside the volume so it is writable whatever uid the container runs as.
mkdir -p "$HOME" 2>/dev/null || true

# A clone user running natively reads wildcards straight out of the repo, but
# setting NAIA_USER_DATA_DIR moves that lookup to <user-data>/wildcards
# (core/wildcard_manager.py:22), which starts out empty. Seed it once from the
# shipped set so a container starts with the same library. Never overwrites: a
# directory that already has anything in it is left exactly as it is.
if [ "$NAIA_SEED_WILDCARDS" != "0" ] && [ -d /app/wildcards ]; then
	mkdir -p "$NAIA_USER_DATA_DIR/wildcards" 2>/dev/null || true
	if [ -z "$(ls -A "$NAIA_USER_DATA_DIR/wildcards" 2>/dev/null)" ]; then
		if cp -a /app/wildcards/. "$NAIA_USER_DATA_DIR/wildcards/" 2>/dev/null; then
			echo "[naia] seeded the default wildcard library into $NAIA_USER_DATA_DIR/wildcards"
		fi
	fi
fi

app_pid=""
proxy_pid=""

stop() {
	trap - TERM INT
	[ -n "$app_pid" ] && kill -TERM "$app_pid" 2>/dev/null
	[ -n "$proxy_pid" ] && kill -TERM "$proxy_pid" 2>/dev/null
	wait 2>/dev/null
	return 0
}
trap stop TERM INT

python NAIA_web_headless.py \
	--host 127.0.0.1 \
	--port "$NAIA_APP_PORT" \
	--no-browser \
	--log-level "$NAIA_LOG_LEVEL" &
app_pid=$!

caddy run --config /etc/caddy/Caddyfile --adapter caddyfile &
proxy_pid=$!

echo "[naia] user data: $NAIA_USER_DATA_DIR"
echo "[naia] listening on container port $NAIA_PROXY_PORT (backend on 127.0.0.1:$NAIA_APP_PORT)"

wait -n
status=$?
stop
exit "$status"
