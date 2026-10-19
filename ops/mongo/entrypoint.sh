#!/bin/bash
# MongoDB for analytics_db (RFC 0008 ANL-1, ANL-D36): a single-node replica set
# (rs0) with authentication. A replica set with auth needs a keyfile for
# internal member authentication; with one member it only has to exist, so a
# fresh one is generated on every start and never leaves the container.
set -euo pipefail

KEYFILE=/tmp/mongo-keyfile
if [ ! -s "$KEYFILE" ]; then
  head -c 756 /dev/urandom | base64 | tr -d '\n' > "$KEYFILE"
fi
chmod 400 "$KEYFILE"
chown mongodb:mongodb "$KEYFILE"

# The image's entrypoint creates MONGO_INITDB_ROOT_USERNAME on first start
# (empty volume) and then execs mongod with these arguments.
exec /usr/local/bin/docker-entrypoint.sh mongod \
  --replSet "${MONGO_REPLICA_SET:-rs0}" \
  --keyFile "$KEYFILE" \
  --bind_ip_all \
  "$@"
