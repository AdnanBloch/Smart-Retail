#!/bin/bash
# Healthy only when this node is a writable primary of rs0. On the first run
# (or after the volume was reset) the replica set is not initiated yet: the
# check initiates it, so no separate init container is needed. rs.initiate()
# on an initiated set is never attempted (rs.status() succeeds first).
set -euo pipefail

mongosh --quiet --port 27017 \
  --username "$MONGO_INITDB_ROOT_USERNAME" \
  --password "$MONGO_INITDB_ROOT_PASSWORD" \
  --authenticationDatabase admin \
  --eval "
    try { const status = rs.status(); }
    catch (e) {
      if (e.codeName !== 'NotYetInitialized') { throw e; }
      const initiated = rs.initiate({_id: '${MONGO_REPLICA_SET:-rs0}', members: [{_id: 0, host: '${MONGO_ADVERTISED_HOST:-mongo:27017}'}]});
    }
    if (!db.hello().isWritablePrimary) { quit(1); }
  "
