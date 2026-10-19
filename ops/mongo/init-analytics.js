// Creates (or updates) the `analytics` user (RFC 0008 R1, §6.2, ANL-D36):
// readWrite on analytics_db, plus `analyticsTtl` — collMod on analytics_db
// only, so ensure_indexes() can change a TTL in place (readWrite cannot).
// Nothing on any other database. Idempotent: safe on an existing volume.
//   make mongo-init
// runs: mongosh -u $MONGO_INITDB_ROOT_USERNAME ... --file /ops/mongo/init-analytics.js
// with ANALYTICS_MONGO_PASSWORD in the environment.
const password = process.env.ANALYTICS_MONGO_PASSWORD;
if (!password) {
  throw new Error("ANALYTICS_MONGO_PASSWORD is not set");
}
const dbName = process.env.ANALYTICS_MONGO_DB || "analytics_db";
const target = db.getSiblingDB(dbName);

const ttlPrivileges = [{ resource: { db: dbName, collection: "" }, actions: ["collMod"] }];
if (target.getRole("analyticsTtl") === null) {
  target.createRole({ role: "analyticsTtl", privileges: ttlPrivileges, roles: [] });
} else {
  target.updateRole("analyticsTtl", { privileges: ttlPrivileges, roles: [] });
}

const roles = [
  { role: "readWrite", db: dbName },
  { role: "analyticsTtl", db: dbName },
];
if (target.getUser("analytics") === null) {
  target.createUser({ user: "analytics", pwd: password, roles });
  print(`created user analytics on ${dbName}`);
} else {
  // Re-applying keeps the grant exact and rotates the password from .env.
  target.updateUser("analytics", { pwd: password, roles });
  print(`updated user analytics on ${dbName}`);
}
