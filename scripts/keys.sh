#!/usr/bin/env bash
# Generate local secrets if missing (RFC 0003 IDN-5, IDN-T3):
#   keys/<kid>.pem       RS256 private key, mounted into identity only
#   keys/jwt_public.pem  public key, mounted into every verifier
#   .env JWT_ACTIVE_KID                the signing key's kid
#   .env SERVICE_CLIENT_SECRET         the temporal-worker client secret (workers only)
#   .env SERVICE_CLIENT_SECRET_SHA256  its digest, the only form identity sees
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p keys
[ -f .env ] || cp .env.example .env

existing=$(find keys -maxdepth 1 -name '*.pem' ! -name 'jwt_public.pem' | head -n1 || true)
if [ -n "$existing" ] && [ -f keys/jwt_public.pem ]; then
  kid=$(basename "$existing" .pem)
  echo "keys: using existing key $kid"
else
  kid="$(date -u +%Y%m%d)-$(openssl rand -hex 4)"
  openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "keys/$kid.pem" 2>/dev/null
  openssl rsa -in "keys/$kid.pem" -pubout -out keys/jwt_public.pem 2>/dev/null
  echo "keys: generated $kid"
fi
# Containers run as uid 10001 and must be able to read the mounted files.
# keys/ is gitignored and local-only.
chmod 644 keys/*.pem

set_env() {
  if grep -q "^$1=" .env; then
    sed -i.bak "s|^$1=.*|$1=$2|" .env && rm -f .env.bak
  else
    echo "$1=$2" >> .env
  fi
}

set_env JWT_ACTIVE_KID "$kid"

secret=$(grep '^SERVICE_CLIENT_SECRET=' .env | cut -d= -f2- || true)
if [ -z "$secret" ]; then
  secret=$(openssl rand -hex 32)
  set_env SERVICE_CLIENT_SECRET "$secret"
  echo "keys: generated the temporal-worker client secret"
fi
set_env SERVICE_CLIENT_SECRET_SHA256 "$(printf %s "$secret" | openssl dgst -sha256 -r | cut -d' ' -f1)"
