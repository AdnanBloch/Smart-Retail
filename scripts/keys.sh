#!/usr/bin/env bash
# Generate the RS256 signing key pair if none exists (RFC 0003 IDN-5).
#   keys/<kid>.pem       private key, mounted into identity only
#   keys/jwt_public.pem  public key, mounted into every verifier
# The kid is written to .env as JWT_ACTIVE_KID.
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

if grep -q '^JWT_ACTIVE_KID=' .env; then
  sed -i.bak "s/^JWT_ACTIVE_KID=.*/JWT_ACTIVE_KID=$kid/" .env && rm -f .env.bak
else
  echo "JWT_ACTIVE_KID=$kid" >> .env
fi
