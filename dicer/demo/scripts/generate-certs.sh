#!/bin/bash
# Generate self-signed TLS certificates for Dicer demo.
set -euo pipefail

SCRIPT_DIR="$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")"
REPO_ROOT="$(readlink -f "$SCRIPT_DIR/../../..")"
# Use /tmp for certificates to avoid polluting the repository.
CERTS_DIR="/tmp/dicer-demo-certs"

echo "Generating TLS certificates in: $CERTS_DIR"

rm -rf "$CERTS_DIR"
mkdir -p "$CERTS_DIR"
cd "$CERTS_DIR"

# Generate CA certificate.
openssl req -x509 -newkey rsa:2048 -keyout ca-key.pem -out ca.pem \
  -days 3650 -nodes -subj "/CN=Dicer Demo CA" 2>/dev/null

# Generate certificate for a service.
gen_cert() {
  local name=$1 cn=$2 sans=${3:-}

  openssl req -newkey rsa:2048 -nodes -keyout ${name}-key-temp.pem \
    -out ${name}.csr -subj "/CN=$cn" 2>/dev/null
  openssl pkcs8 -topk8 -nocrypt -in ${name}-key-temp.pem \
    -out ${name}-key.pem 2>/dev/null

  if [ -n "$sans" ]; then
    openssl x509 -req -in ${name}.csr -CA ca.pem -CAkey ca-key.pem \
      -CAcreateserial -out ${name}-cert.pem -days 3650 \
      -extfile <(printf "subjectAltName=$sans") 2>/dev/null
  else
    openssl x509 -req -in ${name}.csr -CA ca.pem -CAkey ca-key.pem \
      -CAcreateserial -out ${name}-cert.pem -days 3650 2>/dev/null
  fi

  rm ${name}-key-temp.pem ${name}.csr
}

# Generate certificates with DNS SANs for Kubernetes services.
# gRPC clients can use overrideAuthority() to verify against DNS names when connecting via pod IPs.
gen_cert assigner "dicer-assigner.dicer-assigner.svc.cluster.local" \
  "DNS:dicer-assigner.dicer-assigner.svc.cluster.local,DNS:dicer-assigner,DNS:localhost,IP:127.0.0.1"
gen_cert server "demo-server.demo-server.svc.cluster.local" \
  "DNS:demo-server.demo-server.svc.cluster.local,DNS:demo-server,DNS:localhost,IP:127.0.0.1"
gen_cert client "demo-client"

rm -f ca.srl
echo "Certificate generation complete!"
