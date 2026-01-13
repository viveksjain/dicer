#!/bin/bash
# Tears down the Dicer demo environment by deleting the KIND cluster.
# Removes the entire "dicer-demo" cluster and all resources within it.
# Also removes TLS certificates if they were generated.
set -euo pipefail

# Use /tmp for certificates to match setup.sh
CERTS_DIR="/tmp/dicer-demo-certs"

echo "Cleaning up Dicer demo environment..."
echo ""

# Delete the KIND cluster
if kind get clusters 2>/dev/null | grep -q "^dicer-demo$"; then
    kind delete cluster --name dicer-demo
    echo "KIND cluster 'dicer-demo' deleted."
else
    echo "KIND cluster 'dicer-demo' does not exist."
fi

# Remove TLS certificates directory if it exists
if [ -d "$CERTS_DIR" ]; then
    rm -rf "$CERTS_DIR"
    echo "TLS certificates directory removed."
fi

echo ""
echo "Cleanup complete!"
