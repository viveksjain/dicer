#!/bin/bash
# Complete setup script for the Dicer Assigner demo.
# This script performs the full end-to-end setup:
#   1. Verifies prerequisites (KIND, Helm, Bazel, Docker) are installed.
#   2. Creates a KIND cluster.
#   2.5. (Optional) Generates TLS certificates and creates Kubernetes secrets.
#   3. Builds the Dicer Assigner binary with Bazelisk.
#   4. Builds and loads the Dicer Assigner Docker image into KIND.
#   5. Deploys the Dicer Assigner with Helm.
#   6. Builds the Demo Server binary with Bazelisk.
#   7. Builds the Demo Client binary with Bazelisk.
#   8. Builds and loads Demo Server and Client Docker images, then deploys with Helm.
#
# Prerequisites: See scripts/README.md for installation instructions.
# Usage: ./dicer/demo/scripts/setup.sh [--ssl]
#   --ssl: Enable SSL/TLS for all components (default: disabled).
set -euo pipefail

# Parse command line arguments.
USE_SSL=false
while [[ $# -gt 0 ]]; do
  case $1 in
    --ssl)
      USE_SSL=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--ssl]"
      exit 1
      ;;
  esac
done

SCRIPT_DIR="$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")"
REPO_ROOT="$(readlink -f "$SCRIPT_DIR/../../..")"
HELM_CHART_DIR="$REPO_ROOT/dicer/production/assigner/"
# Use /tmp for certificates to avoid polluting the repository.
CERTS_DIR="/tmp/dicer-demo-certs"

echo "=========================================="
echo "Dicer Assigner Demo - Complete Setup"
if [ "$USE_SSL" = true ]; then
  echo "Mode: SSL/TLS Enabled"
else
  echo "Mode: Plaintext (default)"
fi
echo "=========================================="
echo ""

# ============================================================================
# STEP 1: Verify prerequisites
# ============================================================================

# Add $HOME/bin to PATH since install_amd64.sh puts the binaries there.
echo "Adding $HOME/bin to PATH"
export PATH=$PATH:$HOME/bin

echo "Step 1/8: Verifying prerequisites..."
echo ""

MISSING_PREREQS=false

# Check KIND.
if ! command -v kind &> /dev/null; then
    echo "KIND is not installed or not in PATH"
    MISSING_PREREQS=true
else
    echo "KIND is installed ($(kind version))"
fi

# Check Helm.
if ! command -v helm &> /dev/null; then
    echo "Helm is not installed or not in PATH"
    MISSING_PREREQS=true
else
    echo "Helm is installed ($(helm version --short))"
fi

# Check Bazelisk.
if ! command -v bazelisk &> /dev/null; then
    echo "Bazelisk is not installed or not in PATH"
    MISSING_PREREQS=true
else
    echo "Bazelisk is installed ($(bazelisk --version))"
fi

# Check Docker.
if ! command -v docker &> /dev/null; then
    echo "Docker is not installed or not in PATH"
    MISSING_PREREQS=true
elif ! docker ps &> /dev/null; then
    echo "Docker is installed but not running or not accessible"
    MISSING_PREREQS=true
else
    echo "Docker is installed and running"
fi

if [ "$MISSING_PREREQS" = true ]; then
    echo ""
    echo "Missing prerequisites detected!"
    echo "Please install the missing tools. See scripts/README.md for installation instructions."
    exit 1
fi

echo ""
echo "All prerequisites are installed!"
echo ""

# ============================================================================
# STEP 2: Create KIND cluster
# ============================================================================

echo "Step 2/8: Setting up KIND cluster..."
echo ""

# Check if cluster exists and is running.
if kind get clusters 2>/dev/null | grep -q "^dicer-demo$"; then
    # Cluster exists, check if it's actually running.
    if docker ps --filter "name=dicer-demo-control-plane" --filter "status=running" | grep -q "dicer-demo-control-plane"; then
        echo "KIND cluster 'dicer-demo' already exists and is running."
        echo "Reusing existing cluster."
    else
        echo "KIND cluster 'dicer-demo' exists but is not running. Starting..."
        docker start dicer-demo-control-plane
    fi
else
    echo "Creating KIND cluster 'dicer-demo'..."
    kind create cluster --name dicer-demo
    echo "KIND cluster created!"
fi

echo "KIND cluster is ready!"
echo ""

# Ensure kubectl is using the correct context.
kubectl config use-context kind-dicer-demo

# ============================================================================
# STEP 2.5: Generate TLS certificates and create Kubernetes secrets (if SSL enabled)
# ============================================================================

if [ "$USE_SSL" = true ]; then
  echo "Step 2.5/8: Generating TLS certificates and creating Kubernetes secrets..."
  echo ""

  # Generate certificates.
  "$SCRIPT_DIR/generate-certs.sh"

  # Create namespaces before Helm deployment. We need this because TLS secrets must be
  # created in the namespace before the Helm chart is deployed.
  kubectl create namespace dicer-assigner --dry-run=client -o yaml | kubectl apply -f -
  kubectl create namespace demo-server --dry-run=client -o yaml | kubectl apply -f -
  kubectl create namespace demo-client --dry-run=client -o yaml | kubectl apply -f -

  # Create secrets for each service.
  echo "Creating TLS secrets..."

  # Assigner secret.
  kubectl delete secret dicer-assigner-tls -n dicer-assigner --ignore-not-found=true
  kubectl create secret generic dicer-assigner-tls \
    --from-file=cert.pem="$CERTS_DIR/assigner-cert.pem" \
    --from-file=key.pem="$CERTS_DIR/assigner-key.pem" \
    --from-file=ca.pem="$CERTS_DIR/ca.pem" \
    -n dicer-assigner

  # Demo server secret.
  kubectl delete secret demo-server-tls -n demo-server --ignore-not-found=true
  kubectl create secret generic demo-server-tls \
    --from-file=cert.pem="$CERTS_DIR/server-cert.pem" \
    --from-file=key.pem="$CERTS_DIR/server-key.pem" \
    --from-file=ca.pem="$CERTS_DIR/ca.pem" \
    -n demo-server

  # Demo client secret.
  kubectl delete secret demo-client-tls -n demo-client --ignore-not-found=true
  kubectl create secret generic demo-client-tls \
    --from-file=cert.pem="$CERTS_DIR/client-cert.pem" \
    --from-file=key.pem="$CERTS_DIR/client-key.pem" \
    --from-file=ca.pem="$CERTS_DIR/ca.pem" \
    -n demo-client

  echo "TLS certificates generated and Kubernetes secrets created!"
  echo ""
fi

# ============================================================================
# STEP 3: Build Dicer Assigner binary with Bazelisk
# ============================================================================

echo "Step 3/8: Building Dicer Assigner binary..."
echo "Repository root: $REPO_ROOT"
echo ""

cd "$REPO_ROOT"
bazelisk build //dicer/demo:assigner_main_deploy.jar

echo "Bazelisk build complete!"
echo ""

# ============================================================================
# STEP 4: Build Docker image and load into KIND
# ============================================================================

echo "Step 4/8: Building and loading Docker image..."
echo ""

# Copy JAR to build context. First clean up any existing JAR from a potentially incomplete previous
# run.
rm -f "$REPO_ROOT/dicer/demo/assigner.jar"
JAR_PATH="$(readlink -f bazel-bin/dicer/demo/assigner_main_deploy.jar)"
cp "$JAR_PATH" "$REPO_ROOT/dicer/demo/assigner.jar"

# Build Docker image.
docker build -f dicer/demo/deploy/docker/Dockerfile.assigner -t dicer-assigner:demo .

# Clean up temporary JAR.
rm -f "$REPO_ROOT/dicer/demo/assigner.jar"

# Load into KIND.
kind load docker-image dicer-assigner:demo --name dicer-demo

echo "Docker image built and loaded into KIND!"
echo ""

# ============================================================================
# STEP 5: Deploy with Helm
# ============================================================================

echo "Step 5/8: Deploying Dicer Assigner with Helm..."
echo ""

if [ "$USE_SSL" = true ]; then
  ASSIGNER_SSL_VALUES=(-f dicer/demo/deploy/helm/dicer-assigner/values-ssl.yaml)
  echo "Deploying Dicer Assigner with SSL/TLS enabled..."
else
  ASSIGNER_SSL_VALUES=()
  echo "Deploying Dicer Assigner..."
fi
# --reset-values clears any TLS settings from a previous --ssl run.
helm upgrade --install dicer-assigner "$HELM_CHART_DIR" \
  --namespace dicer-assigner \
  -f dicer/demo/deploy/helm/dicer-assigner/values.yaml \
  "${ASSIGNER_SSL_VALUES[@]}" \
  --reset-values \
  --create-namespace \
  --wait \
  --timeout 90s

echo ""
echo "Assigner deployment complete!"
echo ""

# ============================================================================
# STEP 6: Build Demo Server binary with Bazelisk
# ============================================================================

echo "Step 6/8: Building Demo Server binary..."
echo ""

bazelisk build //dicer/demo/src/server:demo_server_main_deploy.jar

echo "Demo Server build complete!"
echo ""

# ============================================================================
# STEP 7: Build Demo Client binary with Bazelisk
# ============================================================================

echo "Step 7/8: Building Demo Client binary..."
echo ""

bazelisk build //dicer/demo/src/client:demo_client_main_deploy.jar

echo "Demo Client build complete!"
echo ""

# ============================================================================
# STEP 8: Build and load Demo Server and Client Docker images
# ============================================================================

echo "Step 8/8: Building and loading Demo Server and Client Docker images..."
echo ""

# Build and load Demo Server image.
rm -f "dicer/demo/demo-server.jar"
cp "bazel-bin/dicer/demo/src/server/demo_server_main_deploy.jar" "dicer/demo/demo-server.jar"

docker build -f dicer/demo/deploy/docker/Dockerfile.demo-server -t demo-server:demo .
rm -f "dicer/demo/demo-server.jar"

kind load docker-image demo-server:demo --name dicer-demo

echo "Demo Server Docker image built and loaded!"

# Build and load Demo Client image.
rm -f "dicer/demo/demo-client.jar"
cp "bazel-bin/dicer/demo/src/client/demo_client_main_deploy.jar" "dicer/demo/demo-client.jar"

docker build -f dicer/demo/deploy/docker/Dockerfile.demo-client -t demo-client:demo .
rm -f "dicer/demo/demo-client.jar"

kind load docker-image demo-client:demo --name dicer-demo

echo "Demo Client Docker image built and loaded!"
echo ""

echo "Updating Helm dependencies..."
helm dependency update dicer/demo/deploy/helm/demo-server/
helm dependency update dicer/demo/deploy/helm/demo-client/
echo ""

if [ "$USE_SSL" = true ]; then
  SERVER_SSL_VALUES=(-f dicer/demo/deploy/helm/demo-server/values-ssl.yaml)
  echo "Deploying Demo Server with SSL/TLS enabled..."
else
  SERVER_SSL_VALUES=()
  echo "Deploying Demo Server..."
fi
# --reset-values clears any TLS settings from a previous --ssl run.
helm upgrade --install demo-server dicer/demo/deploy/helm/demo-server/ \
  --namespace demo-server \
  "${SERVER_SSL_VALUES[@]}" \
  --reset-values \
  --create-namespace \
  --wait \
  --timeout 120s

echo "Demo Server deployed!"
echo ""

if [ "$USE_SSL" = true ]; then
  CLIENT_SSL_VALUES=(-f dicer/demo/deploy/helm/demo-client/values-ssl.yaml)
  echo "Deploying Demo Client with SSL/TLS enabled..."
else
  CLIENT_SSL_VALUES=()
  echo "Deploying Demo Client..."
fi
# --reset-values clears any TLS settings from a previous --ssl run.
helm upgrade --install demo-client dicer/demo/deploy/helm/demo-client/ \
  --namespace demo-client \
  "${CLIENT_SSL_VALUES[@]}" \
  --reset-values \
  --create-namespace \
  --wait \
  --timeout 120s

echo "Demo Server and Client deployed!"
echo ""

# ============================================================================
# Setup Complete - Show status and next steps
# ============================================================================

echo "=========================================="
echo "Dicer Demo Setup Complete!"
echo "=========================================="
echo ""
echo "Example commands to check status:"
echo "  kubectl get pods -n dicer-assigner"
echo "  kubectl get pods -n demo-server"
echo "  kubectl get pods -n demo-client"
echo "  kubectl logs --tail=50 -n demo-server -l app=demo-server"
echo "  kubectl logs --tail=50 -n demo-client -l app=demo-client"
echo ""
echo "Watch the client making requests:"
echo "  kubectl logs -f --tail=50 -n demo-client deployment/demo-client"
echo ""
echo "To clean up:"
echo "  dicer/demo/scripts/cleanup.sh"
echo ""
