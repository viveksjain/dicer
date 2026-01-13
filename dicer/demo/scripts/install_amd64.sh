#!/bin/bash
# Install prerequisites for the Dicer Assigner demo. Note that this script supports x86_64 only.
set -euo pipefail

echo "=========================================="
echo "Installing Prerequisites to $HOME/bin"
echo "=========================================="
echo ""

# Create bin directory if it doesn't exist
mkdir -p $HOME/bin

# Add $HOME/bin to PATH (if not already in your PATH)
export PATH=$PATH:$HOME/bin

# ============================================================================
# Install KIND
# ============================================================================
if command -v kind &> /dev/null; then
    echo "KIND is already installed ($(kind version))"
    echo "Skipping KIND installation."
else
    echo "Installing KIND..."
    curl -Lo $HOME/bin/kind https://kind.sigs.k8s.io/dl/v0.20.0/kind-linux-amd64
    chmod +x $HOME/bin/kind
    kind version
    echo "KIND installation complete!"
fi
echo ""

# ============================================================================
# Install Helm
# ============================================================================
if command -v helm &> /dev/null; then
    echo "Helm is already installed ($(helm version --short))"
    echo "Skipping Helm installation."
else
    echo "Installing Helm..."
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | USE_SUDO=false HELM_INSTALL_DIR=$HOME/bin bash
    chmod +x $HOME/bin/helm
    helm version --short
    echo "Helm installation complete!"
fi
echo ""

# ============================================================================
# Install Bazelisk
# ============================================================================
if command -v bazelisk &> /dev/null; then
    echo "Bazelisk is already installed ($(bazelisk --version))"
    echo "Skipping Bazelisk installation."
else
    echo "Installing Bazelisk..."
    curl -Lo $HOME/bin/bazelisk https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-amd64
    chmod +x $HOME/bin/bazelisk
    bazelisk --version
    echo "Bazelisk installation complete!"
fi
echo ""

# ============================================================================
# Install Docker
# ============================================================================
if command -v docker &> /dev/null; then
    echo "Docker is already installed ($(docker --version))"
    echo "Skipping Docker installation."
else
    echo "Installing Docker..."
    echo "Note: Docker installation requires sudo privileges and will be installed system-wide."
    echo ""

    # Install Docker using the official convenience script
    curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
    sudo sh /tmp/get-docker.sh
    rm /tmp/get-docker.sh

    # Add current user to docker group to run docker without sudo
    sudo usermod -aG docker $USER

    echo "Docker installation complete!"
fi
echo ""

echo "=========================================="
echo "All Prerequisites Installed!"
echo "=========================================="
