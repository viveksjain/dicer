# Dicer Demo Helm Deployment

This directory contains Helm charts for deploying the Dicer demo in a local Kubernetes cluster (KIND). The demo showcases Dicer's distributed sharding capabilities through a client-server cache service architecture.

**For getting started, running scenarios, and general usage, see the [main demo README](../../README.md).**

This document explains the Helm chart structure and deployment internals for users who want to understand or modify the deployment configuration.

## 1. Architecture

The demo deploys three components in separate Kubernetes namespaces:

```
┌─────────────────────────────────────────────────────┐
│  demo-client namespace                              │
│  ┌──────────────────────────────────────────────┐   │
│  │ Demo Client (1 replica)                      │   │
│  │ - Sends cache requests (GET/PUT/DELETE)      │   │
│  │ - Watches assignments from Slicelet          │   │
│  │ - Port 7777: Info/debug endpoint             │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
               |                       │
               |  Cache requests +     │ 
               |  Watch assignments    |
               ▼                       ▼
┌─────────────────────────────────────────────────────┐
│  demo-server namespace                              │
│  ┌──────────────────────────────────────────────┐   │
│  │ Demo Server (2 replicas)                     │   │
│  │ - Port 8080: Client gRPC API (Get/Put/Del)   │   │
│  │ - Port 24510: Slicelet API (assignments)     │   │
│  │ - Port 7777: Info/debug endpoint             │   │
│  │ - Service: demo-server (ClusterIP)           │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
                        │
                        │ Watch assignments from Assigner
                        ▼
┌─────────────────────────────────────────────────────┐
│  dicer-assigner namespace                           │
│  ┌──────────────────────────────────────────────┐   │
│  │ Dicer Assigner (1 replica)                   │   │
│  │ - Generates slice assignments                │   │
│  │ - Monitors Slicelet health via heartbeats    │   │
│  │ - Port 24500: Assignment watch gRPC API      │   │
│  │ - Port 7777: Info/debug endpoint             │   │
│  │ - Service: dicer-assigner (ClusterIP)        │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
```

## 2. Deployment

**You do not need to run Helm commands manually.** Use the [setup.sh](../../scripts/setup.sh) script which handles all deployment steps automatically.

### 2.1 Prerequisites

The [setup.sh](../../scripts/setup.sh) script verifies all prerequisites and will fail early if anything is missing:

1. **KIND cluster**: Creates or reuses existing `dicer-demo` cluster
2. **Docker images**: Builds using Bazel and loads into KIND
3. **Helm 3.x**: Must be installed and available in PATH

### 2.2 Plain Deployment (No TLS)

The [setup.sh](../../scripts/setup.sh) script performs these steps:

1. **Updates Helm dependencies**: Runs `helm dependency update` to download the `demo-common` chart for both demo-server and demo-client
2. **Deploys Assigner**: Installs the production Assigner chart with demo-specific values from `dicer-assigner/values.yaml`
3. **Deploys Demo Server**: Installs demo-server chart with default values (plain HTTP, 2 replicas)
4. **Deploys Demo Client**: Installs demo-client chart with default values (plain HTTP, 1 replica)

All communication uses plain HTTP without encryption.

### 2.3 TLS Deployment

The [setup.sh](../../scripts/setup.sh) script with `--ssl` flag performs these additional steps:

1. **Generates certificates**: Runs [generate-certs.sh](../../scripts/generate-certs.sh) to create self-signed CA and certificates for all components
2. **Creates Kubernetes secrets**: Stores certificates in secrets (`dicer-assigner-tls`, `demo-server-tls`, `demo-client-tls`) in each namespace
3. **Deploys with TLS values**: Uses both base `values.yaml` and `values-ssl.yaml` overlay files for each component

All communication uses mutual TLS (mTLS) with certificate-based authentication.

**Helm values precedence**: When multiple `-f` flags are used, values from later files override earlier ones. TLS-specific values merge with and override base values.

## 3. Directory Structure

```
helm/
├── demo-client/              # Helm chart for demo client
│   ├── Chart.yaml            # Chart metadata, depends on demo-common
│   ├── templates/
│   │   ├── configmap.yaml    # DB_CONF with slicelet host + TLS paths
│   │   ├── deployment.yaml   # 1 replica deployment
│   ├── values.yaml           # Base values (plaintext, 1 replica)
│   └── values-ssl.yaml       # TLS overrides

├── demo-server/              # Helm chart for demo server (Slicelet + gRPC)
│   ├── Chart.yaml            # Chart metadata, depends on demo-common
│   ├── templates/
│   │   ├── configmap.yaml    # DB_CONF with assigner host + TLS paths
│   │   ├── deployment.yaml   # 2 replicas, ports 8080/24510/7777
│   │   └── service.yaml      # ClusterIP service for gRPC and slicelet
│   ├── values.yaml           # Base values (2 replicas, plaintext)
│   └── values-ssl.yaml       # TLS overrides

├── demo-common/              # Shared Helm templates (library chart)
│   ├── Chart.yaml            # Chart metadata
│   └── templates/
│       └── _helpers.tpl      # Template helpers: labels, ports, TLS config

└── dicer-assigner/           # Values overrides for production Assigner chart
    ├── values.yaml           # Base demo settings
    └── values-ssl.yaml       # TLS overrides
```

## 4. Helm Charts Explained

The demo uses three Helm charts that follow a consistent pattern. Both **demo-server** and **demo-client** are full Helm charts with their own templates (deployment, configmap) and depend on **demo-common**, a library chart that provides shared template helpers for labels, ports, and TLS configuration. Namespaces are created automatically by Helm's `--create-namespace` flag. Each chart has a base `values.yaml` for plaintext configuration and a `values-ssl.yaml` overlay that enables TLS and mounts certificates. The templates conditionally include TLS paths based on whether `tls.enabled` is set. The demo-client chart differs only in that it doesn't create a Kubernetes Service since clients don't accept inbound connections.

The **dicer-assigner** chart is different - it doesn't contain templates. Instead, it provides only `values.yaml` and `values-ssl.yaml` files that override the production Assigner chart located at `dicer/production/assigner/`. When deploying, the [setup.sh](../../scripts/setup.sh) script passes these demo-specific values to the production chart using Helm's `-f` flag, allowing the demo to use production-grade templates while customizing settings like replica count and TLS configuration for local development.

## 5. Configuration

For the location configuration, see the [User Guide](../../../../docs/UserGuide.md).

### 5.1 Service Discovery

All components use Kubernetes DNS for service discovery:

| Component | Service FQDN | Port | Purpose |
|-----------|-------------|------|---------|
| Dicer Assigner | `dicer-assigner.dicer-assigner.svc.cluster.local` | 24500 | Assignment watch API |
| Demo Server | `demo-server.demo-server.svc.cluster.local` | 8080 | Client gRPC API (Get/Put/Delete) |
| Demo Server | `demo-server.demo-server.svc.cluster.local` | 24510 | Slicelet assignment API |

**Why FQDN?** Components run in separate namespaces, requiring fully qualified domain names for cross-namespace communication.

**DNS Resolution**:
- `demo-server` (short name): Resolves within same namespace only
- `demo-server.demo-server.svc.cluster.local` (FQDN): Resolves from any namespace

### 5.2 Environment Variables

Environment variables are defined directly in each deployment template.

**Demo Server** (`demo-server/templates/deployment.yaml`):
- **POD_IP**: Pod's IP address (`status.podIP`). Used by Slicelet for registration.
- **POD_UID**: Kubernetes pod UID (`metadata.uid`). Unique identifier for pod lifecycle.
- **NAMESPACE**: Namespace name (`metadata.namespace`). Used for service discovery.
- **LOCATION**: JSON string with `kubernetes_cluster_uri`. Identifies cluster.
- **DB_CONF**: JSON configuration from ConfigMap, includes assigner host and optional TLS paths.

**Demo Client** (`demo-client/templates/deployment.yaml`):
- **LOCATION**: JSON string with `kubernetes_cluster_uri`. Identifies cluster.
- **SLICELET_HOST**: FQDN of demo-server service.
- **DB_CONF**: JSON configuration from ConfigMap, includes optional TLS paths.

### 5.3 Port Configuration

**Demo Server**:
- `8080`: Client gRPC API (Get, Put, Delete operations)
- `24510`: Slicelet API (receives assignments from Assigner, serves assignments to Clients)
- `7777`: Info/debug HTTP endpoint (HTML status page)

**Demo Client**:
- `7777`: Info/debug HTTP endpoint

**Dicer Assigner**:
- `24500`: Assignment watch gRPC API (Slicelets subscribe to assignment updates)
- `7777`: Info/debug HTTP endpoint

## 6. TLS/SSL Configuration

### 6.1 Certificate Generation

The [generate-certs.sh](../../scripts/generate-certs.sh) script creates self-signed certificates for demo purposes and is automatically called by [setup.sh](../../scripts/setup.sh) when using the `--ssl` flag.

The script generates:
- **CA certificate** (`ca.pem`, `ca-key.pem`): Root certificate authority
- **Assigner certificate** (`assigner-cert.pem`, `assigner-key.pem`): Server cert with SAN for `dicer-assigner.dicer-assigner.svc.cluster.local`
- **Demo Server certificate** (`server-cert.pem`, `server-key.pem`): Server cert with SAN for `demo-server.demo-server.svc.cluster.local`
- **Client certificate** (`client-cert.pem`, `client-key.pem`): Client cert for mutual TLS

The script also creates Kubernetes secrets in each namespace (`dicer-assigner-tls`, `demo-server-tls`, `demo-client-tls`) containing the certificate, private key, and CA bundle.

### 6.2 TLS Configuration Properties

Dicer supports PEM-based TLS configuration (industry standard):

Configuration properties injected into application ConfigMaps:
- **`databricks.rpc.cert`**: Path to X.509 certificate file (server or client certificate)
- **`databricks.rpc.key`**: Path to private key file corresponding to certificate
- **`databricks.rpc.truststore`**: Path to CA bundle for validating peer certificates

Property values reference the mounted certificate paths (e.g., `/etc/dicer/certs/cert.pem`). Generated via `demo-common.tls.configJson` template helper and injected into ConfigMaps when `tls.enabled=true`.

### 6.3 TLS Architecture

Mutual TLS (mTLS) is enabled between all components:

```
Demo Client  --[mTLS]-->  Demo Server  --[mTLS]-->  Dicer Assigner
    │                          │                          │
    └─ cert.pem                └─ cert.pem                └─ cert.pem
    └─ key.pem                 └─ key.pem                 └─ key.pem
    └─ ca.pem                  └─ ca.pem                  └─ ca.pem
```

Each component:
1. **Presents its certificate** to peers for authentication
2. **Validates peer certificates** against the CA bundle
3. **Encrypts all traffic** using TLS 1.2+

### 6.4 Verify TLS Certificates

```bash
# Check secret exists
kubectl get secret -n demo-server demo-server-tls

# View certificate details
kubectl get secret -n demo-server demo-server-tls \
  -o jsonpath='{.data.cert\.pem}' | \
  base64 -d | \
  openssl x509 -text -noout

# Verify SAN includes service DNS name
kubectl get secret -n demo-server demo-server-tls \
  -o jsonpath='{.data.cert\.pem}' | \
  base64 -d | \
  openssl x509 -noout -text | \
  grep -A1 "Subject Alternative Name"
```

Expected output includes:
```
DNS:demo-server.demo-server.svc.cluster.local
```

## 7. Troubleshooting

### 7.1 Helm-Specific Issues

**Issue**: Helm release not found
```bash
helm list -n demo-server
```
**Cause**: Release was not installed or was uninstalled.
**Solution**: Run [setup.sh](../../scripts/setup.sh) to install.

**Issue**: Chart dependency not found
```bash
helm dependency list dicer/demo/deploy/helm/demo-server/
```
**Cause**: `demo-common` chart not downloaded.
**Solution**: Run `helm dependency update dicer/demo/deploy/helm/demo-server/` (this is done automatically by setup.sh)

**Issue**: Values not taking effect
```bash
helm get values demo-server -n demo-server
```
**Cause**: Values file not passed with `-f` flag, or overridden by later values.
**Solution**: Check `helm get values` output. Ensure correct `-f` flags in deployment command.

### 7.2 Verify TLS Configuration

Only applicable when deployed with TLS enabled:

```bash
# Check secrets exist
kubectl get secrets -n dicer-assigner dicer-assigner-tls
kubectl get secrets -n demo-server demo-server-tls
kubectl get secrets -n demo-client demo-client-tls

# Verify secret contains correct keys
kubectl describe secret -n demo-server demo-server-tls

# Decode and inspect certificate
kubectl get secret -n demo-server demo-server-tls \
  -o jsonpath='{.data.cert\.pem}' | \
  base64 -d | \
  openssl x509 -text -noout

# Check certificate expiration
kubectl get secret -n demo-server demo-server-tls \
  -o jsonpath='{.data.cert\.pem}' | \
  base64 -d | \
  openssl x509 -noout -dates
```

### 7.3 Check ConfigMaps

```bash
# View server ConfigMap (includes assigner host + TLS paths if enabled)
kubectl get configmap -n demo-server demo-server-config -o yaml

# View client ConfigMap (includes TLS paths if enabled)
kubectl get configmap -n demo-client demo-client-config -o yaml
```

### 7.4 Test Connectivity

```bash
# Port forward to demo-server
kubectl port-forward -n demo-server svc/demo-server 8080:8080

# In another terminal, test gRPC endpoint (requires grpcurl)
grpcurl -plaintext localhost:8080 list

# Expected output:
# com.databricks.dicer.demo.DemoService
# grpc.health.v1.Health
```

## 8. Additional Resources

- **Production Assigner Chart**: [dicer/production/assigner/](../../../production/assigner/) - Full Helm chart for production deployments
- **Demo Scripts**: [dicer/demo/scripts/](../../scripts/) - Setup, cleanup, and certificate generation scripts
- **Demo README**: [dicer/demo/README.md](../../README.md) - Overall demo overview and scenarios
- **Source Code**: [dicer/demo/src/](../../src/) - Client, server, and common code
- **TLS Configuration**: [CommonSslConf.scala](../../../common/src/CommonSslConf.scala) and [ServerConf.scala](../../../caching/util/src/ServerConf.scala) - SSL/TLS configuration implementation

## 9. FAQ

**Q1: Why use Helm instead of plain Kubernetes YAML?**

A: Helm provides templating for reusable configurations, values-based customization, dependency management, versioning/rollback capabilities, and easier upgrades.

**Q2: Why separate charts for client and server?**

A: Mirrors production deployment patterns where client and server are different services deployed independently. Allows scaling, upgrading, and troubleshooting each component separately.

**Q3: What is demo-common and why is it a dependency?**

A: demo-common contains shared Helm template helpers used by both client and server charts. Declared as a local file dependency (`file://../demo-common`) so charts can reference common templates without duplication.

**Q4: Why two values files (values.yaml and values-ssl.yaml)?**

A: Follows Helm best practices for configuration variants. Base values.yaml contains defaults. values-ssl.yaml contains only TLS-specific overrides. Use `-f` flags to merge them: `helm install ... -f values.yaml -f values-ssl.yaml`.

**Q5: How do I change the number of demo-server replicas?**

A: Edit `demo-server/values.yaml` and change `replicaCount: 2`, then re-run [setup.sh](../../scripts/setup.sh). Alternatively, use `--set`: `helm upgrade demo-server ... --set replicaCount=4`.

**Q6: Can I use these charts for production?**

A: No, these are demo charts with simplified configuration (pullPolicy: Never, no resource limits, single assigner replica, no persistent storage, self-signed certificates). Use `dicer/production/assigner/` chart for production deployments.

**Q7: Why do servers use `terminationGracePeriodSeconds: 60`?**

A: Gives Dicer time to detect pod termination, generate new assignments, stream updates to other pods, and allow in-flight requests to complete. This ensures zero downtime during rolling updates.

**Q8: What's the difference between dicer-assigner values in helm/ vs production/assigner/?**

A:
- `helm/dicer-assigner/values.yaml`: Demo overrides (image tag, replica count, location)
- `production/assigner/Chart.yaml`: Full chart with templates, deployment, service, etc.

Demo values are overlays on production chart. Use both together:
```bash
helm install dicer-assigner dicer/production/assigner/ \
  -f dicer/demo/deploy/helm/dicer-assigner/values.yaml
```
