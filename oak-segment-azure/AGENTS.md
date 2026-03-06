# AGENTS.md — oak-segment-azure

## Module Overview

Azure Blob Storage backend for Oak's segment node store. Implements the
`SegmentNodeStorePersistence` SPI (defined in oak-segment-tar) and extends the abstract
reader/writer from oak-segment-remote.

Two SDK variants coexist:
- **Modern (v12)** — `org.apache.jackrabbit.oak.segment.azure` package, uses `com.azure.storage.blob`
- **Legacy (v8)** — `org.apache.jackrabbit.oak.segment.azure.v8` package, uses `com.microsoft.azure.storage`

SDK selection: system property `segment.azure.v12.enabled` (default: v8 for backward compatibility).

## Key Classes

| Class | Role |
|-------|------|
| `AzurePersistence` | `SegmentNodeStorePersistence` implementation (v12). Creates archive manager, journal, lock |
| `AzurePersistenceV8` | Same, using legacy v8 SDK |
| `AzureSegmentStoreService` | OSGi component that routes to v12 or v8 based on system property |
| `AzurePersistenceManager` | Factory: creates `AzurePersistence` from OSGi config (handles auth methods) |
| `AzureArchiveManager` | `SegmentArchiveManager` — manages segment archives as blob collections |
| `AzureSegmentArchiveReader` | Extends `AbstractRemoteSegmentArchiveReader` (from oak-segment-remote) |
| `AzureSegmentArchiveWriter` | Extends `AbstractRemoteSegmentArchiveWriter`, with retry and `WriteAccessController` |
| `AzureRepositoryLock` | Distributed lock via Azure blob lease with background renewal thread |
| `AzureJournalFile` | Journal stored as append blobs, rotated at configurable line limit |
| `Configuration` | OSGi metatype config (account, container, auth credentials, etc.) |

## Authentication Methods

Configured via OSGi properties (in `Configuration`), resolved in `AzurePersistenceManager`:

1. **Connection URL** — full connection string (takes precedence)
2. **Access Key** — `accountName` + `accessKey`
3. **SAS Token** — `sharedAccessSignature`
4. **Service Principal** — `clientId` + `clientSecret` + `tenantId`

Environment variables: `AZURE_ACCOUNT_NAME`, `AZURE_SECRET_KEY`, `AZURE_TENANT_ID`,
`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`

## Repository Lock

`AzureRepositoryLock` uses an Azure blob lease on a `repo.lock` blob:
- Acquires lease (default 60s duration), renews every 5s in a daemon thread
- Blocks writes via `WriteAccessController` after 20s of renewal failures
- Lease auto-expires on process crash (no manual cleanup)
- See `oak.segment.azure.lock.*` system properties below

## System Properties

| Property | Default | Purpose |
|----------|---------|---------|
| `segment.azure.v12.enabled` | false | Use modern v12 SDK instead of legacy v8 |
| `segment.azure.v12.http.verbose.enabled` | false | Verbose HTTP request logging |
| `segment.retry.policy.type` | "fixed" | Retry policy type |
| `segment.azure.retry.attempts` | 5 | Retry count for Azure operations |
| `segment.timeout.execution` | 30 | Read timeout in seconds |
| `segment.azure.batch.copy.size` | 1000 | Batch size for archive copy operations |
| `oak.segment.azure.lock.timeout` | 0 | Lock acquisition timeout (0 = fail immediately) |
| `oak.segment.azure.lock.leaseDurationInSec` | 60 | Blob lease duration |
| `oak.segment.azure.lock.leaseRenewalIntervalInSec` | 5 | Lease renewal frequency |
| `oak.segment.azure.lock.blockWritesAfterInSec` | 20 | Grace period before blocking writes on renewal failure |
| `oak.segment.azure.lock.leaseRenewalTimeoutInMs` | 5000 | Timeout for individual lease renewal calls |
| `org.apache.jackrabbit.oak.segment.azure.journal.lines` | 40000 | Max lines per journal blob before rotation |
| `azure.segment.archive.writer.retries.max` | 16 | Max retries for segment upload |
| `azure.segment.archive.writer.retries.intervalMs` | 5000 | Retry interval for segment upload |

## OSGi Configuration

PID: `org.apache.jackrabbit.oak.segment.azure.AzureSegmentStoreService`

Key properties: `accountName`, `containerName` (default "oak"), `accessKey`,
`rootPath` (default "/oak"), `connectionURL`, `sharedAccessSignature`, `blobEndpoint`,
`clientId`, `clientSecret`, `tenantId`, `role`, `enableSecondaryLocation`

## OSGi Bundle

No packages are exported — this module is a leaf bundle. It imports the SPIs from
oak-segment-tar and oak-segment-remote, and embeds Azure SDK dependencies.

## CLI Tools (`tool` package)

- `AzureCheck` — integrity check against Azure-backed segment store
- `AzureCompact` — offline compaction
- `SegmentCopy` — copy segments between stores
- `SegmentStoreMigrator` — migrate between storage backends

## Testing

Tests run against **Azurite** (Azure Storage emulator) via TestContainers:
- `AzuriteDockerRule` — JUnit rule that starts `mcr.microsoft.com/azure-storage/azurite:3.31.0`
- Tests exist in parallel for both v12 and v8 implementations
- Integration tests (IT suffix) require Docker for Azurite

The `start-azurite.sh` script is deprecated — tests now use TestContainers directly.