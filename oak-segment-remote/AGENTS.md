# AGENTS.md — oak-segment-remote

## Module Overview

Shared base module for cloud segment store backends (oak-segment-azure, oak-segment-aws).
Provides abstract archive reader/writer implementations, an async write queue, persistent
caching (disk and Redis), and write access control.

This module does **not** implement `SegmentNodeStorePersistence` itself — it provides the
building blocks that cloud-specific modules extend.

## Key Classes

| Class | Purpose |
|-------|---------|
| `AbstractRemoteSegmentArchiveReader` | Template for cloud segment readers. Subclasses implement `doReadSegmentToBuffer()` and `doReadDataFile()` |
| `AbstractRemoteSegmentArchiveWriter` | Template for cloud segment writers with optional async queue. Subclasses implement `doWriteArchiveEntry()` and `doWriteDataFile()` |
| `WriteAccessController` | Thread-safe gate for write operations. `disableWriting()` blocks all threads calling `checkWritingAllowed()` until `enableWriting()` is called. Used by repository lock implementations to pause writes during lease renewal failures |
| `RemoteSegmentArchiveEntry` | `SegmentArchiveEntry` implementation carrying UUID, position, length, and generation |
| `RemoteUtilities` | Segment file naming (`{position}.{uuid}`), archive indexing, off-heap buffer allocation |
| `RemoteBlobMetadata` | Serialization of segment metadata to/from blob storage metadata headers |

## Async Write Queue (`queue` package)

`SegmentWriteQueue` provides concurrent segment uploads:
- Thread pool size: `oak.segment.remote.threads` (default 5) + 1 emergency retry thread
- Queue capacity: `oak.segment.remote.queue.size` (default 20)
- Failed writes are retried in a dedicated recovery loop
- Queue is flushed and closed when the archive writer closes

## Persistent Cache (`persistentcache` package)

Two cache implementations for reducing cloud read latency:

| Implementation | Backend | Key config |
|----------------|---------|------------|
| `PersistentDiskCache` | Local filesystem with LRU eviction | `diskCacheDirectory`, `diskCacheMaxSizeMB` (default 512) |
| `PersistentRedisCache` | Redis via Jedis connection pool | `redisCacheHost`, `redisCachePort`, `redisCacheExpireSeconds` (default 2 days) |

OSGi PID: `org.apache.jackrabbit.oak.segment.remote.RemotePersistentCacheService`

## System Properties

| Property | Default | Purpose |
|----------|---------|---------|
| `access.off.heap` | false | Use direct (off-heap) ByteBuffers for segment data |
| `oak.segment.remote.threads` | 5 | Write queue worker thread count |
| `oak.segment.remote.queue.size` | 20 | Write queue capacity |

## OSGi Exports

All packages are exported (used by oak-segment-azure and oak-segment-aws):
```
org.apache.jackrabbit.oak.segment.remote
org.apache.jackrabbit.oak.segment.remote.persistentcache
org.apache.jackrabbit.oak.segment.remote.queue
```

## Testing

- `WriteAccessControllerTest` — write gating behavior
- `SegmentWriteQueueTest` — async queue threading and retry logic
- `PersistentDiskCacheTest` — disk cache with LRU eviction
- `PersistentRedisCacheTest` — Redis cache (uses embedded Redis)