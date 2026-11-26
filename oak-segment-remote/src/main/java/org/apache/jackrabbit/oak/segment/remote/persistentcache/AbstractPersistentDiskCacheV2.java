/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.jackrabbit.oak.segment.remote.persistentcache;

import java.nio.ByteBuffer;
import org.apache.jackrabbit.oak.cache.AbstractCacheStats;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.api.Cache;
import org.apache.jackrabbit.oak.cache.api.CacheBuilder;
import org.apache.jackrabbit.oak.cache.api.CacheStatsSnapshot;
import org.apache.jackrabbit.oak.cache.api.EvictionCause;
import org.apache.jackrabbit.oak.cache.api.Weigher;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.log.LogSilencer;
import org.apache.jackrabbit.oak.commons.properties.SystemPropertySupplier;
import org.apache.jackrabbit.oak.segment.spi.SegmentReadInterruptedException;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.PersistentCache;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class for the V2 persistent disk cache. Owns the shared structure — an Oak {@link Cache} keyed by segment id
 * with size-in-bytes as value, weight-based eviction, an eviction listener that decrements the size counter and
 * deletes the on-disk file, restart-survival rehydration of the cache directory, and the {@link #readSegment}
 * template that runs a loader on a cache miss.
 * <p>
 * Subclasses implement four small hooks to decide <em>how</em> a loaded segment is persisted:
 * <ul>
 *     <li>{@link #handleLoadedSegment(UUID, ByteBuffer)} — called inside the cache-miss loader. The synchronous
 *         variant writes the file here; the asynchronous variant enqueues for a background writer.</li>
 *     <li>{@link #getPendingSegment(UUID)} — on a cache hit, returns an in-flight buffer if one exists, else
 *         null. Default returns null. The asynchronous variant consults its pending-writes buffer.</li>
 *     <li>{@link #doClose()} — variant-specific resource release (e.g., draining the async writer pool). Default
 *         is a no-op.</li>
 *     <li>{@link #onEvictionHook(UUID)} — extra work to perform when an entry is evicted (e.g., discarding any
 *         pending write for the evicted segment). Default returns false.</li>
 * </ul>
 * <p>
 * Subclasses must call {@link #rehydrateFromDisk()} at the end of their own constructor — after any
 * variant-specific state (such as an async writer pool) is initialized — because rehydration can trigger eviction
 * if the on-disk total exceeds the configured weight, and the eviction listener calls {@link #onEvictionHook}.
 */
public abstract class AbstractPersistentDiskCacheV2 implements PersistentCache, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractPersistentDiskCacheV2.class);

    private static final String TEMP_SUFFIX = ".tmp";

    private static final long WRITE_LOG_INTERVAL = 5_000;
    private static final long READ_LOG_INTERVAL = 50_000;

    /**
     * System property controlling idle-based eviction. When positive, an entry not read or written for that
     * long is evicted. Defaults to {@code 0}, disabled.
     */
    static final String IDLE_EVICTION_SECONDS_PROP = "oak.segment.disk.cache.idle.eviction.seconds";

    final DiskCacheIOMonitor diskCacheIOMonitor;
    private final Path directory;
    // Map from segment ID to the segment's size in bytes. The segment data itself is stored on disk at
    // in bucket directories; the cache tracks membership and drives weight-based eviction. Storing
    // the size as the value lets the weigher and the removal listener observe the entry's weight without a disk
    // lookup — and lets onEviction decrement cacheSize by the exact amount inserted, even when eviction races with
    // a writer.
    private final Cache<UUID, Integer> cache;
    private final AbstractCacheStats cacheStats;
    private final AtomicLong cacheSize = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final WriteStatsRecorder writeStats = new WriteStatsRecorder();
    private final ReadStatsRecorder readStats = new ReadStatsRecorder();

    protected final LogSilencer logSilencer = new LogSilencer(10_000, 32);

    protected AbstractPersistentDiskCacheV2(Path directory, int cacheMaxSizeMB, DiskCacheIOMonitor diskCacheIOMonitor,
                                            String name) throws IOException {
        this.diskCacheIOMonitor = diskCacheIOMonitor;
        long maxCacheSizeBytes = cacheMaxSizeMB * 1024L * 1024L;
        this.directory = directory;
        Files.createDirectories(directory);  // no-op if it already exists; pre-existing files are rehydrated below
        createBuckets();

        Weigher<UUID, Integer> weigher = (key, size) -> size;
        CacheBuilder<UUID, Integer> cacheBuilder = CacheBuilder.<UUID, Integer>newBuilder()
                .maximumWeight(maxCacheSizeBytes)
                .recordStats()
                .weigher(weigher)
                .evictionListener(this::onEviction)
                // Forced synchronous so the eviction listener (file deletion, cacheSize accounting,
                // onEvictionHook) runs on the caller's thread, closing the window where a stale, delayed
                // notification for a superseded entry could race a fresh reload of the same segment id.
                .asyncMaintenance(false);
        long idleEvictionSeconds = SystemPropertySupplier.create(IDLE_EVICTION_SECONDS_PROP, 0L)
                .loggingTo(LOG)
                .validateWith(v -> v >= 0)
                .get();
        // Only set expireAfterAccess when idle-based eviction is enabled; a zero duration would
        // otherwise expire entries immediately.
        if (idleEvictionSeconds > 0) {
            cacheBuilder.expireAfterAccess(Duration.ofSeconds(idleEvictionSeconds));
            LOG.info("Persistent disk cache '{}': idle-based eviction enabled, evicting segments idle for more than {} seconds",
                    name, idleEvictionSeconds);
        } else {
            LOG.info("Persistent disk cache '{}': idle-based eviction disabled", name);
        }
        Cache<UUID, Integer> builtCache = cacheBuilder.build();
        this.cache = builtCache;
        // The read path consults the cache twice on every real miss (once via getIfPresent, then via
        // cache.get(loader)), so the cache's missCount is double-counted. The loader runs exactly once
        // per real miss, so loadCount is the true miss count — substitute it here so MBean and log
        // output report a sensible hit rate.
        this.cacheStats = new CacheStats(builtCache, name, weigher, maxCacheSizeBytes) {
            @Override
            protected CacheStatsSnapshot getCurrentStats() {
                CacheStatsSnapshot s = builtCache.stats();
                long loadCount = s.loadSuccessCount() + s.loadFailureCount();
                return new CacheStatsSnapshot(
                        s.hitCount(),
                        loadCount,
                        s.loadSuccessCount(),
                        s.loadFailureCount(),
                        s.totalLoadTime(),
                        s.evictionCount());
            }
        };
    }

    /**
     * Pre-create all 256 bucket directories ({@code 00}–{@code ff}) so the write path never has to create one.
     */
    private void createBuckets() throws IOException {
        for (int i = 0; i < 256; i++) {
            Files.createDirectories(directory.resolve(String.format("%02x", i)));
        }
    }

    // ---------- subclass hooks ----------

    /**
     * Called inside the cache-miss loader, with the buffer returned by the caller-supplied loader. The synchronous
     * variant writes the file here; the asynchronous variant enqueues for a background writer.
     */
    protected abstract void handleLoadedSegment(UUID segmentId, ByteBuffer buffer);

    /**
     * Called on a cache hit. Returns an in-flight buffer if a write for {@code segmentId} is still pending and the
     * file is not yet on disk, otherwise null. Default implementation always returns null; the asynchronous variant
     * overrides to consult its pending-writes buffer.
     */
    protected ByteBuffer getPendingSegment(UUID segmentId) {
        return null;
    }

    /**
     * Called once on the first {@link #close()} call. Default no-op; subclasses release variant-specific resources
     * (e.g., draining an async writer pool).
     */
    protected void doClose() {
        // no-op by default
    }

    /**
     * Called from the eviction listener for every removed entry, before the file is deleted and counters are
     * updated. Returns whether a pending write was discarded, in which case an absent file is expected.
     * Default returns false; subclasses use this to discard any pending write for the evicted segment.
     */
    protected boolean onEvictionHook(UUID segmentId) {
        return false;
    }

    /**
     * Optional suffix appended to the periodic write-stats log line emitted by {@link WriteStatsRecorder}.
     * Default is empty; the async variant overrides this to append the current writer-queue depth.
     */
    protected String writeStatsContext() {
        return "";
    }

    // ---------- public API ----------

    @Override
    @Nullable
    public final ByteBuffer readSegment(long msb, long lsb, @NotNull Callable<ByteBuffer> loader) {
        ensureOpen();
        try {
            return readSegmentOrRetry(new UUID(msb, lsb), loader, false);
        } catch (RuntimeException e) {
            // Don't double-wrap unchecked exceptions — preserves RepositoryNotReachableException,
            // SegmentReadInterruptedException, and any other RuntimeException thrown by the loader
            // so callers can do instanceof checks.
            throw e;
        } catch (InterruptedException e) {
            // Convert to SegmentReadInterruptedException so the segment store does not misclassify
            // it as a SegmentNotFoundException. Clear the interrupt flag defensively: this catches
            // InterruptedException from an arbitrary loader, which may have thrown it directly
            // without clearing the flag, and a left-over flag would poison subsequent reads on this
            // thread. The cancellation is now carried by the thrown SegmentReadInterruptedException.
            Thread.interrupted();
            throw new SegmentReadInterruptedException(
                    "Interrupted while reading segment " + new UUID(msb, lsb) + " from cache", e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ByteBuffer readSegmentOrRetry(UUID segmentId,
                                      Callable<ByteBuffer> loader, boolean alreadyRetried) throws Exception {
        // Fast path: cache hit. Uses getIfPresent so we avoid acquiring the cache's segment lock
        // (cache.get does, even on a hit, because it has to be prepared to run the loader).
        // The cached value is the segment size in bytes — passing it through to readHit
        // skips the fstat syscall there too.
        Integer cachedSize = cache.getIfPresent(segmentId);
        if (cachedSize != null) {
            return readHit(segmentId, cachedSize, loader, alreadyRetried);
        }

        // Slow path: cache miss. Run the loader under the segment lock; the loader stores its
        // buffer in resultHolder so the miss path returns it without a follow-up disk read.
        ByteBuffer[] resultHolder = new ByteBuffer[1];
        Integer loadedSize;
        try {
            loadedSize = cache.get(segmentId, key -> {
                ByteBuffer buffer;
                try {
                    buffer = loader.call();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    // The mapping function cannot declare checked exceptions; wrap so we can rethrow below.
                    throw new LoaderException(e);
                }
                if (buffer == null) {
                    // Loader returning null means "segment doesn't exist" per the PersistentCache contract.
                    // Returning null from the mapping function tells the cache not to create an entry.
                    return null;
                }
                // Defensive rewind, just in case the loader returns a ByteBuffer whose position is not at the start
                buffer.rewind();
                int segmentSize = buffer.remaining();
                handleLoadedSegment(segmentId, buffer);
                long newSize = cacheSize.addAndGet(segmentSize);
                diskCacheIOMonitor.updateCacheSize(newSize, segmentSize);
                resultHolder[0] = buffer;
                return segmentSize;
            });
        } catch (LoaderException e) {
            // Unwrap the checked exception thrown by the loader so callers see the original type.
            throw (Exception) e.getCause();
        }
        if (resultHolder[0] != null) {
            // Loader ran; return the buffer it produced.
            return resultHolder[0];
        }
        if (loadedSize == null) {
            // Loader reported the segment doesn't exist (returned null, no entry cached).
            return null;
        }
        // Race: another thread populated the entry between our getIfPresent and cache.get,
        // so the loader didn't run. Treat as a hit and read from disk.
        return readHit(segmentId, loadedSize, loader, alreadyRetried);
    }

    private ByteBuffer readHit(UUID segmentId, int size,
                           Callable<ByteBuffer> loader, boolean alreadyRetried) throws Exception {
        ByteBuffer pending = getPendingSegment(segmentId);
        if (pending != null) {
            return pending;
        }
        try {
            return readFromDisk(segmentId, size);
        } catch (NoSuchFileException e) {
            // File vanished between the cache reporting a hit and our open — external tampering, manual cleanup,
            // or a subtle bug. Invalidate the phantom entry (its onEviction is a no-op since the file is
            // already gone) and reload through the loader. Bounded to one retry: if it fails again the
            // problem isn't a transient race.
            if (alreadyRetried) {
                throw e;
            }
            LOG.info("Cache file missing for {}; invalidating entry and reloading via loader", segmentId);
            cache.invalidate(segmentId);
            return readSegmentOrRetry(segmentId, loader, true);
        }
    }

    @Override
    public final boolean containsSegment(long msb, long lsb) {
        ensureOpen();
        return cache.getIfPresent(new UUID(msb, lsb)) != null;
    }

    @Override
    public final void writeSegment(long msb, long lsb, ByteBuffer buffer) {
        ensureOpen();
        UUID segmentId = new UUID(msb, lsb);
        if (cache.getIfPresent(segmentId) != null) {
            return;
        }
        try {
            cache.get(segmentId, key -> {
                // FileStore may pass a buffer with a nonzero position. Give writers a zero-based view
                // of just the segment bytes so rewinding cannot expose bytes before that position.
                ByteBuffer segment = buffer.slice().asReadOnlyBuffer();
                int segmentSize = segment.remaining();
                handleLoadedSegment(segmentId, segment);
                long newSize = cacheSize.addAndGet(segmentSize);
                diskCacheIOMonitor.updateCacheSize(newSize, segmentSize);
                return segmentSize;
            });
        } catch (RuntimeException e) {
            // PersistentCache.writeSegment is fire-and-forget (no throws clause); a failed cache write
            // must not surface to the caller's own (authoritative) write path.
            LOG.warn("Failed to cache segment {}", segmentId, e);
        }
    }

    @Override
    public final void cleanUp() {
        cache.cleanUp();
    }

    public final void close() {
        if (closed.compareAndSet(false, true)) {
            doClose();
        }
    }

    public final AbstractCacheStats getCacheStats() {
        return cacheStats;
    }

    // ---------- shared helpers (visible to subclasses) ----------

    /**
     * Re-populate the in-memory cache from existing files in {@link #directory}. Subclasses call this at the end of
     * their own constructor. Walks the directory tree (segments live one level deep in per-bucket subdirectories,
     * see {@link #getSegmentPath}) and inserts each valid UUID-named file into the cache. Iteration order is
     * filesystem-dependent, so the initial LRU order after rehydration is arbitrary — actual access patterns
     * re-establish a meaningful LRU within the first wave of reads. If the on-disk total exceeds the configured
     * weight (unusual — typically only happens if the cache size is reduced between restarts), the cache's
     * weight-based eviction discards the excess during the put loop; {@link #onEviction} decrements cacheSize and
     * deletes the file, so the end state is correct without explicit trimming. Regular files whose names are not
     * valid segment IDs — including leftover {@value #TEMP_SUFFIX} files from a crashed write — are deleted
     * (logged at info level), wherever they sit in the tree. Empty bucket directories are left in place.
     */
    protected final void rehydrateFromDisk() {
        long startNanos = System.nanoTime();
        int[] count = {0};
        try {
            Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                @Override
                public @NotNull FileVisitResult visitFile(@NotNull Path path, @NotNull BasicFileAttributes attrs) {
                    if (!attrs.isRegularFile()) {
                        return FileVisitResult.CONTINUE;
                    }
                    String filename = path.getFileName().toString();
                    UUID segmentId = parseSegmentId(filename);
                    if (segmentId == null) {
                        try {
                            if (Files.deleteIfExists(path)) {
                                LOG.info("Deleted file with non-segment-id name: {}", path);
                            }
                        } catch (IOException e) {
                            LOG.info("Failed to delete non-segment-id file {}", path, e);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                    long segmentSize = attrs.size();
                    if (segmentSize <= 0 || segmentSize > Integer.MAX_VALUE) {
                        LOG.info("Skipping file with invalid size {}: {}", segmentSize, path);
                        return FileVisitResult.CONTINUE;
                    }
                    long newSize = cacheSize.addAndGet(segmentSize);
                    diskCacheIOMonitor.updateCacheSize(newSize, segmentSize);
                    cache.put(segmentId, (int) segmentSize);
                    count[0]++;
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public @NotNull FileVisitResult visitFileFailed(@NotNull Path path, @NotNull IOException exc) {
                    // A single unreadable entry must not abort rehydration of the rest of the cache.
                    LOG.info("Skipping unreadable cache entry {}", path, exc);
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            LOG.warn("Could not finish rehydrating disk cache {}", directory, e);
            return;
        }
        LOG.info("Rehydrated {} segments from {} in {} ms", count[0], directory,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos));
    }

    protected final void writeSegmentInternal(UUID segmentId, ByteBuffer buffer) throws IOException {
        long start = System.nanoTime();
        int segmentBytes = buffer.remaining();
        String filename = segmentId.toString();
        Path bucket = getSegmentBucket(filename);
        // Keep the temp file in the same bucket as the final file
        Path tempPath = bucket.resolve(filename + TEMP_SUFFIX);
        Path finalPath = bucket.resolve(filename);
        File segmentFile = finalPath.toFile();
        long msb = segmentId.getMostSignificantBits();
        long lsb = segmentId.getLeastSignificantBits();
        try {
            // Write to a temp file, then atomic-rename to the final path. This guarantees the final
            // file is either absent or fully written — a JVM crash mid-write leaves the partial bytes
            // at the temp path (cleaned up at the next rehydration since its name is not a valid UUID).
            // CREATE + TRUNCATE_EXISTING covers the case where a previous crashed run left a stale temp
            // file at the same path. Segments are immutable per id, so overwriting the final file via
            // REPLACE_EXISTING is safe.
            try (FileChannel channel = FileChannel.open(tempPath, StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
                diskCacheIOMonitor.beforeSegmentWrite(segmentFile, msb, lsb, segmentBytes);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                buffer.rewind();
            }
            Files.move(tempPath, finalPath,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            long elapsed = System.nanoTime() - start;
            diskCacheIOMonitor.afterSegmentWrite(segmentFile, msb, lsb, segmentBytes, elapsed);
            writeStats.recordWrite(segmentId, segmentBytes, elapsed);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(tempPath);
            } catch (IOException i) {
                LOG.warn("Error while deleting temp segment file {}", tempPath, i);
            }
            throw e;
        }
    }

    /**
     * Maps a segment id to its on-disk path. Segments are stored one directory level deep, in a bucket named
     * by the first two hex characters of the UUID (e.g. {@code <dir>/55/558e8400-...}). A single flat
     * directory with hundreds of thousands of entries can slow filesystem metadata operations;
     * fanning out on the first two hex chars spreads the files
     * across 256 buckets. The first two characters of a {@link UUID#toString()} are always hex digits (they
     * precede the first {@code -} at index 8), so the bucket name never contains a path separator. All 256
     * buckets are pre-created at construction (see {@link #createBuckets}), so this path's parent always exists.
     */
    protected final Path getSegmentPath(String segmentId) {
        return getSegmentBucket(segmentId).resolve(segmentId);
    }

    protected final Path getSegmentBucket(String segmentId) {
        return directory.resolve(segmentId.substring(0, 2));
    }

    /**
     * Logs a disk-cache write failure at WARN, throttled per failure type. A failed cache write is non-fatal:
     * the segment was loaded from the backend and returned to the caller; only durability is lost, and cache
     * membership self-heals on the next read. The key is the exception's class (not its message, which embeds
     * the per-segment path) so a systemic failure (e.g. a full or read-only volume) collapses to one throttled
     * stream rather than one line per segment.
     */
    protected final void logWriteFailure(UUID segmentId, Throwable cause) {
        if (!logSilencer.silence(cause.getClass().getName())) {
            LOG.warn("Could not persist segment {} to disk cache; it will be re-fetched on next access{}",
                    segmentId, LogSilencer.SILENCING_POSTFIX, cause);
        }
    }

    protected final void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Already closed");
        }
    }

    long cacheSize() {
        return cacheSize.get();
    }

    // ---------- private helpers ----------

    private void onEviction(UUID segmentId, Integer size, EvictionCause cause) {
        boolean wasPending = onEvictionHook(segmentId);
        int segmentSize = Objects.requireNonNull(size,
                "eviction notification missing value; cacheSize would drift");
        long newSize = cacheSize.addAndGet(-segmentSize);
        diskCacheIOMonitor.updateCacheSize(newSize, segmentSize);
        Path path = getSegmentPath(segmentId.toString());
        try {
            if (!Files.deleteIfExists(path)) {
                if (wasPending) {
                    LOG.debug("Segment file for id {} is already deleted or not present", path);
                } else {
                    LOG.info("Segment file for id {} is already deleted or not present", path);
                }
            }
        } catch (IOException e) {
            LOG.info("Cached file deletion failed after eviction: {}", path, e);
        }
    }

    private ByteBuffer readFromDisk(UUID segmentId, int size) throws IOException {
        long msb = segmentId.getMostSignificantBits();
        long lsb = segmentId.getLeastSignificantBits();
        long start = System.nanoTime();
        Path segmentPath = getSegmentPath(segmentId.toString());
        File segmentFile = segmentPath.toFile();
        try (FileChannel channel = FileChannel.open(segmentPath, StandardOpenOption.READ)) {
            diskCacheIOMonitor.beforeSegmentRead(segmentFile, msb, lsb, size);
            ByteBuffer buffer = ByteBuffer.allocate(size);
            if (IOUtils.readFully(channel, 0, buffer) < size) {
                throw new EOFException();
            }
            buffer.flip();
            long elapsed = System.nanoTime() - start;
            diskCacheIOMonitor.afterSegmentRead(segmentFile, msb, lsb, size, elapsed);
            readStats.recordRead(segmentId, size, elapsed);
            return buffer;
        } catch (ClosedByInterruptException e) {
            // The thread was interrupted during the NIO read, surfacing as ClosedByInterruptException.
            // Honor the interrupt instead of swallowing it as a cache miss. Clear the flag that NIO
            // leaves set so it does not poison subsequent reads on this thread, then propagate so the
            // caller aborts promptly.
            Thread.interrupted();
            throw new SegmentReadInterruptedException("Interrupted while reading segment " + segmentId + " from cache", e);
        }
    }

    @Nullable
    private static UUID parseSegmentId(String name) {
        try {
            return UUID.fromString(name);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Carries a checked exception thrown by the caller-supplied loader out of the cache's mapping
     * function (which cannot declare checked exceptions). Unwrapped in {@link #readSegmentOrRetry}.
     */
    private static final class LoaderException extends RuntimeException {
        LoaderException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * Accumulates per-write counters across all writer threads and emits a periodic log line every
     * {@link #WRITE_LOG_INTERVAL} segments. The sync variant calls {@link #writeSegmentInternal} on the
     * caller threads (so multiple threads may write concurrently); the async variant calls it on the
     * background writer threads.
     */
    private final class WriteStatsRecorder {
        private final long startNanos = System.nanoTime();
        private final AtomicLong segments = new AtomicLong();
        private final AtomicLong bytes = new AtomicLong();
        private final AtomicLong writeNanos = new AtomicLong();

        void recordWrite(UUID segmentId, int segmentBytes, long durationNanos) {
            long count = segments.incrementAndGet();
            long bytesTotal = bytes.addAndGet(segmentBytes);
            long timeTotal = writeNanos.addAndGet(durationNanos);
            if (count % WRITE_LOG_INTERVAL == 0) {
                // totalTimeWriting is summed across all writer threads while totalTime is wall clock;
                // their ratio can exceed 100% under concurrent writes — a value of N% roughly means
                // N/100 threads were busy writing on average.
                long wallSec = (System.nanoTime() - startNanos) / 1_000_000_000L;
                long writeSec = timeTotal / 1_000_000_000L;
                LOG.info("Wrote segment {} to disk cache in {}us. segmentsWritten: {}, " +
                                "avgWriteTime: {}us, avgSegmentSize: {}, " +
                                "totalTimeWriting: {}s, totalTime: {}s{}",
                        segmentId,
                        durationNanos / 1_000L,
                        count,
                        (timeTotal / count) / 1_000L,
                        IOUtils.humanReadableByteCountBin(bytesTotal / count),
                        writeSec,
                        wallSec,
                        writeStatsContext());
            }
        }
    }

    /**
     * Accumulates per-read counters across all reader threads and emits a periodic log line every
     * {@link #READ_LOG_INTERVAL} segments. Reads happen on the caller's thread (whichever thread invoked
     * {@link #readSegment}), so multiple threads may read concurrently.
     */
    private static final class ReadStatsRecorder {
        private final long startNanos = System.nanoTime();
        private final AtomicLong segments = new AtomicLong();
        private final AtomicLong bytes = new AtomicLong();
        private final AtomicLong readNanos = new AtomicLong();

        void recordRead(UUID segmentId, int segmentBytes, long durationNanos) {
            long count = segments.incrementAndGet();
            long bytesTotal = bytes.addAndGet(segmentBytes);
            long timeTotal = readNanos.addAndGet(durationNanos);
            if (count % READ_LOG_INTERVAL == 0) {
                // totalTimeReading is summed across all reader threads while totalTime is wall clock;
                // their ratio can exceed 100% under concurrent reads — a value of N% roughly means
                // N/100 threads were busy reading on average.
                long wallSec = (System.nanoTime() - startNanos) / 1_000_000_000L;
                long readSec = timeTotal / 1_000_000_000L;
                LOG.info("Read segment {} from disk cache in {}us. segmentsRead: {}, " +
                                "avgReadTime: {}us, avgSegmentSize: {}, " +
                                "totalTimeReading: {}s, totalTime: {}s",
                        segmentId,
                        durationNanos / 1_000L,
                        count,
                        (timeTotal / count) / 1_000L,
                        IOUtils.humanReadableByteCountBin(bytesTotal / count),
                        readSec,
                        wallSec);
            }
        }
    }
}
