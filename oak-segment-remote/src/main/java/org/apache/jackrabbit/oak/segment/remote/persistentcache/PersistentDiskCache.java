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

import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.AbstractPersistentCache;
import org.apache.jackrabbit.oak.segment.spi.persistence.persistentcache.SegmentCacheStats;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.OFF_HEAP;

public class PersistentDiskCache extends AbstractPersistentCache {
    private static final Logger logger = LoggerFactory.getLogger(PersistentDiskCache.class);
    public static final int DEFAULT_MAX_CACHE_SIZE_MB = 512;
    public static final String NAME = "Segment Disk Cache";

    private final File directory;
    private final long maxCacheSizeBytes;
    private final IOMonitor diskCacheIOMonitor;

    final AtomicBoolean cleanupInProgress = new AtomicBoolean(false);

    final AtomicLong evictionCount = new AtomicLong();

    public PersistentDiskCache(File directory, int cacheMaxSizeMB, IOMonitor diskCacheIOMonitor) {
        this.directory = directory;
        this.maxCacheSizeBytes = cacheMaxSizeMB * 1024L * 1024L;
        this.diskCacheIOMonitor = diskCacheIOMonitor;
        if (!directory.exists()) {
            directory.mkdirs();
        }

        segmentCacheStats = new SegmentCacheStats(
                NAME,
                () -> maxCacheSizeBytes,
                () -> Long.valueOf(directory.listFiles().length),
                () -> FileUtils.sizeOfDirectory(directory),
                () -> evictionCount.get());
    }

    @Override
    protected Buffer readSegmentInternal(long msb, long lsb) {
        try {
            String segmentId = new UUID(msb, lsb).toString();
            File segmentFile = new File(directory, segmentId);

            Stopwatch stopwatch = Stopwatch.createStarted();
            if (segmentFile.exists()) {
                diskCacheIOMonitor.beforeSegmentRead(segmentFile, msb, lsb, (int) segmentFile.length());
                try (FileInputStream fis = new FileInputStream(segmentFile); FileChannel channel = fis.getChannel()) {
                    int length = (int) channel.size();

                    Buffer buffer;
                    if (OFF_HEAP) {
                        buffer = Buffer.allocateDirect(length);
                    } else {
                        buffer = Buffer.allocate(length);
                    }
                    if (buffer.readFully(channel, 0) < length) {
                        throw new EOFException();
                    }

                    long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
                    diskCacheIOMonitor.afterSegmentRead(segmentFile, msb, lsb, (int) segmentFile.length(), elapsed);

                    buffer.flip();

                    return buffer;
                } catch (FileNotFoundException e) {
                    logger.info("Segment {} deleted from file system!", segmentId);
                } catch (IOException e) {
                    logger.error("Error loading segment {} from cache:", segmentId, e);
                }
            }
        } catch (Exception e) {
            logger.error("Exception while reading segment {} from the cache:", new UUID(msb, lsb), e);
        }

        return null;
    }

    @Override
    public boolean containsSegment(long msb, long lsb) {
        return new File(directory, new UUID(msb, lsb).toString()).exists();
    }

    @Override
    public void writeSegment(long msb, long lsb, Buffer buffer) {
        String segmentId = new UUID(msb, lsb).toString();
        File segmentFile = new File(directory, segmentId);
        File tempSegmentFile = new File(directory, segmentId + System.nanoTime() + ".part");

        Buffer bufferCopy = buffer.duplicate();

        Runnable task = () -> {
            if (writesPending.add(segmentId)) {
                try {
                    int fileSize;
                    try (FileChannel channel = new FileOutputStream(tempSegmentFile).getChannel()) {
                        fileSize = bufferCopy.write(channel);
                    }
                    try {
                        Files.move(tempSegmentFile.toPath(), segmentFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
                    } catch (AtomicMoveNotSupportedException e) {
                        Files.move(tempSegmentFile.toPath(), segmentFile.toPath());
                    }
                    cacheSize.addAndGet(fileSize);
                } catch (Exception e) {
                    logger.error("Error writing segment {} to cache: {}", segmentId, e);
                    try {
                        Files.deleteIfExists(segmentFile.toPath());
                        Files.deleteIfExists(tempSegmentFile.toPath());
                    } catch (IOException i) {
                        logger.error("Error while deleting corrupted segment file {}", segmentId, i);
                    }
                } finally {
                    writesPending.remove(segmentId);
                }
            }
            cleanUp();
        };

        executor.execute(task);
    }

    private boolean isCacheFull() {
        return cacheSize.get() >= maxCacheSizeBytes;
    }

    @Override
    public void cleanUp() {
        if (!cleanupInProgress.getAndSet(true)) {
            try {
                cleanUpInternal();
            } finally {
                cleanupInProgress.set(false);
            }
        }
    }

    private void cleanUpInternal() {
        if (isCacheFull()) {
            try {
                Stream<SegmentCacheEntry> segmentCacheEntryStream = Files.walk(directory.toPath())
                        .filter(path -> !path.toFile().isDirectory())
                        .map(path -> {
                            try {
                                return new SegmentCacheEntry(path, Files.readAttributes(path, BasicFileAttributes.class).lastAccessTime());
                            } catch (IOException e) {
                                logger.error("Error while getting the last access time for {}", path.toFile().getName());
                                return new SegmentCacheEntry(path, FileTime.fromMillis(Long.MAX_VALUE));
                            }
                        })
                        .sorted();

                StreamConsumer.forEach(segmentCacheEntryStream, (segmentCacheEntry, breaker) -> {

                    if (cacheSize.get() > maxCacheSizeBytes * 0.66) {
                        File segment = segmentCacheEntry.getPath().toFile();
                        cacheSize.addAndGet(-segment.length());
                        segment.delete();
                        evictionCount.incrementAndGet();
                    } else {
                        breaker.stop();
                    }
                });
            } catch (IOException e) {
                logger.error("A problem occurred while cleaning up the cache: ", e);
            }
        }
    }

    private static class SegmentCacheEntry implements Comparable<SegmentCacheEntry>{
        private Path path;
        private FileTime lastAccessTime;

        public SegmentCacheEntry(Path path, FileTime lastAccessTime) {
            this.path = path;
            this.lastAccessTime = lastAccessTime;
        }

        public Path getPath() {
            return path;
        }

        public FileTime getLastAccessTime() {
            return lastAccessTime;
        }

        @Override
        public int compareTo(@NotNull SegmentCacheEntry segmentCacheEntry) {
            return this.lastAccessTime.compareTo(segmentCacheEntry.lastAccessTime);
        }
    }
    static class StreamConsumer {

        public static class Breaker {
            private boolean shouldBreak = false;

            public void stop() {
                shouldBreak = true;
            }

            boolean get() {
                return shouldBreak;
            }
        }

        public static <T> void forEach(Stream<T> stream, BiConsumer<T, Breaker> consumer) {
            Spliterator<T> spliterator = stream.spliterator();
            boolean hadNext = true;
            Breaker breaker = new Breaker();

            while (hadNext && !breaker.get()) {
                hadNext = spliterator.tryAdvance(elem -> {
                    consumer.accept(elem, breaker);
                });
            }
        }
    }
}
