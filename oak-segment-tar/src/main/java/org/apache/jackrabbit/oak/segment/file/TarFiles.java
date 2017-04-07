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
 */

package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.commons.io.FileUtils.listFiles;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.segment.SegmentGraph.SegmentGraphVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TarFiles implements Closeable {

    static class CleanupResult {

        private boolean interrupted;

        private long reclaimedSize;

        private List<File> removableFiles;

        private Set<UUID> reclaimedSegmentIds;

        private CleanupResult() {
            // Prevent external instantiation.
        }

        long getReclaimedSize() {
            return reclaimedSize;
        }

        List<File> getRemovableFiles() {
            return removableFiles;
        }

        Set<UUID> getReclaimedSegmentIds() {
            return reclaimedSegmentIds;
        }

        boolean isInterrupted() {
            return interrupted;
        }

    }

    static class Builder {

        private File directory;

        private boolean memoryMapping;

        private TarRecovery tarRecovery;

        private IOMonitor ioMonitor;

        private FileStoreStats fileStoreStats;

        private long maxFileSize;

        private boolean readOnly;

        Builder withDirectory(File directory) {
            this.directory = checkNotNull(directory);
            return this;
        }

        Builder withMemoryMapping(boolean memoryMapping) {
            this.memoryMapping = memoryMapping;
            return this;
        }

        Builder withTarRecovery(TarRecovery tarRecovery) {
            this.tarRecovery = checkNotNull(tarRecovery);
            return this;
        }

        Builder withIOMonitor(IOMonitor ioMonitor) {
            this.ioMonitor = checkNotNull(ioMonitor);
            return this;
        }

        Builder withFileStoreStats(FileStoreStats fileStoreStats) {
            this.fileStoreStats = checkNotNull(fileStoreStats);
            return this;
        }

        Builder withMaxFileSize(long maxFileSize) {
            checkArgument(maxFileSize > 0);
            this.maxFileSize = maxFileSize;
            return this;
        }

        Builder withReadOnly() {
            this.readOnly = true;
            return this;
        }

        public TarFiles build() throws IOException {
            checkState(directory != null, "Directory not specified");
            checkState(tarRecovery != null, "TAR recovery strategy not specified");
            checkState(ioMonitor != null, "I/O monitor not specified");
            checkState(readOnly || fileStoreStats != null, "File store statistics not specified");
            checkState(readOnly || maxFileSize != 0, "Max file size not specified");
            return new TarFiles(this);
        }

    }

    private static final Logger log = LoggerFactory.getLogger(TarFiles.class);

    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("(data)((0|[1-9][0-9]*)[0-9]{4})([a-z])?.tar");

    private static Map<Integer, Map<Character, File>> collectFiles(File directory) {
        Map<Integer, Map<Character, File>> dataFiles = newHashMap();
        for (File file : listFiles(directory, null, false)) {
            Matcher matcher = FILE_NAME_PATTERN.matcher(file.getName());
            if (matcher.matches()) {
                Integer index = Integer.parseInt(matcher.group(2));
                Map<Character, File> files = dataFiles.get(index);
                if (files == null) {
                    files = newHashMap();
                    dataFiles.put(index, files);
                }
                Character generation = 'a';
                if (matcher.group(4) != null) {
                    generation = matcher.group(4).charAt(0);
                }
                checkState(files.put(generation, file) == null);
            }
        }
        return dataFiles;
    }

    /**
     * Include the ids of all segments transitively reachable through forward
     * references from {@code referencedIds}. See OAK-3864.
     */
    private static void includeForwardReferences(Iterable<TarReader> readers, Set<UUID> referencedIds) throws IOException {
        Set<UUID> fRefs = newHashSet(referencedIds);
        do {
            // Add direct forward references
            for (TarReader reader : readers) {
                reader.calculateForwardReferences(fRefs);
                if (fRefs.isEmpty()) {
                    break; // Optimisation: bail out if no references left
                }
            }
            // ... as long as new forward references are found.
        } while (referencedIds.addAll(fRefs));
    }

    static Builder builder() {
        return new Builder();
    }

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final long maxFileSize;

    private final boolean memoryMapping;

    private final IOMonitor ioMonitor;

    private final boolean readOnly;

    private List<TarReader> readers;

    private TarWriter writer;

    private volatile boolean shutdown;

    private boolean closed;

    private TarFiles(Builder builder) throws IOException {
        maxFileSize = builder.maxFileSize;
        memoryMapping = builder.memoryMapping;
        ioMonitor = builder.ioMonitor;
        readOnly = builder.readOnly;
        Map<Integer, Map<Character, File>> map = collectFiles(builder.directory);
        readers = newArrayListWithCapacity(map.size());
        Integer[] indices = map.keySet().toArray(new Integer[map.size()]);
        Arrays.sort(indices);
        for (int i = indices.length - 1; i >= 0; i--) {
            if (readOnly) {
                readers.add(TarReader.openRO(map.get(indices[i]), memoryMapping, true, builder.tarRecovery, ioMonitor));
            } else {
                readers.add(TarReader.open(map.get(indices[i]), memoryMapping, builder.tarRecovery, ioMonitor));
            }
        }
        if (!readOnly) {
            int writeNumber = 0;
            if (indices.length > 0) {
                writeNumber = indices[indices.length - 1] + 1;
            }
            writer = new TarWriter(builder.directory, builder.fileStoreStats, writeNumber, builder.ioMonitor);
        }
    }

    private void checkOpen() {
        checkState(!closed, "This instance has been closed");
    }

    private void checkReadWrite() {
        checkState(!readOnly, "This instance is read-only");
    }

    @Override
    public void close() throws IOException {
        shutdown = true;
        lock.writeLock().lock();
        try {
            checkOpen();
            closed = true;
            Closer closer = Closer.create();
            closer.register(writer);
            writer = null;
            for (TarReader reader : readers) {
                closer.register(reader);
            }
            readers = null;
            closer.close();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public String toString() {
        lock.readLock().lock();
        try {
            return "TarFiles{readers=" + readers + ", writer=" + writer + "}";
        } finally {
            lock.readLock().unlock();
        }
    }

    long size() {
        lock.readLock().lock();
        try {
            checkOpen();
            long size = 0;
            if (!readOnly) {
                size = writer.fileLength();
            }
            for (TarReader reader : readers) {
                size += reader.size();
            }
            return size;
        } finally {
            lock.readLock().unlock();
        }
    }

    int readerCount() {
        lock.readLock().lock();
        try {
            checkOpen();
            return readers.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    void flush() throws IOException {
        checkReadWrite();
        lock.readLock().lock();
        try {
            checkOpen();
            writer.flush();
        } finally {
            lock.readLock().unlock();
        }
    }

    boolean containsSegment(long msb, long lsb) {
        lock.readLock().lock();
        try {
            checkOpen();
            if (!readOnly) {
                if (writer.containsEntry(msb, lsb)) {
                    return true;
                }
            }
            for (TarReader reader : readers) {
                if (reader.containsEntry(msb, lsb)) {
                    return true;
                }
            }
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    ByteBuffer readSegment(long msb, long lsb) {
        lock.readLock().lock();
        try {
            checkOpen();
            try {
                if (!readOnly) {
                    ByteBuffer buffer = writer.readEntry(msb, lsb);
                    if (buffer != null) {
                        return buffer;
                    }
                }
                for (TarReader reader : readers) {
                    ByteBuffer buffer = reader.readEntry(msb, lsb);
                    if (buffer != null) {
                        return buffer;
                    }
                }
            } catch (IOException e) {
                log.warn("Unable to read from TAR file {}", writer, e);
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    void writeSegment(UUID id, byte[] buffer, int offset, int length, int generation, Set<UUID> references, Set<String> binaryReferences) throws IOException {
        checkReadWrite();
        lock.writeLock().lock();
        try {
            checkOpen();
            long size = writer.writeEntry(
                    id.getMostSignificantBits(),
                    id.getLeastSignificantBits(),
                    buffer,
                    offset,
                    length,
                    generation
            );
            if (references != null) {
                for (UUID reference : references) {
                    writer.addGraphEdge(id, reference);
                }
            }
            if (binaryReferences != null) {
                for (String reference : binaryReferences) {
                    writer.addBinaryReference(generation, id, reference);
                }
            }
            if (size >= maxFileSize) {
                newWriter();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void newWriter() throws IOException {
        TarWriter newWriter = writer.createNextGeneration();
        if (newWriter == writer) {
            return;
        }
        File writeFile = writer.getFile();
        List<TarReader> list = newArrayListWithCapacity(1 + readers.size());
        list.add(TarReader.open(writeFile, memoryMapping, ioMonitor));
        list.addAll(readers);
        readers = list;
        writer = newWriter;
    }

    CleanupResult cleanup(Supplier<Set<UUID>> referencesSupplier, Predicate<Integer> reclaimGeneration) throws IOException {
        checkReadWrite();

        CleanupResult result = new CleanupResult();
        result.removableFiles = new ArrayList<>();
        result.reclaimedSegmentIds = new HashSet<>();

        Map<TarReader, TarReader> cleaned = newLinkedHashMap();

        lock.writeLock().lock();
        lock.readLock().lock();
        try {
            try {
                checkOpen();
                newWriter();
            } finally {
                lock.writeLock().unlock();
            }

            // At this point the write lock is downgraded to a read lock for
            // better concurrency. It is always necessary to access TarReader
            // and TarWriter instances while holding a lock (either in read or
            // write mode) to prevent a concurrent #close(). In this case, we
            // don't need an exclusive access to the TarReader instances.

            // TODO now that the two protected sections have been merged thanks
            // to lock downgrading, check if the following code can be
            // simplified.

            for (TarReader reader : readers) {
                cleaned.put(reader, reader);
                result.reclaimedSize += reader.size();
            }

            // The set of references has to be computed while holding the lock.
            // This prevents a time-of-check to time-of-use race condition. See
            // OAK-6046 for further details.

            Set<UUID> references = referencesSupplier.get();

            Set<UUID> reclaim = newHashSet();
            for (TarReader reader : cleaned.keySet()) {
                if (shutdown) {
                    result.interrupted = true;
                    return result;
                }
                reader.mark(references, reclaim, reclaimGeneration);
                log.info("{}: size of bulk references/reclaim set {}/{}", reader, references.size(), reclaim.size());
            }
            for (TarReader reader : cleaned.keySet()) {
                if (shutdown) {
                    result.interrupted = true;
                    return result;
                }
                cleaned.put(reader, reader.sweep(reclaim, result.reclaimedSegmentIds));
            }
        } finally {
            lock.readLock().unlock();
        }

        List<TarReader> oldReaders = newArrayList();
        lock.writeLock().lock();
        try {
            // Replace current list of reader with the cleaned readers taking care not to lose
            // any new reader that might have come in through concurrent calls to newWriter()
            checkOpen();
            List<TarReader> sweptReaders = newArrayList();
            for (TarReader reader : readers) {
                if (cleaned.containsKey(reader)) {
                    TarReader newReader = cleaned.get(reader);
                    if (newReader != null) {
                        sweptReaders.add(newReader);
                        result.reclaimedSize -= newReader.size();
                    }
                    // if these two differ, the former represents the swept version of the latter
                    if (newReader != reader) {
                        oldReaders.add(reader);
                    }
                } else {
                    sweptReaders.add(reader);
                }
            }
            readers = sweptReaders;
        } finally {
            lock.writeLock().unlock();
        }

        for (TarReader oldReader : oldReaders) {
            try {
                oldReader.close();
            } catch (IOException e) {
                log.error("Unable to close swept TAR reader", e);
            }
            result.removableFiles.add(oldReader.getFile());
        }

        return result;
    }

    void collectBlobReferences(ReferenceCollector collector, int minGeneration) throws IOException {
        lock.writeLock().lock();
        lock.readLock().lock();
        try {
            try {
                checkOpen();
                if (!readOnly) {
                    newWriter();
                }
            } finally {
                lock.writeLock().unlock();
            }

            // At this point the write lock is downgraded to a read lock for
            // better concurrency. It is always necessary to access TarReader
            // and TarWriter instances while holding a lock (either in read or
            // write mode) to prevent a concurrent #close(). In this case, we
            // don't need an exclusive access to the TarReader instances.

            for (TarReader reader : readers) {
                reader.collectBlobReferences(collector, minGeneration);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    Iterable<UUID> getSegmentIds() {
        lock.readLock().lock();
        try {
            checkOpen();
            List<UUID> ids = new ArrayList<>();
            for (TarReader reader : readers) {
                ids.addAll(reader.getUUIDs());
            }
            return ids;
        } finally {
            lock.readLock().unlock();
        }
    }

    Map<UUID, List<UUID>> getGraph(String fileName) throws IOException {
        Set<UUID> index = null;
        Map<UUID, List<UUID>> graph = null;

        lock.readLock().lock();
        try {
            checkOpen();
            for (TarReader reader : readers) {
                if (fileName.equals(reader.getFile().getName())) {
                    index = reader.getUUIDs();
                    graph = reader.getGraph(false);
                    break;
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        Map<UUID, List<UUID>> result = new HashMap<>();
        if (index != null) {
            for (UUID uuid : index) {
                result.put(uuid, null);
            }
        }
        if (graph != null) {
            result.putAll(graph);
        }
        return result;
    }

    Map<String, Set<UUID>> getIndices() {
        lock.readLock().lock();
        try {
            checkOpen();
            Map<String, Set<UUID>> index = new HashMap<>();
            for (TarReader reader : readers) {
                index.put(reader.getFile().getAbsolutePath(), reader.getUUIDs());
            }
            return index;
        } finally {
            lock.readLock().unlock();
        }
    }

    void traverseSegmentGraph(Set<UUID> roots, SegmentGraphVisitor visitor) throws IOException {
        lock.readLock().lock();
        try {
            checkOpen();
            includeForwardReferences(readers, roots);
            for (TarReader reader : readers) {
                reader.traverseSegmentGraph(roots, visitor);
            }
        } finally {
            lock.readLock().unlock();
        }
    }

}
