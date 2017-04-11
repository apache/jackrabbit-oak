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
import static com.google.common.collect.Maps.newHashMap;
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
import java.util.LinkedHashMap;
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
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.segment.SegmentGraph.SegmentGraphVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TarFiles implements Closeable {

    private static class Node {

        final TarReader reader;

        final Node next;

        Node(TarReader reader, Node next) {
            this.reader = reader;
            this.next = next;
        }
    }

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

    private static Node reverse(Node n) {
        Node r = null;
        while (n != null) {
            r = new Node(n.reader, r);
            n = n.next;
        }
        return r;
    }

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

    private static void includeForwardReferences(Node head, Set<UUID> referencedIds) throws IOException {
        Set<UUID> references = newHashSet(referencedIds);
        do {
            // Add direct forward references
            Node n = head;
            while (n != null) {
                n.reader.calculateForwardReferences(references);
                if (references.isEmpty()) {
                    break; // Optimisation: bail out if no references left
                }
                n = n.next;
            }
            // ... as long as new forward references are found.
        } while (referencedIds.addAll(references));
    }

    static Builder builder() {
        return new Builder();
    }

    private final long maxFileSize;

    private final boolean memoryMapping;

    private final IOMonitor ioMonitor;

    /**
     * Guards access to the {@link #readers} and {@link #writer} references.
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Points to the first node of the linked list of TAR readers. Every node in
     * the linked list is immutable. Thus, you need to to hold {@link #lock}
     * while reading the value of the reference, but you can release it before
     * iterating through the list.
     */
    private Node readers;

    /**
     * The currently used TAR writer. Its access is protected by {@link #lock}.
     */
    private TarWriter writer;

    /**
     * If {@code true}, a user requested this instance to close. This flag is
     * used in long running, background operations - like {@link
     * #cleanup(Supplier, Predicate)} - to be responsive to termination.
     */
    private volatile boolean shutdown;

    private TarFiles(Builder builder) throws IOException {
        maxFileSize = builder.maxFileSize;
        memoryMapping = builder.memoryMapping;
        ioMonitor = builder.ioMonitor;
        Map<Integer, Map<Character, File>> map = collectFiles(builder.directory);
        Integer[] indices = map.keySet().toArray(new Integer[map.size()]);
        Arrays.sort(indices);

        // TAR readers are stored in descending index order. The following loop
        // iterates the indices in ascending order, but prepends - instead of
        // appending - the corresponding TAR readers to the linked list. This
        // results in a properly ordered linked list.

        for (Integer index : indices) {
            TarReader r;
            if (builder.readOnly) {
                r = TarReader.openRO(map.get(index), memoryMapping, true, builder.tarRecovery, ioMonitor);
            } else {
                r = TarReader.open(map.get(index), memoryMapping, builder.tarRecovery, ioMonitor);
            }
            readers = new Node(r, readers);
        }
        if (builder.readOnly) {
            return;
        }
        int writeNumber = 0;
        if (indices.length > 0) {
            writeNumber = indices[indices.length - 1] + 1;
        }
        writer = new TarWriter(builder.directory, builder.fileStoreStats, writeNumber, builder.ioMonitor);
    }

    @Override
    public void close() throws IOException {
        shutdown = true;

        TarWriter writer;
        Node n;

        lock.writeLock().lock();
        try {
            writer = this.writer;
            n = this.readers;
        } finally {
            lock.writeLock().unlock();
        }

        IOException exception = null;

        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                exception = e;
            }
        }

        while (n != null) {
            try {
                n.reader.close();
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
            n = n.next;
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public String toString() {
        String w = null;
        Node n;

        lock.readLock().lock();
        try {
            if (writer != null) {
                w = writer.toString();
            }
            n = readers;
        } finally {
            lock.readLock().unlock();
        }

        List<TarReader> rs = new ArrayList<>();
        while (n != null) {
            rs.add(n.reader);
            n = n.next;
        }
        return String.format("TarFiles{readers=%s,writer=%s", rs, w);
    }

    long size() {
        long size = 0;
        Node readers;

        lock.readLock().lock();
        try {
            if (writer != null) {
                size = writer.fileLength();
            }
            readers = this.readers;
        } finally {
            lock.readLock().unlock();
        }

        Node n = readers;
        while (n != null) {
            size += n.reader.size();
            n = n.next;
        }
        return size;
    }

    int readerCount() {
        Node n;

        lock.readLock().lock();
        try {
            n = readers;
        } finally {
            lock.readLock().unlock();
        }

        int count = 0;
        while (n != null) {
            count++;
            n = n.next;
        }
        return count;
    }

    void flush() throws IOException {
        lock.readLock().lock();
        try {
            writer.flush();
        } finally {
            lock.readLock().unlock();
        }
    }

    boolean containsSegment(long msb, long lsb) {
        Node n;

        lock.readLock().lock();
        try {
            if (writer != null) {
                if (writer.containsEntry(msb, lsb)) {
                    return true;
                }
            }
            n = this.readers;
        } finally {
            lock.readLock().unlock();
        }

        while (n != null) {
            if (n.reader.containsEntry(msb, lsb)) {
                return true;
            }
            n = n.next;
        }
        return false;
    }

    ByteBuffer readSegment(long msb, long lsb) {
        try {
            Node n;

            lock.readLock().lock();
            try {
                if (writer != null) {
                    ByteBuffer b = writer.readEntry(msb, lsb);
                    if (b != null) {
                        return b;
                    }
                }
                n = readers;
            } finally {
                lock.readLock().unlock();
            }

            while (n != null) {
                ByteBuffer b = n.reader.readEntry(msb, lsb);
                if (b != null) {
                    return b;
                }
                n = n.next;
            }
        } catch (IOException e) {
            log.warn("Unable to read from TAR file", e);
        }

        return null;
    }

    void writeSegment(UUID id, byte[] buffer, int offset, int length, int generation, Set<UUID> references, Set<String> binaryReferences) throws IOException {
        lock.writeLock().lock();
        try {
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

    /**
     * Creates a new TAR writer with a higher index number, reopens the previous
     * TAR writer as a TAR reader, and adds the TAR reader to the linked list.
     * <p>
     * This method must be invoked while holding {@link #lock} in write mode,
     * because it modifies the references {@link #writer} and {@link #readers}.
     *
     * @throws IOException If an error occurs while operating on the TAR readers
     *                     or the TAR writer.
     */
    private void newWriter() throws IOException {
        TarWriter newWriter = writer.createNextGeneration();
        if (newWriter == writer) {
            return;
        }
        readers = new Node(TarReader.open(writer.getFile(), memoryMapping, ioMonitor), readers);
        writer = newWriter;
    }

    CleanupResult cleanup(Supplier<Set<UUID>> referencesSupplier, Predicate<Integer> reclaimPredicate) throws IOException {
        CleanupResult result = new CleanupResult();
        result.removableFiles = new ArrayList<>();
        result.reclaimedSegmentIds = new HashSet<>();

        Set<UUID> references;
        Node readers;

        lock.writeLock().lock();
        lock.readLock().lock();
        ;
        try {
            try {
                newWriter();
            } finally {
                lock.writeLock().unlock();
            }
            readers = this.readers;
            references = referencesSupplier.get();
        } finally {
            lock.readLock().unlock();
        }

        Map<TarReader, TarReader> cleaned = new LinkedHashMap<>();

        {
            Node n = readers;
            while (n != null) {
                cleaned.put(n.reader, n.reader);
                result.reclaimedSize += n.reader.size();
                n = n.next;
            }
        }

        Set<UUID> reclaim = newHashSet();

        for (TarReader reader : cleaned.keySet()) {
            if (shutdown) {
                result.interrupted = true;
                return result;
            }
            reader.mark(references, reclaim, reclaimPredicate);
        }

        for (TarReader reader : cleaned.keySet()) {
            if (shutdown) {
                result.interrupted = true;
                return result;
            }
            cleaned.put(reader, reader.sweep(reclaim, result.reclaimedSegmentIds));
        }

        Node closeables;
        long reclaimed;

        while (true) {
            closeables = null;
            reclaimed = 0;

            Node swept = null;
            Node n = readers;

            // The following loops creates a modified version of `readers` and
            // saves it into `swept`. Some TAR readers in `readers` have been
            // swept by the previous code and must be replaced with a slimmer
            // TAR reader with the same index but a higher generation.

            while (n != null) {
                if (cleaned.containsKey(n.reader)) {

                    // We distinguish three cases. First, the original TAR
                    // reader is unmodified. This happens with no content or not
                    // enough content could be swept from the original TAR
                    // reader. Second, some content could be swept from the
                    // original TAR reader and a new TAR reader with the same
                    // index and a higher generation was created. Third, all the
                    // content from the original TAR reader could be swept.

                    TarReader r = cleaned.get(n.reader);

                    if (r != null) {

                        // We are either in the first or in the second case.
                        // Save the TAR reader (either the original or the one
                        // with a higher generation) in the resulting linked list.

                        swept = new Node(r, swept);
                        reclaimed += r.size();
                    }

                    if (r != n.reader) {

                        // We are either in the second or third case. Save the
                        // original TAR reader in a list of TAR readers that
                        // will be closed at the end of this methods.

                        closeables = new Node(n.reader, closeables);
                    }
                } else {

                    // This reader was not involved in the mark-and-sweep. This
                    // might happen in iterations of this loop successive to the
                    // first, when we re-read `readers` and recompute `swept`
                    // all over again.

                    swept = new Node(n.reader, swept);
                }
                n = n.next;
            }

            // `swept` is in the reverse order because we prepended new nodes
            // to it. We have to reverse it before we save it into `readers`.

            swept = reverse(swept);

            // Following is a compare-and-set operation. We based the
            // computation of `swept` of a specific value of `readers`. If
            // `readers` is still the same as the one we started with, we just
            // update `readers` and exit from the loop. Otherwise, we read the
            // value of `readers` and recompute `swept` based on this value.

            lock.writeLock().lock();
            try {
                if (this.readers == readers) {
                    this.readers = swept;
                    break;
                } else {
                    readers = this.readers;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

        result.reclaimedSize -= reclaimed;

        {
            Node n = closeables;
            while (n != null) {
                try {
                    n.reader.close();
                } catch (IOException e) {
                    log.warn("Unable to close swept TAR reader", e);
                }
                result.removableFiles.add(n.reader.getFile());
                n = n.next;
            }
        }

        return result;
    }

    void collectBlobReferences(ReferenceCollector collector, int minGeneration) throws IOException {
        Node n;

        lock.writeLock().lock();
        try {
            if (writer != null) {
                newWriter();
            }
            n = readers;
        } finally {
            lock.writeLock().unlock();
        }

        while (n != null) {
            n.reader.collectBlobReferences(collector, minGeneration);
            n = n.next;
        }
    }

    Iterable<UUID> getSegmentIds() {
        Node n;

        lock.readLock().lock();
        try {
            n = readers;
        } finally {
            lock.readLock().unlock();
        }

        List<UUID> ids = new ArrayList<>();
        while (n != null) {
            ids.addAll(n.reader.getUUIDs());
            n = n.next;
        }
        return ids;
    }

    Map<UUID, List<UUID>> getGraph(String fileName) throws IOException {
        Node n;

        lock.readLock().lock();
        try {
            n = readers;
        } finally {
            lock.readLock().unlock();
        }

        Set<UUID> index = null;
        Map<UUID, List<UUID>> graph = null;

        while (n != null) {
            TarReader r = n.reader;
            if (fileName.equals(r.getFile().getName())) {
                index = r.getUUIDs();
                graph = r.getGraph(false);
                break;
            }
            n = n.next;
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
        Node n;

        lock.readLock().lock();
        try {
            n = readers;
        } finally {
            lock.readLock().unlock();
        }

        Map<String, Set<UUID>> index = new HashMap<>();
        while (n != null) {
            index.put(n.reader.getFile().getAbsolutePath(), n.reader.getUUIDs());
            n = n.next;
        }
        return index;
    }

    void traverseSegmentGraph(Set<UUID> roots, SegmentGraphVisitor visitor) throws IOException {
        Node n;

        lock.readLock().lock();
        try {
            n = readers;
        } finally {
            lock.readLock().unlock();
        }

        includeForwardReferences(n, roots);
        while (n != null) {
            n.reader.traverseSegmentGraph(roots, visitor);
            n = n.next;
        }
    }

}
