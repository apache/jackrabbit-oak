/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.segment.file.tar;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.nio.ByteBuffer.wrap;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.segment.file.tar.GCGeneration.newGCGeneration;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.GRAPH_MAGIC;
import static org.apache.jackrabbit.oak.segment.file.tar.index.IndexLoader.newIndexLoader;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndex;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.BinaryReferencesIndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.binaries.InvalidBinaryReferencesIndexException;
import org.apache.jackrabbit.oak.segment.file.tar.index.Index;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexEntry;
import org.apache.jackrabbit.oak.segment.file.tar.index.IndexLoader;
import org.apache.jackrabbit.oak.segment.file.tar.index.InvalidIndexException;
import org.apache.jackrabbit.oak.segment.util.ReaderAtEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TarReader implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TarReader.class);

    private static final IndexLoader indexLoader = newIndexLoader(BLOCK_SIZE);

    /**
     * Pattern of the segment entry names. Note the trailing (\\..*)? group
     * that's included for compatibility with possible future extensions.
     */
    private static final Pattern NAME_PATTERN = Pattern.compile(
            "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
            + "(\\.([0-9a-f]{8}))?(\\..*)?");

    private static int getEntrySize(int size) {
        return BLOCK_SIZE + size + TarWriter.getPaddingSize(size);
    }

    static TarReader open(File file, boolean memoryMapping, IOMonitor ioMonitor) throws IOException {
        TarReader reader = openFirstFileWithValidIndex(singletonList(file), memoryMapping, ioMonitor);
        if (reader != null) {
            return reader;
        } else {
            throw new IOException("Failed to open tar file " + file);
        }
    }

    /**
     * Creates a {@link TarReader} instance for reading content from a tar file.
     * If there exist multiple generations of the same tar file, they are all
     * passed to this method. The latest generation with a valid tar index
     * (which is a good indication of general validity of the file) is opened
     * and the other generations are removed to clean things up. If none of the
     * generations has a valid index, then something must have gone wrong and
     * we'll try recover as much content as we can from the existing tar
     * generations.
     *
     * @param files         The generations of the same TAR file.
     * @param memoryMapping If {@code true}, opens the TAR file with memory
     *                      mapping enabled.
     * @param recovery      Strategy for recovering a damaged TAR file.
     * @param ioMonitor     Callbacks to track internal operations for the open
     *                      TAR file.
     * @return An instance of {@link TarReader}.
     */
    static TarReader open(Map<Character, File> files, boolean memoryMapping, TarRecovery recovery, IOMonitor ioMonitor) throws IOException {
        SortedMap<Character, File> sorted = newTreeMap();
        sorted.putAll(files);

        List<File> list = newArrayList(sorted.values());
        Collections.reverse(list);

        TarReader reader = openFirstFileWithValidIndex(list, memoryMapping, ioMonitor);
        if (reader != null) {
            return reader;
        }

        // no generation has a valid index, so recover as much as we can
        log.warn("Could not find a valid tar index in {}, recovering...", list);
        LinkedHashMap<UUID, byte[]> entries = newLinkedHashMap();
        for (File file : sorted.values()) {
            collectFileEntries(file, entries, true);
        }

        // regenerate the first generation based on the recovered data
        File file = sorted.values().iterator().next();
        generateTarFile(entries, file, recovery, ioMonitor);

        reader = openFirstFileWithValidIndex(singletonList(file), memoryMapping, ioMonitor);
        if (reader != null) {
            return reader;
        } else {
            throw new IOException("Failed to open recovered tar file " + file);
        }
    }

    static TarReader openRO(Map<Character, File> files, boolean memoryMapping, TarRecovery recovery, IOMonitor ioMonitor) throws IOException {
        // for readonly store only try the latest generation of a given
        // tar file to prevent any rollback or rewrite
        File file = files.get(Collections.max(files.keySet()));
        TarReader reader = openFirstFileWithValidIndex(singletonList(file), memoryMapping, ioMonitor);
        if (reader != null) {
            return reader;
        }
        log.warn("Could not find a valid tar index in {}, recovering read-only", file);
        // collecting the entries (without touching the original file) and
        // writing them into an artificial tar file '.ro.bak'
        LinkedHashMap<UUID, byte[]> entries = newLinkedHashMap();
        collectFileEntries(file, entries, false);
        file = findAvailGen(file, ".ro.bak");
        generateTarFile(entries, file, recovery, ioMonitor);
        reader = openFirstFileWithValidIndex(singletonList(file), memoryMapping, ioMonitor);
        if (reader != null) {
            return reader;
        }
        throw new IOException("Failed to open tar file " + file);
    }

    /**
     * Collects all entries from the given file and optionally backs-up the
     * file, by renaming it to a ".bak" extension
     *
     * @param file    The TAR file.
     * @param entries The map where the recovered entries will be collected
     *                into.
     * @param backup  If {@code true}, performs a backup of the TAR file.
     */
    private static void collectFileEntries(File file, LinkedHashMap<UUID, byte[]> entries, boolean backup) throws IOException {
        log.info("Recovering segments from tar file {}", file);
        try {
            RandomAccessFile access = new RandomAccessFile(file, "r");
            try {
                recoverEntries(file, access, entries);
            } finally {
                access.close();
            }
        } catch (IOException e) {
            log.warn("Could not read tar file {}, skipping...", file, e);
        }

        if (backup) {
            backupSafely(file);
        }
    }

    /**
     * Regenerates a tar file from a list of entries.
     *
     * @param entries   Map of entries to recover. The entries will be recovered
     *                  in the iteration order of this {@link LinkedHashMap}.
     * @param file      The output file that will contain the recovered
     *                  entries.
     * @param recovery  The recovery strategy to execute.
     * @param ioMonitor An instance of {@link IOMonitor}.
     */
    private static void generateTarFile(LinkedHashMap<UUID, byte[]> entries, File file, TarRecovery recovery, IOMonitor ioMonitor) throws IOException {
        log.info("Regenerating tar file {}", file);

        try (TarWriter writer = new TarWriter(file, ioMonitor)) {
            for (Entry<UUID, byte[]> entry : entries.entrySet()) {
                try {
                    recovery.recoverEntry(entry.getKey(), entry.getValue(), new EntryRecovery() {

                        @Override
                        public void recoverEntry(long msb, long lsb, byte[] data, int offset, int size, GCGeneration generation) throws IOException {
                            writer.writeEntry(msb, lsb, data, offset, size, generation);
                        }

                        @Override
                        public void recoverGraphEdge(UUID from, UUID to) {
                            writer.addGraphEdge(from, to);
                        }

                        @Override
                        public void recoverBinaryReference(GCGeneration generation, UUID segmentId, String reference) {
                            writer.addBinaryReference(generation, segmentId, reference);
                        }

                    });
                } catch (IOException e) {
                    throw new IOException(String.format("Unable to recover entry %s for file %s", entry.getKey(), file), e);
                }
            }
        }
    }

    /**
     * Backup this tar file for manual inspection. Something went wrong earlier
     * so we want to prevent the data from being accidentally removed or
     * overwritten.
     *
     * @param file File to backup.
     */
    private static void backupSafely(File file) throws IOException {
        File backup = findAvailGen(file, ".bak");
        log.info("Backing up {} to {}", file, backup.getName());
        if (!file.renameTo(backup)) {
            log.warn("Renaming failed, so using copy to backup {}", file);
            FileUtils.copyFile(file, backup);
            if (!file.delete()) {
                throw new IOException(
                        "Could not remove broken tar file " + file);
            }
        }
    }

    /**
     * Fine next available generation number so that a generated file doesn't
     * overwrite another existing file.
     *
     * @param file The file to backup.
     * @param ext  The extension of the backed up file.
     */
    private static File findAvailGen(File file, String ext) {
        File parent = file.getParentFile();
        String name = file.getName();
        File backup = new File(parent, name + ext);
        for (int i = 2; backup.exists(); i++) {
            backup = new File(parent, name + "." + i + ext);
        }
        return backup;
    }

    private static TarReader openFirstFileWithValidIndex(List<File> files, boolean memoryMapping, IOMonitor ioMonitor) {
        for (File file : files) {
            String name = file.getName();
            try {
                RandomAccessFile access = new RandomAccessFile(file, "r");
                try {
                    Index index = loadAndValidateIndex(access, name);
                    if (index == null) {
                        log.info("No index found in tar file {}, skipping...", name);
                    } else {
                        // found a file with a valid index, drop the others
                        for (File other : files) {
                            if (other != file) {
                                log.info("Removing unused tar file {}", other.getName());
                                other.delete();
                            }
                        }

                        if (memoryMapping) {
                            try {
                                FileAccess mapped = new FileAccess.Mapped(access);
                                return new TarReader(file, mapped, index, ioMonitor);
                            } catch (IOException e) {
                                log.warn("Failed to mmap tar file {}. Falling back to normal file " +
                                        "IO, which will negatively impact repository performance. " +
                                        "This problem may have been caused by restrictions on the " +
                                        "amount of virtual memory available to the JVM. Please make " +
                                        "sure that a 64-bit JVM is being used and that the process " +
                                        "has access to unlimited virtual memory (ulimit option -v).",
                                        name, e);
                            }
                        }

                        FileAccess random = new FileAccess.Random(access);
                        // prevent the finally block from closing the file
                        // as the returned TarReader will take care of that
                        access = null;
                        return new TarReader(file, random, index, ioMonitor);
                    }
                } finally {
                    if (access != null) {
                        access.close();
                    }
                }
            } catch (IOException e) {
                log.warn("Could not read tar file {}, skipping...", name, e);
            }
        }

        return null;
    }

    /**
     * Tries to read an existing index from the given tar file. The index is
     * returned if it is found and looks valid (correct checksum, passes sanity
     * checks).
     *
     * @param file The TAR file.
     * @param name Name of the TAR file, for logging purposes.
     * @return An instance of {@link ByteBuffer} populated with the content of
     * the index. If the TAR doesn't contain any index, {@code null} is returned
     * instead.
     */
    private static Index loadAndValidateIndex(RandomAccessFile file, String name) throws IOException {
        long length = file.length();

        if (length % BLOCK_SIZE != 0) {
            log.warn("Unable to load index of file {}: Invalid alignment", name);
            return null;
        }
        if (length < 6 * BLOCK_SIZE) {
            log.warn("Unable to load index of file {}: File too short", name);
            return null;
        }
        if (length > Integer.MAX_VALUE) {
            log.warn("Unable to load index of file {}: File too long", name);
            return null;
        }

        ReaderAtEnd r = (whence, size) -> {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            file.seek(length - 2 * BLOCK_SIZE - whence);
            file.readFully(buffer.array());
            return buffer;
        };

        try {
            return indexLoader.loadIndex(r);
        } catch (InvalidIndexException e) {
            log.warn("Unable to load index of file {}: {}", name, e.getMessage());
        }

        return null;
    }

    /**
     * Scans through the tar file, looking for all segment entries.
     *
     * @param file    The path of the TAR file.
     * @param access  The contents of the TAR file.
     * @param entries The map that will contain the recovered entries. The
     *                entries are inserted in the {@link LinkedHashMap} in the
     *                order they appear in the TAR file.
     */
    private static void recoverEntries(File file, RandomAccessFile access, LinkedHashMap<UUID, byte[]> entries) throws IOException {
        byte[] header = new byte[BLOCK_SIZE];
        while (access.getFilePointer() + BLOCK_SIZE <= access.length()) {
            // read the tar header block
            access.readFully(header);

            // compute the header checksum
            int sum = 0;
            for (int i = 0; i < BLOCK_SIZE; i++) {
                sum += header[i] & 0xff;
            }

            // identify possible zero block
            if (sum == 0 && access.getFilePointer() + 2 * BLOCK_SIZE == access.length()) {
                return; // found the zero blocks at the end of the file
            }

            // replace the actual stored checksum with spaces for comparison
            for (int i = 148; i < 148 + 8; i++) {
                sum -= header[i] & 0xff;
                sum += ' ';
            }

            byte[] checkbytes = String.format("%06o\0 ", sum).getBytes(UTF_8);
            for (int i = 0; i < checkbytes.length; i++) {
                if (checkbytes[i] != header[148 + i]) {
                    log.warn("Invalid entry checksum at offset {} in tar file {}, skipping...",
                             access.getFilePointer() - BLOCK_SIZE, file);
                }
            }

            // The header checksum passes, so read the entry name and size
            ByteBuffer buffer = wrap(header);
            String name = readString(buffer, 100);
            buffer.position(124);
            int size = readNumber(buffer, 12);
            if (access.getFilePointer() + size > access.length()) {
                // checksum was correct, so the size field should be accurate
                log.warn("Partial entry {} in tar file {}, ignoring...", name, file);
                return;
            }

            Matcher matcher = NAME_PATTERN.matcher(name);
            if (matcher.matches()) {
                UUID id = UUID.fromString(matcher.group(1));

                String checksum = matcher.group(3);
                if (checksum != null || !entries.containsKey(id)) {
                    byte[] data = new byte[size];
                    access.readFully(data);

                    // skip possible padding to stay at block boundaries
                    long position = access.getFilePointer();
                    long remainder = position % BLOCK_SIZE;
                    if (remainder != 0) {
                        access.seek(position + (BLOCK_SIZE - remainder));
                    }

                    if (checksum != null) {
                        CRC32 crc = new CRC32();
                        crc.update(data);
                        if (crc.getValue() != Long.parseLong(checksum, 16)) {
                            log.warn("Checksum mismatch in entry {} of tar file {}, skipping...",
                                     name, file);
                            continue;
                        }
                    }

                    entries.put(id, data);
                }
            } else if (!name.equals(file.getName() + ".idx")) {
                log.warn("Unexpected entry {} in tar file {}, skipping...",
                         name, file);
                long position = access.getFilePointer() + size;
                long remainder = position % BLOCK_SIZE;
                if (remainder != 0) {
                    position += BLOCK_SIZE - remainder;
                }
                access.seek(position);
            }
        }
    }

    private final File file;

    private final FileAccess access;

    private final Index index;

    private volatile boolean hasGraph;

    private final IOMonitor ioMonitor;

    private TarReader(File file, FileAccess access, Index index, IOMonitor ioMonitor) {
        this.file = file;
        this.access = access;
        this.index = index;
        this.ioMonitor = ioMonitor;
    }

    long size() {
        return file.length();
    }

    /**
     * Reads and returns the identifier of every segment included in the index
     * of this TAR file.
     *
     * @return An instance of {@link Set}.
     */
    Set<UUID> getUUIDs() {
        return index.getUUIDs();
    }

    /**
     * Check if the requested entry exists in this TAR file.
     *
     * @param msb The most significant bits of the entry identifier.
     * @param lsb The least significant bits of the entry identifier.
     * @return {@code true} if the entry exists in this TAR file, {@code false}
     * otherwise.
     */
    boolean containsEntry(long msb, long lsb) {
        return findEntry(msb, lsb) != -1;
    }

    /**
     * If the given segment is in this file, get the byte buffer that allows
     * reading it.
     * <p>
     * Whether or not this will read from the file depends on whether memory
     * mapped files are used or not.
     * 
     * @param msb the most significant bits of the segment id
     * @param lsb the least significant bits of the segment id
     * @return the byte buffer, or null if not in this file.
     */
    ByteBuffer readEntry(long msb, long lsb) throws IOException {
        int idx = findEntry(msb, lsb);
        if (idx == -1) {
            return null;
        }
        return readEntry(msb, lsb, index.entry(idx));
    }

    private ByteBuffer readEntry(long msb, long lsb, IndexEntry entry) throws IOException {
        return readSegment(msb, lsb, entry.getPosition(), entry.getLength());
    }

    /**
     * Find the position of the given entry in this TAR file.
     *
     * @param msb The most significant bits of the entry identifier.
     * @param lsb The least significant bits of the entry identifier.
     * @return The position of the entry in the TAR file, or {@code -1} if the
     * entry is not found.
     */
    private int findEntry(long msb, long lsb) {
        return index.findEntry(msb, lsb);
    }

    /**
     * Read the entries in this TAR file.
     *
     * @return An array of {@link TarEntry}.
     */
    @Nonnull
    TarEntry[] getEntries() {
        TarEntry[] entries = new TarEntry[index.count()];
        for (int i = 0; i < entries.length; i++) {
            IndexEntry e = index.entry(i);
            entries[i]  = new TarEntry(
                    e.getMsb(),
                    e.getLsb(),
                    e.getPosition(),
                    e.getLength(),
                    newGCGeneration(
                            e.getGeneration(),
                            e.getFullGeneration(),
                            e.isCompacted()
                    )
            );
        }
        Arrays.sort(entries, TarEntry.OFFSET_ORDER);
        return entries;
    }

    /**
     * Read the references of an entry in this TAR file.
     *
     * @param id    The identifier of the entry.
     * @param graph The content of the graph of this TAR file.
     * @return The references of the provided TAR entry.
     */
    @Nonnull
    private static List<UUID> getReferences(UUID id, Map<UUID, List<UUID>> graph) {
        List<UUID> references = graph.get(id);

        if (references == null) {
            return Collections.emptyList();
        }

        return references;
    }

    /**
     * Collect the references of those BLOBs that are reachable from the entries
     * in this TAR file.
     * <p>
     * The user-provided {@link Predicate} determines if entries belonging to a
     * specific generation should be inspected for binary references of not.
     * Given a generation number as input, if the predicate returns {@code
     * true}, entries from that generation will be skipped. If the predicate
     * returns {@code false}, entries from that generation will be inspected for
     * references.
     * <p>
     * The provided {@link Consumer} is callback object that will be invoked for
     * every reference found in the inspected entries.
     *
     * @param collector      An instance of {@link Consumer}.
     * @param skipGeneration An instance of {@link Predicate}.
     */
    void collectBlobReferences(@Nonnull Consumer<String> collector, Predicate<GCGeneration> skipGeneration) {
        BinaryReferencesIndex references = getBinaryReferences();

        if (references == null) {
            return;
        }

        references.forEach((generation, full, compacted, segment, reference) -> {
            if (skipGeneration.apply(newGCGeneration(generation, full, compacted))) {
                return;
            }
            collector.accept(reference);
        });
    }

    /**
     * Mark entries that can be reclaimed.
     * <p>
     * A data segment is reclaimable iff its generation is in the {@code
     * reclaimGeneration} predicate. A bulk segment is reclaimable if it is not
     * in {@code bulkRefs} or if it is transitively reachable through a non
     * reclaimable data segment.
     * <p>
     * The algorithm implemented by this method uses a couple of supporting data
     * structures.
     * <p>
     * The first of the supporting data structures is the set of bulk segments
     * to keep. When this method is invoked, this set initially contains the set
     * of bulk segments that are currently in use. The algorithm removes a
     * reference from this set if the corresponding bulk segment is not
     * referenced (either directly or transitively) from a marked data segment.
     * The algorithm adds a reference to this set if a marked data segment is
     * references the corresponding bulk segment. When this method returns, the
     * references in this set represent bulk segments that are currently in use
     * and should not be removed.
     * <p>
     * The second of the supporting data structures is the set of segments to
     * reclaim. This set contains references to bulk and data segments. A
     * reference to a bulk segment is added if the bulk segment is not
     * referenced (either directly or transitively) by marked data segment. A
     * reference to a data segment is added if the user-provided predicate
     * returns {@code true} for that segment. When this method returns, this set
     * contains segments that are not marked and can be removed.
     *
     * @param references  The set of bulk segments to keep.
     * @param reclaimable The set of segments to remove.
     * @param context     An instance of {@link CleanupContext}.
     */
    void mark(Set<UUID> references, Set<UUID> reclaimable, CleanupContext context) throws IOException {
        Map<UUID, List<UUID>> graph = getGraph();
        TarEntry[] entries = getEntries();
        for (int i = entries.length - 1; i >= 0; i--) {
            // A bulk segments is *always* written before any data segment referencing it.
            // Backward iteration ensures we see all references to bulk segments before
            // we see the bulk segment itself. Therefore we can remove a bulk reference
            // from the bulkRefs set once we encounter it, which save us some memory and
            // CPU on subsequent look-ups.
            TarEntry entry = entries[i];
            UUID id = new UUID(entry.msb(), entry.lsb());
            if (context.shouldReclaim(id, entry.generation(), references.remove(id))) {
                reclaimable.add(id);
            } else {
                for (UUID refId : getReferences(id, graph)) {
                    if (context.shouldFollow(id, refId)) {
                        references.add(refId);
                    }
                }
            }
        }
    }

    /**
     * Try to remove every segment contained in a user-provided set.
     * <p>
     * This method might refuse to remove the segments under the following
     * circumstances.
     * <p>
     * First, if this TAR files does not contain any of the segments that are
     * supposed to be removed. In this case, the method returns {@code null}.
     * <p>
     * Second, if this method contains some of the segments that are supposed to
     * be removed, but the reclaimable space is be less than 1/4 of the current
     * size of the TAR file. In this case, this method returns this {@link
     * TarReader}.
     * <p>
     * Third, if this TAR file is in the highest generation possible ('z') and
     * thus a new generation for this TAR file can't be created. In this case,
     * the method returns this {@link TarReader}.
     * <p>
     * Fourth, if a new TAR file has been created but it is unreadable for
     * unknown reasons. In this case, this method returns this {@link
     * TarReader}.
     * <p>
     * If none of the above conditions apply, this method returns a new {@link
     * TarReader} instance tha points to a TAR file that doesn't contain the
     * removed segments. The returned {@link TarReader} will belong to the next
     * generation of this {@link TarReader}. In this case, the {@code reclaimed}
     * set will be updated to contain the identifiers of the segments that were
     * removed from this TAR file.
     *
     * @param reclaim   Set of segment sto reclaim.
     * @param reclaimed Set of reclaimed segments. It will be update if this TAR
     *                  file is rewritten.
     * @return Either this {@link TarReader}, or a new instance of {@link
     * TarReader}, or {@code null}.
     */
    TarReader sweep(@Nonnull Set<UUID> reclaim, @Nonnull Set<UUID> reclaimed) throws IOException {
        String name = file.getName();
        log.debug("Cleaning up {}", name);

        Set<UUID> cleaned = newHashSet();
        int afterSize = 0;
        int beforeSize = 0;
        int afterCount = 0;

        TarEntry[] entries = getEntries();
        for (int i = 0; i < entries.length; i++) {
            TarEntry entry = entries[i];
            beforeSize += getEntrySize(entry.size());
            UUID id = new UUID(entry.msb(), entry.lsb());
            if (reclaim.contains(id)) {
                cleaned.add(id);
                entries[i] = null;
            } else {
                afterSize += getEntrySize(entry.size());
                afterCount += 1;
            }
        }
      
        if (afterCount == 0) {
            log.debug("None of the entries of {} are referenceable.", name);
            return null;
        }
        if (afterSize >= beforeSize * 3 / 4 && hasGraph()) {
            // the space savings are not worth it at less than 25%,
            // unless this tar file lacks a pre-compiled segment graph
            // in which case we'll always generate a new tar file with
            // the graph to speed up future garbage collection runs.
            log.debug("Not enough space savings. ({}/{}). Skipping clean up of {}",
                    access.length() - afterSize, access.length(), name);
            return this;
        }
        if (!hasGraph()) {
            log.warn("Recovering {}, which is missing its graph.", name);
        }

        int pos = name.length() - "a.tar".length();
        char generation = name.charAt(pos);
        if (generation == 'z') {
            log.debug("No garbage collection after reaching generation z: {}", name);
            return this;
        }

        File newFile = new File(
                file.getParentFile(),
                name.substring(0, pos) + (char) (generation + 1) + ".tar");

        log.debug("Writing new generation {}", newFile.getName());
        TarWriter writer = new TarWriter(newFile, ioMonitor);
        for (TarEntry entry : entries) {
            if (entry != null) {
                long msb = entry.msb();
                long lsb = entry.lsb();
                int offset = entry.offset();
                int size = entry.size();
                GCGeneration gen = entry.generation();
                byte[] data = new byte[size];
                readSegment(msb, lsb, offset, size).get(data);
                writer.writeEntry(msb, lsb, data, 0, size, gen);
            }
        }

        // Reconstruct the graph index for non-cleaned segments.

        Map<UUID, List<UUID>> graph = getGraph();

        for (Entry<UUID, List<UUID>> e : graph.entrySet()) {
            if (cleaned.contains(e.getKey())) {
                continue;
            }

            Set<UUID> vertices = newHashSet();

            for (UUID vertex : e.getValue()) {
                if (cleaned.contains(vertex)) {
                    continue;
                }

                vertices.add(vertex);
            }

            for (UUID vertex : vertices) {
                writer.addGraphEdge(e.getKey(), vertex);
            }
        }

        // Reconstruct the binary reference index for non-cleaned segments.

        BinaryReferencesIndex references = getBinaryReferences();

        if (references != null) {
            references.forEach((gen, full, compacted, id, reference) -> {
                if (cleaned.contains(id)) {
                    return;
                }
                writer.addBinaryReference(newGCGeneration(gen, full, compacted), id, reference);
            });
        }

        writer.close();

        TarReader reader = openFirstFileWithValidIndex(singletonList(newFile), access.isMemoryMapped(), ioMonitor);
        if (reader != null) {
            reclaimed.addAll(cleaned);
            return reader;
        } else {
            log.warn("Failed to open cleaned up tar file {}", file);
            return this;
        }
    }

    @Override
    public void close() throws IOException {
        access.close();
    }

    /**
     * Loads and parses the optional pre-compiled graph entry from the given tar
     * file.
     *
     * @return The parsed graph, or {@code null} if one was not found.
     */
    Map<UUID, List<UUID>> getGraph() throws IOException {
        ByteBuffer graph = loadGraph();
        if (graph == null) {
            return null;
        } else {
            return parseGraph(graph);
        }
    }

    private boolean hasGraph() {
        if (!hasGraph) {
            try {
                loadGraph();
            } catch (IOException ignore) { }
        }
        return hasGraph;
    }

    private int getIndexEntrySize() {
        return getEntrySize(index.size());
    }

    private int getGraphEntrySize() {
        ByteBuffer buffer;

        try {
            buffer = loadGraph();
        } catch (IOException e) {
            log.warn("Exception while loading pre-compiled tar graph", e);
            return 0;
        }

        if (buffer == null) {
            return 0;
        }

        return getEntrySize(buffer.getInt(buffer.limit() - 8));
    }

    /**
     * Read the index of binary references from this TAR file.
     * <p>
     * The index of binary references is a two-level map. The key to the first
     * level of the map is the generation. The key to the second level of the
     * map is the identifier of a data segment in this TAR file. The value of
     * the second-level map is the set of binary references contained in the
     * segment.
     *
     * @return An instance of {@link Map}.
     */
    BinaryReferencesIndex getBinaryReferences() {
        BinaryReferencesIndex index = null;
        try {
            index = loadBinaryReferences();
        } catch (InvalidBinaryReferencesIndexException | IOException e) {
            log.warn("Exception while loading binary reference", e);
        }
        return index;
    }

    private BinaryReferencesIndex loadBinaryReferences() throws IOException, InvalidBinaryReferencesIndexException {
        int end = access.length() - 2 * BLOCK_SIZE - getIndexEntrySize() - getGraphEntrySize();
        return BinaryReferencesIndexLoader.loadBinaryReferencesIndex((whence, size) -> access.read(end - whence, size));
    }

    /**
     * Loads the optional pre-compiled graph entry from the given tar file.
     *
     * @return graph buffer, or {@code null} if one was not found
     * @throws IOException if the tar file could not be read
     */
    private ByteBuffer loadGraph() throws IOException {
        int pos = access.length() - 2 * BLOCK_SIZE - getIndexEntrySize();

        ByteBuffer meta = access.read(pos - 16, 16);

        int crc32 = meta.getInt();
        int count = meta.getInt();
        int bytes = meta.getInt();
        int magic = meta.getInt();

        if (magic != GRAPH_MAGIC) {
            log.warn("Invalid graph magic number in {}", file);
            return null;
        }

        if (count < 0) {
            log.warn("Invalid number of entries in {}", file);
            return null;
        }

        if (bytes < 4 + count * 34) {
            log.warn("Invalid entry size in {}", file);
            return null;
        }

        ByteBuffer graph = access.read(pos - bytes, bytes);

        byte[] b = new byte[bytes - 16];

        graph.mark();
        graph.get(b);
        graph.reset();

        CRC32 checksum = new CRC32();
        checksum.update(b);

        if (crc32 != (int) checksum.getValue()) {
            log.warn("Invalid graph checksum in tar file {}", file);
            return null;
        }

        hasGraph = true;

        return graph;
    }

    private ByteBuffer readSegment(long msb, long lsb, int offset, int size) throws IOException {
        ioMonitor.beforeSegmentRead(file, msb, lsb, size);
        Stopwatch stopwatch = Stopwatch.createStarted();
        ByteBuffer buffer = access.read(offset, size);
        long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
        ioMonitor.afterSegmentRead(file, msb, lsb, size, elapsed);
        return buffer;
    }

    private static Map<UUID, List<UUID>> parseGraph(ByteBuffer buffer) {
        int nEntries = buffer.getInt(buffer.limit() - 12);

        Map<UUID, List<UUID>> graph = newHashMapWithExpectedSize(nEntries);

        for (int i = 0; i < nEntries; i++) {
            long msb = buffer.getLong();
            long lsb = buffer.getLong();
            int nVertices = buffer.getInt();

            List<UUID> vertices = newArrayListWithCapacity(nVertices);

            for (int j = 0; j < nVertices; j++) {
                long vMsb = buffer.getLong();
                long vLsb = buffer.getLong();
                vertices.add(new UUID(vMsb, vLsb));
            }

            graph.put(new UUID(msb, lsb), vertices);
        }

        return graph;
    }

    private static String readString(ByteBuffer buffer, int fieldSize) {
        byte[] b = new byte[fieldSize];
        buffer.get(b);
        int n = 0;
        while (n < fieldSize && b[n] != 0) {
            n++;
        }
        return new String(b, 0, n, UTF_8);
    }

    private static int readNumber(ByteBuffer buffer, int fieldSize) {
        byte[] b = new byte[fieldSize];
        buffer.get(b);
        int number = 0;
        for (int i = 0; i < fieldSize; i++) {
            int digit = b[i] & 0xff;
            if ('0' <= digit && digit <= '7') {
                number = number * 8 + digit - '0';
            } else {
                break;
            }
        }
        return number;
    }

    /**
     * Return the path of this TAR file.
     *
     * @return An instance of {@link File}.
     */
    File getFile() {
        return file;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return file.toString();
    }

}
