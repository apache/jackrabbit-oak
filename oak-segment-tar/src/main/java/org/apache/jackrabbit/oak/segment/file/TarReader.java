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
package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newHashMapWithExpectedSize;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static java.nio.ByteBuffer.wrap;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.segment.Segment.REF_COUNT_OFFSET;
import static org.apache.jackrabbit.oak.segment.Segment.getGcGeneration;
import static org.apache.jackrabbit.oak.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.segment.file.TarWriter.BINARY_REFERENCES_MAGIC;
import static org.apache.jackrabbit.oak.segment.file.TarWriter.GRAPH_MAGIC;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import javax.annotation.Nonnull;

import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.segment.SegmentGraph.SegmentGraphVisitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TarReader implements Closeable {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(TarReader.class);

    private static final Logger GC_LOG = LoggerFactory.getLogger(TarReader.class.getName() + "-GC");

    /** Magic byte sequence at the end of the index block. */
    private static final int INDEX_MAGIC = TarWriter.INDEX_MAGIC;

    /**
     * Pattern of the segment entry names. Note the trailing (\\..*)? group
     * that's included for compatibility with possible future extensions.
     */
    private static final Pattern NAME_PATTERN = Pattern.compile(
            "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
            + "(\\.([0-9a-f]{8}))?(\\..*)?");

    /** The tar file block size. */
    private static final int BLOCK_SIZE = TarWriter.BLOCK_SIZE;

    static int getEntrySize(int size) {
        return BLOCK_SIZE + size + TarWriter.getPaddingSize(size);
    }

    static TarReader open(File file, boolean memoryMapping) throws IOException {
        TarReader reader = openFirstFileWithValidIndex(
                singletonList(file), memoryMapping);
        if (reader != null) {
            return reader;
        } else {
            throw new IOException("Failed to open tar file " + file);
        }
    }

    /**
     * Creates a TarReader instance for reading content from a tar file.
     * If there exist multiple generations of the same tar file, they are
     * all passed to this method. The latest generation with a valid tar
     * index (which is a good indication of general validity of the file)
     * is opened and the other generations are removed to clean things up.
     * If none of the generations has a valid index, then something must have
     * gone wrong and we'll try recover as much content as we can from the
     * existing tar generations.
     *
     * @param files
     * @param memoryMapping
     * @return
     * @throws IOException
     */
    static TarReader open(Map<Character, File> files, boolean memoryMapping)
            throws IOException {
        SortedMap<Character, File> sorted = newTreeMap();
        sorted.putAll(files);

        List<File> list = newArrayList(sorted.values());
        Collections.reverse(list);

        TarReader reader = openFirstFileWithValidIndex(list, memoryMapping);
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
        generateTarFile(entries, file);

        reader = openFirstFileWithValidIndex(singletonList(file), memoryMapping);
        if (reader != null) {
            return reader;
        } else {
            throw new IOException("Failed to open recovered tar file " + file);
        }
    }

    static TarReader openRO(Map<Character, File> files, boolean memoryMapping,
            boolean recover) throws IOException {
        // for readonly store only try the latest generation of a given
        // tar file to prevent any rollback or rewrite
        File file = files.get(Collections.max(files.keySet()));

        TarReader reader = openFirstFileWithValidIndex(singletonList(file),
                memoryMapping);
        if (reader != null) {
            return reader;
        }
        if (recover) {
            log.warn(
                    "Could not find a valid tar index in {}, recovering read-only",
                    file);
            // collecting the entries (without touching the original file) and
            // writing them into an artificial tar file '.ro.bak'
            LinkedHashMap<UUID, byte[]> entries = newLinkedHashMap();
            collectFileEntries(file, entries, false);
            file = findAvailGen(file, ".ro.bak");
            generateTarFile(entries, file);
            reader = openFirstFileWithValidIndex(singletonList(file),
                    memoryMapping);
            if (reader != null) {
                return reader;
            }
        }

        throw new IOException("Failed to open tar file " + file);
    }

    /**
     * Collects all entries from the given file and optionally backs-up the
     * file, by renaming it to a ".bak" extension
     * 
     * @param file
     * @param entries
     * @param backup
     * @throws IOException
     */
    private static void collectFileEntries(File file,
            LinkedHashMap<UUID, byte[]> entries, boolean backup)
            throws IOException {
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
     * @param entries
     * @param file
     * @throws IOException
     */
    private static void generateTarFile(LinkedHashMap<UUID, byte[]> entries,
            File file) throws IOException {
        log.info("Regenerating tar file {}", file);
        TarWriter writer = new TarWriter(file);
        for (Map.Entry<UUID, byte[]> entry : entries.entrySet()) {
            UUID uuid = entry.getKey();
            byte[] data = entry.getValue();
            int generation = getGcGeneration(wrap(data), uuid);
            writer.writeEntry(
                    uuid.getMostSignificantBits(),
                    uuid.getLeastSignificantBits(),
                    data, 0, data.length, generation);
        }
        writer.close();
    }

    /**
     * Backup this tar file for manual inspection. Something went
     * wrong earlier so we want to prevent the data from being
     * accidentally removed or overwritten.
     *
     * @param file
     * @throws IOException
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
     * @param file
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

    private static TarReader openFirstFileWithValidIndex(List<File> files, boolean memoryMapping) {
        for (File file : files) {
            String name = file.getName();
            try {
                RandomAccessFile access = new RandomAccessFile(file, "r");
                try {
                    ByteBuffer index = loadAndValidateIndex(access, name);
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
                                // re-read the index, now with memory mapping
                                int indexSize = index.remaining();
                                index = mapped.read(
                                        mapped.length() - indexSize - 16 - 1024,
                                        indexSize);
                                return new TarReader(file, mapped, index);
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
                        return new TarReader(file, random, index);
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
     * returned if it is found and looks valid (correct checksum, passes
     * sanity checks).
     *
     * @param file tar file
     * @param name name of the tar file, for logging purposes
     * @return tar index, or {@code null} if not found or not valid
     * @throws IOException if the tar file could not be read
     */
    private static ByteBuffer loadAndValidateIndex(
            RandomAccessFile file, String name)
            throws IOException {
        long length = file.length();
        if (length % BLOCK_SIZE != 0
                || length < 6 * BLOCK_SIZE
                || length > Integer.MAX_VALUE) {
            log.warn("Unexpected size {} of tar file {}", length, name);
            return null; // unexpected file size
        }

        // read the index metadata just before the two final zero blocks
        ByteBuffer meta = ByteBuffer.allocate(16);
        file.seek(length - 2 * BLOCK_SIZE - 16);
        file.readFully(meta.array());
        int crc32 = meta.getInt();
        int count = meta.getInt();
        int bytes = meta.getInt();
        int magic = meta.getInt();

        if (magic != INDEX_MAGIC) {
            return null; // magic byte mismatch
        }

        if (count < 1 || bytes < count * TarEntry.SIZE + 16 || bytes % BLOCK_SIZE != 0) {
            log.warn("Invalid index metadata in tar file {}", name);
            return null; // impossible entry and/or byte counts
        }

        // this involves seeking backwards in the file, which might not
        // perform well, but that's OK since we only do this once per file
        ByteBuffer index = ByteBuffer.allocate(count * TarEntry.SIZE);
        file.seek(length - 2 * BLOCK_SIZE - 16 - count * TarEntry.SIZE);
        file.readFully(index.array());
        index.mark();

        CRC32 checksum = new CRC32();
        long limit = length - 2 * BLOCK_SIZE - bytes - BLOCK_SIZE;
        long lastmsb = Long.MIN_VALUE;
        long lastlsb = Long.MIN_VALUE;
        byte[] entry = new byte[TarEntry.SIZE];
        for (int i = 0; i < count; i++) {
            index.get(entry);
            checksum.update(entry);

            ByteBuffer buffer = wrap(entry);
            long msb   = buffer.getLong();
            long lsb   = buffer.getLong();
            int offset = buffer.getInt();
            int size   = buffer.getInt();

            if (lastmsb > msb || (lastmsb == msb && lastlsb > lsb)) {
                log.warn("Incorrect index ordering in tar file {}", name);
                return null;
            } else if (lastmsb == msb && lastlsb == lsb && i > 0) {
                log.warn("Duplicate index entry in tar file {}", name);
                return null;
            } else if (offset < 0 || offset % BLOCK_SIZE != 0) {
                log.warn("Invalid index entry offset in tar file {}", name);
                return null;
            } else if (size < 1 || offset + size > limit) {
                log.warn("Invalid index entry size in tar file {}", name);
                return null;
            }

            lastmsb = msb;
            lastlsb = lsb;
        }

        if (crc32 != (int) checksum.getValue()) {
            log.warn("Invalid index checksum in tar file {}", name);
            return null; // checksum mismatch
        }

        index.reset();
        return index;
    }

    /**
     * Scans through the tar file, looking for all segment entries.
     *
     * @throws IOException if the tar file could not be read
     */
    private static void recoverEntries(
            File file, RandomAccessFile access,
            LinkedHashMap<UUID, byte[]> entries) throws IOException {
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

    private final ByteBuffer index;

    private volatile boolean closed;

    private volatile boolean hasGraph;

    private TarReader(File file, FileAccess access, ByteBuffer index) {
        this.file = file;
        this.access = access;
        this.index = index;
    }

    long size() {
        return file.length();
    }

    /**
     * Returns the number of segments in this tar file.
     *
     * @return number of segments
     */
    int count() {
        return index.capacity() / TarEntry.SIZE;
    }

    /**
     * Iterates over all entries in this tar file and calls
     * {@link TarEntryVisitor#visit(long, long, File, int, int)} on them.
     *
     * @param visitor entry visitor
     */
    void accept(TarEntryVisitor visitor) {
        int position = index.position();
        while (position < index.limit()) {
            visitor.visit(
                    index.getLong(position),
                    index.getLong(position + 8),
                    file,
                    index.getInt(position + 16),
                    index.getInt(position + 20));
            position += TarEntry.SIZE;
        }
    }

    Set<UUID> getUUIDs() {
        Set<UUID> uuids = newHashSetWithExpectedSize(index.remaining() / TarEntry.SIZE);
        int position = index.position();
        while (position < index.limit()) {
            uuids.add(new UUID(
                    index.getLong(position),
                    index.getLong(position + 8)));
            position += TarEntry.SIZE;
        }
        return uuids;
    }

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
     * @return the byte buffer, or null if not in this file
     */
    ByteBuffer readEntry(long msb, long lsb) throws IOException {
        int position = findEntry(msb, lsb);
        if (position != -1) {
            return access.read(
                    index.getInt(position + 16),
                    index.getInt(position + 20));
        } else {
            return null;
        }
    }

    /**
     * Find the position of the given segment in the tar file.
     * It uses the tar index if available.
     * 
     * @param msb the most significant bits of the segment id
     * @param lsb the least significant bits of the segment id
     * @return the position in the file, or -1 if not found
     */
    private int findEntry(long msb, long lsb) {
        // The segment identifiers are randomly generated with uniform
        // distribution, so we can use interpolation search to find the
        // matching entry in the index. The average runtime is O(log log n).

        int lowIndex = 0;
        int highIndex = index.remaining() / TarEntry.SIZE - 1;
        float lowValue = Long.MIN_VALUE;
        float highValue = Long.MAX_VALUE;
        float targetValue = msb;

        while (lowIndex <= highIndex) {
            int guessIndex = lowIndex + Math.round(
                    (highIndex - lowIndex)
                    * (targetValue - lowValue)
                    / (highValue - lowValue));
            int position = index.position() + guessIndex * TarEntry.SIZE;
            long m = index.getLong(position);
            if (msb < m) {
                highIndex = guessIndex - 1;
                highValue = m;
            } else if (msb > m) {
                lowIndex = guessIndex + 1;
                lowValue = m;
            } else {
                // getting close...
                long l = index.getLong(position + 8);
                if (lsb < l) {
                    highIndex = guessIndex - 1;
                    highValue = m;
                } else if (lsb > l) {
                    lowIndex = guessIndex + 1;
                    lowValue = m;
                } else {
                    // found it!
                    return position;
                }
            }
        }

        // not found
        return -1;
    }

    @Nonnull
    private TarEntry[] getEntries() {
        TarEntry[] entries = new TarEntry[index.remaining() / TarEntry.SIZE];
        int position = index.position();
        for (int i = 0; position < index.limit(); i++) {
            entries[i]  = new TarEntry(
                    index.getLong(position),
                    index.getLong(position + 8),
                    index.getInt(position + 16),
                    index.getInt(position + 20),
                    index.getInt(position + 24));
            position += TarEntry.SIZE;
        }
        Arrays.sort(entries, TarEntry.OFFSET_ORDER);
        return entries;
    }

    @Nonnull
    private List<UUID> getReferences(TarEntry entry, UUID id, Map<UUID, List<UUID>> graph) throws IOException {
        if (graph != null) {
            List<UUID> uuids = graph.get(id);
            return uuids == null ? Collections.<UUID>emptyList() : uuids;
        } else {
            // a pre-compiled graph is not available, so read the
            // references directly from this segment
            ByteBuffer segment = access.read(
                    entry.offset(),
                    Math.min(entry.size(), 16 * 256));
            int pos = segment.position();
            int refCount = segment.get(pos + REF_COUNT_OFFSET) & 0xff;
            int refEnd = pos + 16 * (refCount + 1);
            List<UUID> refIds = newArrayList();
            for (int refPos = pos + 16; refPos < refEnd; refPos += 16) {
                refIds.add(new UUID(
                        segment.getLong(refPos),
                        segment.getLong(refPos + 8)));
            }
            return refIds;
        }
    }

    /**
     * Build the graph of segments reachable from an initial set of segments
     * @param roots     the initial set of segments
     * @param visitor   visitor receiving call back while following the segment graph
     * @throws IOException
     */
    public void traverseSegmentGraph(
        @Nonnull Set<UUID> roots,
        @Nonnull SegmentGraphVisitor visitor) throws IOException {
        checkNotNull(roots);
        checkNotNull(visitor);
        Map<UUID, List<UUID>> graph = getGraph(false);

        TarEntry[] entries = getEntries();
        for (int i = entries.length - 1; i >= 0; i--) {
            TarEntry entry = entries[i];
            UUID id = new UUID(entry.msb(), entry.lsb());
            if (roots.remove(id) && isDataSegmentId(entry.lsb())) {
                // this is a referenced data segment, so follow the graph
                for (UUID refId : getReferences(entry, id, graph)) {
                    visitor.accept(id, refId);
                    roots.add(refId);
                }
            } else {
                // this segment is not referenced anywhere
                visitor.accept(id, null);
            }
        }
    }

    /**
     * Calculate the ids of the segments directly referenced from {@code referenceIds}
     * through forward references.
     *
     * @param referencedIds  The initial set of ids to start from. On return it
     *                       contains the set of direct forward references.
     *
     * @throws IOException
     */
    void calculateForwardReferences(Set<UUID> referencedIds) throws IOException {
        Map<UUID, List<UUID>> graph = getGraph(false);
        TarEntry[] entries = getEntries();
        for (int i = entries.length - 1; i >= 0; i--) {
            TarEntry entry = entries[i];
            UUID id = new UUID(entry.msb(), entry.lsb());
            if (referencedIds.remove(id)) {
                if (isDataSegmentId(entry.lsb())) {
                    referencedIds.addAll(getReferences(entry, id, graph));
                }
            }
        }
    }

    /**
     * Collect the references of those blobs that are reachable from any segment with a
     * generation at or above {@code minGeneration}.
     * @param collector
     * @param minGeneration
     */
    void collectBlobReferences(ReferenceCollector collector, int minGeneration) {
        Map<Integer, Map<UUID, Set<String>>> generations = getBinaryReferences();

        if (generations == null) {
            return;
        }

        for (Entry<Integer, Map<UUID, Set<String>>> entry : generations.entrySet()) {
            if (entry.getKey() < minGeneration) {
                continue;
            }

            for (Set<String> references : entry.getValue().values()) {
                for (String reference : references) {
                    collector.addReference(reference, null);
                }
            }
        }
    }

    /**
     * Collect reclaimable segments.
     * A data segment is reclaimable iff its generation is in the {@code reclaimGeneration}
     * predicate.
     * A bulk segment is reclaimable if it is not in {@code bulkRefs} or if it is transitively
     * reachable through a non reclaimable data segment.
     *
     * @param bulkRefs  bulk segment gc roots
     * @param reclaim   reclaimable segments
     * @param reclaimGeneration  reclaim generation predicate for data segments
     * @throws IOException
     */
    void mark(Set<UUID> bulkRefs, Set<UUID> reclaim, Predicate<Integer> reclaimGeneration)
    throws IOException {
        Map<UUID, List<UUID>> graph = getGraph(true);
        TarEntry[] entries = getEntries();
        for (int i = entries.length - 1; i >= 0; i--) {
            // A bulk segments is *always* written before any data segment referencing it.
            // Backward iteration ensures we see all references to bulk segments before
            // we see the bulk segment itself. Therefore we can remove a bulk reference
            // from the bulkRefs set once we encounter it, which save us some memory and
            // CPU on subsequent look-ups.
            TarEntry entry = entries[i];
            UUID id = new UUID(entry.msb(), entry.lsb());
            if ((!isDataSegmentId(entry.lsb()) && !bulkRefs.remove(id)) ||
                (isDataSegmentId(entry.lsb()) && reclaimGeneration.apply(entry.generation()))) {
                // non referenced bulk segment or old data segment
                reclaim.add(id);
            } else {
                if (isDataSegmentId(entry.lsb())) {
                    for (UUID refId : getReferences(entry, id, graph)) {
                        if (!isDataSegmentId(refId.getLeastSignificantBits())) {
                            // keep the extra check for bulk segments for the case where a
                            // pre-compiled graph is not available (graph == null) and
                            // getReferences also includes data references
                            bulkRefs.add(refId);
                        }
                    }
                }
            }
        }
    }

    /**
     * Remove reclaimable segments and collect actually reclaimed segments.
     * @param reclaim       segments to reclaim
     * @param reclaimed     actually reclaimed segments
     * @return              reader resulting from the reclamation process
     * @throws IOException
     */
    TarReader sweep(@Nonnull Set<UUID> reclaim, @Nonnull Set<UUID> reclaimed) throws IOException {
        String name = file.getName();
        log.debug("Cleaning up {}", name);

        Set<UUID> cleaned = newHashSet();
        int size = 0;
        int count = 0;
        TarEntry[] entries = getEntries();
        for (int i = 0; i < entries.length; i++) {
            TarEntry entry = entries[i];
            UUID id = new UUID(entry.msb(), entry.lsb());
            if (reclaim.contains(id)) {
                cleaned.add(id);
                entries[i] = null;
            } else {
                size += getEntrySize(entry.size());
                count += 1;
            }
        }
        size += getEntrySize(TarEntry.SIZE * count + 16);
        size += 2 * BLOCK_SIZE;

        if (count == 0) {
            log.debug("None of the entries of {} are referenceable.", name);
            logCleanedSegments(cleaned);
            return null;
        }
        if (size >= access.length() * 3 / 4 && hasGraph()) {
            // the space savings are not worth it at less than 25%,
            // unless this tar file lacks a pre-compiled segment graph
            // in which case we'll always generate a new tar file with
            // the graph to speed up future garbage collection runs.
            log.debug("Not enough space savings. ({}/{}). Skipping clean up of {}",
                    access.length() - size, access.length(), name);
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
        TarWriter writer = new TarWriter(newFile);
        for (TarEntry entry : entries) {
            if (entry != null) {
                byte[] data = new byte[entry.size()];
                access.read(entry.offset(), entry.size()).get(data);
                writer.writeEntry(
                        entry.msb(), entry.lsb(), data, 0, entry.size(), entry.generation());
            }
        }

        // Reconstruct the binary reference index for non-cleaned segments.

        Map<Integer, Map<UUID, Set<String>>> references = getBinaryReferences();

        for (Entry<Integer, Map<UUID, Set<String>>> ge : references.entrySet()) {
            for (Entry<UUID, Set<String>> se : ge.getValue().entrySet()) {
                if (cleaned.contains(se.getKey())) {
                    continue;
                }
                for (String reference : se.getValue()) {
                    writer.addBinaryReference(ge.getKey(), se.getKey(), reference);
                }
            }

        }

        writer.close();

        TarReader reader = openFirstFileWithValidIndex(
                singletonList(newFile), access.isMemoryMapped());
        if (reader != null) {
            logCleanedSegments(cleaned);
            reclaimed.addAll(cleaned);
            return reader;
        } else {
            log.warn("Failed to open cleaned up tar file {}", file);
            return this;
        }
    }

    private void logCleanedSegments(Set<UUID> cleaned) {
        StringBuilder uuids = new StringBuilder();
        String newLine = System.getProperty("line.separator", "\n") + "        ";

        int c = 0;
        String sep = "";
        for (UUID uuid : cleaned) {
            uuids.append(sep);
            if (c++ % 4 == 0) {
                uuids.append(newLine);
            }
            uuids.append(uuid);
            sep = ", ";
        }

        GC_LOG.info("TarMK cleaned segments from {}: {}", file.getName(), uuids);
    }

    /**
     * @return  {@code true} iff this reader has been closed
     * @see #close()
     */
    boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        access.close();
    }

    //-----------------------------------------------------------< private >--

    /**
     * Loads and parses the optional pre-compiled graph entry from the given tar
     * file.
     *
     * @return the parsed graph, or {@code null} if one was not found
     * @throws IOException if the tar file could not be read
     */
    Map<UUID, List<UUID>> getGraph(boolean bulkOnly) throws IOException {
        ByteBuffer graph = loadGraph();
        if (graph == null) {
            return null;
        } else {
            return parseGraph(graph, bulkOnly);
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
        return getEntrySize(index.remaining() + 16);
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

    Map<Integer, Map<UUID, Set<String>>> getBinaryReferences() {
        ByteBuffer buffer;

        try {
            buffer = loadBinaryReferences();
        } catch (IOException e) {
            log.warn("Exception while loading binary reference", e);
            return null;
        }

        if (buffer == null) {
            return null;
        }

        return parseBinaryReferences(buffer);
    }

    private ByteBuffer loadBinaryReferences() throws IOException {
        int end = access.length() - 2 * BLOCK_SIZE - getIndexEntrySize() - getGraphEntrySize();

        ByteBuffer meta = access.read(end - 16, 16);

        int crc32 = meta.getInt();
        int count = meta.getInt();
        int size = meta.getInt();
        int magic = meta.getInt();

        if (magic != BINARY_REFERENCES_MAGIC) {
            log.warn("Invalid binary references magic number");
            return null;
        }

        if (count < 0 || size < count * 22 + 16) {
            log.warn("Invalid binary references size or count");
            return null;
        }

        ByteBuffer buffer = access.read(end - size, size);

        byte[] data = new byte[size - 16];
        buffer.mark();
        buffer.get(data);
        buffer.reset();

        CRC32 checksum = new CRC32();
        checksum.update(data);

        if ((int) (checksum.getValue()) != crc32) {
            log.warn("Invalid binary references checksum");
            return null;
        }

        return buffer;
    }

    private Map<Integer, Map<UUID, Set<String>>> parseBinaryReferences(ByteBuffer buffer) {
        int nGenerations = buffer.getInt(buffer.limit() - 12);

        Map<Integer, Map<UUID, Set<String>>> binaryReferences = newHashMapWithExpectedSize(nGenerations);

        for (int i = 0; i < nGenerations; i++) {
            int generation = buffer.getInt();
            int segmentCount = buffer.getInt();

            Map<UUID, Set<String>> segments = newHashMapWithExpectedSize(segmentCount);

            for (int j = 0; j < segmentCount; j++) {
                long msb = buffer.getLong();
                long lsb = buffer.getLong();
                int referenceCount = buffer.getInt();

                Set<String> references = Sets.newHashSetWithExpectedSize(referenceCount);

                for (int k = 0; k < referenceCount; k++) {
                    int length = buffer.getInt();

                    byte[] data = new byte[length];
                    buffer.get(data);

                    references.add(new String(data, Charsets.UTF_8));
                }

                segments.put(new UUID(msb, lsb), references);
            }

            binaryReferences.put(generation, segments);
        }

        return binaryReferences;
    }

    /**
     * Loads the optional pre-compiled graph entry from the given tar file.
     *
     * @return graph buffer, or {@code null} if one was not found
     * @throws IOException if the tar file could not be read
     */
    private ByteBuffer loadGraph() throws IOException {
        // read the graph metadata just before the tar index entry
        int pos = access.length() - 2 * BLOCK_SIZE - getIndexEntrySize();
        ByteBuffer meta = access.read(pos - 16, 16);
        int crc32 = meta.getInt();
        int count = meta.getInt();
        int bytes = meta.getInt();
        int magic = meta.getInt();

        if (magic != GRAPH_MAGIC) {
            return null; // magic byte mismatch
        }

        if (count < 0 || bytes < count * 16 + 16 || BLOCK_SIZE + bytes > pos) {
            log.warn("Invalid graph metadata in tar file {}", file);
            return null; // impossible uuid and/or byte counts
        }

        // this involves seeking backwards in the file, which might not
        // perform well, but that's OK since we only do this once per file
        ByteBuffer graph = access.read(pos - bytes, bytes);

        byte[] b = new byte[bytes - 16];
        graph.mark();
        graph.get(b);
        graph.reset();

        CRC32 checksum = new CRC32();
        checksum.update(b);
        if (crc32 != (int) checksum.getValue()) {
            log.warn("Invalid graph checksum in tar file {}", file);
            return null; // checksum mismatch
        }

        hasGraph = true;
        return graph;
    }

    private static Map<UUID, List<UUID>> parseGraph(ByteBuffer graphByteBuffer, boolean bulkOnly) {
        int count = graphByteBuffer.getInt(graphByteBuffer.limit() - 12);

        ByteBuffer buffer = graphByteBuffer.duplicate();
        buffer.limit(graphByteBuffer.limit() - 16);

        List<UUID> uuids = newArrayListWithCapacity(count);
        for (int i = 0; i < count; i++) {
            uuids.add(new UUID(buffer.getLong(), buffer.getLong()));
        }

        Map<UUID, List<UUID>> graph = newHashMap();
        while (buffer.hasRemaining()) {
            UUID uuid = uuids.get(buffer.getInt());
            List<UUID> list = newArrayList();
            int refid = buffer.getInt();
            while (refid != -1) {
                UUID ref = uuids.get(refid);
                if (!bulkOnly || !isDataSegmentId(ref.getLeastSignificantBits())) {
                    list.add(ref);
                }
                refid = buffer.getInt();
            }
            graph.put(uuid, list);
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

    File getFile() {
        return file;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return file.toString();
    }

}
