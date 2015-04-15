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
package org.apache.jackrabbit.oak.plugins.segment.file;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.REF_COUNT_OFFSET;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentId.isDataSegmentId;
import static org.apache.jackrabbit.oak.plugins.segment.file.TarWriter.GRAPH_MAGIC;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.segment.CompactionMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TarReader {

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
            log.info("Recovering segments from tar file {}", file);
            try {
                RandomAccessFile access = new RandomAccessFile(file, "r");
                try {
                    recoverEntries(file, access, entries);
                } finally {
                    access.close();
                }
            } catch (IOException e) {
                log.warn("Could not read tar file " + file + ", skipping...", e);
            }

            backupSafely(file);
        }

        // regenerate the first generation based on the recovered data
        File file = sorted.values().iterator().next();
        log.info("Regenerating tar file " + file);
        TarWriter writer = new TarWriter(file);
        for (Map.Entry<UUID, byte[]> entry : entries.entrySet()) {
            UUID uuid = entry.getKey();
            byte[] data = entry.getValue();
            writer.writeEntry(
                    uuid.getMostSignificantBits(),
                    uuid.getLeastSignificantBits(),
                    data, 0, data.length);
        }
        writer.close();

        reader = openFirstFileWithValidIndex(singletonList(file), memoryMapping);
        if (reader != null) {
            return reader;
        } else {
            throw new IOException("Failed to open recovered tar file " + file);
        }
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
        File parent = file.getParentFile();
        String name = file.getName();

        File backup = new File(parent, name + ".bak");
        for (int i = 2; backup.exists(); i++) {
            backup = new File(parent, name + "." + i + ".bak");
        }

        log.info("Backing up " + file + " to " + backup.getName());
        if (!file.renameTo(backup)) {
            log.warn("Renaming failed, so using copy to backup {}", file);
            FileUtils.copyFile(file, backup);
            if (!file.delete()) {
                throw new IOException(
                        "Could not remove broken tar file " + file);
            }
        }
    }

    private static TarReader openFirstFileWithValidIndex(
            List<File> files, boolean memoryMapping) throws IOException {
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
                                log.warn("Failed to mmap tar file " + name
                                                 + ". Falling back to normal file IO,"
                                                 + " which will negatively impact"
                                                 + " repository performance. This"
                                                 + " problem may have been caused by"
                                                 + " restrictions on the amount of"
                                                 + " virtual memory available to the"
                                                 + " JVM. Please make sure that a"
                                                 + " 64-bit JVM is being used and"
                                                 + " that the process has access to"
                                                 + " unlimited virtual memory"
                                                 + " (ulimit option -v).",
                                         e);
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
                log.warn("Could not read tar file " + name + ", skipping...", e);
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

        if (count < 1 || bytes < count * 24 + 16 || bytes % BLOCK_SIZE != 0) {
            log.warn("Invalid index metadata in tar file {}", name);
            return null; // impossible entry and/or byte counts
        }

        // this involves seeking backwards in the file, which might not
        // perform well, but that's OK since we only do this once per file
        ByteBuffer index = ByteBuffer.allocate(count * 24);
        file.seek(length - 2 * BLOCK_SIZE - 16 - count * 24);
        file.readFully(index.array());
        index.mark();

        CRC32 checksum = new CRC32();
        long limit = length - 2 * BLOCK_SIZE - bytes - BLOCK_SIZE;
        long lastmsb = Long.MIN_VALUE;
        long lastlsb = Long.MIN_VALUE;
        byte[] entry = new byte[24];
        for (int i = 0; i < count; i++) {
            index.get(entry);
            checksum.update(entry);

            ByteBuffer buffer = ByteBuffer.wrap(entry);
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
                    continue;
                }
            }

            // The header checksum passes, so read the entry name and size
            ByteBuffer buffer = ByteBuffer.wrap(header);
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
        return index.capacity() / 24;
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
            position += 24;
        }
    }

    Set<UUID> getUUIDs() {
        Set<UUID> uuids = newHashSetWithExpectedSize(index.remaining() / 24);
        int position = index.position();
        while (position < index.limit()) {
            uuids.add(new UUID(
                    index.getLong(position),
                    index.getLong(position + 8)));
            position += 24;
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
        int highIndex = index.remaining() / 24 - 1;
        float lowValue = Long.MIN_VALUE;
        float highValue = Long.MAX_VALUE;
        float targetValue = msb;

        while (lowIndex <= highIndex) {
            int guessIndex = lowIndex + Math.round(
                    (highIndex - lowIndex)
                    * (targetValue - lowValue)
                    / (highValue - lowValue));
            int position = index.position() + guessIndex * 24;
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

    /**
     * Garbage collects segments in this file. First it collects the set of
     * segments that are referenced / reachable, then (if more than 25% is
     * garbage) creates a new generation of the file.
     * <p>
     * The old generation files are not removed (they can't easily be removed,
     * for memory mapped files).
     * 
     * @param referencedIds the referenced segment ids (input and output).
     * @return this (if the file is kept as is), or the new generation file, or
     *         null if the file is fully garbage
     */
    synchronized TarReader cleanup(Set<UUID> referencedIds, CompactionMap cm) throws IOException {
        Set<UUID> cleaned = newHashSet();
        Map<UUID, List<UUID>> graph = getGraph();

        TarEntry[] sorted = new TarEntry[index.remaining() / 24];
        int position = index.position();
        for (int i = 0; position < index.limit(); i++) {
            sorted[i]  = new TarEntry(
                    index.getLong(position),
                    index.getLong(position + 8),
                    index.getInt(position + 16),
                    index.getInt(position + 20));
            position += 24;
        }
        Arrays.sort(sorted, TarEntry.OFFSET_ORDER);

        int size = 0;
        int count = 0;
        for (int i = sorted.length - 1; i >= 0; i--) {
            TarEntry entry = sorted[i];
            UUID id = new UUID(entry.msb(), entry.lsb());
            if (!referencedIds.remove(id)) {
                // this segment is not referenced anywhere
                cleaned.add(id);
                sorted[i] = null;
            } else {
                if (isDataSegmentId(entry.lsb())) {
                    size += getEntrySize(entry.size());
                    count += 1;

                    // this is a referenced data segment, so follow the graph
                    if (graph != null) {
                        List<UUID> refids = graph.get(id);
                        if (refids != null) {
                            for (UUID r : refids) {
                                if (isDataSegmentId(r.getLeastSignificantBits())) {
                                    referencedIds.add(r);
                                } else {
                                    if (cm != null && cm.wasCompacted(id)) {
                                        // skip bulk compacted segment
                                        // references
                                    } else {
                                        referencedIds.add(r);
                                    }
                                }
                            }
                        }
                    } else {
                        // a pre-compiled graph is not available, so read the
                        // references directly from this segment
                        ByteBuffer segment = access.read(
                                entry.offset(),
                                Math.min(entry.size(), 16 * 256));
                        int pos = segment.position();
                        int refcount = segment.get(pos + REF_COUNT_OFFSET) & 0xff;
                        int refend = pos + 16 * (refcount + 1);
                        for (int refpos = pos + 16; refpos < refend; refpos += 16) {
                            UUID r = new UUID(segment.getLong(refpos),
                                    segment.getLong(refpos + 8));
                            if (isDataSegmentId(r.getLeastSignificantBits())) {
                                referencedIds.add(r);
                            } else {
                                if (cm != null && cm.wasCompacted(id)) {
                                    // skip bulk compacted segment references
                                } else {
                                    referencedIds.add(r);
                                }
                            }
                        }
                    }
                } else {
                    // bulk segments compaction check
                    if (cm != null && cm.wasCompacted(id)) {
                        cleaned.add(id);
                        sorted[i] = null;
                    } else {
                        size += getEntrySize(entry.size());
                        count += 1;
                    }
                }
            }
        }
        size += getEntrySize(24 * count + 16);
        size += 2 * BLOCK_SIZE;

        if (count == 0) {
            // none of the entries within this tar file are referenceable
            logCleanedSegments(cleaned);
            return null;
        } else if (size >= access.length() * 3 / 4 && graph != null) {
            // the space savings are not worth it at less than 25%,
            // unless this tar file lacks a pre-compiled segment graph
            // in which case we'll always generate a new tar file with
            // the graph to speed up future garbage collection runs.
            return this;
        }

        String name = file.getName();
        int pos = name.length() - "a.tar".length();
        char generation = name.charAt(pos);
        if (generation == 'z') {
            // no garbage collection after reaching generation z
            return this;
        }

        File newFile = new File(
                file.getParentFile(),
                name.substring(0, pos) + (char) (generation + 1) + ".tar");
        TarWriter writer = new TarWriter(newFile);
        for (int i = 0; i < sorted.length; i++) {
            TarEntry entry = sorted[i];
            if (entry != null) {
                byte[] data = new byte[entry.size()];
                access.read(entry.offset(), entry.size()).get(data);
                writer.writeEntry(
                        entry.msb(), entry.lsb(), data, 0, entry.size());
            }
        }
        writer.close();

        TarReader reader = openFirstFileWithValidIndex(
                singletonList(newFile), access.isMemoryMapped());
        if (reader != null) {
            logCleanedSegments(cleaned);
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

        GC_LOG.info("Cleaned segments from {}: {}", file.getName(), uuids);
    }

    File close() throws IOException {
        access.close();
        return file;
    }

    //-----------------------------------------------------------< private >--

    /**
     * Loads and parses the optional pre-compiled graph entry from the given tar
     * file.
     *
     * @return the parsed graph, or {@code null} if one was not found
     * @throws IOException if the tar file could not be read
     */
    Map<UUID, List<UUID>> getGraph() throws IOException {
        ByteBuffer graph = loadGraph();
        if (graph == null) {
            return null;
        } else {
            return parseGraph(graph);
        }
    }

    /**
     * Loads the optional pre-compiled graph entry from the given tar file.
     *
     * @return graph buffer, or {@code null} if one was not found
     * @throws IOException if the tar file could not be read
     */
    private ByteBuffer loadGraph() throws IOException {
        // read the graph metadata just before the tar index entry
        int pos = access.length() - 2 * BLOCK_SIZE - getEntrySize(index.remaining());
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

        return graph;
    }

    private static Map<UUID, List<UUID>> parseGraph(ByteBuffer graphByteBuffer) {
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
                list.add(uuids.get(refid));
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
