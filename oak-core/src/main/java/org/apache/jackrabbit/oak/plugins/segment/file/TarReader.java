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
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSetWithExpectedSize;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.REF_COUNT_OFFSET;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentId.isDataSegmentId;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TarReader {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(TarReader.class);

    /** Magic byte sequence at the end of the index block. */
    private static final int INDEX_MAGIC = TarWriter.INDEX_MAGIC;

    /** Pattern of the segment entry names */
    private static final Pattern NAME_PATTERN = Pattern.compile(
            "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
            + "(\\.([0-9a-f]{8}))?");

    /** The tar file block size. */
    private static final int BLOCK_SIZE = TarWriter.BLOCK_SIZE;

    private static final int getEntrySize(int size) {
        return BLOCK_SIZE + size + TarWriter.getPaddingSize(size);
    }

    static TarReader open(Map<Character, File> files, boolean memoryMapping)
            throws IOException {
        Character[] generations =
                files.keySet().toArray(new Character[files.size()]);
        Arrays.sort(generations);
        for (int i = generations.length - 1; i >= 0; i--) {
            File file = files.get(generations[i]);
            try {
                TarReader reader = new TarReader(file, memoryMapping);
                if (reader.index != null) {
                    // found a generation with a valid index, drop the others
                    for (File other : files.values()) {
                        if (other != file) {
                            log.info("Removing unused tar file {}", other);
                            other.delete();
                        }
                    }
                    return reader;
                } else {
                    reader.close();
                }
            } catch (IOException e) {
                log.warn("Failed to access tar file " + file, e);
            }
        }

        // no generation has a valid index, so recover as much as we can
        LinkedHashMap<UUID, byte[]> entries = newLinkedHashMap();
        for (File file : files.values()) {
            try {
                FileAccess access = FileAccess.open(file, memoryMapping);
                try {
                    recoverEntries(file, access, entries);
                } finally {
                    access.close();
                }
            } catch (IOException e) {
                log.warn("Failed to access tar file " + file, e);
            }
        }

        // regenerate the first generation based on the recovered data
        File file = files.get(generations[0]);
        File backup = new File(file.getParentFile(), file.getName() + ".bak");
        if (backup.exists()) {
            log.info("Removing old backup file " + backup);
            backup.delete();
        }
        if (!file.renameTo(backup)) {
            throw new IOException("Could not backup tar file " + file);
        }

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

        log.info("Tar file regenerated, removing backup file " + backup);
        backup.delete();
        for (File other : files.values()) {
            if (other != file) {
                log.info("Removing unused tar file {}", other);
                other.delete();
            }
        }

        return new TarReader(file, memoryMapping);
    }

    /**
     * Scans through the tar file, looking for all segment entries.
     *
     * @return map of all segment entries in this tar file
     * @throws IOException if the tar file could not be read
     */
    private static void recoverEntries(
            File file, FileAccess access, LinkedHashMap<UUID, byte[]> entries)
            throws IOException {
        int position = 0;
        int length = access.length();
        while (position + TarWriter.BLOCK_SIZE <= length) {
            // read the tar header block
            ByteBuffer header = access.read(position, BLOCK_SIZE);
            int pos = header.position();
            String name = readString(header, 100);
            header.position(pos + 124);
            int size = readNumber(header, 12);

            if (name.isEmpty() && size == 0) {
                return; // no more entries in this file
            } else if (position + BLOCK_SIZE + size > length) {
                log.warn("Invalid entry {} in tar file {}", name, file);
                return; // invalid entry, stop here
            }

            Matcher matcher = NAME_PATTERN.matcher(name);
            if (matcher.matches()) {
                UUID id = UUID.fromString(matcher.group(1));

                String checksum = matcher.group(3);
                if (checksum == null && entries.containsKey(id)) {
                    // entry already loaded, so skip
                } else {
                    byte[] data = new byte[size];
                    access.read(position + BLOCK_SIZE, size).get(data);

                    if (checksum == null) {
                        entries.put(id, data);
                    } else {
                        CRC32 crc = new CRC32();
                        crc.update(data);
                        if (crc.getValue() == Long.parseLong(checksum, 16)) {
                            entries.put(id, data);
                        } else {
                            log.warn("Checksum mismatch in entry {} of tar file {}", name, file);
                        }
                    }
                }
            } else if (!name.equals(file.getName() + ".idx")) {
                log.warn("Ignoring unexpected entry {} in tar file {}",
                        name, file);
            }

            position += getEntrySize(size);
        }
    }

    private final File file;

    private final FileAccess access;

    private final ByteBuffer index;

    TarReader(File file, boolean memoryMapping) throws IOException {
        this.file = file;
        this.access = FileAccess.open(file, memoryMapping);

        ByteBuffer index = null;
        try {
            index = loadAndValidateIndex();
        } catch (IOException e) {
            log.warn("Unable to access tar file " + file, e);
        }
        this.index = index;
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

    synchronized TarReader cleanup(Set<UUID> referencedIds) throws IOException {
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
                sorted[i] = null;
            } else {
                size += getEntrySize(entry.size());
                count += 1;

                if (isDataSegmentId(entry.lsb())) {
                    // this is a referenced data segment, so follow the graph
                    ByteBuffer segment = access.read(
                            entry.offset(),
                            Math.min(entry.size(), 16 * 256));
                    int pos = segment.position();
                    int refcount = segment.get(pos + REF_COUNT_OFFSET) & 0xff;
                    int refend = pos + 16 * (refcount + 1);
                    for (int refpos = pos + 16; refpos < refend; refpos += 16) {
                        referencedIds.add(new UUID(
                                segment.getLong(refpos),
                                segment.getLong(refpos + 8)));
                    }
                }
            }
        }
        size += getEntrySize(24 * count + 16);
        size += 2 * BLOCK_SIZE;

        if (count == 0) {
            // none of the entries within this tar file are referenceable
            return null;
        } else if (size >= access.length() * 3 / 4) {
            // the space savings are not worth it at less than 25%
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

        return new TarReader(newFile, access.isMemoryMapped());
    }

    File close() throws IOException {
        access.close();
        return file;
    }

    //-----------------------------------------------------------< private >--

    /**
     * Tries to read an existing index from the tar file. The index is
     * returned if it is found and looks valid (correct checksum, passes
     * sanity checks).
     *
     * @return tar index, or {@code null} if not found or not valid
     * @throws IOException if the tar file could not be read
     */
    private ByteBuffer loadAndValidateIndex() throws IOException {
        long length = file.length();
        if (length % BLOCK_SIZE != 0
                || length < 6 * BLOCK_SIZE
                || length > Integer.MAX_VALUE) {
            log.warn("Unexpected size {} of tar file {}", length, file);
            return null; // unexpected file size
        }

        // read the index metadata just before the two final zero blocks
        ByteBuffer meta = access.read((int) (length - 2 * BLOCK_SIZE - 16), 16);
        int crc32 = meta.getInt();
        int count = meta.getInt();
        int bytes = meta.getInt();
        int magic = meta.getInt();

        if (magic != INDEX_MAGIC) {
            log.warn("No index found in tar file {}", file);
            return null; // magic byte mismatch
        }

        if (count < 1 || bytes < count * 24 + 16 || bytes % BLOCK_SIZE != 0) {
            log.warn("Invalid index metadata in tar file {}", file);
            return null; // impossible entry and/or byte counts
        }

        ByteBuffer index = access.read(
                (int) (length - 2 * BLOCK_SIZE - 16 - count * 24),
                count * 24);
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
                log.warn("Incorrect index ordering in tar file {}", file);
                return null;
            } else if (lastmsb == msb && lastlsb == lsb && i > 0) {
                log.warn("Duplicate index entry in tar file {}", file);
                return null;
            } else if (offset < 0 || offset % BLOCK_SIZE != 0) {
                log.warn("Invalid index entry offset in tar file {}", file);
                return null;
            } else if (size < 1 || offset + size > limit) {
                log.warn("Invalid index entry size in tar file {}", file);
                return null;
            }

            lastmsb = msb;
            lastlsb = lsb;
        }

        if (crc32 != (int) checksum.getValue()) {
            log.warn("Invalid index checksum in tar file {}", file);
            return null; // checksum mismatch
        }

        index.reset();
        return index;
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

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return file.toString();
    }

}
