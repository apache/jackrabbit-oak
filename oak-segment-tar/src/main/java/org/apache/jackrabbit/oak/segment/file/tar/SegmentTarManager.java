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
import static org.apache.jackrabbit.oak.segment.file.tar.TarConstants.BLOCK_SIZE;
import static org.apache.jackrabbit.oak.segment.spi.persistence.Buffer.wrap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.jackrabbit.oak.segment.file.tar.index.Index;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentTarManager implements SegmentArchiveManager {

    /**
     * Pattern of the segment entry names. Note the trailing (\\..*)? group
     * that's included for compatibility with possible future extensions.
     */
    private static final Pattern NAME_PATTERN = Pattern.compile(
            "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
                    + "(\\.([0-9a-f]{8}))?(\\..*)?");

    private static final Logger log = LoggerFactory.getLogger(SegmentTarManager.class);

    private final File segmentstoreDir;

    private final FileStoreMonitor fileStoreMonitor;

    private final IOMonitor ioMonitor;

    private final boolean memoryMapping;

    private final boolean offHeapAccess;

    public SegmentTarManager(File segmentstoreDir, FileStoreMonitor fileStoreMonitor, IOMonitor ioMonitor, boolean memoryMapping,
            boolean offHeapAccess) {
        this.segmentstoreDir = segmentstoreDir;
        this.fileStoreMonitor = fileStoreMonitor;
        this.ioMonitor = ioMonitor;
        this.memoryMapping = memoryMapping;
        this.offHeapAccess = offHeapAccess;
    }

    @Override
    public List<String> listArchives() {
        return Arrays.asList(segmentstoreDir.list(new SuffixFileFilter(".tar")));
    }

    @Override
    public SegmentArchiveReader open(String name) throws IOException {
        File file = new File(segmentstoreDir, name);
        RandomAccessFile access = new RandomAccessFile(file, "r");
        try {
            Index index = SegmentTarReader.loadAndValidateIndex(access, name);
            if (index == null) {
                log.info("No index found in tar file {}, skipping...", name);
                return null;
            } else {
                if (memoryMapping) {
                    try {
                        FileAccess mapped = new FileAccess.Mapped(access);
                        return new SegmentTarReader(file, mapped, index, ioMonitor);
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

                FileAccess random = null;
                if (offHeapAccess) {
                    random = new FileAccess.RandomOffHeap(access);
                } else {
                    random = new FileAccess.Random(access);
                }

                // prevent the finally block from closing the file
                // as the returned TarReader will take care of that
                access = null;
                return new SegmentTarReader(file, random, index, ioMonitor);
            }
        } finally {
            if (access != null) {
                access.close();
            }
        }
    }

    @Override
    public SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        return open(archiveName);
    }

    @Override
    public SegmentArchiveWriter create(String archiveName) {
        return new SegmentTarWriter(new File(segmentstoreDir, archiveName), fileStoreMonitor, ioMonitor);
    }

    @Override
    public boolean delete(String archiveName) {
        try {
            return Files.deleteIfExists(new File(segmentstoreDir, archiveName).toPath());
        } catch (IOException e) {
            log.error("Can't remove archive {}", archiveName, e);
            return false;
        }
    }

    @Override
    public boolean renameTo(String from, String to) {
        try {
            Files.move(new File(segmentstoreDir, from).toPath(), new File(segmentstoreDir, to).toPath());
            return true;
        } catch (IOException e) {
            log.error("Can't move archive {} to {}", from, to, e);
            return false;
        }
    }

    @Override
    public void copyFile(String from, String to) throws IOException {
        Files.copy(new File(segmentstoreDir, from).toPath(), new File(segmentstoreDir, to).toPath());
    }

    @Override
    public boolean exists(String archiveName) {
        return new File(segmentstoreDir, archiveName).exists();
    }

    @Override
    public void recoverEntries(String archiveName, LinkedHashMap<UUID, byte[]> entries) throws IOException {
        File file = new File(segmentstoreDir, archiveName);
        RandomAccessFile access = new RandomAccessFile(file, "r");
        try {
            recoverEntries(file, access, entries);
        } finally {
            access.close();
        }
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
            Buffer buffer = wrap(header);
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
                        crc.update(data, 0, data.length);
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

    private static String readString(Buffer buffer, int fieldSize) {
        byte[] b = new byte[fieldSize];
        buffer.get(b);
        int n = 0;
        while (n < fieldSize && b[n] != 0) {
            n++;
        }
        return new String(b, 0, n, UTF_8);
    }

    private static int readNumber(Buffer buffer, int fieldSize) {
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

}
