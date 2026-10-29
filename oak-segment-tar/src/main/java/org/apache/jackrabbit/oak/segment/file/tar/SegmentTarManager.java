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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentTarManager implements SegmentArchiveManager {

    /**
     * Pattern of the segment entry names. Note the trailing (\\..*)? group
     * that's included for compatibility with possible future extensions.
     */
    static final Pattern NAME_PATTERN = Pattern.compile(
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
            FileAccess fileAccess = null;
            if (memoryMapping) {
                try {
                    fileAccess = new FileAccess.Mapped(access);
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

            if (fileAccess == null) {
                if (offHeapAccess) {
                    fileAccess = new FileAccess.RandomOffHeap(access);
                } else {
                    fileAccess = new FileAccess.Random(access);
                }
            }

            // prevent the finally block from closing the file
            // as the returned TarReader will take care of that
            access = null;
            return SegmentTarReader.create(file, fileAccess, ioMonitor);
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
        try (SegmentTarReader.FileIOReadAccess access = new SegmentTarReader.FileIOReadAccess(file)) {
            SegmentTarRecovery.recoverEntries(access, entries);
        }
    }

    @Override
    public void backup(@NotNull String archiveName, @NotNull String backupArchiveName, @NotNull Set<UUID> recoveredEntries) throws IOException {

        if (!renameTo(archiveName, backupArchiveName)) {
            log.warn("Renaming failed, so using copy to backup {}", archiveName);
            copyFile(archiveName, backupArchiveName);
            if (!delete(archiveName)) {
                throw new IOException(
                        "Could not remove broken tar file " + archiveName);
            }
        }
    }
}
