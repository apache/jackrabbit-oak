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
package org.apache.jackrabbit.oak.segment.split;

import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class SplitPersistence implements SegmentNodeStorePersistence {

    private final SegmentNodeStorePersistence roPersistence;

    private final SegmentNodeStorePersistence rwPersistence;

    private final Optional<String> lastRoArchive;

    public SplitPersistence(SegmentNodeStorePersistence roPersistence, SegmentNodeStorePersistence rwPersistence) throws IOException {
        this.roPersistence = roPersistence;
        this.rwPersistence = rwPersistence;

        ManifestFile manifest = rwPersistence.getManifestFile();
        if (!manifest.exists()) {
            initialize();
        }
        Properties properties = manifest.load();
        lastRoArchive = Optional.ofNullable(properties.getProperty("split.lastRoArchive"));
    }

    private void initialize() throws IOException {
        Properties properties = roPersistence.getManifestFile().load();
        properties.setProperty("split.initialized", "true");
        Optional<String> lastArchive = getLastArchive();
        lastArchive.ifPresent(a -> properties.setProperty("split.lastRoArchive", a));
        rwPersistence.getManifestFile().save(properties);

        GCJournalFile gcJournalFile = rwPersistence.getGCJournalFile();
        for (String line : roPersistence.getGCJournalFile().readLines()) {
            gcJournalFile.writeLine(line);
        }

        List<String> journalLines = new ArrayList<>();
        try (JournalFileReader journalFileReader = roPersistence.getJournalFile().openJournalReader()) {
            String journalLine;
            while ((journalLine = journalFileReader.readLine()) != null) {
                journalLines.add(journalLine);
            }
        }

        Collections.reverse(journalLines);

        try (JournalFileWriter journalFileWriter = rwPersistence.getJournalFile().openJournalWriter()) {
            for (String line : journalLines) {
                journalFileWriter.writeLine(line);
            }
        }
    }

    private Optional<String> getLastArchive() throws IOException {
        SegmentArchiveManager manager = roPersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter());
        List<String> archives = manager.listArchives();
        if (archives.isEmpty()) {
            return Optional.empty();
        } else {
            Collections.sort(archives);
            return Optional.of(archives.get(archives.size() - 1));
        }
    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean memoryMapping, boolean offHeapAccess, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor) throws IOException {
        if (lastRoArchive.isPresent()) {
            return new SplitSegmentArchiveManager(
                    roPersistence.createArchiveManager(memoryMapping, offHeapAccess, ioMonitor, fileStoreMonitor),
                    rwPersistence.createArchiveManager(memoryMapping, offHeapAccess, ioMonitor, fileStoreMonitor),
                    lastRoArchive.get());
        } else {
            return rwPersistence.createArchiveManager(memoryMapping, offHeapAccess, ioMonitor, fileStoreMonitor);
        }
    }

    @Override
    public boolean segmentFilesExist() {
        return lastRoArchive.isPresent() || rwPersistence.segmentFilesExist();
    }

    @Override
    public JournalFile getJournalFile() {
        return rwPersistence.getJournalFile();
    }

    @Override
    public GCJournalFile getGCJournalFile() throws IOException {
        return rwPersistence.getGCJournalFile();
    }

    @Override
    public ManifestFile getManifestFile() throws IOException {
        return rwPersistence.getManifestFile();
    }

    @Override
    public RepositoryLock lockRepository() throws IOException {
        return rwPersistence.lockRepository();
    }

}
