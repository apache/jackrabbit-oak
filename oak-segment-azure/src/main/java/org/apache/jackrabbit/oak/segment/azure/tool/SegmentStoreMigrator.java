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
package org.apache.jackrabbit.oak.segment.azure.tool;

import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.fetchByteArray;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.storeDescription;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;

import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.SegmentStoreType;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.Buffer;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SegmentStoreMigrator {

    private static final Logger log = LoggerFactory.getLogger(SegmentStoreMigrator.class);

    private static final int READ_THREADS = 20;

    private final SegmentNodeStorePersistence source;

    private final SegmentNodeStorePersistence target;

    private final String sourceName;

    private final String targetName;

    private ExecutorService executor = Executors.newFixedThreadPool(READ_THREADS + 1);

    private SegmentStoreMigrator(Builder builder) {
        this.source = builder.source;
        this.target = builder.target;
        this.sourceName = builder.sourceName;
        this.targetName = builder.targetName;
    }

    public void migrate() throws IOException, ExecutionException, InterruptedException {
        migrateJournal();
        migrateGCJournal();
        migrateManifest();
        migrateArchives();
    }

    private void migrateJournal() throws IOException {
        log.info("{}/journal.log -> {}", sourceName, targetName);
        if (!source.getJournalFile().exists()) {
            log.info("No journal at {}; skipping.", sourceName);
            return;
        }
        List<String> journal = new ArrayList<>();
        try (JournalFileReader reader = source.getJournalFile().openJournalReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                journal.add(line);
            }
        }
        Collections.reverse(journal);
        try (JournalFileWriter writer = target.getJournalFile().openJournalWriter()) {
            for (String line : journal) {
                writer.writeLine(line);
            }
        }
    }

    private void migrateGCJournal() throws IOException {
        log.info("{}/gc.log -> {}", sourceName, targetName);
        GCJournalFile targetGCJournal = target.getGCJournalFile();
        for (String line : source.getGCJournalFile().readLines()) {
            targetGCJournal.writeLine(line);
        }
    }

    private void migrateManifest() throws IOException {
        log.info("{}/manifest -> {}", sourceName, targetName);
        if (!source.getManifestFile().exists()) {
            log.info("No manifest at {}; skipping.", sourceName);
            return;
        }
        Properties manifest = source.getManifestFile().load();
        target.getManifestFile().save(manifest);
    }

    private void migrateArchives() throws IOException, ExecutionException, InterruptedException {
        if (!source.segmentFilesExist()) {
            log.info("No segment archives at {}; skipping.", sourceName);
            return;
        }
        SegmentArchiveManager sourceManager = source.createArchiveManager(false, false, new IOMonitorAdapter(),
                new FileStoreMonitorAdapter());
        SegmentArchiveManager targetManager = target.createArchiveManager(false, false, new IOMonitorAdapter(),
                new FileStoreMonitorAdapter());
        for (String archiveName : sourceManager.listArchives()) {
            log.info("{}/{} -> {}", sourceName, archiveName, targetName);
            try (SegmentArchiveReader reader = sourceManager.forceOpen(archiveName)) {
                SegmentArchiveWriter writer = targetManager.create(archiveName);
                try {
                    migrateSegments(reader, writer);
                    migrateBinaryRef(reader, writer);
                    migrateGraph(reader, writer);
                } finally {
                    writer.close();
                }
            }
        }
    }

    private void migrateSegments(SegmentArchiveReader reader, SegmentArchiveWriter writer)
            throws ExecutionException, InterruptedException, IOException {
        List<Future<Segment>> futures = new ArrayList<>();
        for (SegmentArchiveEntry entry : reader.listSegments()) {
            futures.add(executor.submit(() -> {
                Segment segment = new Segment(entry);
                segment.read(reader);
                return segment;
            }));
        }

        for (Future<Segment> future : futures) {
            Segment segment = future.get();
            segment.write(writer);
        }
    }

    private void migrateBinaryRef(SegmentArchiveReader reader, SegmentArchiveWriter writer) throws IOException {
        Buffer binaryReferences = reader.getBinaryReferences();
        if (binaryReferences != null) {
            byte[] array = fetchByteArray(binaryReferences);
            writer.writeBinaryReferences(array);
        }
    }

    private void migrateGraph(SegmentArchiveReader reader, SegmentArchiveWriter writer) throws IOException {
        if (reader.hasGraph()) {
            Buffer graph = reader.getGraph();
            byte[] array = fetchByteArray(graph);
            writer.writeGraph(array);
        }
    }

    private static class Segment {

        private final SegmentArchiveEntry entry;

        private volatile Buffer data;

        private Segment(SegmentArchiveEntry entry) {
            this.entry = entry;
        }

        private void read(SegmentArchiveReader reader) throws IOException {
            data = reader.readSegment(entry.getMsb(), entry.getLsb());
        }

        private void write(SegmentArchiveWriter writer) throws IOException {
            final byte[] array = data.array();
            final int offset = 0;
            writer.writeSegment(entry.getMsb(), entry.getLsb(), array, offset, entry.getLength(), entry.getGeneration(),
                    entry.getFullGeneration(), entry.isCompacted());
        }

        @Override
        public String toString() {
            return new UUID(entry.getMsb(), entry.getLsb()).toString();
        }
    }

    public static class Builder {

        private SegmentNodeStorePersistence source;

        private SegmentNodeStorePersistence target;

        private String sourceName;

        private String targetName;

        public Builder withSource(File dir) {
            this.source = new TarPersistence(dir);
            this.sourceName = storeDescription(SegmentStoreType.TAR, dir.getPath());
            return this;
        }

        public Builder withSource(CloudBlobDirectory dir) throws URISyntaxException, StorageException {
            this.source = new AzurePersistence(dir);
            this.sourceName = storeDescription(SegmentStoreType.AZURE, dir.getContainer().getName() + "/" + dir.getPrefix());
            return this;
        }

        public Builder withSourcePersistence(SegmentNodeStorePersistence source, String sourceName) {
            this.source = source;
            this.sourceName = sourceName;
            return this;
        }

        public Builder withTargetPersistence(SegmentNodeStorePersistence target, String targetName) {
            this.target = target;
            this.targetName = targetName;
            return this;
        }

        public Builder withTarget(File dir) {
            this.target = new TarPersistence(dir);
            this.targetName = storeDescription(SegmentStoreType.TAR, dir.getPath());
            return this;
        }

        public Builder withTarget(CloudBlobDirectory dir) throws URISyntaxException, StorageException {
            this.target = new AzurePersistence(dir);
            this.targetName = storeDescription(SegmentStoreType.AZURE, dir.getContainer().getName() + "/" + dir.getPrefix());
            return this;
        }

        public SegmentStoreMigrator build() {
            return new SegmentStoreMigrator(this);
        }
    }
}