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
package org.apache.jackrabbit.oak.segment.aws.tool;

import static org.apache.jackrabbit.oak.segment.aws.tool.AwsToolUtils.fetchByteArray;
import static org.apache.jackrabbit.oak.segment.aws.tool.AwsToolUtils.storeDescription;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.aws.AwsContext;
import org.apache.jackrabbit.oak.segment.aws.AwsPersistence;
import org.apache.jackrabbit.oak.segment.aws.tool.AwsToolUtils.SegmentStoreType;
import org.apache.jackrabbit.oak.segment.file.tar.TarPersistence;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
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

public class AwsSegmentStoreMigrator implements Closeable  {

    private static final Logger log = LoggerFactory.getLogger(AwsSegmentStoreMigrator.class);

    private static final int READ_THREADS = 20;

    private final SegmentNodeStorePersistence source;

    private final SegmentNodeStorePersistence target;

    private final String sourceName;

    private final String targetName;

    private final boolean appendMode;

    private final Integer revisionCount;

    private ExecutorService executor = Executors.newFixedThreadPool(READ_THREADS + 1);

    private AwsSegmentStoreMigrator(Builder builder) {
        this.source = builder.source;
        this.target = builder.target;
        this.sourceName = builder.sourceName;
        this.targetName = builder.targetName;
        this.appendMode = builder.appendMode;
        this.revisionCount = builder.revisionCount;
    }

    public void migrate() throws IOException, ExecutionException, InterruptedException {
        runWithRetry(() -> migrateJournal(), 16, 5);
        runWithRetry(() -> migrateGCJournal(), 16, 5);
        runWithRetry(() -> migrateManifest(), 16, 5);
        migrateArchives();
    }

    private Void migrateJournal() throws IOException {
        log.info("{}/journal.log -> {}", sourceName, targetName);
        if (!source.getJournalFile().exists()) {
            log.info("No journal at {}; skipping.", sourceName);
            return null;
        }
        List<String> journal = new ArrayList<>();

        try (JournalFileReader reader = source.getJournalFile().openJournalReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.length() > 0 && !line.trim().equals("")) {
                    journal.add(line);
                }
                if (journal.size() == revisionCount) {
                    break;
                }
            }
        }
        Collections.reverse(journal);

        try (JournalFileWriter writer = target.getJournalFile().openJournalWriter()) {
            writer.truncate();
            for (String line : journal) {
                writer.writeLine(line);
            }
        }
        return null;
    }

    private Void migrateGCJournal() throws IOException {
        log.info("{}/gc.log -> {}", sourceName, targetName);
        GCJournalFile targetGCJournal = target.getGCJournalFile();
        if (appendMode) {
            targetGCJournal.truncate();
        }
        List<String> lines = source.getGCJournalFile().readLines();
        if (lines.size() > 0) {
            targetGCJournal.writeLine(lines.get(lines.size() - 1));
        }
        return null;
    }

    private Void migrateManifest() throws IOException {
        log.info("{}/manifest -> {}", sourceName, targetName);
        if (!source.getManifestFile().exists()) {
            log.info("No manifest at {}; skipping.", sourceName);
            return null;
        }
        Properties manifest = source.getManifestFile().load();
        target.getManifestFile().save(manifest);
        return null;
    }

    private void migrateArchives() throws IOException, ExecutionException, InterruptedException {
        if (!source.segmentFilesExist()) {
            log.info("No segment archives at {}; skipping.", sourceName);
            return;
        }
        SegmentArchiveManager sourceManager = source.createArchiveManager(false, false, new IOMonitorAdapter(),
                new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveManager targetManager = target.createArchiveManager(false, false, new IOMonitorAdapter(),
                new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        List<String> targetArchives = targetManager.listArchives();

        if (appendMode && !targetArchives.isEmpty()) {
            //last archive can be updated since last copy and needs to be recopied
            String lastArchive = targetArchives.get(targetArchives.size() - 1);
            targetArchives.remove(lastArchive);
        }

        for (String archiveName : sourceManager.listArchives()) {
            log.info("{}/{} -> {}", sourceName, archiveName, targetName);
            if (appendMode && targetArchives.contains(archiveName)) {
                log.info("Already exists, skipping.");
                continue;
            }
            try (SegmentArchiveReader reader = sourceManager.forceOpen(archiveName)) {
                SegmentArchiveWriter writer = targetManager.create(archiveName);
                try {
                    migrateSegments(reader, writer);
                    migrateBinaryRef(reader, writer);
                    migrateGraph(reader, writer);
                } catch (Exception e) {
                    log.error("Can't write archive", e);
                    throw e;
                } finally {
                    writer.close();
                }
            }
        }
    }

    private void migrateSegments(SegmentArchiveReader reader, SegmentArchiveWriter writer) throws ExecutionException, InterruptedException, IOException {
        List<Future<Segment>> futures = new ArrayList<>();
        for (SegmentArchiveEntry entry : reader.listSegments()) {
            futures.add(executor.submit(() -> runWithRetry(() -> {
                Segment segment = new Segment(entry);
                segment.read(reader);
                return segment;
            }, 16, 5)));
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

    private static <T> T runWithRetry(Producer<T> producer, int maxAttempts, int intervalSec) throws IOException {
        IOException ioException = null;
        RepositoryNotReachableException repoNotReachableException = null;
        for (int i = 0; i < maxAttempts; i++) {
            try {
                return producer.produce();
            } catch (IOException e) {
                log.error("Can't execute the operation. Retrying (attempt {})", i, e);
                ioException = e;
            } catch (RepositoryNotReachableException e) {
                log.error("Can't execute the operation. Retrying (attempt {})", i, e);
                repoNotReachableException = e;
            }
            try {
                Thread.sleep(intervalSec * 1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }
        if (ioException != null) {
            throw ioException;
        } else if (repoNotReachableException != null) {
            throw repoNotReachableException;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
        try {
            while (!executor.awaitTermination(100, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    @FunctionalInterface
    private interface Producer<T> {
        T produce() throws IOException;
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

        private boolean appendMode;

        private Integer revisionCount = Integer.MAX_VALUE;

        public Builder withSource(File dir) {
            this.source = new TarPersistence(dir);
            this.sourceName = storeDescription(SegmentStoreType.TAR, dir.getPath());
            return this;
        }

        public Builder withSource(AwsContext awsContext) {
            this.source = new AwsPersistence(awsContext);
            this.sourceName = storeDescription(SegmentStoreType.AWS, awsContext.getConfig());
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

        public Builder withTarget(AwsContext awsContext) {
            this.target = new AwsPersistence(awsContext);
            this.targetName = storeDescription(SegmentStoreType.AWS, awsContext.getConfig());
            return this;
        }

        public Builder setAppendMode() {
            this.appendMode = true;
            return this;
        }

        public Builder withRevisionCount(Integer revisionCount) {
            this.revisionCount = revisionCount;
            return this;
        }

        public AwsSegmentStoreMigrator build() {
            return new AwsSegmentStoreMigrator(this);
        }
    }
}