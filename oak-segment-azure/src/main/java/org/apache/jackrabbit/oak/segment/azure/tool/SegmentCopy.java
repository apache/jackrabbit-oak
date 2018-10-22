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
package org.apache.jackrabbit.oak.segment.azure.tool;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.fetchByteArray;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newSegmentNodeStorePersistence;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.printMessage;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.printableStopwatch;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.storeDescription;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.storeTypeFromPathOrUri;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.SegmentStoreType;
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
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.tool.Check;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * Perform a full-copy of repository data at segment level.
 */
public class SegmentCopy {
    /**
     * Create a builder for the {@link SegmentCopy} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link SegmentCopy} command.
     */
    public static class Builder {

        private String source;

        private String destination;

        private SegmentNodeStorePersistence srcPersistence;

        private SegmentNodeStorePersistence destPersistence;

        private boolean verbose;

        private PrintWriter outWriter;

        private PrintWriter errWriter;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The source path/URI to an existing segment store. This parameter is required.
         *
         * @param source
         *            the source path/URI to an existing segment store.
         * @return this builder.
         */
        public Builder withSource(String source) {
            this.source = checkNotNull(source);
            return this;
        }

        /**
         * The destination path/URI to an existing segment store. This parameter is
         * required.
         *
         * @param destination
         *            the destination path/URI to an existing segment store.
         * @return this builder.
         */
        public Builder withDestination(String destination) {
            this.destination = checkNotNull(destination);
            return this;
        }

        /**
         * The destination {@link SegmentNodeStorePersistence}.
         *
         * @param srcPersistence
         *            the destination {@link SegmentNodeStorePersistence}.
         * @return this builder.
         */
        public Builder withSrcPersistencee(SegmentNodeStorePersistence srcPersistence) {
            this.srcPersistence = checkNotNull(srcPersistence);
            return this;
        }

        /**
         * The destination {@link SegmentNodeStorePersistence}.
         *
         * @param destPersistence
         *            the destination {@link SegmentNodeStorePersistence}.
         * @return this builder.
         */
        public Builder withDestPersistence(SegmentNodeStorePersistence destPersistence) {
            this.destPersistence = checkNotNull(destPersistence);
            return this;
        }

        /**
         * The text output stream writer used to print normal output.
         *
         * @param outWriter
         *            the output writer.
         * @return this builder.
         */
        public Builder withOutWriter(PrintWriter outWriter) {
            this.outWriter = outWriter;
            return this;
        }

        /**
         * The text error stream writer used to print erroneous output.
         *
         * @param errWriter
         *            the error writer.
         * @return this builder.
         */
        public Builder withErrWriter(PrintWriter errWriter) {
            this.errWriter = errWriter;
            return this;
        }

        /**
         * Whether to show detailed output about current copy operation or not.
         *
         * @param verbose,
         *            <code>true</code> to print detailed output, <code>false</code>
         *            otherwise.
         * @return this builder.
         */
        public Builder withVerbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        /**
         * Create an executable version of the {@link Check} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public SegmentCopy build() {
            if (srcPersistence == null && destPersistence == null) {
                checkNotNull(source);
                checkNotNull(destination);
            }
            return new SegmentCopy(this);
        }
    }

    private final String source;

    private final String destination;

    private final boolean verbose;

    private final PrintWriter outWriter;

    private final PrintWriter errWriter;

    private SegmentNodeStorePersistence srcPersistence;

    private SegmentNodeStorePersistence destPersistence;

    public SegmentCopy(Builder builder) {
        this.source = builder.source;
        this.destination = builder.destination;
        this.srcPersistence = builder.srcPersistence;
        this.destPersistence = builder.destPersistence;
        this.verbose = builder.verbose;
        this.outWriter = builder.outWriter;
        this.errWriter = builder.errWriter;
    }

    public int run() {
        Stopwatch watch = Stopwatch.createStarted();
        RepositoryLock srcRepositoryLock = null;

        SegmentStoreType srcType = storeTypeFromPathOrUri(source);
        SegmentStoreType destType = storeTypeFromPathOrUri(destination);

        try {
            if (srcPersistence == null || destPersistence == null) {
                srcPersistence = newSegmentNodeStorePersistence(srcType, source);
                destPersistence = newSegmentNodeStorePersistence(destType, destination);
            }

            printMessage(outWriter, "Started segment-copy transfer!");
            printMessage(outWriter, "Source: {0}", storeDescription(srcType, source));
            printMessage(outWriter, "Destination: {0}", storeDescription(destType, destination));

            try {
                srcPersistence.lockRepository();
            } catch (Exception e) {
                throw new Exception(MessageFormat.format(
                        "Cannot lock source segment store {0} for starting copying process. Giving up!",
                        storeDescription(srcType, source)));
            }

            printMessage(outWriter, "Copying archives...");
            // TODO: copy only segments not transfered
            IOMonitor ioMonitor = new IOMonitorAdapter();
            FileStoreMonitor fileStoreMonitor = new FileStoreMonitorAdapter();

            SegmentArchiveManager srcArchiveManager = srcPersistence.createArchiveManager(false, false, ioMonitor,
                    fileStoreMonitor);
            SegmentArchiveManager destArchiveManager = destPersistence.createArchiveManager(false, false, ioMonitor,
                    fileStoreMonitor);
            copyArchives(srcArchiveManager, destArchiveManager);

            printMessage(outWriter, "Copying journal...");
            // TODO: delete destination journal file if present
            JournalFile srcJournal = srcPersistence.getJournalFile();
            JournalFile destJournal = destPersistence.getJournalFile();
            copyJournal(srcJournal, destJournal);

            printMessage(outWriter, "Copying gc journal...");
            // TODO: delete destination gc journal file if present
            GCJournalFile srcGcJournal = srcPersistence.getGCJournalFile();
            GCJournalFile destGcJournal = destPersistence.getGCJournalFile();
            for (String line : srcGcJournal.readLines()) {
                destGcJournal.writeLine(line);
            }

            printMessage(outWriter, "Copying manifest...");
            // TODO: delete destination manifest file if present
            ManifestFile srcManifest = srcPersistence.getManifestFile();
            ManifestFile destManifest = destPersistence.getManifestFile();
            Properties properties = srcManifest.load();
            destManifest.save(properties);
        } catch (Exception e) {
            watch.stop();
            printMessage(errWriter, "A problem occured while copying archives from {0} to {1} ", source, destination);
            e.printStackTrace(errWriter);
            return 1;
        } finally {
            if (srcRepositoryLock != null) {
                try {
                    srcRepositoryLock.unlock();
                } catch (IOException e) {
                    printMessage(errWriter, "A problem occured while unlocking source repository {0} ",
                            storeDescription(srcType, source));
                    e.printStackTrace(errWriter);
                }
            }
        }

        watch.stop();
        printMessage(outWriter, "Segment-copy succeeded in {0}", printableStopwatch(watch));

        return 0;
    }

    private void copyArchives(SegmentArchiveManager srcArchiveManager, SegmentArchiveManager destArchiveManager)
            throws IOException {
        List<String> srcArchiveNames = srcArchiveManager.listArchives();
        Collections.sort(srcArchiveNames);
        int archiveCount = srcArchiveNames.size();
        int crtCount = 0;

        for (String archiveName : srcArchiveNames) {
            crtCount++;
            printMessage(outWriter, "{0} - {1}/{2}", archiveName, crtCount, archiveCount);
            if (verbose) {
                printMessage(outWriter, "    |");
            }

            SegmentArchiveWriter archiveWriter = destArchiveManager.create(archiveName);
            SegmentArchiveReader archiveReader = srcArchiveManager.open(archiveName);
            List<SegmentArchiveEntry> segmentEntries = archiveReader.listSegments();
            for (SegmentArchiveEntry segmentEntry : segmentEntries) {
                writeSegment(segmentEntry, archiveReader, archiveWriter);
            }

            ByteBuffer binRefBuffer = archiveReader.getBinaryReferences();
            byte[] binRefData = fetchByteArray(binRefBuffer);

            archiveWriter.writeBinaryReferences(binRefData);

            ByteBuffer graphBuffer = archiveReader.getGraph();
            byte[] graphData = fetchByteArray(graphBuffer);

            archiveWriter.writeGraph(graphData);
            archiveWriter.close();
        }
    }

    private void writeSegment(SegmentArchiveEntry segmentEntry, SegmentArchiveReader archiveReader,
            SegmentArchiveWriter archiveWriter) throws IOException {
        long msb = segmentEntry.getMsb();
        long lsb = segmentEntry.getLsb();
        if (verbose) {
            printMessage(outWriter, "    - {0}", new UUID(msb, lsb));
        }

        int size = segmentEntry.getLength();
        int offset = 0;
        int generation = segmentEntry.getGeneration();
        int fullGeneration = segmentEntry.getFullGeneration();
        boolean isCompacted = segmentEntry.isCompacted();

        ByteBuffer byteBuffer = archiveReader.readSegment(msb, lsb);
        byte[] data = fetchByteArray(byteBuffer);

        archiveWriter.writeSegment(msb, lsb, data, offset, size, generation, fullGeneration, isCompacted);
        archiveWriter.flush();
    }

    private void copyJournal(JournalFile srcJournal, JournalFile destJournal) throws IOException {
        try (JournalFileReader srcJournalReader = srcJournal.openJournalReader();
                JournalFileWriter destJournalWriter = destJournal.openJournalWriter()) {

            Deque<String> linesStack = new ArrayDeque<>();
            String line = null;
            while ((line = srcJournalReader.readLine()) != null) {
                linesStack.push(line);
            }

            while (!linesStack.isEmpty()) {
                line = linesStack.pop();
                destJournalWriter.writeLine(line);
            }
        }
    }
}