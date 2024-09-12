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

import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newSegmentNodeStorePersistence;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.printMessage;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.printableStopwatch;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.storeDescription;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.storeTypeFromPathOrUri;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureStorageCredentialManagerV8;
import org.apache.jackrabbit.oak.segment.azure.tool.SegmentStoreMigrator.Segment;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.SegmentStoreType;
import org.apache.jackrabbit.oak.segment.azure.util.Retrier;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.tool.Check;

import org.apache.jackrabbit.guava.common.base.Stopwatch;

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

        private PrintWriter outWriter;

        private PrintWriter errWriter;

        private Integer revisionsCount = Integer.MAX_VALUE;

        private Boolean flat = false;

        private Boolean appendMode = false;

        private Integer maxSizeGb = 1;

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
            this.source = requireNonNull(source);
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
            this.destination = requireNonNull(destination);
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
            this.srcPersistence = requireNonNull(srcPersistence);
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
            this.destPersistence = requireNonNull(destPersistence);
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
         * The last {@code revisionsCount} revisions to be copied.
         * This parameter is not required and defaults to {@code 1}.
         *
         * @param revisionsCount number of revisions to copied.
         * @return this builder.
         */
        public Builder withRevisionsCount(Integer revisionsCount) {
            this.revisionsCount = revisionsCount;
            return this;
        }

        /**
         * If enabled, the segments hierarchy will be copied without any
         * TAR archive being created, in a flat hierarchy.
         *
         * @param flat flag controlling the copying in flat hierarchy.
         * @return this builder.
         */
        public Builder withFlat(Boolean flat) {
            this.flat = flat;
            return this;
        }

        /**
         * If enabled, existing segments will be skipped instead of
         * overwritten in the copy process.
         *
         * @param appendMode flag controlling the segment write behavior.
         * @return this builder.
         */
        public Builder withAppendMode(Boolean appendMode) {
            this.appendMode = appendMode;
            return this;
        }

        /**
         * Parameter for configuring the maximum size of the segment store transfer
         *
         * @param maxSizeGb the maximum size up to which repository data will be copied
         * @return this builder.
         */
        public Builder withMaxSizeGb(Integer maxSizeGb) {
            this.maxSizeGb = maxSizeGb;
            return this;
        }

        /**
         * Create an executable version of the {@link Check} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public SegmentCopy build() {
            if (srcPersistence == null && destPersistence == null) {
                requireNonNull(source);
                requireNonNull(destination);
            }

            return new SegmentCopy(this);
        }
    }

    private static final int READ_THREADS = 20;

    private static final Retrier RETRIER = Retrier.withParams(16, 5000);

    private final String source;

    private final String destination;

    private final PrintWriter outWriter;

    private final PrintWriter errWriter;

    private final Integer revisionCount;

    private final Boolean flat;

    private final Boolean appendMode;

    private final Integer maxSizeGb;

    private SegmentNodeStorePersistence srcPersistence;

    private SegmentNodeStorePersistence destPersistence;

    private ExecutorService executor = Executors.newFixedThreadPool(READ_THREADS + 1);
    private final AzureStorageCredentialManagerV8 azureStorageCredentialManagerV8;

    public SegmentCopy(Builder builder) {
        this.source = builder.source;
        this.destination = builder.destination;
        this.srcPersistence = builder.srcPersistence;
        this.destPersistence = builder.destPersistence;
        this.revisionCount = builder.revisionsCount;
        this.flat = builder.flat;
        this.appendMode = builder.appendMode;
        this.maxSizeGb = builder.maxSizeGb;
        this.outWriter = builder.outWriter;
        this.errWriter = builder.errWriter;
        this.azureStorageCredentialManagerV8 = new AzureStorageCredentialManagerV8();
    }

    public int run() {
        Stopwatch watch = Stopwatch.createStarted();

        SegmentStoreType srcType = storeTypeFromPathOrUri(source);
        SegmentStoreType destType = storeTypeFromPathOrUri(destination);

        String srcDescription = storeDescription(srcType, source);
        String destDescription = storeDescription(destType, destination);

        if (flat && destType == SegmentStoreType.TAR) {
            try {
                srcPersistence = newSegmentNodeStorePersistence(srcType, source, azureStorageCredentialManagerV8);

                SegmentArchiveManager sourceManager = srcPersistence.createArchiveManager(false, false,
                        new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());

                int maxArchives = maxSizeGb * 4;
                int count = 0;

                List<String> archivesList = sourceManager.listArchives();
                archivesList.sort(Collections.reverseOrder());

                for (String archiveName : archivesList) {
                    if (count == maxArchives - 1) {
                        printMessage(outWriter, "Stopping transfer after reaching {0} GB at archive {1}", maxSizeGb,
                                archiveName);
                        break;
                    }

                    printMessage(outWriter, "{0}/{1} -> {2}", source, archiveName, destination);

                    SegmentArchiveReader reader = sourceManager.forceOpen(archiveName);

                    List<Future<Segment>> futures = new ArrayList<>();
                    for (SegmentArchiveEntry entry : reader.listSegments()) {
                        futures.add(executor.submit(() -> RETRIER.execute(() -> {
                            Segment segment = new Segment(entry);
                            segment.read(reader);
                            return segment;
                        })));
                    }

                    File directory = new File(destination);
                    directory.mkdir();

                    for (Future<Segment> future : futures) {
                        Segment segment = future.get();
                        RETRIER.execute(() -> {
                            final byte[] array = segment.data.array();
                            String segmentId = new UUID(segment.entry.getMsb(), segment.entry.getLsb()).toString();
                            File segmentFile = new File(directory, segmentId);
                            File tempSegmentFile = new File(directory, segmentId + System.nanoTime() + ".part");
                            Buffer buffer = Buffer.wrap(array);

                            Buffer bufferCopy = buffer.duplicate();

                            try {
                                try (FileChannel channel = new FileOutputStream(tempSegmentFile).getChannel()) {
                                    bufferCopy.write(channel);
                                }
                                try {
                                    Files.move(tempSegmentFile.toPath(), segmentFile.toPath(),
                                            StandardCopyOption.ATOMIC_MOVE);
                                } catch (AtomicMoveNotSupportedException e) {
                                    Files.move(tempSegmentFile.toPath(), segmentFile.toPath());
                                }
                            } catch (Exception e) {
                                printMessage(errWriter, "Error writing segment {0} to cache: {1} ", segmentId, e);
                                e.printStackTrace(errWriter);
                                try {
                                    Files.deleteIfExists(segmentFile.toPath());
                                    Files.deleteIfExists(tempSegmentFile.toPath());
                                } catch (IOException i) {
                                    printMessage(errWriter, "Error while deleting corrupted segment file {0} {1}",
                                            segmentId, i);
                                }
                            }
                        });
                    }

                    count++;
                }
            } catch (IOException | InterruptedException | ExecutionException e) {
                watch.stop();
                printMessage(errWriter, "A problem occured while copying archives from {0} to {1} ", source,
                        destination);
                e.printStackTrace(errWriter);
                return 1;
            } finally {
                azureStorageCredentialManagerV8.close();
            }
        } else {
            try {
                if (srcPersistence == null || destPersistence == null) {
                    srcPersistence = newSegmentNodeStorePersistence(srcType, source, azureStorageCredentialManagerV8);
                    destPersistence = newSegmentNodeStorePersistence(destType, destination, azureStorageCredentialManagerV8);
                }

                printMessage(outWriter, "Started segment-copy transfer!");
                printMessage(outWriter, "Source: {0}", srcDescription);
                printMessage(outWriter, "Destination: {0}", destDescription);

                SegmentStoreMigrator.Builder migratorBuilder = new SegmentStoreMigrator.Builder()
                        .withSourcePersistence(srcPersistence, srcDescription)
                        .withTargetPersistence(destPersistence, destDescription)
                        .withRevisionCount(revisionCount);

                if (appendMode) {
                    migratorBuilder.setAppendMode();
                }

                migratorBuilder.build().migrate();

            } catch (Exception e) {
                watch.stop();
                printMessage(errWriter, "A problem occured while copying archives from {0} to {1} ", source,
                        destination);
                e.printStackTrace(errWriter);
                return 1;
            } finally {
                azureStorageCredentialManagerV8.close();
            }

        }

        watch.stop();
        printMessage(outWriter, "Segment-copy succeeded in {0}", printableStopwatch(watch));

        return 0;
    }
}
