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

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.createArchiveManager;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.createAzurePersistence;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.decorateWithCache;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newFileStore;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.printableStopwatch;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.jackrabbit.oak.commons.time.Stopwatch;

import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.CompactorType;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.GCJournal;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.split.SplitPersistence;
import org.apache.jackrabbit.oak.segment.tool.Compact;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

/**
 * Perform an offline compaction of an existing Azure Segment Store.
 */
public class AzureCompact {

    /**
     * Create a builder for the {@link Compact} command.
     *
     * @return an instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Collect options for the {@link Compact} command.
     */
    public static class Builder {

        private String path;

        private String targetPath;

        private String rootPrefix;

        private String targetRootPrefix;

        private boolean force;

        private long gcLogInterval = 150000;

        private int segmentCacheSize = 2048;

        private GCType gcType = GCType.FULL;

        private CompactorType compactorType = CompactorType.PARALLEL_COMPACTOR;

        private int concurrency = 1;

        private String persistentCachePath;

        private Integer persistentCacheSizeGb;

        private int garbageThresholdGb;

        private int garbageThresholdPercentage;

        private BlobContainerClient sourceBlobContainerClient;

        private BlobContainerClient destinationBlobContainerClient;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The path (URI) to an existing segment store. This parameter is required.
         *
         * @param path
         *            the path to an existing segment store.
         * @return this builder.
         */
        public Builder withPath(String path) {
            this.path = requireNonNull(path);
            return this;
        }

        /**
         * The path (URI) to the target segment store.
         *
         * @param targetPath
         *             the path to the target segment store.
         * @return this builder
         */
        public Builder withTargetPath(String targetPath) {
            this.targetPath = requireNonNull(targetPath);
            return this;
        }

        /**
         * The root directory to an existing segment store.
         *
         * @param rootPrefix
         *               the directory to an existing segment store.
         * @return the builder
         */
        public Builder withRootPrefix(String rootPrefix) {
            this.rootPrefix = rootPrefix;
            return this;
        }

        /**
         * The root directory to the target segment store.
         *
         * @param targetRootPrefix
         *                  the root directory to the target segmen store.
         * @return this builder
         */
        public Builder withTargetRootPrefix(String targetRootPrefix) {
            this.targetRootPrefix = targetRootPrefix;
            return this;
        }

        /**
         * Whether to fail if run on an older version of the store of force upgrading
         * its format.
         *
         * @param force
         *            upgrade iff {@code true}
         * @return this builder.
         */
        public Builder withForce(boolean force) {
            this.force = force;
            return this;
        }

        /**
         * The size of the segment cache in MB. The default of
         * {@link SegmentCache#DEFAULT_SEGMENT_CACHE_MB} when this method is not
         * invoked.
         *
         * @param segmentCacheSize
         *            cache size in MB
         * @return this builder
         * @throws IllegalArgumentException
         *             if {@code segmentCacheSize} is not a positive integer.
         */
        public Builder withSegmentCacheSize(int segmentCacheSize) {
            checkArgument(segmentCacheSize > 0, "segmentCacheSize must be strictly positive");
            this.segmentCacheSize = segmentCacheSize;
            return this;
        }

        /**
         * The number of nodes after which an update about the compaction process is
         * logged. Set to a negative number to disable progress logging. If not
         * specified, it defaults to 150,000 nodes.
         *
         * @param gcLogInterval
         *            The log interval.
         * @return this builder.
         */
        public Builder withGCLogInterval(long gcLogInterval) {
            this.gcLogInterval = gcLogInterval;
            return this;
        }

        /**
         * The garbage collection type used. If not specified it defaults to full compaction
         * @param gcType the GC type
         * @return this builder
         */
        public Builder withGCType(GCType gcType) {
            this.gcType = gcType;
            return this;
        }

        /**
         * The compactor type to be used by compaction. If not specified it defaults to
         * "parallel" compactor
         * @param compactorType the compactor type
         * @return this builder
         */
        public Builder withCompactorType(CompactorType compactorType) {
            this.compactorType = compactorType;
            return this;
        }

        /**
         * The number of threads to be used for compaction. This only applies to the "parallel" compactor
         * @param concurrency the number of threads
         * @return this builder
         */
        public Builder withConcurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        /**
         * The path where segments in the persistent cache will be stored.
         *
         * @param persistentCachePath
         *             the path to the persistent cache.
         * @return this builder
         */
        public Builder withPersistentCachePath(String persistentCachePath) {
            this.persistentCachePath = requireNonNull(persistentCachePath);
            return this;
        }

        /**
         * The maximum size in GB of the persistent disk cache.
         *
         * @param persistentCacheSizeGb
         *             the maximum size of the persistent cache.
         * @return this builder
         */
        public Builder withPersistentCacheSizeGb(Integer persistentCacheSizeGb) {
            this.persistentCacheSizeGb = requireNonNull(persistentCacheSizeGb);
            return this;
        }

        /**
         * The minimum garbage size in GB for the compaction to run.
         * @param garbageThresholdGb
         *           the minimum garbage size in GB for the compaction to run.
         *
         * @return this builder
         */
        public Builder withGarbageThresholdGb(int garbageThresholdGb) {
            this.garbageThresholdGb = garbageThresholdGb;
            return this;
        }

        /**
         * The minimum garbage size in percentage for the compaction to run.
         * @param garbageThresholdPercentage
         *          the minimum garbage size in percentage for the compaction to run.
         * @return this builder
         */
        public Builder withGarbageThresholdPercentage(int garbageThresholdPercentage) {
            this.garbageThresholdPercentage = garbageThresholdPercentage;
            return this;
        }

        public Builder withSourceBlobContainerClient(BlobContainerClient sourceBlobContainerClient) {
            this.sourceBlobContainerClient = requireNonNull(sourceBlobContainerClient);
            return this;
        }

        public Builder withDestinationBlobContainerClient(BlobContainerClient destinationBlobContainerClient) {
            this.destinationBlobContainerClient = requireNonNull(destinationBlobContainerClient);
            return this;
        }

        /**
         * Create an executable version of the {@link Compact} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public AzureCompact build() {
            if (sourceBlobContainerClient == null || destinationBlobContainerClient == null) {
                requireNonNull(path);
                requireNonNull(targetPath);
            }
            return new AzureCompact(this);
        }
    }

    private static final long GB = 1024 * 1024 * 1024;

    private final String path;

    private final String targetPath;

    private final String rootPrefix;

    private final String targetRootPrefix;

    private final int segmentCacheSize;

    private final boolean strictVersionCheck;

    private final long gcLogInterval;

    private final GCType gcType;

    private final CompactorType compactorType;

    private final int concurrency;

    private final String persistentCachePath;

    private final Integer persistentCacheSizeGb;

    private final int garbageThresholdGb;

    private final int garbageThresholdPercentage;

    private final BlobContainerClient sourceBlobContainerClient;

    private final BlobContainerClient destinationBlobContainerClient;

    private AzureCompact(Builder builder) {
        this.path = builder.path;
        this.targetPath = builder.targetPath;
        this.rootPrefix = builder.rootPrefix;
        this.targetRootPrefix = builder.targetRootPrefix;
        this.segmentCacheSize = builder.segmentCacheSize;
        this.strictVersionCheck = !builder.force;
        this.gcLogInterval = builder.gcLogInterval;
        this.gcType = builder.gcType;
        this.compactorType = builder.compactorType;
        this.concurrency = builder.concurrency;
        this.persistentCachePath = builder.persistentCachePath;
        this.persistentCacheSizeGb = builder.persistentCacheSizeGb;
        this.garbageThresholdGb = builder.garbageThresholdGb;
        this.garbageThresholdPercentage = builder.garbageThresholdPercentage;
        this.sourceBlobContainerClient = builder.sourceBlobContainerClient;
        this.destinationBlobContainerClient = builder.destinationBlobContainerClient;
    }

    public int run() throws IOException, URISyntaxException {
        Stopwatch watch = Stopwatch.createStarted();

        SegmentNodeStorePersistence roPersistence;
        SegmentNodeStorePersistence rwPersistence;
        BlobContainerClient targetContainer;
        if (sourceBlobContainerClient != null && destinationBlobContainerClient != null) {
            roPersistence = new AzurePersistence(sourceBlobContainerClient, rootPrefix);
            rwPersistence = new AzurePersistence(destinationBlobContainerClient, targetRootPrefix);
            targetContainer = destinationBlobContainerClient;
        } else {
            roPersistence = createAzurePersistence(path);

            AzurePersistence rwAzurePersistence = createAzurePersistence(targetPath);
            targetContainer = rwAzurePersistence.getReadBlobContainerClient();
            rwPersistence = rwAzurePersistence;
        }

        if (persistentCachePath != null) {
            roPersistence = decorateWithCache(roPersistence, persistentCachePath, persistentCacheSizeGb);
        }

        SegmentNodeStorePersistence splitPersistence = new SplitPersistence(roPersistence, rwPersistence);

        SegmentArchiveManager roArchiveManager = createArchiveManager(roPersistence, true);
        SegmentArchiveManager rwArchiveManager = createArchiveManager(rwPersistence, false);

        System.out.printf("Compacting %s\n", path != null ? path : sourceBlobContainerClient.getBlobContainerUrl());
        System.out.printf(" to %s\n", targetPath != null ? targetPath : destinationBlobContainerClient.getBlobContainerUrl());
        System.out.printf("    before\n");
        List<String> beforeArchives = Collections.emptyList();
        try {
            beforeArchives = roArchiveManager.listArchives();
        } catch (IOException e) {
            System.err.println(e);
        }

        printArchives(System.out, beforeArchives);

        GCGeneration gcGeneration = null;
        String root = null;

        try (FileStore store =newFileStore(splitPersistence,
                Files.createTempDirectory(getClass().getSimpleName() + "-").toFile(),
                strictVersionCheck, segmentCacheSize,
                gcLogInterval, compactorType, concurrency)) {
            if (garbageThresholdGb > 0 && garbageThresholdPercentage > 0) {
                System.out.printf("    -> minimum garbage threshold set to %d GB or %d%%\n", garbageThresholdGb, garbageThresholdPercentage);
                long currentSize = store.size();
                if (!isGarbageOverMinimumThreshold(currentSize, roPersistence)) {
                    targetContainer.delete();
                    return 0;
                }
            }
            System.out.printf("    -> compacting\n");

            boolean success = false;
            switch (gcType) {
                case FULL:
                    success = store.compactFull();
                    break;
                case TAIL:
                    success = store.compactTail();
                    break;
            }

            if (!success) {
                System.out.printf("Compaction cancelled after %s.\n", printableStopwatch(watch));
                return 1;
            }

            System.out.printf("    -> [skipping] cleaning up\n");
            gcGeneration = store.getHead().getGcGeneration();
            root = store.getHead().getRecordId().toString10();
        } catch (Exception e) {
            watch.stop();
            e.printStackTrace(System.err);
            System.out.printf("Compaction failed after %s.\n", printableStopwatch(watch));
            return 1;
        }

        watch.stop();
        System.out.printf("    after\n");
        List<String> afterArchives = Collections.emptyList();
        try {
            afterArchives = rwArchiveManager.listArchives();
        } catch (IOException e) {
            System.err.println(e);
        }
        printArchives(System.out, afterArchives);
        System.out.printf("Compaction succeeded in %s.\n", printableStopwatch(watch));

        long newSize = printTargetRepoSizeInfo(targetContainer);
        persistGCJournal(rwPersistence, newSize, gcGeneration, root);

        return 0;
    }

    private void persistGCJournal(SegmentNodeStorePersistence rwPersistence, long newSize, GCGeneration gcGeneration, String root) throws IOException {
        GCJournalFile gcJournalFile = rwPersistence.getGCJournalFile();
        if (gcJournalFile != null) {
            GCJournal gcJournal = new GCJournal(gcJournalFile);
            gcJournal.persist(0, newSize, gcGeneration, 0, root);
        }
    }

    private boolean isGarbageOverMinimumThreshold(long currentSize, SegmentNodeStorePersistence roPersistence) throws IOException {
        long previousSize = 0;

        GCJournalFile gcJournalFile = roPersistence.getGCJournalFile();
        if (gcJournalFile != null) {
            GCJournal gcJournal = new GCJournal(gcJournalFile);
            GCJournal.GCJournalEntry gcJournalEntry = gcJournal.read();
            previousSize = gcJournalEntry.getRepoSize();
        }

        long potentialGarbage = currentSize - previousSize;
        if (currentSize < previousSize || (potentialGarbage < garbageThresholdGb * GB && potentialGarbage < currentSize * garbageThresholdPercentage / 100)) {
            System.out.printf("    -> [skipping] not enough garbage -> previous size: %d, current size: %d\n", previousSize, currentSize);
            return false;
        }

        return true;
    }

    private long printTargetRepoSizeInfo(BlobContainerClient blobContainerClient) {
        System.out.printf("Calculating the size of container %s\n", blobContainerClient.getBlobContainerName());
        long size = 0;
        ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
        listBlobsOptions.setDetails(new BlobListDetails().setRetrieveMetadata(true));

        for (BlobItem blobItem : blobContainerClient.listBlobs(listBlobsOptions, null)) {
            size += blobItem.getProperties().getContentLength();
        }
        System.out.printf("The size is: %d MB \n", size / 1024 / 1024);
        return size;
    }

    private static void printArchives(PrintStream s, List<String> archives) {
        for (String a : archives) {
            s.printf("        %s\n", a);
        }
    }
}
