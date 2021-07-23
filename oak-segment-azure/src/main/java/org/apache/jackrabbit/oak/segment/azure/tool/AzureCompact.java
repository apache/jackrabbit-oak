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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.createArchiveManager;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.createCloudBlobDirectory;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newFileStore;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newSegmentNodeStorePersistence;
import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.printableStopwatch;

import com.google.common.base.Stopwatch;
import com.google.common.io.Files;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.ListBlobItem;

import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.SegmentStoreType;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.CompactorType;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.persistence.split.SplitPersistence;
import org.apache.jackrabbit.oak.segment.tool.Compact;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.EnumSet;
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

        private boolean force;

        private long gcLogInterval = 150000;

        private int segmentCacheSize = 2048;

        private CompactorType compactorType = CompactorType.CHECKPOINT_COMPACTOR;

        private String persistentCachePath;

        private Integer persistentCacheSizeGb;

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
            this.path = checkNotNull(path);
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
            this.targetPath = checkNotNull(targetPath);
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
         * The compactor type to be used by compaction. If not specified it defaults to
         * "diff" compactor
         * @param compactorType the compactor type
         * @return this builder
         */
        public Builder withCompactorType(CompactorType compactorType) {
            this.compactorType = compactorType;
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
            this.persistentCachePath = checkNotNull(persistentCachePath);
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
            this.persistentCacheSizeGb = checkNotNull(persistentCacheSizeGb);
            return this;
        }

        /**
         * Create an executable version of the {@link Compact} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public AzureCompact build() {
            checkNotNull(path);
            return new AzureCompact(this);
        }
    }

    private final String path;

    private final String targetPath;

    private final int segmentCacheSize;

    private final boolean strictVersionCheck;

    private final long gcLogInterval;

    private final CompactorType compactorType;

    private String persistentCachePath;

    private Integer persistentCacheSizeGb;

    private AzureCompact(Builder builder) {
        this.path = builder.path;
        this.targetPath = builder.targetPath;
        this.segmentCacheSize = builder.segmentCacheSize;
        this.strictVersionCheck = !builder.force;
        this.gcLogInterval = builder.gcLogInterval;
        this.compactorType = builder.compactorType;
        this.persistentCachePath = builder.persistentCachePath;
        this.persistentCacheSizeGb = builder.persistentCacheSizeGb;
    }

    public int run() throws IOException, StorageException, URISyntaxException {
        Stopwatch watch = Stopwatch.createStarted();
        SegmentNodeStorePersistence roPersistence = newSegmentNodeStorePersistence(SegmentStoreType.AZURE, path, persistentCachePath, persistentCacheSizeGb);
        SegmentNodeStorePersistence rwPersistence = newSegmentNodeStorePersistence(SegmentStoreType.AZURE, targetPath);

        SegmentNodeStorePersistence splitPersistence = new SplitPersistence(roPersistence, rwPersistence);

        SegmentArchiveManager roArchiveManager = createArchiveManager(roPersistence);
        SegmentArchiveManager rwArchiveManager = createArchiveManager(rwPersistence);

        System.out.printf("Compacting %s\n", path);
        System.out.printf(" to %s\n", targetPath);
        System.out.printf("    before\n");
        List<String> beforeArchives = Collections.emptyList();
        try {
            beforeArchives = roArchiveManager.listArchives();
        } catch (IOException e) {
            System.err.println(e);
        }

        printArchives(System.out, beforeArchives);
        System.out.printf("    -> compacting\n");

        try (FileStore store = newFileStore(splitPersistence, Files.createTempDir(), strictVersionCheck, segmentCacheSize,
                gcLogInterval, compactorType)) {
            if (!store.compactFull()) {
                System.out.printf("Compaction cancelled after %s.\n", printableStopwatch(watch));
                return 1;
            }

            System.out.printf("    -> [skipping] cleaning up\n");
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

        CloudBlobDirectory targetDirectory = createCloudBlobDirectory(targetPath.substring(3));
        CloudBlobContainer targetContainer = targetDirectory.getContainer();
        printTargetRepoSizeInfo(targetContainer);

        return 0;
    }

    private long printTargetRepoSizeInfo(CloudBlobContainer container) {
        System.out.printf("Calculating the size of container %s\n", container.getName());
        long size = 0;
        for (ListBlobItem i : container.listBlobs(null, true, EnumSet.of(BlobListingDetails.METADATA), null, null)) {
            if (i instanceof CloudBlob) {
                size += ((CloudBlob) i).getProperties().getLength();
            }
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
