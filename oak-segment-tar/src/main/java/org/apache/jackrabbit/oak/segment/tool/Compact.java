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

package org.apache.jackrabbit.oak.segment.tool;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.JournalReader;

/**
 * Perform an offline compaction of an existing segment store.
 */
public class Compact {

    private final long logAt = Long.getLong("compaction-progress-log", 150000);

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

        private File path;

        @CheckForNull
        private Boolean mmap;

        private boolean force;

        private int segmentCacheSize = DEFAULT_SEGMENT_CACHE_MB;

        private Builder() {
            // Prevent external instantiation.
        }

        /**
         * The path to an existing segment store. This parameter is required.
         *
         * @param path the path to an existing segment store.
         * @return this builder.
         */
        public Builder withPath(File path) {
            this.path = checkNotNull(path);
            return this;
        }

        /**
         * Whether to use memory mapped access or file access.
         * @param mmap  {@code true} for memory mapped access, {@code false} for file access
         *              {@code null} to determine the access mode from the system architecture:
         *              memory mapped on 64 bit systems, file access on  32 bit systems.
         * @return this builder.
         */
        public Builder withMmap(@Nullable Boolean mmap) {
            this.mmap = mmap;
            return this;
        }

        /**
         * Whether to fail if run on an older version of the store of force upgrading its format.
         * @param force   upgrade iff {@code true}
         * @return this builder.
         */
        public Builder withForce(boolean force) {
            this.force = force;
            return this;
        }

        /**
         * The size of the segment cache in MB. The default of {@link SegmentCache#DEFAULT_SEGMENT_CACHE_MB}
         * when this method is not invoked.
         * @param segmentCacheSize   cache size in MB
         * @return this builder
         * @throws IllegalArgumentException  if {@code segmentCacheSize} is not a positive integer.
         */
        public Builder withSegmentCacheSize(int segmentCacheSize) {
            checkArgument(segmentCacheSize > 0, "segmentCacheSize must be positive");
            this.segmentCacheSize = segmentCacheSize;
            return this;
        }

        /**
         * Create an executable version of the {@link Compact} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Compact build() {
            checkNotNull(path);
            return new Compact(this);
        }

    }

    private final File path;

    @CheckForNull
    private final Boolean mmap;

    private final int segmentCacheSize;

    private final boolean strictVersionCheck;

    private Compact(Builder builder) {
        this.path = builder.path;
        this.mmap = builder.mmap;
        this.segmentCacheSize = builder.segmentCacheSize;
        this.strictVersionCheck = !builder.force;
    }

    public void run() throws IOException, InvalidFileStoreVersionException {
        try (FileStore store = newFileStore()) {
            store.compactFull();
        }

        System.out.println("    -> cleaning up");
        try (FileStore store = newFileStore()) {
            store.cleanup();
            File journal = new File(path, "journal.log");
            String head;
            try (JournalReader journalReader = new JournalReader(journal)) {
                head = journalReader.next().getRevision() + " root " + System.currentTimeMillis() + "\n";
            }

            try (RandomAccessFile journalFile = new RandomAccessFile(journal, "rw")) {
                System.out.println("    -> writing new " + journal.getName() + ": " + head);
                journalFile.setLength(0);
                journalFile.writeBytes(head);
                journalFile.getChannel().force(false);
            }
        }
    }

    private FileStore newFileStore() throws IOException, InvalidFileStoreVersionException {
        FileStoreBuilder fileStoreBuilder = fileStoreBuilder(path.getAbsoluteFile())
                .withStrictVersionCheck(strictVersionCheck)
                .withSegmentCacheSize(segmentCacheSize)
                .withGCOptions(defaultGCOptions()
                    .setOffline()
                    .setGCLogInterval(logAt));

        return mmap == null
            ? fileStoreBuilder.build()
            : fileStoreBuilder.withMemoryMapping(mmap).build();
    }

}
