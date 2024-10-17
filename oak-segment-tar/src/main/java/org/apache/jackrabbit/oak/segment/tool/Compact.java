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

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.jackrabbit.guava.common.collect.Sets.difference;
import static java.util.Collections.emptySet;
import static org.apache.commons.io.FileUtils.sizeOfDirectory;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GCType;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.CompactorType;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFileWriter;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.JournalReader;
import org.apache.jackrabbit.oak.segment.file.tar.LocalJournalFile;
import org.jetbrains.annotations.Nullable;

/**
 * Perform an offline compaction of an existing segment store.
 */
public class Compact {

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

        private Boolean mmap;

        private String os;

        private boolean force;

        private long gcLogInterval = 150000;

        private int segmentCacheSize = DEFAULT_SEGMENT_CACHE_MB;

        private GCType gcType = GCType.FULL;

        private CompactorType compactorType = CompactorType.PARALLEL_COMPACTOR;

        private int concurrency = 1;

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
            this.path = requireNonNull(path);
            return this;
        }

        /**
         * Whether to use memory mapped access or file access.
         *
         * @param mmap {@code true} for memory mapped access, {@code false} for
         *             file access {@code null} to determine the access mode
         *             from the system architecture: memory mapped on 64 bit
         *             systems, file access on  32 bit systems.
         * @return this builder.
         */
        public Builder withMmap(@Nullable Boolean mmap) {
            this.mmap = mmap;
            return this;
        }

        /**
         * Which operating system the code is running on.
         *
         * @param os The operating system as returned by the "os.name" standard
         *           system property.
         * @return this builder.
         */
        public Builder withOs(String os) {
            this.os = requireNonNull(os);
            return this;
        }

        /**
         * Whether to fail if run on an older version of the store of force
         * upgrading its format.
         *
         * @param force upgrade iff {@code true}
         * @return this builder.
         */
        public Builder withForce(boolean force) {
            this.force = force;
            return this;
        }

        /**
         * The size of the segment cache in MB. The default of {@link
         * SegmentCache#DEFAULT_SEGMENT_CACHE_MB} when this method is not
         * invoked.
         *
         * @param segmentCacheSize cache size in MB
         * @return this builder
         * @throws IllegalArgumentException if {@code segmentCacheSize} is not a
         *                                  positive integer.
         */
        public Builder withSegmentCacheSize(int segmentCacheSize) {
            checkArgument(segmentCacheSize > 0, "segmentCacheSize must be strictly positive");
            this.segmentCacheSize = segmentCacheSize;
            return this;
        }

        /**
         * The number of nodes after which an update about the compaction
         * process is logged. Set to a negative number to disable progress
         * logging. If not specified, it defaults to 150,000 nodes.
         *
         * @param gcLogInterval The log interval.
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
         * Create an executable version of the {@link Compact} command.
         *
         * @return an instance of {@link Runnable}.
         */
        public Compact build() {
            requireNonNull(path);
            return new Compact(this);
        }

    }

    private enum FileAccessMode {

        ARCH_DEPENDENT(null, "default access mode"),

        MEMORY_MAPPED(true, "memory mapped access mode"),

        REGULAR(false, "regular access mode"),

        REGULAR_ENFORCED(false, "enforced regular access mode");

        final Boolean memoryMapped;

        final String description;

        FileAccessMode(Boolean memoryMapped, String description) {
            this.memoryMapped = memoryMapped;
            this.description = description;
        }

    }

    private static FileAccessMode newFileAccessMode(Boolean arg, String os) {
        if (os != null && os.toLowerCase().contains("windows")) {
            return FileAccessMode.REGULAR_ENFORCED;
        }
        if (arg == null) {
            return FileAccessMode.ARCH_DEPENDENT;
        }
        if (arg) {
            return FileAccessMode.MEMORY_MAPPED;
        }
        return FileAccessMode.REGULAR;
    }

    private static Set<File> listFiles(File directory) {
        File[] files = directory.listFiles();
        if (files == null) {
            return emptySet();
        }
        return CollectionUtils.toSet(files);
    }

    private static void printFiles(PrintStream s, Set<File> files) {
        for (File f : files) {
            s.printf("        %s, %s\n", getLastModified(f), f.getName());
        }
    }

    private static String getLastModified(File f) {
        return new Date(f.lastModified()).toString();
    }

    private static Set<String> fileNames(Set<File> files) {
        Set<String> names = new HashSet<>();
        for (File f : files) {
            names.add(f.getName());
        }
        return names;
    }

    private static String printableSize(long size) {
        return String.format("%s (%d bytes)", humanReadableByteCount(size), size);
    }

    private static String printableStopwatch(Stopwatch s) {
        return String.format("%s (%ds)", s, s.elapsed(TimeUnit.SECONDS));
    }

    private final File path;

    private final File journal;

    private final FileAccessMode fileAccessMode;

    private final int segmentCacheSize;

    private final boolean strictVersionCheck;

    private final long gcLogInterval;

    private final GCType gcType;

    private final CompactorType compactorType;

    private final int concurrency;

    private Compact(Builder builder) {
        this.path = builder.path;
        this.journal = new File(builder.path, "journal.log");
        this.fileAccessMode = newFileAccessMode(builder.mmap, builder.os);
        this.segmentCacheSize = builder.segmentCacheSize;
        this.strictVersionCheck = !builder.force;
        this.gcLogInterval = builder.gcLogInterval;
        this.gcType = builder.gcType;
        this.compactorType = builder.compactorType;
        this.concurrency = builder.concurrency;
    }

    public int run() {
        System.out.printf("Compacting %s with %s and %s compactor type\n", path, fileAccessMode.description, compactorType.description());
        System.out.printf("    before\n");
        Set<File> beforeFiles = listFiles(path);
        printFiles(System.out, beforeFiles);
        System.out.printf("    size %s\n", printableSize(sizeOfDirectory(path)));
        System.out.printf("    -> compacting\n");

        Stopwatch watch = Stopwatch.createStarted();

        try (FileStore store = newFileStore()) {
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
            System.out.printf("    -> cleaning up\n");
            store.cleanup();
            JournalFile journal = new LocalJournalFile(path, "journal.log");
            String head;
            try (JournalReader journalReader = new JournalReader(journal)) {
                head = String.format("%s root %s\n", journalReader.next().getRevision(), System.currentTimeMillis());
            }

            try (JournalFileWriter journalWriter = journal.openJournalWriter()) {
                System.out.printf("    -> writing new %s: %s\n", journal.getName(), head);
                journalWriter.truncate();
                journalWriter.writeLine(head);
            }
        } catch (Exception e) {
            watch.stop();
            e.printStackTrace(System.err);
            System.out.printf("Compaction failed after %s.\n", printableStopwatch(watch));
            return 1;
        }

        watch.stop();
        System.out.printf("    after\n");
        Set<File> afterFiles = listFiles(path);
        printFiles(System.out, afterFiles);
        System.out.printf("    size %s\n", printableSize(sizeOfDirectory(path)));
        System.out.printf("    removed files %s\n", fileNames(difference(beforeFiles, afterFiles)));
        System.out.printf("    added files %s\n", fileNames(difference(afterFiles, beforeFiles)));
        System.out.printf("Compaction succeeded in %s.\n", printableStopwatch(watch));
        return 0;
    }

    private FileStore newFileStore() throws IOException, InvalidFileStoreVersionException {
        FileStoreBuilder builder = fileStoreBuilder(path.getAbsoluteFile())
            .withStrictVersionCheck(strictVersionCheck)
            .withSegmentCacheSize(segmentCacheSize)
            .withGCOptions(defaultGCOptions()
                .setOffline()
                .setGCLogInterval(gcLogInterval)
                .setCompactorType(compactorType)
                .setConcurrency(concurrency));
        if (fileAccessMode.memoryMapped != null) {
            builder.withMemoryMapping(fileAccessMode.memoryMapped);
        }
        return builder.build();
    }

}
