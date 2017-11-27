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

package org.apache.jackrabbit.oak.run;

import static org.apache.jackrabbit.oak.segment.FileStoreHelper.isValidFileStoreOrFail;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.segment.SegmentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentVersion;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.tool.Backup;
import org.apache.jackrabbit.oak.segment.tool.Check;
import org.apache.jackrabbit.oak.segment.tool.Compact;
import org.apache.jackrabbit.oak.segment.tool.DebugSegments;
import org.apache.jackrabbit.oak.segment.tool.DebugStore;
import org.apache.jackrabbit.oak.segment.tool.DebugTars;
import org.apache.jackrabbit.oak.segment.tool.Diff;
import org.apache.jackrabbit.oak.segment.tool.History;
import org.apache.jackrabbit.oak.segment.tool.Restore;
import org.apache.jackrabbit.oak.segment.tool.Revisions;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

final class SegmentTarUtils {

    private static final boolean TAR_STORAGE_MEMORY_MAPPED = Boolean.getBoolean("tar.memoryMapped");

    private static final int TAR_SEGMENT_CACHE_SIZE = Integer.getInteger("cache", 256);

    private SegmentTarUtils() {
        // Prevent instantiation
    }

    static NodeStore bootstrapNodeStore(String path, Closer closer) throws IOException {
        try {
            return SegmentNodeStoreBuilders.builder(bootstrapFileStore(path, closer)).build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    static BlobReferenceRetriever newBlobReferenceRetriever(String path, Closer closer) throws IOException {
        try {
            return new SegmentBlobReferenceRetriever(closer.register(openFileStore(path, false)));
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    static void backup(File source, File target) {
        Backup.builder()
                .withSource(source)
                .withTarget(target)
                .build()
                .run();
    }

    static void restore(File source, File target) {
        Restore.builder()
                .withSource(source)
                .withTarget(target)
                .build()
                .run();
    }

    static void debug(String... args) {
        File file = new File(args[0]);

        List<String> tars = new ArrayList<>();
        List<String> segs = new ArrayList<>();

        for (int i = 1; i < args.length; i++) {
            if (args[i].endsWith(".tar")) {
                tars.add(args[i]);
            } else {
                segs.add(args[i]);
            }
        }

        if (tars.size() > 0) {
            debugTars(file, tars);
        }

        if (segs.size() > 0) {
            debugSegments(file, segs);
        }

        if (tars.isEmpty() && segs.isEmpty()) {
            debugStore(file);
        }
    }

    private static void debugTars(File store, List<String> tars) {
        DebugTars.Builder builder = DebugTars.builder().withPath(store);

        for (String tar : tars) {
            builder.withTar(tar);
        }

        builder.build().run();
    }

    private static void debugSegments(File store, List<String> segments) {
        DebugSegments.Builder builder = DebugSegments.builder().withPath(store);

        for (String segment : segments) {
            builder.withSegment(segment);
        }

        builder.build().run();
    }

    private static void debugStore(File store) {
        DebugStore.builder()
                .withPath(store)
                .build()
                .run();
        ;
    }

    static void history(File directory, File journal, String path, int depth) {
        History.builder()
                .withPath(directory)
                .withJournal(journal)
                .withNode(path)
                .withDepth(depth)
                .build()
                .run();
    }

    static void check(File dir, String journalFileName, long debugLevel, boolean checkBinaries, Set<String> filterPaths, boolean ioStatistics, 
            PrintWriter outWriter, PrintWriter errWriter) {
        Check.builder()
                .withPath(dir)
                .withJournal(journalFileName)
                .withDebugInterval(debugLevel)
                .withCheckBinaries(checkBinaries)
                .withFilterPaths(filterPaths)
                .withIOStatistics(ioStatistics)
                .withOutWriter(outWriter)
                .withErrWriter(errWriter)
                .build()
                .run();
    }

    static void diff(File store, File out, boolean listOnly, String interval, boolean incremental, String path, boolean ignoreSNFEs) throws IOException {
        if (listOnly) {
            revisions(store, out);
        } else {
            diff(store, out, interval, incremental, path, ignoreSNFEs);
        }
    }

    private static void revisions(File path, File output) {
        Revisions.builder()
                .withPath(path)
                .withOutput(output)
                .build()
                .run();
    }

    private static void diff(File path, File output, String interval, boolean incremental, String filter, boolean ignoreMissingSegments) {
        Diff.builder()
                .withPath(path)
                .withOutput(output)
                .withInterval(interval)
                .withIncremental(incremental)
                .withFilter(filter)
                .withIgnoreMissingSegments(ignoreMissingSegments)
                .build()
                .run();
    }

    private static FileStore bootstrapFileStore(String path, Closer closer) throws IOException, InvalidFileStoreVersionException {
        return closer.register(bootstrapFileStore(path));
    }

    private static FileStore bootstrapFileStore(String path) throws IOException, InvalidFileStoreVersionException {
        return fileStoreBuilder(new File(path))
            .withStrictVersionCheck(true)
            .build();
    }

    private static ReadOnlyFileStore openReadOnlyFileStore(File path, boolean memoryMapped)
            throws IOException, InvalidFileStoreVersionException {
        return fileStoreBuilder(isValidFileStoreOrFail(path))
                .withSegmentCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(memoryMapped)
                .buildReadOnly();
    }

    private static FileStoreBuilder newFileStoreBuilder(String directory, boolean force) throws IOException, InvalidFileStoreVersionException {
        return fileStoreBuilder(checkFileStoreVersionOrFail(directory, force))
                .withSegmentCacheSize(TAR_SEGMENT_CACHE_SIZE)
                .withMemoryMapping(TAR_STORAGE_MEMORY_MAPPED);
    }

    private static FileStore openFileStore(String directory, boolean force) throws IOException, InvalidFileStoreVersionException {
        return newFileStoreBuilder(directory, force)
            .withStrictVersionCheck(true)
            .build();
    }

    private static File checkFileStoreVersionOrFail(String path, boolean force) throws IOException, InvalidFileStoreVersionException {
        File directory = new File(path);
        if (!directory.exists()) {
            return directory;
        }
        ReadOnlyFileStore store = openReadOnlyFileStore(directory, false);
        try {
            SegmentVersion segmentVersion = getSegmentVersion(store);
            if (segmentVersion != LATEST_VERSION) {
                if (force) {
                    System.out.printf("Segment version mismatch. Found %s, expected %s. Forcing execution.\n", segmentVersion, LATEST_VERSION);
                } else {
                    throw new RuntimeException(String.format("Segment version mismatch. Found %s, expected %s. Aborting.", segmentVersion, LATEST_VERSION));
                }
            }
        } finally {
            store.close();
        }
        return directory;
    }

    private static SegmentVersion getSegmentVersion(ReadOnlyFileStore fileStore) {
        return fileStore.getRevisions().getHead().getSegment().getSegmentVersion();
    }

}
