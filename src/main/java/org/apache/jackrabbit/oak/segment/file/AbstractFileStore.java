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
package org.apache.jackrabbit.oak.segment.file;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.segment.CachingSegmentReader;
import org.apache.jackrabbit.oak.segment.Revisions;
import org.apache.jackrabbit.oak.segment.SegmentCache;
import org.apache.jackrabbit.oak.segment.SegmentId;
import org.apache.jackrabbit.oak.segment.SegmentIdFactory;
import org.apache.jackrabbit.oak.segment.SegmentNodeState;
import org.apache.jackrabbit.oak.segment.SegmentReader;
import org.apache.jackrabbit.oak.segment.SegmentStore;
import org.apache.jackrabbit.oak.segment.SegmentTracker;
import org.apache.jackrabbit.oak.segment.SegmentWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

/**
 * The storage implementation for tar files.
 */
public abstract class AbstractFileStore implements SegmentStore, Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractFileStore.class);

    private static final String MANIFEST_FILE_NAME = "manifest";

    /**
     * This value can be used as an invalid store version, since the store
     * version is defined to be strictly greater than zero.
     */
    private static final int INVALID_STORE_VERSION = 0;

    /**
     * The store version is an always incrementing number, strictly greater than
     * zero, that is changed every time there is a backwards incompatible
     * modification to the format of the segment store.
     */
    static final int CURRENT_STORE_VERSION = 1;

    private static final Pattern FILE_NAME_PATTERN =
            Pattern.compile("(data|bulk)((0|[1-9][0-9]*)[0-9]{4})([a-z])?.tar");

    static final String FILE_NAME_FORMAT = "data%05d%s.tar";

    @Nonnull
    final SegmentTracker tracker;

    @Nonnull
    final CachingSegmentReader segmentReader;

    final File directory;

    private final BlobStore blobStore;

    final boolean memoryMapping;

    @Nonnull
    final SegmentCache segmentCache;

    @Nonnull
    private final SegmentIdFactory segmentIdFactory = new SegmentIdFactory() {

        @Override
        @Nonnull
        public SegmentId newSegmentId(long msb, long lsb) {
            return new SegmentId(AbstractFileStore.this, msb, lsb);
        }

    };

    AbstractFileStore(final FileStoreBuilder builder) throws InvalidFileStoreVersionException, IOException {
        this.directory = builder.getDirectory();
        this.tracker = new SegmentTracker();
        this.blobStore = builder.getBlobStore();
        this.segmentCache = new SegmentCache(builder.getSegmentCacheSize());
        this.segmentReader = new CachingSegmentReader(new Supplier<SegmentWriter>() {
            @Override
            public SegmentWriter get() {
                return getWriter();
            }
        }, blobStore, builder.getStringCacheSize(), builder.getTemplateCacheSize());
        this.memoryMapping = builder.getMemoryMapping();
    }

     File getManifestFile() {
        return new File(directory, MANIFEST_FILE_NAME);
    }

     Manifest openManifest() throws IOException {
        File file = getManifestFile();

        if (file.exists()) {
            return Manifest.load(file);
        }

        return null;
    }

     Manifest checkManifest(Manifest manifest) throws InvalidFileStoreVersionException {
        if (manifest == null) {
            throw new InvalidFileStoreVersionException("Using oak-segment-tar, but oak-segment should be used");
        }

        int storeVersion = manifest.getStoreVersion(INVALID_STORE_VERSION);

        // A store version less than or equal to the highest invalid value means
        // that something or someone is messing up with the manifest. This error
        // is not recoverable and is thus represented as an ISE.

        if (storeVersion <= INVALID_STORE_VERSION) {
            throw new IllegalStateException("Invalid store version");
        }

        if (storeVersion < CURRENT_STORE_VERSION) {
            throw new InvalidFileStoreVersionException("Using a too recent version of oak-segment-tar");
        }

        if (storeVersion > CURRENT_STORE_VERSION) {
            throw new InvalidFileStoreVersionException("Using a too old version of oak-segment tar");
        }

        return manifest;
    }

    @Nonnull
    public CacheStatsMBean getSegmentCacheStats() {
        return segmentCache.getCacheStats();
    }

    @Nonnull
    public CacheStatsMBean getStringCacheStats() {
        return segmentReader.getStringCacheStats();
    }

    @Nonnull
    public CacheStatsMBean getTemplateCacheStats() {
        return segmentReader.getTemplateCacheStats();
    }

    static Map<Integer, Map<Character, File>> collectFiles(File directory) {
        Map<Integer, Map<Character, File>> dataFiles = newHashMap();
        Map<Integer, File> bulkFiles = newHashMap();

        for (File file : directory.listFiles()) {
            Matcher matcher = FILE_NAME_PATTERN.matcher(file.getName());
            if (matcher.matches()) {
                Integer index = Integer.parseInt(matcher.group(2));
                if ("data".equals(matcher.group(1))) {
                    Map<Character, File> files = dataFiles.get(index);
                    if (files == null) {
                        files = newHashMap();
                        dataFiles.put(index, files);
                    }
                    Character generation = 'a';
                    if (matcher.group(4) != null) {
                        generation = matcher.group(4).charAt(0);
                    }
                    checkState(files.put(generation, file) == null);
                } else {
                    checkState(bulkFiles.put(index, file) == null);
                }
            }
        }

        if (!bulkFiles.isEmpty()) {
            log.info("Upgrading TarMK file names in {}", directory);

            if (!dataFiles.isEmpty()) {
                // first put all the data segments at the end of the list
                Integer[] indices =
                        dataFiles.keySet().toArray(new Integer[dataFiles.size()]);
                Arrays.sort(indices);
                int position = Math.max(
                        indices[indices.length - 1] + 1,
                        bulkFiles.size());
                for (Integer index : indices) {
                    Map<Character, File> files = dataFiles.remove(index);
                    Integer newIndex = position++;
                    for (Character generation : newHashSet(files.keySet())) {
                        File file = files.get(generation);
                        File newFile = new File(
                                directory,
                                format(FILE_NAME_FORMAT, newIndex, generation));
                        log.info("Renaming {} to {}", file, newFile);
                        file.renameTo(newFile);
                        files.put(generation, newFile);
                    }
                    dataFiles.put(newIndex, files);
                }
            }

            // then add all the bulk segments at the beginning of the list
            Integer[] indices =
                    bulkFiles.keySet().toArray(new Integer[bulkFiles.size()]);
            Arrays.sort(indices);
            int position = 0;
            for (Integer index : indices) {
                File file = bulkFiles.remove(index);
                Integer newIndex = position++;
                File newFile = new File(
                        directory, format(FILE_NAME_FORMAT, newIndex, "a"));
                log.info("Renaming {} to {}", file, newFile);
                file.renameTo(newFile);
                dataFiles.put(newIndex, singletonMap('a', newFile));
            }
        }

        return dataFiles;
    }

    @Nonnull
    public SegmentTracker getTracker() {
        return tracker;
    }

    public abstract SegmentWriter getWriter();

    @Nonnull
    public SegmentReader getReader() {
        return segmentReader;
    }

    /**
     * @return the {@link Revisions} object bound to the current store.
     */
    public abstract Revisions getRevisions();

    /**
     * Convenience method for accessing the root node for the current head.
     * This is equivalent to
     * <pre>
     * fileStore.getReader().readHeadState(fileStore.getRevisions())
     * </pre>
     * @return the current head node state
     */
    @Nonnull
    public SegmentNodeState getHead() {
        return segmentReader.readHeadState(getRevisions());
    }

    @Override
    @Nonnull
    public SegmentId newSegmentId(long msb, long lsb) {
        return tracker.newSegmentId(msb, lsb, segmentIdFactory);
    }

    @Override
    @Nonnull
    public SegmentId newBulkSegmentId() {
        return tracker.newBulkSegmentId(segmentIdFactory);
    }

    @Override
    @Nonnull
    public SegmentId newDataSegmentId() {
        return tracker.newDataSegmentId(segmentIdFactory);
    }

    /**
     * @return  the external BlobStore (if configured) with this store, {@code null} otherwise.
     */
    @CheckForNull
    public BlobStore getBlobStore() {
        return blobStore;
    }

    static void closeAndLogOnFail(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ioe) {
                // ignore and log
                log.error(ioe.getMessage(), ioe);
            }
        }
    }
}
