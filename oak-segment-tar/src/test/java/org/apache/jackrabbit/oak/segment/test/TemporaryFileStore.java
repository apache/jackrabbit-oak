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

package org.apache.jackrabbit.oak.segment.test;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.Segment;
import org.apache.jackrabbit.oak.segment.SegmentNotFoundExceptionListener;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class TemporaryFileStore extends ExternalResource {

    private final TemporaryFolder folder;

    private final TemporaryBlobStore blobStore;

    private final boolean standby;

    private final String name;

    private ScheduledExecutorService executor;

    private volatile FileStore store;

    private int binariesInlineThreshold = Segment.MEDIUM_LIMIT;

    public TemporaryFileStore(TemporaryFolder folder, boolean standby) {
        this(folder, null, standby, null);
    }

    public TemporaryFileStore(TemporaryFolder folder, TemporaryBlobStore blobStore, boolean standby) {
        this(folder, blobStore, standby, null);
    }

    public TemporaryFileStore(TemporaryFolder folder, TemporaryBlobStore blobStore, boolean standby, String name) {
        this.folder = folder;
        this.blobStore = blobStore;
        this.standby = standby;
        this.name = name;
    }

    public TemporaryFileStore(TemporaryFolder folder, TemporaryBlobStore blobStore, int binariesInlineThreshold) {
        this(folder, blobStore, false, null);
        this.binariesInlineThreshold = binariesInlineThreshold;
    }

    protected void init() throws InvalidFileStoreVersionException, IOException {
        executor = Executors.newSingleThreadScheduledExecutor();
        FileStoreBuilder builder = fileStoreBuilder(name == null ? folder.newFolder() : folder.newFolder(name))
                .withMaxFileSize(1)
                .withMemoryMapping(false)
                .withBinariesInlineThreshold(binariesInlineThreshold)
                .withNodeDeduplicationCacheSize(1)
                .withSegmentCacheSize(256)
                .withStringCacheSize(0)
                .withTemplateCacheSize(0)
                .withStatisticsProvider(new DefaultStatisticsProvider(executor));
        if (standby) {
            SegmentGCOptions gcOptions = SegmentGCOptions.defaultGCOptions()
                .setRetainedGenerations(1);
            builder
                .withGCOptions(gcOptions)
                .withSnfeListener(SegmentNotFoundExceptionListener.IGNORE_SNFE)
                .withEagerSegmentCaching(true);
        }
        if (blobStore != null) {
            builder.withBlobStore(blobStore.blobStore());
        }
        store = builder.build();
    }

    @Override
    protected void before() throws Throwable {
        // delay initialisation until the store is accessed
    }

    @Override
    protected void after() {
        try {
            if (store != null) {
                store.close();
            }
        } finally {
            new ExecutorCloser(executor).close();
        }
    }

    public FileStore fileStore() {
        // We use the local variable so we access the volatile {this.store} only once.
        // see: https://stackoverflow.com/a/3580658
        FileStore store = this.store;
        if (store != null) {
            return store;
        }
        synchronized (this) {
            if (this.store == null) {
                try {
                    init();
                } catch (InvalidFileStoreVersionException | IOException e) {
                    throw new IllegalStateException("Initialisation failed", e);
                }
            }
            return this.store;
        }
    }

}
