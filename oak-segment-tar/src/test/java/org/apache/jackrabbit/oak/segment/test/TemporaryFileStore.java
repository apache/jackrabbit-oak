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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class TemporaryFileStore extends ExternalResource {

    private final TemporaryFolder folder;

    private final TemporaryBlobStore blobStore;

    private ScheduledExecutorService executor;

    private FileStore store;

    public TemporaryFileStore(TemporaryFolder folder) {
        this.folder = folder;
        this.blobStore = null;
    }

    public TemporaryFileStore(TemporaryFolder folder, TemporaryBlobStore blobStore) {
        this.folder = folder;
        this.blobStore = blobStore;
    }

    @Override
    protected void before() throws Throwable {
        executor = Executors.newSingleThreadScheduledExecutor();
        FileStoreBuilder builder = fileStoreBuilder(folder.newFolder())
                .withMaxFileSize(1)
                .withMemoryMapping(false)
                .withNodeDeduplicationCacheSize(1)
                .withSegmentCacheSize(0)
                .withStringCacheSize(0)
                .withTemplateCacheSize(0)
                .withStatisticsProvider(new DefaultStatisticsProvider(executor));
        if (blobStore != null) {
            builder.withBlobStore(blobStore.blobStore());
        }
        store = builder.build();
    }

    @Override
    protected void after() {
        try {
            store.close();
        } finally {
            new ExecutorCloser(executor).close();
        }
    }

    public FileStore fileStore() {
        return store;
    }

}
