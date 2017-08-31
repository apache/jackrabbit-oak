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
package org.apache.jackrabbit.oak.segment.standby.benchmark;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;

class BenchmarkBase {

    private static File createTmpTargetDir(String name) throws IOException {
        return Files.createTempDirectory(Paths.get("target"), name).toFile();
    }

    private Closer closer;

    FileStore primaryStore;

    FileStore standbyStore;

    void setUpServerAndClient() throws Exception {
        closer = Closer.create();

        File primaryDirectory = createTmpTargetDir("primary-");
        closer.register(() -> FileUtils.deleteDirectory(primaryDirectory));

        ScheduledExecutorService primaryExecutor = Executors.newSingleThreadScheduledExecutor();
        closer.register(new ExecutorCloser(primaryExecutor));

        primaryStore = setupPrimary(primaryDirectory, primaryExecutor);
        closer.register(primaryStore);

        File standbyDirectory = createTmpTargetDir("standby-");
        closer.register(() -> FileUtils.deleteDirectory(standbyDirectory));

        ScheduledExecutorService standbyExecutor = Executors.newSingleThreadScheduledExecutor();
        closer.register(new ExecutorCloser(standbyExecutor));

        standbyStore = setupStandby(standbyDirectory, standbyExecutor);
        closer.register(standbyStore);
    }

    void closeServerAndClient() {
        try {
            closer.close();
        } catch (IOException e) {
            e.printStackTrace(System.err);
        }
    }

    private static FileStore newFileStore(File directory, ScheduledExecutorService executor) throws Exception {
        return fileStoreBuilder(directory)
                .withMaxFileSize(1)
                .withMemoryMapping(false)
                .withNodeDeduplicationCacheSize(1)
                .withSegmentCacheSize(0)
                .withStringCacheSize(0)
                .withTemplateCacheSize(0)
                .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                .build();
    }

    private FileStore setupPrimary(File directory, ScheduledExecutorService executor) throws Exception {
        return newFileStore(directory, executor);
    }

    private FileStore setupStandby(File directory, ScheduledExecutorService executor) throws Exception {
        return newFileStore(directory, executor);
    }

}
