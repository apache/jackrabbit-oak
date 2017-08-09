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

import static java.io.File.createTempFile;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;

public class BenchmarkBase {
    static final int port = Integer.getInteger("standby.server.port", 52800);
    static final String LOCALHOST = "127.0.0.1";
    static final int MB = 1024 * 1024;
    static final int timeout = Integer.getInteger("standby.test.timeout", 500);

    File directoryS;
    FileStore storeS;
    ScheduledExecutorService executorS;

    File directoryC;
    FileStore storeC;
    ScheduledExecutorService executorC;

    public void setUpServerAndClient() throws Exception {
        directoryS = createTmpTargetDir(getClass().getSimpleName() + "-Server");
        executorS = Executors.newSingleThreadScheduledExecutor();
        storeS = setupPrimary(directoryS, executorS);

        // client
        directoryC = createTmpTargetDir(getClass().getSimpleName() + "-Client");
        executorC = Executors.newSingleThreadScheduledExecutor();
        storeC = setupSecondary(directoryC, executorC);
    }

    public void closeServerAndClient() {
        storeS.close();
        storeC.close();
        
        try {
            FileUtils.deleteDirectory(directoryS);
            FileUtils.deleteDirectory(directoryC);
        } catch (IOException e) {
            // ignore
        } finally {
            if (executorS != null) {
                new ExecutorCloser(executorS).close();
            }

            if (executorC != null) {
                new ExecutorCloser(executorC).close();
            }
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

    protected FileStore setupPrimary(File directory, ScheduledExecutorService executor) throws Exception {
        return newFileStore(directory, executor);
    }

    protected FileStore setupSecondary(File directory, ScheduledExecutorService executor) throws Exception {
        return newFileStore(directory, executor);
    }

    public StandbyClientSync newStandbyClientSync(FileStore store) throws Exception {
        return newStandbyClientSync(store, port, false);
    }

    public StandbyClientSync newStandbyClientSync(FileStore store, int port) throws Exception {
        return newStandbyClientSync(store, port, false);
    }

    public StandbyClientSync newStandbyClientSync(FileStore store, int port, boolean secure) throws Exception {
        return new StandbyClientSync(LOCALHOST, port, store, secure, timeout, false);
    }

    private static File createTmpTargetDir(String name) throws IOException {
        File f = createTempFile(name, "dir", new File("target"));
        f.delete();
        f.mkdir();
        return f;
    }
}
