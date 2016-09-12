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

package org.apache.jackrabbit.oak.segment.standby;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.lang3.SystemUtils;
import org.apache.jackrabbit.oak.commons.CIHelper;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClient;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class TestBase {

    static final int port = Integer.getInteger("standby.server.port",
            52800);

    static final int proxyPort = Integer.getInteger(
            "standby.proxy.port", 51913);

    final static String LOCALHOST = "127.0.0.1";

    static final int timeout = Integer.getInteger("standby.test.timeout", 500);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    FileStore storeS;
    ScheduledExecutorService executorS;

    FileStore storeC;
    ScheduledExecutorService executorC;

    FileStore storeC2;
    ScheduledExecutorService executorC2;

    /*
     Java 6 on Windows doesn't support dual IP stacks, so we will skip our IPv6
     tests.
    */
    protected final boolean noDualStackSupport = SystemUtils.IS_OS_WINDOWS && SystemUtils.IS_JAVA_1_6;

    @BeforeClass
    public static void assumptions() {
        assumeTrue(!CIHelper.travis());
    }

    public void setUpServerAndClient() throws Exception {
        executorS = Executors.newSingleThreadScheduledExecutor();
        storeS = setupPrimary(folder.newFolder("server"), executorS);

        // client
        executorC = Executors.newSingleThreadScheduledExecutor();
        storeC = setupSecondary(folder.newFolder("client-1"), executorC);
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

    protected FileStore getPrimary() {
        return storeS;
    }

    protected FileStore setupSecondary(File directory, ScheduledExecutorService executor) throws Exception {
        return newFileStore(directory, executor);
    }

    protected FileStore getSecondary() {
        return storeC;
    }

    public void setUpServerAndTwoClients() throws Exception {
        setUpServerAndClient();

        executorC2 = Executors.newSingleThreadScheduledExecutor();
        storeC2 = newFileStore(folder.newFolder("client-2"), executorC2);
    }

    public void closeServerAndClient() {
        if (storeS != null) {
            storeS.close();
        }

        if (storeC != null) {
            storeC.close();
        }

        if (executorS != null) {
            new ExecutorCloser(executorS).close();
        }

        if (executorC != null) {
            new ExecutorCloser(executorC).close();
        }
    }

    public void closeServerAndTwoClients() {
        closeServerAndClient();

        if (storeC2 != null) {
            storeC2.close();
        }

        if (executorC2 != null) {
            new ExecutorCloser(executorC2).close();
        }
    }

    public static int getTestTimeout() {
        return timeout;
    }

    public StandbyClient newStandbyClient(FileStore store) throws Exception {
        return newStandbyClient(store, port, false);
    }

    public StandbyClient newStandbyClient(FileStore store, int port)
            throws Exception {
        return newStandbyClient(store, port, false);
    }

    public StandbyClient newStandbyClient(FileStore store, int port,
            boolean secure) throws Exception {
        return new StandbyClient(LOCALHOST, port, store, secure, timeout, false);
    }

}
