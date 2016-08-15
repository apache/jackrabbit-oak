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
package org.apache.jackrabbit.oak.plugins.segment.standby;

import static org.apache.jackrabbit.oak.plugins.segment.SegmentTestUtils.createTmpTargetDir;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.jackrabbit.oak.commons.CIHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper;
import org.apache.jackrabbit.oak.commons.FixturesHelper.Fixture;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.plugins.segment.standby.client.StandbyClient;
import org.junit.BeforeClass;

public class TestBase {

    static final int port = Integer.getInteger("standby.server.port",
            52800);

    static final int proxyPort = Integer.getInteger(
            "standby.proxy.port", 51913);

    final static String LOCALHOST = "127.0.0.1";

    static final int timeout = Integer.getInteger("standby.test.timeout", 500);

    private static final Set<Fixture> FIXTURES = FixturesHelper.getFixtures();

    File directoryS;
    FileStore storeS;

    File directoryC;
    FileStore storeC;

    File directoryC2;
    FileStore storeC2;

    /*
     Java 6 on Windows doesn't support dual IP stacks, so we will skip our IPv6
     tests.
    */
    protected final boolean noDualStackSupport = SystemUtils.IS_OS_WINDOWS && SystemUtils.IS_JAVA_1_6;

    @BeforeClass
    public static void assumptions() {
        assumeTrue(!CIHelper.travis());
        assumeTrue(FIXTURES.contains(Fixture.SEGMENT_MK));
    }

    public void setUpServerAndClient() throws Exception {
        // server
        directoryS = createTmpTargetDir(getClass().getSimpleName()+"-Server");
        storeS = setupPrimary(directoryS);

        // client
        directoryC = createTmpTargetDir(getClass().getSimpleName()+"-Client");
        storeC = setupSecondary(directoryC);
    }

    private static FileStore newFileStore(File directory) throws Exception {
        return FileStore.builder(directory)
            .withMaxFileSize(1)
            .withMemoryMapping(false)
            .withNoCache()
            .build();
    }

    protected FileStore setupPrimary(File directory) throws Exception {
        return newFileStore(directory);
    }

    protected FileStore getPrimary() {
        return storeS;
    }

    protected FileStore setupSecondary(File directory) throws Exception {
        return newFileStore(directoryC);
    }

    protected FileStore getSecondary() {
        return storeC;
    }

    public void setUpServerAndTwoClients() throws Exception {
        setUpServerAndClient();

        directoryC2 = createTmpTargetDir(getClass().getSimpleName()+"-Client2");
        storeC2 = newFileStore(directoryC2);
    }

    public void closeServerAndClient() {
        storeS.close();
        storeC.close();
        try {
            FileUtils.deleteDirectory(directoryS);
            FileUtils.deleteDirectory(directoryC);
        } catch (IOException e) {
        }
    }

    public void closeServerAndTwoClients() {
        closeServerAndClient();
        storeC2.close();
        try {
            FileUtils.deleteDirectory(directoryC2);
        } catch (IOException e) {
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
