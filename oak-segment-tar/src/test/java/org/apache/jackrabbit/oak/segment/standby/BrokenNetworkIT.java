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

import static org.apache.jackrabbit.oak.segment.SegmentTestUtils.addTestContent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;

import org.apache.jackrabbit.oak.commons.junit.TemporaryPort;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.apache.jackrabbit.oak.segment.test.TemporaryFileStore;
import org.apache.jackrabbit.oak.segment.test.proxy.NetworkErrorProxy;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class BrokenNetworkIT extends TestBase {

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryFileStore serverFileStore = new TemporaryFileStore(folder, false);

    private TemporaryFileStore clientFileStore1 = new TemporaryFileStore(folder, true);

    private TemporaryFileStore clientFileStore2 = new TemporaryFileStore(folder, true);

    private NetworkErrorProxy proxy;

    @Rule
    public RuleChain chain = RuleChain.outerRule(folder)
            .around(serverFileStore)
            .around(clientFileStore1);

    @Rule
    public TemporaryPort serverPort = new TemporaryPort();

    @Rule
    public TemporaryPort proxyPort = new TemporaryPort();

    @Before
    public void beforeClass() {
        proxy = new NetworkErrorProxy(proxyPort.getPort(), getServerHost(), serverPort.getPort());
    }

    @After
    public void afterClass() {
        proxy.close();
    }

    @Test
    public void testProxy() throws Exception {
        FileStore serverStore = serverFileStore.fileStore();
        FileStore clientStore = clientFileStore1.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(serverStore).build();
        addTestContent(store, "server");
        serverStore.flush();

        try (
                StandbyServerSync serverSync = StandbyServerSync.builder()
                        .withPort(serverPort.getPort())
                        .withFileStore(serverStore)
                        .withBlobChunkSize(MB)
                        .withSecureConnection(false)
                        .build();
                StandbyClientSync clientSync = StandbyClientSync.builder()
                        .withHost(getServerHost())
                        .withPort(serverPort.getPort())
                        .withFileStore(clientStore)
                        .withSecureConnection(false)
                        .withReadTimeoutMs(getClientTimeout())
                        .withAutoClean(false)
                        .withSpoolFolder(folder.newFolder())
                        .build()
        ) {
            serverSync.start();
            clientSync.run();
        }

        assertEquals(serverStore.getHead(), clientStore.getHead());
    }

    @Test
    public void testProxySSL() throws Exception {
        FileStore storeS = serverFileStore.fileStore();
        FileStore storeC = clientFileStore1.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(storeS).build();
        addTestContent(store, "server");
        storeS.flush();

        try (
                StandbyServerSync serverSync = StandbyServerSync.builder()
                        .withPort(serverPort.getPort())
                        .withFileStore(storeS)
                        .withBlobChunkSize(MB)
                        .withSecureConnection(true)
                        .build();
                StandbyClientSync clientSync = StandbyClientSync.builder()
                        .withHost(getServerHost())
                        .withPort(serverPort.getPort())
                        .withFileStore(storeC)
                        .withSecureConnection(true)
                        .withReadTimeoutMs(getClientTimeout())
                        .withAutoClean(false)
                        .withSpoolFolder(folder.newFolder())
                        .build()
        ) {
            serverSync.start();
            clientSync.run();
        }

        assertEquals(storeS.getHead(), storeC.getHead());
    }

    @Test
    public void testProxySkippedBytes() throws Exception {
        useProxy(false, 100, 1, -1, false);
    }

    @Test
    public void testProxySSLSkippedBytes() throws Exception {
        useProxy(true, 400, 10, -1, false);
    }

    @Test
    public void testProxySkippedBytesIntermediateChange() throws Exception {
        useProxy(false, 100, 1, -1, true);
    }

    @Test
    public void testProxySSLSkippedBytesIntermediateChange() throws Exception {
        useProxy(true, 400, 10, -1, true);
    }

    @Test
    public void testProxyFlippedStartByte() throws Exception {
        useProxy(false, 0, 0, 0, false);
    }

    @Test
    public void testProxyFlippedStartByteSSL() throws Exception {
        useProxy(true, 0, 0, 0, false);
    }

    @Test
    public void testProxyFlippedIntermediateByte() throws Exception {
        useProxy(false, 0, 0, 150, false);
    }

    @Test
    public void testProxyFlippedIntermediateByteSSL() throws Exception {
        useProxy(true, 0, 0, 560, false);
    }

    @Test
    public void testProxyFlippedEndByte() throws Exception {
        useProxy(false, 0, 0, 220, false);
    }

    @Test
    public void testProxyFlippedEndByteSSL() throws Exception {
        useProxy(true, 0, 0, 575, false);
    }

    private void useProxy(boolean ssl, int skipPosition, int skipBytes, int flipPosition, boolean intermediateChange) throws Exception {
        FileStore storeS = serverFileStore.fileStore();
        FileStore storeC = clientFileStore1.fileStore();
        FileStore storeC2 = clientFileStore2.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(storeS).build();
        addTestContent(store, "server");
        storeS.flush();  // this speeds up the test a little bit...

        try (
                StandbyServerSync serverSync =  StandbyServerSync.builder()
                        .withPort(serverPort.getPort())
                        .withFileStore(storeS)
                        .withSecureConnection(ssl)
                        .build();
                StandbyClientSync clientSync = StandbyClientSync.builder()
                        .withPort(proxyPort.getPort())
                        .withFileStore(storeC)
                        .withSecureConnection(ssl)
                        .build();
        ) {
            proxy.skipBytes(skipPosition, skipBytes);
            proxy.flipByte(flipPosition);
            proxy.connect();

            serverSync.start();

            clientSync.run();

            if (skipBytes > 0 || flipPosition >= 0) {
                assertFalse("stores are not expected to be equal", storeS.getHead().equals(storeC.getHead()));
                assertEquals(storeC2.getHead(), storeC.getHead());

                proxy.reset();
                if (intermediateChange) {
                    addTestContent(store, "server2");
                    storeS.flush();
                }
                clientSync.run();
            }

            assertEquals(storeS.getHead(), storeC.getHead());
        }
    }

}