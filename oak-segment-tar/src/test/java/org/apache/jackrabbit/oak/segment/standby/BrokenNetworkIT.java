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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class BrokenNetworkIT extends TestBase {

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryFileStore serverFileStore = new TemporaryFileStore(folder, false);

    private TemporaryFileStore clientFileStore1 = new TemporaryFileStore(folder, true);

    @Rule
    public RuleChain chain = RuleChain.outerRule(folder)
        .around(serverFileStore)
        .around(clientFileStore1);

    @Rule
    public TemporaryPort serverPort = new TemporaryPort();

    @Rule
    public TemporaryPort proxyPort = new TemporaryPort();

    @Test
    public void testProxy() throws Exception {
        FileStore serverStore = serverFileStore.fileStore();
        FileStore clientStore = clientFileStore1.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(serverStore).build();
        addTestContent(store, "server");
        serverStore.flush();

        try (
            StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), serverStore, MB, false);
            StandbyClientSync clientSync = new StandbyClientSync(getServerHost(), serverPort.getPort(), clientStore, false, getClientTimeout(), false, folder.newFolder());
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
            StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), storeS, MB, true);
            StandbyClientSync clientSync = new StandbyClientSync(getServerHost(), serverPort.getPort(), storeC, true, getClientTimeout(), false, folder.newFolder());
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
        FileStore serverStore = serverFileStore.fileStore();
        FileStore clientStore = clientFileStore1.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(serverStore).build();
        addTestContent(store, "server");
        serverStore.flush();

        try (StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), serverStore, MB, ssl)) {
            serverSync.start();

            File spoolFolder = folder.newFolder();

            try (
                NetworkErrorProxy ignored = new NetworkErrorProxy(proxyPort.getPort(), getServerHost(), serverPort.getPort(), flipPosition, skipPosition, skipBytes);
                StandbyClientSync clientSync = new StandbyClientSync(getServerHost(), proxyPort.getPort(), clientStore, ssl, getClientTimeout(), false, spoolFolder)
            ) {
                clientSync.run();
            }

            assertFalse("stores are equal", serverStore.getHead().equals(clientStore.getHead()));

            if (intermediateChange) {
                addTestContent(store, "server2");
                serverStore.flush();
            }

            try (StandbyClientSync clientSync = new StandbyClientSync(getServerHost(), serverPort.getPort(), clientStore, ssl, getClientTimeout(), false, spoolFolder)) {
                clientSync.run();
            }
        }

        assertEquals("stores are not equal", serverStore.getHead(), clientStore.getHead());
    }

}
