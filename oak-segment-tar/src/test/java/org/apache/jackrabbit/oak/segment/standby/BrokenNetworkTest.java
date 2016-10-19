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

import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BrokenNetworkTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        setUpServerAndTwoClients();
    }

    @After
    public void after() {
        closeServerAndTwoClients();
    }

    @Test
    public void testProxy() throws Exception {
        useProxy(false);
    }

    @Test
    public void testProxySSL() throws Exception {
        useProxy(true);
    }

    @Test
    public void testProxySkippedBytes() throws Exception {
        useProxy(false, 100, 1, false);
    }

    @Test
    public void testProxySSLSkippedBytes() throws Exception {
        useProxy(true, 400, 10, false);
    }

    @Test
    public void testProxySkippedBytesIntermediateChange() throws Exception {
        useProxy(false, 100, 1, true);
    }

    @Test
    public void testProxySSLSkippedBytesIntermediateChange() throws Exception {
        useProxy(true, 400, 10, true);
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

    // private helper

    private void useProxy(boolean ssl) throws Exception {
        useProxy(ssl, 0, 0, false);
    }

    private void useProxy(boolean ssl, int skipPosition, int skipBytes, boolean intermediateChange) throws Exception {
        useProxy(ssl, skipPosition, skipBytes, -1, intermediateChange);
    }

    private void useProxy(boolean ssl, int skipPosition, int skipBytes, int flipPosition, boolean intermediateChange) throws Exception {
        NodeStore store = SegmentNodeStoreBuilders.builder(storeS).build();
        addTestContent(store, "server");
        storeS.flush();  // this speeds up the test a little bit...

        try (
                NetworkErrorProxy p = new NetworkErrorProxy(proxyPort, LOCALHOST, port);
                StandbyServerSync serverSync = new StandbyServerSync(port, storeS, ssl);
                StandbyClientSync clientSync = newStandbyClientSync(storeC, proxyPort, ssl);
        ) {
            p.skipBytes(skipPosition, skipBytes);
            p.flipByte(flipPosition);
            p.run();

            serverSync.start();

            clientSync.run();

            if (skipBytes > 0 || flipPosition >= 0) {
                assertFalse("stores are not expected to be equal", storeS.getHead().equals(storeC.getHead()));
                assertEquals(storeC2.getHead(), storeC.getHead());

                p.reset();
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
