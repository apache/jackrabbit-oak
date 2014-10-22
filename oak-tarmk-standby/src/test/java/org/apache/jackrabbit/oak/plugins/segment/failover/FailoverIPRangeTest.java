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
package org.apache.jackrabbit.oak.plugins.segment.failover;

import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.failover.client.FailoverClient;
import org.apache.jackrabbit.oak.plugins.segment.failover.server.FailoverServer;

import org.apache.jackrabbit.oak.spi.state.NodeStore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.segment.SegmentTestUtils.addTestContent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class FailoverIPRangeTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        setUpServerAndClient();
    }

    @After
    public void after() {
        closeServerAndClient();
    }

    @Test
    public void testFailoverAllClients() throws Exception {
        createTestWithConfig(null, true);
    }

    @Test
    public void testFailoverLocalClient() throws Exception {
        createTestWithConfig(new String[]{"127.0.0.1"}, true);
    }

    @Test
    public void testFailoverLocalClientUseIPv6() throws Exception {
        if (!noDualStackSupport) {
            createTestWithConfig("::1", new String[]{"::1"}, true);
        }
    }

    @Test
    public void testFailoverWrongClient() throws Exception {
        createTestWithConfig(new String[]{"127.0.0.2"}, false);
    }

    @Test
    public void testFailoverWrongClientIPv6() throws Exception {
        if (!noDualStackSupport) {
            createTestWithConfig(new String[]{"::2"}, false);
        }
    }

    @Test
    public void testFailoverLocalhost() throws Exception {
        createTestWithConfig(new String[]{"localhost"}, true);
    }

    @Test
    public void testFailoverInvalidName() throws Exception {
        createTestWithConfig(new String[]{"foobar"}, false);
    }

    @Test
    public void testFailoverValidIPRangeStart() throws Exception {
        createTestWithConfig(new String[]{"127.0.0.1-127.0.0.2"}, true);
    }

    @Test
    public void testFailoverValidIPRangeEnd() throws Exception {
        createTestWithConfig(new String[]{"127.0.0.0-127.0.0.1"}, true);
    }

    @Test
    public void testFailoverValidIPRange() throws Exception {
        createTestWithConfig(new String[]{"127.0.0.0-127.0.0.2"}, true);
    }

    @Test
    public void testFailoverInvalidRange() throws Exception {
        createTestWithConfig(new String[]{"127.0.0.2-127.0.0.1"}, false);
    }

    @Test
    public void testFailoverCorrectList() throws Exception {
        createTestWithConfig(new String[]{"foobar","127-128","126.0.0.1", "127.0.0.0-127.255.255.255"}, true);
    }

    @Test
    public void testFailoverCorrectListIPv6() throws Exception {
        if (!noDualStackSupport) {
            createTestWithConfig(new String[]{"foobar", "122-126", "::1", "126.0.0.1", "127.0.0.0-127.255.255.255"}, true);
        }
    }

    @Test
    public void testFailoverWrongList() throws Exception {
        createTestWithConfig(new String[]{"foobar", "126.0.0.1", "::2", "128.0.0.1-255.255.255.255", "128.0.0.0-127.255.255.255"}, false);
    }

    @Test
    public void testFailoverCorrectListUseIPv6() throws Exception {
        if (!noDualStackSupport) {
            createTestWithConfig("::1", new String[]{"foobar","127-128", "0:0:0:0:0:0:0:1", "126.0.0.1", "127.0.0.0-127.255.255.255"}, true);
        }
    }

    @Test
    public void testFailoverCorrectListIPv6UseIPv6() throws Exception {
        if (!noDualStackSupport) {
            createTestWithConfig("::1", new String[]{"foobar", "122-126", "::1", "126.0.0.1", "127.0.0.0-127.255.255.255"}, true);
        }
    }

    @Test
    public void testFailoverWrongListUseIPv6() throws Exception {
        if (!noDualStackSupport) {
            createTestWithConfig("::1", new String[]{"foobar", "126.0.0.1", "::2", "128.0.0.1-255.255.255.255", "128.0.0.0-127.255.255.255"}, false);
        }
    }

    private void createTestWithConfig(String[] ipRanges, boolean expectedToWork) throws Exception {
        createTestWithConfig("127.0.0.1", ipRanges, expectedToWork);
    }

    private void createTestWithConfig(String host, String[] ipRanges, boolean expectedToWork) throws Exception {
        NodeStore store = new SegmentNodeStore(storeS);
        final FailoverServer server = new FailoverServer(port, storeS, ipRanges);
        server.start();
        addTestContent(store, "server");
        storeS.flush();  // this speeds up the test a little bit...

        FailoverClient cl = new FailoverClient(host, port, storeC);
        cl.run();

        try {
            if (expectedToWork) {
                assertEquals(storeS.getHead(), storeC.getHead());
            }
            else {
                assertFalse("stores are equal but shouldn't!", storeS.getHead().equals(storeC.getHead()));
            }
        } finally {
            server.close();
            cl.close();
        }

    }

}
