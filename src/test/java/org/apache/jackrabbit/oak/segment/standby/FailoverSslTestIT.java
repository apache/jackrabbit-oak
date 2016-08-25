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
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClient;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServer;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FailoverSslTestIT extends TestBase {

    @Before
    public void setUp() throws Exception {
        setUpServerAndClient();
    }

    @After
    public void after() {
        closeServerAndClient();
    }

    @Test
    public void testFailoverSecure() throws Exception {

        NodeStore store = SegmentNodeStoreBuilders.builder(storeS).build();
        final StandbyServer server = new StandbyServer(port, storeS, true);
        server.start();
        addTestContent(store, "server");
        storeS.flush();  // this speeds up the test a little bit...

        StandbyClient cl = newStandbyClient(storeC, port, true);
        cl.run();

        try {
            assertEquals(storeS.getHead(), storeC.getHead());
        } finally {
            server.close();
            cl.close();
        }
    }

    @Test
    public void testFailoverSecureServerPlainClient() throws Exception {

        NodeStore store = SegmentNodeStoreBuilders.builder(storeS).build();
        final StandbyServer server = new StandbyServer(port, storeS, true);
        server.start();
        addTestContent(store, "server");
        storeS.flush();  // this speeds up the test a little bit...

        StandbyClient cl = newStandbyClient(storeC);
        cl.run();

        try {
            assertFalse("stores are equal but shouldn't!", storeS.getHead().equals(storeC.getHead()));
        } finally {
            server.close();
            cl.close();
        }
    }

    @Test
    public void testFailoverPlainServerSecureClient() throws Exception {

        NodeStore store = SegmentNodeStoreBuilders.builder(storeS).build();
        final StandbyServer server = new StandbyServer(port, storeS);
        server.start();
        addTestContent(store, "server");
        storeS.flush();  // this speeds up the test a little bit...

        StandbyClient cl = newStandbyClient(storeC, port, true);
        cl.run();

        try {
            assertFalse("stores are equal but shouldn't!", storeS.getHead().equals(storeC.getHead()));
        } finally {
            server.close();
            cl.close();
        }
    }
}
