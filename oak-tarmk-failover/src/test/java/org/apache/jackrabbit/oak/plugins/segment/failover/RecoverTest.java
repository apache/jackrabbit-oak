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


import org.apache.jackrabbit.oak.plugins.segment.DebugSegmentStore;
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

public class RecoverTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        setUpServerAndClient();
    }

    @After
    public void after() {
        closeServerAndClient();
    }

    @Test
    public void testBrokenConnection() throws Exception {

        NodeStore store = new SegmentNodeStore(storeS);
        DebugSegmentStore s = new DebugSegmentStore(storeS);
        final FailoverServer server = new FailoverServer(port, s);
        s.createReadErrors = true;
        server.start();
        addTestContent(store, "server");

        FailoverClient cl = new FailoverClient("127.0.0.1", port, storeC);
        cl.run();

        try {
            assertFalse("store are not expected to be equal", storeS.getHead().equals(storeC.getHead()));
            s.createReadErrors = false;
            cl.run();
            assertEquals(storeS.getHead(), storeC.getHead());
        } finally {
            server.close();
            cl.close();
        }

    }
}
