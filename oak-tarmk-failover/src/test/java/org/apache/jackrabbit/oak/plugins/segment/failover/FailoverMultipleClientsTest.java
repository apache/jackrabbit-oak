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
import org.apache.jackrabbit.oak.plugins.segment.SegmentTestUtils;
import org.apache.jackrabbit.oak.plugins.segment.failover.client.FailoverClient;
import org.apache.jackrabbit.oak.plugins.segment.failover.server.FailoverServer;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class FailoverMultipleClientsTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        setUpServerAndTwoClients();
    }

    @After
    public void after() {
        closeServerAndTwoClients();
    }

    @Test
    public void testMultipleClients() throws Exception {
        NodeStore store = new SegmentNodeStore(storeS);
        final FailoverServer server = new FailoverServer(port, storeS);
        server.start();
        SegmentTestUtils.addTestContent(store, "server");

        FailoverClient cl1 = new FailoverClient("127.0.0.1", port, storeC);
        FailoverClient cl2 = new FailoverClient("127.0.0.1", port, storeC2);

        try {
            assertFalse("first client has invalid initial store!", storeS.getHead().equals(storeC.getHead()));
            assertFalse("second client has invalid initial store!", storeS.getHead().equals(storeC2.getHead()));
            assertEquals(storeC.getHead(), storeC2.getHead());

            cl1.run();
            cl2.run();

            assertEquals(storeS.getHead(), storeC.getHead());
            assertEquals(storeS.getHead(), storeC2.getHead());

            cl1.stop();
            SegmentTestUtils.addTestContent(store, "test");
            cl2.run();

            assertEquals(storeS.getHead(), storeC2.getHead());
            Assert.assertFalse("first client updated in stopped state!", storeS.getHead().equals(storeC.getHead()));

            cl1.start();
            assertEquals(storeS.getHead(), storeC.getHead());
        } finally {
            server.close();
            cl1.close();
            cl2.close();
        }
    }

}
