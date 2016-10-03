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

public class RecoverTestIT extends TestBase {

    @Before
    public void setUp() throws Exception {
        setUpServerAndClient();
    }

    @After
    public void after() {
        closeServerAndClient();
    }

    @Test
    public void testLocalChanges() throws Exception {

        NodeStore store = SegmentNodeStoreBuilders.builder(storeC).build();
        addTestContent(store, "client");

        final StandbyServerSync serverSync = new StandbyServerSync(port, storeS);
        serverSync.start();
        store = SegmentNodeStoreBuilders.builder(storeS).build();
        addTestContent(store, "server");
        storeS.flush();

        StandbyClientSync cl = newStandbyClientSync(storeC);
        try {
            assertFalse("stores are not expected to be equal", storeS.getHead().equals(storeC.getHead()));
            cl.run();
            assertEquals(storeS.getHead(), storeC.getHead());
        } finally {
            serverSync.close();
            cl.close();
        }

    }
}
