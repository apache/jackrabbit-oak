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

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.lang.management.ManagementFactory;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClient;
import org.apache.jackrabbit.oak.segment.standby.jmx.StandbyStatusMBean;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServer;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class BulkTest extends TestBase {

    @Before
    public void setUp() throws Exception {
        setUpServerAndClient();
    }

    @After
    public void after() {
        closeServerAndClient();
    }

    @Test
    @Ignore("OAK-4707")
    public void test100Nodes() throws Exception {
        test(100, 1, 1, 3000, 3100);
    }

    @Test
    @Ignore("OAK-4707")
    public void test1000Nodes() throws Exception {
        test(1000, 1, 1, 53000, 55000);
    }

    @Test
    @Ignore("OAK-4707")
    public void test10000Nodes() throws Exception {
        test(10000, 1, 1, 245000, 246000);
    }

    @Test
    @Ignore("OAK-4707")
    public void test100000Nodes() throws Exception {
        test(100000, 9, 9, 2210000, 2220000);
    }

    @Test
    @Ignore("OAK-4707")
    public void test1MillionNodes() throws Exception {
        test(1000000, 87, 87, 22700000, 22800000);
    }

    @Test
    @Ignore("OAK-4707")
    public void test1MillionNodesUsingSSL() throws Exception {
        test(1000000, 87, 87, 22700000, 22800000, true);
    }

/*
    @Test
    public void test10MillionNodes() throws Exception {
        test(10000000, 856, 856, 223000000, 224000000);
    }
*/

    // private helper

    private void test(int number, int minExpectedSegments, int maxExpectedSegments, long minExpectedBytes, long maxExpectedBytes) throws Exception {
        test(number, minExpectedSegments, maxExpectedSegments, minExpectedBytes, maxExpectedBytes, false);
    }

    private void test(int number, int minExpectedSegments, int maxExpectedSegments, long minExpectedBytes, long maxExpectedBytes,
                      boolean useSSL) throws Exception {
        NodeStore store = SegmentNodeStoreBuilders.builder(storeS).build();
        NodeBuilder rootbuilder = store.getRoot().builder();
        NodeBuilder b = rootbuilder.child("store");
        for (int j=0; j<=number / 1000; j++) {
            NodeBuilder builder = b.child("Folder#" + j);
            for (int i = 0; i <(number < 1000 ? number : 1000); i++) {
                builder.child("Test#" + i).setProperty("ts", System.currentTimeMillis());
            }
        }
        store.merge(rootbuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        storeS.flush();

        final StandbyServer server = new StandbyServer(port, storeS, useSSL);
        server.start();

        System.setProperty(StandbyClient.CLIENT_ID_PROPERTY_NAME, "Bar");
        StandbyClient cl = newStandbyClient(storeC, port, useSSL);

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=*");
        ObjectName clientStatus = new ObjectName(cl.getMBeanName());
        ObjectName serverStatus = new ObjectName(server.getMBeanName());

        long start = System.currentTimeMillis();
        cl.run();

        try {
            Set<ObjectName> instances = jmxServer.queryNames(status, null);
            assertEquals(3, instances.size());

            ObjectName connectionStatus = null;
            for (ObjectName s : instances) {
                if (!s.equals(clientStatus) && !s.equals(serverStatus)) connectionStatus = s;
            }
            assertNotNull(connectionStatus);

            long segments = ((Long)jmxServer.getAttribute(connectionStatus, "TransferredSegments")).longValue();
            long bytes = ((Long)jmxServer.getAttribute(connectionStatus, "TransferredSegmentBytes")).longValue();

            System.out.println("did transfer " + segments + " segments with " + bytes + " bytes in " + (System.currentTimeMillis() - start) / 1000 + " seconds.");

            assertEquals(storeS.getHead(), storeC.getHead());

            //compare(segments, "segment", minExpectedSegments, maxExpectedSegments);
            //compare(bytes, "byte", minExpectedBytes, maxExpectedBytes);

        } finally {
            server.close();
            cl.close();
        }
    }

    private void compare(long current, String unit, long expectedMin, long expectedMax) {
        assertTrue("current number of " + unit + "s (" + current + ") is less than minimum expected: " + expectedMin, current >= expectedMin);
        assertTrue("current number of " + unit + "s (" + current + ") is bigger than maximum expected: " + expectedMax, current <= expectedMax);
    }
}
