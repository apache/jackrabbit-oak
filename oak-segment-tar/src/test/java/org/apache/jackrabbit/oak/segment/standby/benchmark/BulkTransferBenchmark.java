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

package org.apache.jackrabbit.oak.segment.standby.benchmark;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.jmx.StandbyStatusMBean;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class BulkTransferBenchmark extends BenchmarkBase {

    public void setUp() throws Exception {
        setUpServerAndClient();
    }

    public void after() {
        closeServerAndClient();
    }

    public void test100Nodes() throws Exception {
        test(100, 1, 1, 3000, 3100);
    }

    public void test1000Nodes() throws Exception {
        test(1000, 1, 1, 53000, 55000);
    }

    public void test10000Nodes() throws Exception {
        test(10000, 1, 1, 245000, 246000);
    }

    public void test100000Nodes() throws Exception {
        test(100000, 9, 9, 2210000, 2220000);
    }

    public void test1MillionNodes() throws Exception {
        test(1000000, 87, 87, 22700000, 22800000);
    }

    public void test1MillionNodesUsingSSL() throws Exception {
        test(1000000, 87, 87, 22700000, 22800000, true);
    }

    public void test10MillionNodes() throws Exception {
        test(10000000, 856, 856, 223000000, 224000000);
    }

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

        final StandbyServerSync serverSync = new StandbyServerSync(port, storeS, 1 * MB, useSSL);
        serverSync.start();

        System.setProperty(StandbyClientSync.CLIENT_ID_PROPERTY_NAME, "Bar");
        StandbyClientSync clientSync = newStandbyClientSync(storeC, port, useSSL);

        final MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName status = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=*");
        ObjectName clientStatus = new ObjectName(clientSync.getMBeanName());
        ObjectName serverStatus = new ObjectName(serverSync.getMBeanName());

        long start = System.currentTimeMillis();
        clientSync.run();

        try {
            Set<ObjectName> instances = jmxServer.queryNames(status, null);
            ObjectName connectionStatus = null;
            for (ObjectName s : instances) {
                if (!s.equals(clientStatus) && !s.equals(serverStatus)) connectionStatus = s;
            }
            assert(connectionStatus != null);

            long segments = ((Long)jmxServer.getAttribute(connectionStatus, "TransferredSegments")).longValue();
            long bytes = ((Long)jmxServer.getAttribute(connectionStatus, "TransferredSegmentBytes")).longValue();

            System.out.println("did transfer " + segments + " segments with " + bytes + " bytes in " + (System.currentTimeMillis() - start) / 1000 + " seconds.");
        } finally {
            serverSync.close();
            clientSync.close();
        }
    }
    
    public static void main(String[] args) {
        BulkTransferBenchmark benchmark = new BulkTransferBenchmark();
        
        String[] methodNames = new String[] {
                "test100Nodes",
                "test1000Nodes",
                "test10000Nodes",
                "test100000Nodes",
                "test1MillionNodes",
                "test1MillionNodesUsingSSL",
                "test10MillionNodes"
        };
        
        for (String methodName : methodNames) {
            try {
                Method method = benchmark.getClass().getMethod(methodName);
                
                benchmark.setUp();
                method.invoke(benchmark);
                benchmark.after();
            } catch (Exception e) {
                e.printStackTrace();
            } 
        }
    }
}
