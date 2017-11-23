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
import java.util.Random;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.jmx.StandbyStatusMBean;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class BulkTransferBenchmark extends BenchmarkBase {

    private static final String HOST = "127.0.0.1";

    private static final int PORT = Integer.getInteger("standby.server.port", 52800);

    private static final int TIMEOUT = Integer.getInteger("standby.test.timeout", 500);

    private void test100Nodes() throws Exception {
        test("100 nodes", 100);
    }

    private void test1000Nodes() throws Exception {
        test("1K nodes", 1000);
    }

    private void test10000Nodes() throws Exception {
        test("10K nodes", 10000);
    }

    private void test100000Nodes() throws Exception {
        test("100K nodes", 100000);
    }

    private void test1MillionNodes() throws Exception {
        test("1M nodes", 1000000);
    }

    private void test1MillionNodesUsingSSL() throws Exception {
        test("1M nodes with SSL", 1000000, true);
    }

    private void test10MillionNodes() throws Exception {
        test("10M nodes", 10000000);
    }

    private void test(String name, int number) throws Exception {
        test(name, number, false);
    }

    private void createNodes(int nodeCount) throws Exception {
        NodeStore store = SegmentNodeStoreBuilders.builder(primaryStore).build();
        NodeBuilder rootBuilder = store.getRoot().builder();
        createNodes(rootBuilder.child("store"), nodeCount, new Random());
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        primaryStore.flush();
    }

    private static void createNodes(NodeBuilder builder, int nodeCount, Random random) {
        for (int j = 0; j <= nodeCount / 1000; j++) {
            NodeBuilder folder = builder.child("Folder#" + j);
            for (int i = 0; i < (nodeCount < 1000 ? nodeCount : 1000); i++) {
                folder.child("Test#" + i).setProperty("ts", random.nextLong());
            }
        }
    }

    private void test(String name, int nodeCount, boolean useSSL) throws Exception {
        createNodes(nodeCount);

        try (StandbyServerSync serverSync = new StandbyServerSync(PORT, primaryStore, 1024 * 1024, useSSL);
             StandbyClientSync clientSync = new StandbyClientSync(HOST, PORT, standbyStore, useSSL, TIMEOUT, false)) {
            serverSync.start();

            MBeanServer jmxServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName status = new ObjectName(StandbyStatusMBean.JMX_NAME + ",id=*");
            ObjectName clientStatus = new ObjectName(clientSync.getMBeanName());
            ObjectName serverStatus = new ObjectName(serverSync.getMBeanName());

            Stopwatch stopwatch = Stopwatch.createStarted();
            clientSync.run();
            stopwatch.stop();

            Set<ObjectName> instances = jmxServer.queryNames(status, null);
            ObjectName connectionStatus = null;
            for (ObjectName s : instances) {
                if (!s.equals(clientStatus) && !s.equals(serverStatus)) {
                    connectionStatus = s;
                }
            }
            assert (connectionStatus != null);

            long segments = (Long) jmxServer.getAttribute(connectionStatus, "TransferredSegments");
            long bytes = (Long) jmxServer.getAttribute(connectionStatus, "TransferredSegmentBytes");

            System.out.printf("%s: segments = %d, segments size = %d bytes, time = %s\n", name, segments, bytes, stopwatch);
        }
    }

    private interface Test {

        void run() throws Exception;

    }

    public static void main(String[] args) {
        BulkTransferBenchmark benchmark = new BulkTransferBenchmark();

        Test[] tests = new Test[] {
            benchmark::test100Nodes,
            benchmark::test1000Nodes,
            benchmark::test10000Nodes,
            benchmark::test100000Nodes,
            benchmark::test1MillionNodes,
            benchmark::test1MillionNodesUsingSSL,
            benchmark::test10MillionNodes
        };

        for (Test test : tests) {
            try {
                benchmark.setUpServerAndClient();
                test.run();
            } catch (Exception e) {
                e.printStackTrace(System.err);
            } finally {
                benchmark.closeServerAndClient();
            }
        }
    }

}
