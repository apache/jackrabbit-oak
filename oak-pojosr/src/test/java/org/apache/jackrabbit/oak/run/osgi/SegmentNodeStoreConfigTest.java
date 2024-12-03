/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.jackrabbit.oak.run.osgi;

import static org.junit.Assert.assertNotNull;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Assert;
import org.junit.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentNodeStoreConfigTest extends AbstractRepositoryFactoryTest {

    private static final Logger log = LoggerFactory.getLogger(SegmentNodeStoreConfigTest.class);

    @Test
    public void testDeadlock() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config);

        CountDownLatch deactivateLatch = new CountDownLatch(1);
        CountDownLatch trackerLatch = new CountDownLatch(1);
        CountDownLatch mainLatch = new CountDownLatch(1);
        final NodeStoreTracker tracker = new NodeStoreTracker(registry.getBundleContext(),
                trackerLatch,
                deactivateLatch,
                mainLatch);

        //1. Get NodeStore created
        createConfig(
                Map.of("org.apache.jackrabbit.oak.segment.SegmentNodeStoreService",
                        Map.of("cache", 256, "tarmk.mode", 32)));
        getServiceWithWait(NodeStore.class);
        assertNotNull(tracker.getStores());

        final CountDownLatch allWellLatch = new CountDownLatch(1);
        //4. Get roots and thus wait on latch holding the lock
        // Create a new thread
        new Thread(() -> {
            tracker.getRoots();
            allWellLatch.countDown();
        }).start();

        //3. Mutate the config. This results in NodeStore deactivate
        //which first wait for nodeStoreLatch and then on NodeStoreTracker lock
        createConfig(Map.of("org.apache.jackrabbit.oak.segment.SegmentNodeStoreService",
                Map.of("cache", 200, "tarmk.mode", 32)));

        //4. Wait for the "GetRoots" to indicate that it has acquired "trackerLock"
        mainLatch.await();

        Assert.assertNull("Deadlock detected", ManagementFactory.getThreadMXBean().findDeadlockedThreads());
        //5. Let NodeStore deactivate thread proceed and wait for "trackerLock"
        deactivateLatch.countDown();
        log.info("Letting the deactivate call proceed");

        //6. Let "GetRoots" thread proceed and make it fetch Root and thus wait for
        //lock in SegmentNodeStoreService
        trackerLatch.countDown();
        log.info("Letting the getRoots call proceed");

        Assert.assertNull("Deadlock detected", ManagementFactory.getThreadMXBean().findDeadlockedThreads());
        allWellLatch.await();
        tracker.close();
    }

    @Override
    protected PojoServiceRegistry getRegistry() {
        return registry;
    }

    private PojoServiceRegistry registry;

    private static class NodeStoreTracker extends ServiceTracker<NodeStore, NodeStore> {

        public NodeStoreTracker(BundleContext context, CountDownLatch trackerLatch, CountDownLatch deactivateLatch,
                CountDownLatch mainLatch) {
            super(context, NodeStore.class.getName(), null);
            this.trackerLatch = trackerLatch;
            this.deactivateLatch = deactivateLatch;
            this.mainLatch = mainLatch;
            super.open();
        }

        @Override
        public void removedService(ServiceReference<NodeStore> reference, NodeStore service) {
            try {
                deactivateLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            synchronized (trackerLock) {
                stores.remove(reference);
            }

            super.removedService(reference, service);
        }

        public List<NodeState> getRoots() {
            List<NodeState> result = new ArrayList<NodeState>();
            synchronized (trackerLock) {
                mainLatch.countDown();
                try {
                    trackerLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                for (NodeStore store : stores.values()) {
                    try {
                        result.add(store.getRoot());
                    } catch (Exception e) {
                        //Exception expected
                        log.info("Exception expected", e);
                    }

                }

            }

            return result;
        }

        public List<NodeStore> getStores() {
            List<NodeStore> result;
            synchronized (trackerLock) {
                result = new ArrayList<NodeStore>(stores.values());
            }

            return result;
        }

        private final Map<ServiceReference<NodeStore>, NodeStore> stores = Maps.newHashMap();
        private final Object trackerLock = new Object();
        private final CountDownLatch trackerLatch;
        private final CountDownLatch deactivateLatch;
        private final CountDownLatch mainLatch;
    }
}
