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

package org.apache.jackrabbit.oak.run.osgi

import com.google.common.collect.Lists
import com.google.common.collect.Maps
import org.apache.felix.connect.launch.PojoServiceRegistry
import org.apache.jackrabbit.oak.spi.state.NodeState
import org.apache.jackrabbit.oak.spi.state.NodeStore
import org.junit.Test
import org.osgi.framework.BundleContext
import org.osgi.framework.ServiceReference
import org.osgi.util.tracker.ServiceTracker

import java.lang.management.ManagementFactory
import java.util.concurrent.CountDownLatch

import static org.junit.Assert.assertNull

class SegmentNodeStoreConfigTest extends AbstractRepositoryFactoryTest {

    private PojoServiceRegistry registry

    @Test
    public void testDeadlock() throws Exception {
        registry = repositoryFactory.initializeServiceRegistry(config)


        CountDownLatch deactivateLatch = new CountDownLatch(1)
        CountDownLatch trackerLatch = new CountDownLatch(1)
        CountDownLatch mainLatch = new CountDownLatch(1)
        NodeStoreTracker tracker = new NodeStoreTracker(registry.getBundleContext(), trackerLatch, deactivateLatch, mainLatch)

        //1. Get NodeStore created
        createConfig([
                'org.apache.jackrabbit.oak.segment.SegmentNodeStoreService': [
                        cache: 256,
                        "tarmk.mode": 32
                ]
        ])
        getServiceWithWait(NodeStore.class)
        assert tracker.stores

        CountDownLatch allWellLatch = new CountDownLatch(1)
        //4. Get roots and thus wait on latch holding the lock
        Thread.start("GetRoots") { tracker.getRoots(); allWellLatch.countDown() }

        //3. Mutate the config. This results in NodeStore deactivate
        //which first wait for nodeStoreLatch and then on NodeStoreTracker lock
        createConfig([
                'org.apache.jackrabbit.oak.segment.SegmentNodeStoreService': [
                        cache: 200,
                        "tarmk.mode": 32
                ]
        ])

        //4. Wait for the "GetRoots" to indicate that it has acquired "trackerLock"
        mainLatch.await()

        assertNull("Deadlock detected", ManagementFactory.getThreadMXBean().findDeadlockedThreads())
        //5. Let NodeStore deactivate thread proceed and wait for "trackerLock"
        deactivateLatch.countDown()
        println ("Letting the deactivate call proceed")

        //6. Let "GetRoots" thread proceed and make it fetch Root and thus wait for
        //lock in SegmentNodeStoreService
        trackerLatch.countDown()
        println ("Letting the getRoots call proceed")

        assertNull("Deadlock detected", ManagementFactory.getThreadMXBean().findDeadlockedThreads())
        allWellLatch.await()
        tracker.close()
    }

    @Override
    protected PojoServiceRegistry getRegistry() {
        return registry
    }

    private static class NodeStoreTracker extends ServiceTracker<NodeStore, NodeStore> {
        private final Map<ServiceReference<NodeStore>, NodeStore> stores = Maps.newHashMap();
        private final Object trackerLock = new Object();
        private final CountDownLatch trackerLatch;
        private final CountDownLatch deactivateLatch;
        private final CountDownLatch mainLatch;

        NodeStoreTracker(BundleContext context,
                         CountDownLatch trackerLatch,
                         CountDownLatch deactivateLatch,
                         CountDownLatch mainLatch) {
            super(context, NodeStore.class.name, null)
            this.trackerLatch = trackerLatch
            this.deactivateLatch = deactivateLatch
            this.mainLatch = mainLatch
            super.open();
        }

        @Override
        void removedService(ServiceReference<NodeStore> reference, NodeStore service) {
            deactivateLatch.await()
            synchronized (trackerLock) {
                stores.remove(reference)
            }
            super.removedService(reference, service)
        }

        @Override
        NodeStore addingService(ServiceReference<NodeStore> reference) {
            NodeStore store = super.addingService(reference)
            synchronized (trackerLock) {
                stores.put(reference, store)
            }
            return store;
        }

        public List<NodeState> getRoots() {
            List<NodeState> result = Lists.newArrayList();
            synchronized (trackerLock) {
                mainLatch.countDown()
                trackerLatch.await()

                for (NodeStore store : stores.values()) {
                    try {
                        result.add(store.getRoot());
                    } catch (Exception e) {
                        //Exception expected
                        e.printStackTrace()
                    }
                }
            }
            return result;
        }

        public List<NodeStore> getStores() {
            List<NodeStore> result = Lists.newArrayList();
            synchronized (trackerLock) {
                result.addAll(stores.values())
            }
            return result;
        }
    }
}
