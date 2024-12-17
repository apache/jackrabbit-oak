/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.mongo;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.apache.jackrabbit.oak.commons.CIHelper;
import org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.SimpleRecoveryHandler;
import org.apache.jackrabbit.oak.plugins.document.TestUtils;
import org.apache.jackrabbit.oak.plugins.document.spi.lease.LeaseFailureHandler;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

import static eu.rekawek.toxiproxy.model.ToxicDirection.DOWNSTREAM;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.setClusterNodeInfoClock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class LeaseUpdateSocketTimeoutIT {

    private static final DockerImageName TOXIPROXY_IMAGE = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.11.0");

    private static final int MONGODB_DEFAULT_PORT = 27017;

    private static final int LEASE_SO_TIMEOUT = 50;

    @Rule
    public Network network = Network.newNetwork();

    @Rule
    public MongoDBContainer mongoDBContainer = new MongoDBContainer(MongoDockerRule.getDockerImageName())
            .withNetwork(network)
            .withNetworkAliases("mongo")
            .withExposedPorts(MONGODB_DEFAULT_PORT);

    @Rule
    public ToxiproxyContainer tp = new ToxiproxyContainer(TOXIPROXY_IMAGE)
            .withNetwork(network);

    private Proxy proxy;

    private Clock clock;

    private DocumentStore store;

    private final FailureHandler handler = new FailureHandler();

    @BeforeClass
    public static void dockerAvailable() {
        assumeTrue(MongoDockerRule.isDockerAvailable());
        assumeFalse(CIHelper.jenkins()); // OAK-9443
    }

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        setClusterNodeInfoClock(clock);
        ToxiproxyClient toxiproxyClient = new ToxiproxyClient(tp.getHost(), tp.getControlPort());
        proxy = toxiproxyClient.createProxy("mongo", "0.0.0.0:8666", "mongo:" + MONGODB_DEFAULT_PORT);
        String uri = "mongodb://" + tp.getHost() + ":" + tp.getMappedPort(8666);
        store = new MongoDocumentNodeStoreBuilder()
                .setMongoDB(uri, "oak", 0)
                .setLeaseSocketTimeout(LEASE_SO_TIMEOUT)
                .getDocumentStore();
    }

    @After
    public void after() {
        store.dispose();
        TestUtils.resetClusterNodeInfoClockToDefault();
    }

    @Test
    public void leaseUpdateFailureOnSocketTimeout() throws Exception {
        ClusterNodeInfo info = newClusterNodeInfo();
        waitLeaseUpdateInterval();
        CountDownLatch latch = new CountDownLatch(1);
        List<Exception> exceptions = new ArrayList<>();
        Thread t = new Thread(() -> {
            try {
                proxy.toxics().latency("latency", DOWNSTREAM, LEASE_SO_TIMEOUT * 2);
                latch.countDown();
                Thread.sleep(1000);
                proxy.toxics().get("latency").remove();
            } catch (Exception e) {
                exceptions.add(e);
            }
        });
        t.start();
        latch.await();
        try {
            info.renewLease();
            fail("must fail with DocumentStoreException");
        } catch (DocumentStoreException e) {
            // expected
            assertRootException(e, SocketTimeoutException.class);
        }
        t.join();

        for (Exception e : exceptions) {
            fail(e.getMessage());
        }

        long leaseEnd = info.getLeaseEndTime();
        // must succeed next time
        waitLeaseUpdateInterval();
        assertTrue(info.renewLease());
        assertTrue(info.getLeaseEndTime() > leaseEnd);
        assertFalse(handler.isLeaseFailure());
    }

    private void assertRootException(Throwable t,
                                     Class<?> clazz) {
        while (t.getCause() != null) {
            t = t.getCause();
        }
        assertEquals(clazz, t.getClass());
    }

    private ClusterNodeInfo newClusterNodeInfo() {
        ClusterNodeInfo info = ClusterNodeInfo.getInstance(store,
                new SimpleRecoveryHandler(store, clock), null, null, 1);
        info.setLeaseFailureHandler(handler);
        return info;
    }

    private void waitLeaseUpdateInterval() throws Exception {
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS + 1);
    }

    static final class FailureHandler implements LeaseFailureHandler {

        private final AtomicBoolean leaseFailure = new AtomicBoolean();

        @Override
        public void handleLeaseFailure() {
            leaseFailure.set(true);
        }

        public boolean isLeaseFailure() {
            return leaseFailure.get();
        }
    }
}
