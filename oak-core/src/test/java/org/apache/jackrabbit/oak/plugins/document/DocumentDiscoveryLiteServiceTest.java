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
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jcr.PropertyType;
import javax.jcr.Value;
import javax.jcr.ValueFormatException;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Descriptors;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.commons.junit.LogDumper;
import org.apache.jackrabbit.oak.commons.junit.LogLevelModifier;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.mongodb.DB;

import junitx.util.PrivateAccessor;

/**
 * Tests for the DocumentDiscoveryLiteService
 */
public class DocumentDiscoveryLiteServiceTest {

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

    /**
     * container for what should represent an instance, but is not a complete
     * one, hence 'simplified'. it contains most importantly the
     * DocuemntNodeStore and the discoveryLite service
     */
    class SimplifiedInstance {

        private DocumentDiscoveryLiteService service;
        private DocumentNodeStore ns;
        private final Descriptors descriptors;
        private Map<String, Object> registeredServices;
        private final long lastRevInterval;
        private volatile boolean lastRevStopped = false;
        private volatile boolean writeSimulationStopped = false;
        private Thread lastRevThread;
        private Thread writeSimulationThread;
        public String workingDir;

        SimplifiedInstance(DocumentDiscoveryLiteService service, DocumentNodeStore ns, Descriptors descriptors,
                Map<String, Object> registeredServices, long lastRevInterval, String workingDir) {
            this.service = service;
            this.ns = ns;
            this.workingDir = workingDir;
            this.descriptors = descriptors;
            this.registeredServices = registeredServices;
            this.lastRevInterval = lastRevInterval;
            if (lastRevInterval > 0) {
                startLastRevThread();
            }
        }

        @Override
        public String toString() {
            return "SimplifiedInstance[cid=" + ns.getClusterId() + "]";
        }

        private void startLastRevThread() {
            lastRevStopped = false;
            lastRevThread = new Thread(new Runnable() {

                @Override
                public void run() {
                    while (!lastRevStopped) {
                        SimplifiedInstance.this.ns.getLastRevRecoveryAgent().performRecoveryIfNeeded();
                        try {
                            Thread.sleep(SimplifiedInstance.this.lastRevInterval);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }

            });
            lastRevThread.setDaemon(true);
            lastRevThread.setName("lastRevThread[cid=" + ns.getClusterId() + "]");
            lastRevThread.start();
        }

        void stopLastRevThread() throws InterruptedException {
            lastRevStopped = true;
            lastRevThread.join();
        }

        boolean isFinal() throws Exception {
            final JsonObject clusterViewObj = getClusterViewObj();
            if (clusterViewObj == null) {
                throw new IllegalStateException("should always have that final flag set");
            }

            String finalStr = clusterViewObj.getProperties().get("final");

            return Boolean.valueOf(finalStr);
        }

        boolean hasActiveIds(String clusterViewStr, int... expected) throws Exception {
            return hasIds(clusterViewStr, "active", expected);
        }

        boolean hasDeactivatingIds(String clusterViewStr, int... expected) throws Exception {
            return hasIds(clusterViewStr, "deactivating", expected);
        }

        boolean hasInactiveIds(String clusterViewStr, int... expected) throws Exception {
            return hasIds(clusterViewStr, "inactive", expected);
        }

        private boolean hasIds(final String clusterViewStr, final String key, int... expectedIds) throws Exception {
            final JsonObject clusterViewObj = asJsonObject(clusterViewStr);
            String actualIdsStr = clusterViewObj == null ? null : clusterViewObj.getProperties().get(key);

            boolean actualEmpty = actualIdsStr == null || actualIdsStr.length() == 0 || actualIdsStr.equals("[]");
            boolean expectedEmpty = expectedIds == null || expectedIds.length == 0;

            if (actualEmpty && expectedEmpty) {
                return true;
            }
            if (actualEmpty != expectedEmpty) {
                return false;
            }

            final List<Integer> actualList = Arrays
                    .asList(ClusterViewDocument.csvToIntegerArray(actualIdsStr.substring(1, actualIdsStr.length() - 1)));
            if (expectedIds.length != actualList.size()) {
                return false;
            }
            for (int i = 0; i < expectedIds.length; i++) {
                int anExpectedId = expectedIds[i];
                if (!actualList.contains(anExpectedId)) {
                    return false;
                }
            }
            return true;
        }

        JsonObject getClusterViewObj() throws Exception {
            final String json = getClusterViewStr();
            return asJsonObject(json);
        }

        private JsonObject asJsonObject(final String json) {
            if (json == null) {
                return null;
            }
            JsopTokenizer t = new JsopTokenizer(json);
            t.read('{');
            JsonObject o = JsonObject.create(t);
            return o;
        }

        String getClusterViewStr() throws Exception {
            return getDescriptor(DocumentDiscoveryLiteService.OAK_DISCOVERYLITE_CLUSTERVIEW);
        }

        String getDescriptor(String key) throws Exception {
            final Value value = descriptors.getValue(key);
            if (value == null) {
                return null;
            }
            if (value.getType() != PropertyType.STRING) {
                return null;
            }
            try {
                return value.getString();
            } catch (ValueFormatException vfe) {
                return null;
            }
        }

        public void dispose() {
            logger.info("Disposing " + this);
            try {
                stopSimulatingWrites();
            } catch (InterruptedException e) {
                fail("interrupted");
            }
            if (lastRevThread != null) {
                try {
                    stopLastRevThread();
                } catch (InterruptedException ok) {
                    fail("interrupted");
                }
                lastRevThread = null;
            }
            if (service != null) {
                service.deactivate();
                service = null;
            }
            if (ns != null) {
                ns.dispose();
                ns = null;
            }
            if (registeredServices != null) {
                registeredServices.clear();
                registeredServices = null;
            }
        }

        /**
         * shutdown simulates the normal, graceful, shutdown
         * 
         * @throws InterruptedException
         */
        public void shutdown() throws InterruptedException {
            stopSimulatingWrites();
            stopLastRevThread();
            ns.dispose();
            service.deactivate();
        }

        /**
         * crash simulates a kill -9, sort of
         * 
         * @throws Throwable
         */
        public void crash() throws Throwable {
            logger.info("crash: stopping simulating writes...");
            stopSimulatingWrites();
            logger.info("crash: stopping lastrev thread...");
            stopLastRevThread();
            logger.info("crash: stopped lastrev thread, now setting lease to end within 1 sec");

            boolean renewed = setLeaseTime(1000 /* 1 sec */, 10 /*10ms*/);
            if (!renewed) {
                logger.info("halt");
                fail("did not renew clusterid lease");
            }

            logger.info("crash: now stopping background read/update");
            stopAllBackgroundThreads();
            // but don't do the following from DocumentNodeStore.dispose():
            // * don't do the last internalRunBackgroundUpdateOperations - as
            // we're trying to simulate a crash here
            // * don't dispose clusterNodeInfo to leave the node in active state

            // the DocumentDiscoveryLiteService currently can simply be
            // deactivated, doesn't differ much from crashing
            service.deactivate();
            logger.info("crash: crash simulation done.");
        }

        /**
         * very hacky way of doing the following: make sure this instance's
         * clusterNodes entry is marked with a very short (1 sec off) lease end
         * time so that the crash detection doesn't take a minute (as it would
         * by default)
         */
        private boolean setLeaseTime(final int leaseTime, final int leaseUpdateInterval) throws NoSuchFieldException {
            ns.getClusterInfo().setLeaseTime(leaseTime);
            ns.getClusterInfo().setLeaseUpdateInterval(leaseUpdateInterval);
            PrivateAccessor.setField(ns.getClusterInfo(), "leaseEndTime", System.currentTimeMillis() + (leaseTime / 3) - 10 /* 10ms safety margin */);
            boolean renewed = ns.renewClusterIdLease();
            return renewed;
        }

        private AtomicBoolean getIsDisposed() throws NoSuchFieldException {
            AtomicBoolean isDisposed = (AtomicBoolean) PrivateAccessor.getField(ns, "isDisposed");
            return isDisposed;
        }

        private void stopAllBackgroundThreads() throws NoSuchFieldException {
            // get all those background threads...
            Thread backgroundReadThread = (Thread) PrivateAccessor.getField(ns, "backgroundReadThread");
            assertNotNull(backgroundReadThread);
            Thread backgroundUpdateThread = (Thread) PrivateAccessor.getField(ns, "backgroundUpdateThread");
            assertNotNull(backgroundUpdateThread);
            Thread leaseUpdateThread = (Thread) PrivateAccessor.getField(ns, "leaseUpdateThread");
            assertNotNull(leaseUpdateThread);

            // start doing what DocumentNodeStore.dispose() would do - except do
            // it very fine controlled, basically:
            // make sure to stop backgroundReadThread, backgroundUpdateThread
            // and leaseUpdateThread
            // but then nothing else.
            final AtomicBoolean isDisposed = getIsDisposed();
            assertFalse(isDisposed.getAndSet(true));
            // notify background threads waiting on isDisposed
            synchronized (isDisposed) {
                isDisposed.notifyAll();
            }
            try {
                backgroundReadThread.join(5000);
                assertTrue(!backgroundReadThread.isAlive());
            } catch (InterruptedException e) {
                // ignore
            }
            try {
                backgroundUpdateThread.join(5000);
                assertTrue(!backgroundUpdateThread.isAlive());
            } catch (InterruptedException e) {
                // ignore
            }
            try {
                leaseUpdateThread.join(5000);
                assertTrue(!leaseUpdateThread.isAlive());
            } catch (InterruptedException e) {
                // ignore
            }
        }

        public void stopBgReadThread() throws NoSuchFieldException {
            final Thread backgroundReadThread = (Thread) PrivateAccessor.getField(ns, "backgroundReadThread");
            assertNotNull(backgroundReadThread);
            final Runnable bgReadRunnable = (Runnable) PrivateAccessor.getField(backgroundReadThread, "target");
            assertNotNull(bgReadRunnable);
            final AtomicBoolean bgReadIsDisposed = new AtomicBoolean(false);
            PrivateAccessor.setField(bgReadRunnable, "isDisposed", bgReadIsDisposed);
            assertFalse(bgReadIsDisposed.getAndSet(true));
            try {
                backgroundReadThread.join(5000);
                assertTrue(!backgroundReadThread.isAlive());
            } catch (InterruptedException e) {
                // ignore
            }
            // big of heavy work, but now the backgroundReadThread is stopped
            // and all the others are still running
        }

        public void addNode(String path) throws CommitFailedException {
            NodeBuilder root = ns.getRoot().builder();
            NodeBuilder child = root;
            String[] split = path.split("/");
            for (int i = 1; i < split.length; i++) {
                child = child.child(split[i]);
            }
            logger.info("addNode: " + ns.getClusterId() + " is merging path " + path);
            ns.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        public void setProperty(String path, String key, String value) throws CommitFailedException {
            NodeBuilder root = ns.getRoot().builder();
            NodeBuilder child = root;
            String[] split = path.split("/");
            for (int i = 1; i < split.length; i++) {
                child = child.child(split[i]);
            }
            child.setProperty(key, value);
            logger.info("setProperty: " + ns.getClusterId() + " is merging path/property " + path + ", key=" + key + ", value="
                    + value);
            ns.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        public void setLeastTimeout(long timeoutInMs, long updateIntervalInMs) throws NoSuchFieldException {
            ns.getClusterInfo().setLeaseTime(timeoutInMs);
            ns.getClusterInfo().setLeaseUpdateInterval(updateIntervalInMs);
            PrivateAccessor.setField(ns.getClusterInfo(), "leaseEndTime", System.currentTimeMillis() - 1000);
        }

        private void startSimulatingWrites(final long writeInterval) {
            writeSimulationStopped = false;
            writeSimulationThread = new Thread(new Runnable() {

                final Random random = new Random();

                @Override
                public void run() {
                    while (!writeSimulationStopped) {
                        try {
                            writeSomething();
                            Thread.sleep(SimplifiedInstance.this.lastRevInterval);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                private void writeSomething() throws CommitFailedException {
                    final String path = "/" + ns.getClusterId() + "/" + random.nextInt(100) + "/" + random.nextInt(100) + "/"
                            + random.nextInt(100);
                    logger.info("Writing [" + ns.getClusterId() + "]" + path);
                    addNode(path);
                }

            });
            writeSimulationThread.setDaemon(true);
            writeSimulationThread.start();
        }

        void stopSimulatingWrites() throws InterruptedException {
            writeSimulationStopped = true;
            if (writeSimulationThread != null) {
                writeSimulationThread.join();
            }
        }

        /** OAK-3292 : when on a machine without a mac address, the 'random:' prefix is used and instances
         * that have timed out are automagially removed by ClusterNodeInfo.createInstance - that poses
         * a problem to testing - so this method exposes whether the instance has such a 'random:' prefix
         * and thus allows to take appropriate action
         */
        public boolean hasRandomMachineId() {
            //TODO: this might not be the most stable way - but avoids having to change ClusterNodeInfo
            return ns.getClusterInfo().toString().contains("random:");
        }

    }

    interface Expectation {
        /**
         * check if the expectation is fulfilled, return true if it is, return a
         * descriptive error msg if not
         **/
        String fulfilled() throws Exception;
    }

    class ViewExpectation implements Expectation {

        private int[] activeIds;
        private int[] deactivatingIds;
        private int[] inactiveIds;
        private final SimplifiedInstance discoveryLiteCombo;
        private boolean isFinal = true;

        ViewExpectation(SimplifiedInstance discoveryLiteCombo) {
            this.discoveryLiteCombo = discoveryLiteCombo;
        }

        private int[] asIntArray(Integer[] arr) {
            int[] result = new int[arr.length];
            for (int i = 0; i < arr.length; i++) {
                result[i] = arr[i];
            }
            return result;
        }

        void setActiveIds(Integer[] activeIds) {
            this.activeIds = asIntArray(activeIds);
        }

        void setActiveIds(int... activeIds) {
            this.activeIds = activeIds;
        }

        void setDeactivatingIds(int... deactivatingIds) {
            this.deactivatingIds = deactivatingIds;
        }

        void setInactiveIds(Integer[] inactiveIds) {
            this.inactiveIds = asIntArray(inactiveIds);
        }

        void setInactiveIds(int... inaactiveIds) {
            this.inactiveIds = inaactiveIds;
        }

        @Override
        public String fulfilled() throws Exception {
            final String clusterViewStr = discoveryLiteCombo.getClusterViewStr();
            if (clusterViewStr == null) {
                if (activeIds.length != 0) {
                    return "no clusterView, but expected activeIds: " + beautify(activeIds);
                }
                if (deactivatingIds.length != 0) {
                    return "no clusterView, but expected deactivatingIds: " + beautify(deactivatingIds);
                }
                if (inactiveIds.length != 0) {
                    return "no clusterView, but expected inactiveIds: " + beautify(inactiveIds);
                }
            }
            if (!discoveryLiteCombo.hasActiveIds(clusterViewStr, activeIds)) {
                return "activeIds dont match, expected: " + beautify(activeIds) + ", got clusterView: " + clusterViewStr;
            }
            if (!discoveryLiteCombo.hasDeactivatingIds(clusterViewStr, deactivatingIds)) {
                return "deactivatingIds dont match, expected: " + beautify(deactivatingIds) + ", got clusterView: "
                        + clusterViewStr;
            }
            if (!discoveryLiteCombo.hasInactiveIds(clusterViewStr, inactiveIds)) {
                return "inactiveIds dont match, expected: " + beautify(inactiveIds) + ", got clusterView: " + clusterViewStr;
            }
            if (discoveryLiteCombo.isFinal() != isFinal) {
                return "final flag does not match. expected: " + isFinal + ", but is: " + discoveryLiteCombo.isFinal();
            }
            return null;
        }

        private String beautify(int[] ids) {
            if (ids == null) {
                return "";
            }
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < ids.length; i++) {
                if (i != 0) {
                    sb.append(",");
                }
                sb.append(ids[i]);
            }
            return sb.toString();
        }

        public void setFinal(boolean isFinal) {
            this.isFinal = isFinal;
        }

    }

//    private static final boolean MONGO_DB = true;
     private static final boolean MONGO_DB = false;

    private static final int SEED = Integer.getInteger(DocumentDiscoveryLiteServiceTest.class.getSimpleName() + "-seed",
            new Random().nextInt());

    private List<DocumentMK> mks = Lists.newArrayList();
    private MemoryDocumentStore ds;
    private MemoryBlobStore bs;

    final Logger logger = LoggerFactory.getLogger(this.getClass());

    private List<SimplifiedInstance> allInstances = new LinkedList<SimplifiedInstance>();

    @Rule
    public TestRule logDumper = new LogDumper(50000);

    @Rule
    public TestRule logLevelModifier = new LogLevelModifier()
                                            .newConsoleAppender("console")
                                            .addAppenderFilter("console", "info")
                                            .addAppenderFilter("file", "info")
                                            .setLoggerLevel("org.apache.jackrabbit.oak", "debug");

    @Test
    public void testActivateDeactivate() throws Exception {
        // then test normal start with a DocumentNodeStore
        DocumentMK mk1 = createMK(1, 0);
        DocumentDiscoveryLiteService discoveryLite = new DocumentDiscoveryLiteService();
        PrivateAccessor.setField(discoveryLite, "nodeStore", mk1.nodeStore);
        BundleContext bc = mock(BundleContext.class);
        ComponentContext c = mock(ComponentContext.class);
        when(c.getBundleContext()).thenReturn(bc);
        discoveryLite.activate(c);
        verify(c, times(0)).disableComponent(DocumentDiscoveryLiteService.COMPONENT_NAME);
        discoveryLite.deactivate();
    }

    // subsequent tests should get a DocumentDiscoveryLiteService setup from the
    // start
    private DocumentNodeStore createNodeStore(String workingDir) throws SecurityException, Exception {
        String prevWorkingDir = ClusterNodeInfo.WORKING_DIR;
        try {
            // ensure that we always get a fresh cluster[node]id
            ClusterNodeInfo.WORKING_DIR = workingDir;

            // then create the DocumentNodeStore
            DocumentMK mk1 = createMK(
                    0 /* to make sure the clusterNodes collection is used **/,
                    500 /* asyncDelay: background interval */);

            logger.info("createNodeStore: created DocumentNodeStore with cid=" + mk1.nodeStore.getClusterId() + ", workingDir="
                    + workingDir);
            return mk1.nodeStore;
        }
        finally {
            ClusterNodeInfo.WORKING_DIR = prevWorkingDir;
        }
    }

    private SimplifiedInstance createInstance() throws Exception {
        final String workingDir = UUID.randomUUID().toString();
        return createInstance(workingDir);
    }

    private SimplifiedInstance createInstance(String workingDir) throws SecurityException, Exception {
        DocumentNodeStore ns = createNodeStore(workingDir);
        return createInstance(ns, workingDir);
    }

    private SimplifiedInstance createInstance(DocumentNodeStore ns, String workingDir) throws NoSuchFieldException {
        DocumentDiscoveryLiteService discoveryLite = new DocumentDiscoveryLiteService();
        PrivateAccessor.setField(discoveryLite, "nodeStore", ns);
        BundleContext bc = mock(BundleContext.class);
        ComponentContext c = mock(ComponentContext.class);
        when(c.getBundleContext()).thenReturn(bc);
        final Map<String, Object> registeredServices = new HashMap<String, Object>();
        when(bc.registerService(anyString(), anyObject(), (Properties) anyObject())).then(new Answer<ServiceRegistration>() {
            @Override
            public ServiceRegistration answer(InvocationOnMock invocation) {
                registeredServices.put((String) invocation.getArguments()[0], invocation.getArguments()[1]);
                return null;
            }
        });
        discoveryLite.activate(c);
        Descriptors d = (Descriptors) registeredServices.get(Descriptors.class.getName());
        final SimplifiedInstance result = new SimplifiedInstance(discoveryLite, ns, d, registeredServices, 500, workingDir);
        allInstances.add(result);
        logger.info("Created " + result);
        return result;
    }

    private void waitFor(Expectation expectation, int timeout, String msg) throws Exception {
        final long tooLate = System.currentTimeMillis() + timeout;
        while (true) {
            final String fulfillmentResult = expectation.fulfilled();
            if (fulfillmentResult == null) {
                // everything's fine
                return;
            }
            if (System.currentTimeMillis() > tooLate) {
                fail("expectation not fulfilled within " + timeout + "ms: " + msg + ", fulfillment result: " + fulfillmentResult);
            }
            Thread.sleep(100);
        }
    }

    @Test
    public void testOneNode() throws Exception {
        final SimplifiedInstance s1 = createInstance();
        final ViewExpectation expectation = new ViewExpectation(s1);
        expectation.setActiveIds(s1.ns.getClusterId());
        waitFor(expectation, 2000, "see myself as active");
    }

    @Test
    public void testTwoNodesWithCleanShutdown() throws Exception {
        final SimplifiedInstance s1 = createInstance();
        final SimplifiedInstance s2 = createInstance();
        final ViewExpectation expectation1 = new ViewExpectation(s1);
        final ViewExpectation expectation2 = new ViewExpectation(s2);
        expectation1.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        expectation2.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        waitFor(expectation1, 2000, "first should see both as active");
        waitFor(expectation2, 2000, "second should see both as active");

        s2.shutdown();
        final ViewExpectation expectation1AfterShutdown = new ViewExpectation(s1);
        expectation1AfterShutdown.setActiveIds(s1.ns.getClusterId());
        expectation1AfterShutdown.setInactiveIds(s2.ns.getClusterId());
        waitFor(expectation1AfterShutdown, 2000, "first should only see itself after shutdown");
    }

    @Test
    public void testTwoNodesWithCrash() throws Throwable {
        final SimplifiedInstance s1 = createInstance();
        final SimplifiedInstance s2 = createInstance();
        final ViewExpectation expectation1 = new ViewExpectation(s1);
        final ViewExpectation expectation2 = new ViewExpectation(s2);
        expectation1.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        expectation2.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        waitFor(expectation1, 2000, "first should see both as active");
        waitFor(expectation2, 2000, "second should see both as active");

        s2.crash();

        final ViewExpectation expectation1AfterShutdown = new ViewExpectation(s1);
        expectation1AfterShutdown.setActiveIds(s1.ns.getClusterId());
        expectation1AfterShutdown.setInactiveIds(s2.ns.getClusterId());
        waitFor(expectation1AfterShutdown, 2000, "first should only see itself after shutdown");
    }

    @Test
    public void testTwoNodesWithCrashAndLongduringRecovery() throws Throwable {
        doTestTwoNodesWithCrashAndLongduringDeactivation(false);
    }

    @Test
    public void testTwoNodesWithCrashAndLongduringRecoveryAndBacklog() throws Throwable {
        doTestTwoNodesWithCrashAndLongduringDeactivation(true);
    }

    void doTestTwoNodesWithCrashAndLongduringDeactivation(boolean withBacklog) throws Throwable {
        final int TEST_WAIT_TIMEOUT = 10000;
        final SimplifiedInstance s1 = createInstance();
        final SimplifiedInstance s2 = createInstance();
        final ViewExpectation expectation1 = new ViewExpectation(s1);
        final ViewExpectation expectation2 = new ViewExpectation(s2);
        expectation1.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        expectation2.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        waitFor(expectation1, TEST_WAIT_TIMEOUT, "first should see both as active");
        waitFor(expectation2, TEST_WAIT_TIMEOUT, "second should see both as active");

        // before crashing s2, make sure that s1's lastRevRecovery thread
        // doesn't run
        s1.stopLastRevThread();
        if (withBacklog) {
            // plus also stop s1's backgroundReadThread - in case we want to
            // test backlog handling
            s1.stopBgReadThread();

            // and then, if we want to do backlog testing, then s2 should write
            // something
            // before it crashes, so here it comes:
            s2.addNode("/foo/bar");
            s2.setProperty("/foo/bar", "prop", "value");
        }

        // then crash s2
        s2.crash();

        // then wait 2 sec
        Thread.sleep(2000);

        // at this stage, while s2 has crashed, we have stopped s1's
        // lastRevRecoveryThread, so we should still see both as active
        logger.info(s1.getClusterViewStr());
        final ViewExpectation expectation1AfterCrashBeforeLastRevRecovery = new ViewExpectation(s1);
        expectation1AfterCrashBeforeLastRevRecovery.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        waitFor(expectation1AfterCrashBeforeLastRevRecovery, TEST_WAIT_TIMEOUT, "first should still see both as active");

        // the next part is a bit tricky: we want to fine-control the
        // lastRevRecoveryThread's acquire/release locking.
        // the chosen way to do this is to make heavy use of mockito and two
        // semaphores:
        // when acquireRecoveryLock is called, that thread should wait for the
        // waitBeforeLocking semaphore to be released
        final MissingLastRevSeeker missingLastRevUtil = (MissingLastRevSeeker) PrivateAccessor
                .getField(s1.ns.getLastRevRecoveryAgent(), "missingLastRevUtil");
        assertNotNull(missingLastRevUtil);
        MissingLastRevSeeker mockedLongduringMissingLastRevUtil = mock(MissingLastRevSeeker.class, delegatesTo(missingLastRevUtil));
        final Semaphore waitBeforeLocking = new Semaphore(0);
        doAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocation) throws Throwable {
                logger.info("going to waitBeforeLocking");
                waitBeforeLocking.acquire();
                logger.info("done with waitBeforeLocking");
                return missingLastRevUtil.acquireRecoveryLock((Integer) invocation.getArguments()[0],
                        (Integer) invocation.getArguments()[1]);
            }
        }).when(mockedLongduringMissingLastRevUtil).acquireRecoveryLock(anyInt(), anyInt());
        PrivateAccessor.setField(s1.ns.getLastRevRecoveryAgent(), "missingLastRevUtil", mockedLongduringMissingLastRevUtil);

        // so let's start the lastRevThread again and wait for that
        // waitBeforeLocking semaphore to be hit
        s1.startLastRevThread();
        waitFor(new Expectation() {

            @Override
            public String fulfilled() throws Exception {
                if (!waitBeforeLocking.hasQueuedThreads()) {
                    return "no thread queued";
                }
                return null;
            }

        }, TEST_WAIT_TIMEOUT, "lastRevRecoveryThread should acquire a lock");

        // at this stage the crashed s2 is still not in recovery mode, so let's
        // check:
        logger.info(s1.getClusterViewStr());
        final ViewExpectation expectation1AfterCrashBeforeLastRevRecoveryLocking = new ViewExpectation(s1);
        expectation1AfterCrashBeforeLastRevRecoveryLocking.setActiveIds(s1.ns.getClusterId(), s2.ns.getClusterId());
        waitFor(expectation1AfterCrashBeforeLastRevRecoveryLocking, TEST_WAIT_TIMEOUT, "first should still see both as active");

        // one thing, before we let the waitBeforeLocking go, setup the release
        // semaphore/mock:
        final Semaphore waitBeforeUnlocking = new Semaphore(0);
        Mockito.doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) throws InterruptedException {
                logger.info("Going to waitBeforeUnlocking");
                waitBeforeUnlocking.acquire();
                logger.info("Done with waitBeforeUnlocking");
                missingLastRevUtil.releaseRecoveryLock((Integer) invocation.getArguments()[0]);
                return null;
            }
        }).when(mockedLongduringMissingLastRevUtil).releaseRecoveryLock(anyInt());

        // let go (or tschaedere loh)
        waitBeforeLocking.release();

        // then, right after we let the waitBeforeLocking semaphore go, we
        // should see s2 in recovery mode
        final ViewExpectation expectation1AfterCrashWhileLastRevRecoveryLocking = new ViewExpectation(s1);
        expectation1AfterCrashWhileLastRevRecoveryLocking.setActiveIds(s1.ns.getClusterId());
        expectation1AfterCrashWhileLastRevRecoveryLocking.setDeactivatingIds(s2.ns.getClusterId());
        waitFor(expectation1AfterCrashWhileLastRevRecoveryLocking, TEST_WAIT_TIMEOUT, "first should still see s2 as recovering");

        // ok, meanwhile, the lastRevRecoveryAgent should have hit the ot
        waitFor(new Expectation() {

            @Override
            public String fulfilled() throws Exception {
                if (!waitBeforeUnlocking.hasQueuedThreads()) {
                    return "no thread queued";
                }
                return null;
            }

        }, TEST_WAIT_TIMEOUT, "lastRevRecoveryThread should want to release a lock");

        // so then, we should still see the same state
        waitFor(expectation1AfterCrashWhileLastRevRecoveryLocking, TEST_WAIT_TIMEOUT, "first should still see s2 as recovering");

        logger.info("Waiting 1,5sec");
        Thread.sleep(1500);
        logger.info("Waiting done");

        // first, lets check to see what the view looks like - should be
        // unchanged:
        waitFor(expectation1AfterCrashWhileLastRevRecoveryLocking, TEST_WAIT_TIMEOUT, "first should still see s2 as recovering");

        // let waitBeforeUnlocking go
        logger.info("releasing waitBeforeUnlocking, state: " + s1.getClusterViewStr());
        waitBeforeUnlocking.release();
        logger.info("released waitBeforeUnlocking");

        if (!withBacklog) {
            final ViewExpectation expectationWithoutBacklog = new ViewExpectation(s1);
            expectationWithoutBacklog.setActiveIds(s1.ns.getClusterId());
            expectationWithoutBacklog.setInactiveIds(s2.ns.getClusterId());
            waitFor(expectationWithoutBacklog, TEST_WAIT_TIMEOUT, "finally we should see s2 as completely inactive");
        } else {
            // wait just 2 sec to see if the bgReadThread is really stopped
            logger.info("sleeping 2 sec");
            Thread.sleep(2000);
            logger.info("sleeping 2 sec done, state: " + s1.getClusterViewStr());

            // when that's the case, check the view - it should now be in a
            // special 'final=false' mode
            final ViewExpectation expectationBeforeBgRead = new ViewExpectation(s1);
            expectationBeforeBgRead.setActiveIds(s1.ns.getClusterId());
            expectationBeforeBgRead.setDeactivatingIds(s2.ns.getClusterId());
            expectationBeforeBgRead.setFinal(false);
            waitFor(expectationBeforeBgRead, TEST_WAIT_TIMEOUT, "first should only see itself after shutdown");

            // ook, now we explicitly do a background read to get out of the
            // backlog situation
            s1.ns.runBackgroundReadOperations();

            final ViewExpectation expectationAfterBgRead = new ViewExpectation(s1);
            expectationAfterBgRead.setActiveIds(s1.ns.getClusterId());
            expectationAfterBgRead.setInactiveIds(s2.ns.getClusterId());
            waitFor(expectationAfterBgRead, TEST_WAIT_TIMEOUT, "finally we should see s2 as completely inactive");
        }
    }

    /**
     * This test creates a large number of documentnodestores which it starts,
     * runs, stops in a random fashion, always testing to make sure the
     * clusterView is correct
     */
    @Test
    public void testLargeStartStopFiesta() throws Throwable {
        logger.info("testLargeStartStopFiesta: start, seed="+SEED);
        final List<SimplifiedInstance> instances = new LinkedList<SimplifiedInstance>();
        final Map<Integer, String> inactiveIds = new HashMap<Integer, String>();
        final Random random = new Random(SEED);
        final int LOOP_CNT = 50; // with too many loops have also seen mongo
                                 // connections becoming starved thus test
                                 // failed
        final int CHECK_EVERY = 3;
        final int MAX_NUM_INSTANCES = 8;
        for (int i = 0; i < LOOP_CNT; i++) {
            if (i % CHECK_EVERY == 0) {
                checkFiestaState(instances, inactiveIds.keySet());
            }
            final int nextInt = random.nextInt(5);
            // logger.info("testLargeStartStopFiesta: iteration "+i+" with case
            // "+nextInt);
            String workingDir = UUID.randomUUID().toString();
            switch (nextInt) {
                case 0: {
                    // increase likelihood of creating instances..
                    // but reuse an inactive one if possible
                    if (inactiveIds.size() > 0) {
                        logger.info("Case 0 - reactivating an instance...");
                        final int n = random.nextInt(inactiveIds.size());
                        final Integer cid = new LinkedList<Integer>(inactiveIds.keySet()).get(n);
                        final String reactivatedWorkingDir = inactiveIds.remove(cid);
                        if (reactivatedWorkingDir == null) {
                            fail("reactivatedWorkingDir null for n=" + n + ", cid=" + cid + ", other inactives: " + inactiveIds);
                        }
                        assertNotNull(reactivatedWorkingDir);
                        logger.info("Case 0 - reactivated instance " + cid + ", workingDir=" + reactivatedWorkingDir);
                        workingDir = reactivatedWorkingDir;
                        logger.info("Case 0: creating instance");
                        final SimplifiedInstance newInstance = createInstance(workingDir);
                        if (newInstance.hasRandomMachineId()) {
                            // OAK-3292 : on an instance which has no networkInterface with a mac address,
                            // the machineId chosen by ClusterNodeInfo will be 'random:'.. and
                            // ClusterNodeInfo.createInstance will feel free to remove it when the lease
                            // has timed out
                            // that really renders it very difficult to continue testing here,
                            // since this test is all about keeping track who became inactive etc 
                            // and ClusterNodeInfo.createInstance removing it 'at a certain point' is difficult
                            // and not very useful to test..
                            //
                            // so: stop testing at this point:
                            return;
                        }
                        newInstance.setLeastTimeout(5000, 1000);
                        newInstance.startSimulatingWrites(500);
                        logger.info("Case 0: created instance: " + newInstance.ns.getClusterId());
                        if (newInstance.ns.getClusterId() != cid) {
                            logger.info(
                                    "Case 0: reactivated instance did not take over cid - probably a testing artifact. expected cid: {}, actual cid: {}",
                                    cid, newInstance.ns.getClusterId());
                            inactiveIds.put(cid, reactivatedWorkingDir);
                            // remove the newly reactivated from the inactives -
                            // although it shouldn't be there, it might!
                            inactiveIds.remove(newInstance.ns.getClusterId());
                        }
                        instances.add(newInstance);
                    }
                    break;
                }
                case 1: {
                    // creates a new instance
                    if (instances.size() < MAX_NUM_INSTANCES) {
                        logger.info("Case 1: creating instance");
                        final SimplifiedInstance newInstance = createInstance(workingDir);
                        if (newInstance.hasRandomMachineId()) {
                            // OAK-3292 : on an instance which has no networkInterface with a mac address,
                            // the machineId chosen by ClusterNodeInfo will be 'random:'.. and
                            // ClusterNodeInfo.createInstance will feel free to remove it when the lease
                            // has timed out
                            // that really renders it very difficult to continue testing here,
                            // since this test is all about keeping track who became inactive etc 
                            // and ClusterNodeInfo.createInstance removing it 'at a certain point' is difficult
                            // and not very useful to test..
                            //
                            // so: stop testing at this point:
                            return;
                        }
                        newInstance.setLeastTimeout(5000, 1000);
                        newInstance.startSimulatingWrites(500);
                        logger.info("Case 1: created instance: " + newInstance.ns.getClusterId());
                        instances.add(newInstance);
                        // OAK-3292 : in case a previously crashed or shut-down instance is created again here
                        //            make sure to remove it from inactive (if it in the inactive list at all)
                        inactiveIds.remove(newInstance.ns.getClusterId());
                    }
                    break;
                }
                case 2: {
                    // do nothing
                    break;
                }
                case 3: {
                    // shutdown instance
                    if (instances.size() > 1) {
                        // before shutting down: make sure we have a stable view
                        // (we could otherwise not correctly startup too)
                        checkFiestaState(instances, inactiveIds.keySet());
                        final SimplifiedInstance instance = instances.remove(random.nextInt(instances.size()));
                        assertNotNull(instance.workingDir);
                        logger.info("Case 3: Shutdown instance: " + instance.ns.getClusterId());
                        inactiveIds.put(instance.ns.getClusterId(), instance.workingDir);
                        instance.shutdown();
                    }
                    break;
                }
                case 4: {
                    // crash instance
                    if (instances.size() > 1) {
                        // before crashing make sure we have a stable view (we
                        // could otherwise not correctly startup too)
                        checkFiestaState(instances, inactiveIds.keySet());
                        final SimplifiedInstance instance = instances.remove(random.nextInt(instances.size()));
                        assertNotNull(instance.workingDir);
                        logger.info("Case 4: Crashing instance: " + instance.ns.getClusterId());
                        inactiveIds.put(instance.ns.getClusterId(), instance.workingDir);
                        instance.addNode("/" + instance.ns.getClusterId() + "/stuffForRecovery/" + random.nextInt(10000));
                        instance.crash();
                    }
                    break;
                }
            }
        }
    }

    private void dumpChildren(DocumentNodeState root) {
        logger.info("testEmptyParentRecovery: root: " + root);
        Iterator<String> it = root.getChildNodeNames().iterator();
        while (it.hasNext()) {
            String n = it.next();
            logger.info("testEmptyParentRecovery: a child: '" + n + "'");
        }
    }

    private void checkFiestaState(final List<SimplifiedInstance> instances, Set<Integer> inactiveIds) throws Exception {
        final List<Integer> activeIds = new LinkedList<Integer>();
        for (Iterator<SimplifiedInstance> it = instances.iterator(); it.hasNext();) {
            SimplifiedInstance anInstance = it.next();
            activeIds.add(anInstance.ns.getClusterId());
        }
        logger.info("checkFiestaState: checking state. expected active: "+activeIds+", inactive: "+inactiveIds);
        for (Iterator<SimplifiedInstance> it = instances.iterator(); it.hasNext();) {
            SimplifiedInstance anInstance = it.next();

            final ViewExpectation e = new ViewExpectation(anInstance);
            e.setActiveIds(activeIds.toArray(new Integer[activeIds.size()]));
            e.setInactiveIds(inactiveIds.toArray(new Integer[inactiveIds.size()]));
            waitFor(e, 60000, "checkFiestaState failed for " + anInstance + ", with instances: " + instances + ", and inactiveIds: "
                    + inactiveIds);
        }
    }

    @Before
    @After
    public void clear() {
        logger.info("clear: seed="+SEED);
        for (SimplifiedInstance i : allInstances) {
            i.dispose();
        }
        for (DocumentMK mk : mks) {
            mk.dispose();
        }
        mks.clear();
        if (MONGO_DB) {
            MongoConnection connection = connectionFactory.getConnection();
            if (connection != null) {
                DB db = connection.getDB();
                if (db != null) {
                    MongoUtils.dropCollections(db);
                }
            }
        }
    }

    private DocumentMK createMK(int clusterId, int asyncDelay) {
        if (MONGO_DB) {
            DB db = connectionFactory.getConnection().getDB();
            return register(new DocumentMK.Builder().setMongoDB(db).setLeaseCheck(false).setClusterId(clusterId)
                    .setAsyncDelay(asyncDelay).open());
        } else {
            if (ds == null) {
                ds = new MemoryDocumentStore();
            }
            if (bs == null) {
                bs = new MemoryBlobStore();
            }
            return createMK(clusterId, asyncDelay, ds, bs);
        }
    }

    private DocumentMK createMK(int clusterId, int asyncDelay, DocumentStore ds, BlobStore bs) {
        return register(new DocumentMK.Builder().setDocumentStore(ds).setBlobStore(bs).setClusterId(clusterId).setLeaseCheck(false)
                .setAsyncDelay(asyncDelay).open());
    }

    private DocumentMK register(DocumentMK mk) {
        mks.add(mk);
        return mk;
    }

}
