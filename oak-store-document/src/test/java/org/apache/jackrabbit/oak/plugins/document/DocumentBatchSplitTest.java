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

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

/**
 * Check correct splitting of documents (OAK-926 & OAK-1342).
 */
@RunWith(Parameterized.class)
public class DocumentBatchSplitTest {

    private static final Logger LOG = LoggerFactory.getLogger(DocumentBatchSplitTest.class);

    private String createOrUpdateBatchSize;
    private boolean createOrUpdateBatchSizeIsNull;

    private DocumentStoreFixture fixture;
    protected DocumentMK mk;

    public DocumentBatchSplitTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters(name="{0}")
    public static java.util.Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = new ArrayList<>();
        fixtures.add(new Object[] {new DocumentStoreFixture.MemoryFixture()});

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if(mongo.isAvailable()){
            fixtures.add(new Object[] {mongo});
        }
        return fixtures;
    }

    @After
    public void tearDown() throws Exception {
        if (mk != null) {
            mk.dispose();
            mk = null;
        }
        fixture.dispose();
        // reset log level to default
        enableLevel("org", null);
    }

    @Before
    public void backupProperty() {
        createOrUpdateBatchSize = System.getProperty("oak.documentMK.createOrUpdateBatchSize");
        if (createOrUpdateBatchSize == null) {
            createOrUpdateBatchSizeIsNull = true;
        }
    }

    @After
    public void restoreProperty() {
        if (createOrUpdateBatchSize != null) {
            System.setProperty("oak.documentMK.createOrUpdateBatchSize", createOrUpdateBatchSize);
        } else if (createOrUpdateBatchSizeIsNull) {
            System.clearProperty("oak.documentMK.createOrUpdateBatchSize");
        }
    }

    @Test
    @Ignore(value = "useful for benchmarking only, long execution duration")
    public void batchSplitBenchmark() throws Exception {
        int[] batchSizes = new int[] {1,2,10,30,50,75,100,200,300,400,500,1000,2000,5000,10000};
        for (int batchSize : batchSizes) {
            batchSplitTest(batchSize, 10000);
            batchSplitTest(batchSize, 10000);
        }
    }

    @Test
    public void largeBatchSplit() throws Exception {
        batchSplitTest(200, 1000);
    }

    @Test
    public void mediumBatchSplit() throws Exception {
        batchSplitTest(50, 1000);
    }

    @Test
    public void smallBatchSplit() throws Exception {
        batchSplitTest(2, 1000);
    }

    @Test
    public void noBatchSplit() throws Exception {
        batchSplitTest(1, 1000);
    }

    /** Make sure we have a test that has log level set to DEBUG */
    @Test
    public void debugLogLevelBatchSplit() throws Exception {
        enableLevel("org", Level.DEBUG);
        batchSplitTest(50, 1000);
    }

    private void batchSplitTest(int batchSize, int splitDocCnt) throws Exception {
        LOG.info("batchSplitTest: batchSize = " + batchSize+ ", splitDocCnt = " + splitDocCnt +
                ", fixture = " + fixture);
        // this tests wants to use CountingDocumentStore
        // plus it wants to set the batchSize
        if (mk != null) {
            mk.dispose();
            mk = null;
        }
        if (fixture.getName().equals("MongoDB")) {
            MongoUtils.dropCollections(MongoUtils.DB);
        }

        System.setProperty("oak.documentMK.createOrUpdateBatchSize", String.valueOf(batchSize));

        DocumentMK.Builder mkBuilder = new DocumentMK.Builder();
        DocumentStore delegateStore = fixture.createDocumentStore();
        TimingDocumentStoreWrapper timingStore = new TimingDocumentStoreWrapper(delegateStore);
        CountingDocumentStore store = new CountingDocumentStore(timingStore);
        mkBuilder.setDocumentStore(store);
        // disable automatic background operations
        mkBuilder.setAsyncDelay(0);
        mk = mkBuilder.open();
        DocumentNodeStore ns = mk.getNodeStore();
        assertEquals(batchSize, ns.getCreateOrUpdateBatchSize());

        NodeBuilder builder = ns.getRoot().builder();
        for(int child = 0; child < 100; child++) {
            builder.child("testchild-" + child);
        }
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        for(int i=0; i<2; i++) {
            builder = ns.getRoot().builder();
            for(int child = 0; child < splitDocCnt; child++) {
                PropertyState binary = binaryProperty("prop", randomBytes(5 * 1024));
                builder.child("testchild-" + child).setProperty(binary);
            }
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        timingStore.reset();
        store.resetCounters();
        final long start = System.currentTimeMillis();
        ns.runBackgroundUpdateOperations();
        int createOrUpdateCalls = store.getNumCreateOrUpdateCalls(NODES);
        final long remoteCallMeasurement = timingStore.getAndResetOverallTime();
        final long totalSplitDuration = (System.currentTimeMillis() - start);
        final long localSplitPart = totalSplitDuration - remoteCallMeasurement;
        LOG.info("batchSplitTest: batchSize = " + batchSize +
                ", splitDocCnt = " + splitDocCnt +
                ", createOrUpdateCalls = " + createOrUpdateCalls +
                ", fixture = " + fixture.getName() +
                ", split total ms = " + (System.currentTimeMillis() - start) +
                " (thereof local = " + localSplitPart +
                ", remote = " + remoteCallMeasurement + ")");
        int expected = 2 * (splitDocCnt / batchSize)       /* 2 calls per batch */
                + 2 * Math.min(1, splitDocCnt % batchSize) /* 1 additional pair for the last batch */
                + 1;                                       /* 1 more for backgroundWrite's update to root */
        assertTrue("batchSize = " + batchSize
                + ", splitDocCnt = " + splitDocCnt
                + ", expected=" + expected
                + ", createOrUpdates=" + createOrUpdateCalls,
                createOrUpdateCalls >= expected && createOrUpdateCalls <= expected + 2);
        VersionGarbageCollector gc = ns.getVersionGarbageCollector();

        int actualSplitDocGCCount = 0;
        long timeout = ns.getClock().getTime() + 20000;
        while(actualSplitDocGCCount < splitDocCnt && ns.getClock().getTime() < timeout) {
            VersionGCStats stats = gc.gc(1, TimeUnit.MILLISECONDS);
            actualSplitDocGCCount += stats.splitDocGCCount;
            if (actualSplitDocGCCount != splitDocCnt) {
                LOG.info("batchSplitTest: Expected " + splitDocCnt + ", actual " + actualSplitDocGCCount);
                // advance time a bit to ensure gc does clean up the split docs
                ns.getClock().waitUntil(ns.getClock().getTime() + 1000);
                ns.runBackgroundUpdateOperations();
            }
        }

        // make sure those splitDocCnt split docs are deleted
        assertTrue("gc not as expected: expected " + splitDocCnt
                + ", got " + actualSplitDocGCCount, splitDocCnt <= actualSplitDocGCCount);

        mk.dispose();
        mk = null;
    }

    private byte[] randomBytes(int num) {
        Random random = new Random(42);
        byte[] data = new byte[num];
        random.nextBytes(data);
        return data;
    }

    // TODO: from DocumentStoreStatsTest
    // but there are various places such as RevisionsCommand, BroadcastTest that have similar code.
    // we might want to move this to a new common util/helper
    private static void enableLevel(String logName, Level level){
        ((LoggerContext)LoggerFactory.getILoggerFactory())
                .getLogger(logName).setLevel(level);
    }
}
