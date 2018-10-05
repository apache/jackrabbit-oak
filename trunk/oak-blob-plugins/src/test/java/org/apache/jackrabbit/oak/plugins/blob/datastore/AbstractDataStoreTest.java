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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.jcr.RepositoryException;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.core.data.RandomInputStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test base class for {@link DataStore} which covers all scenarios.
 * Copied from {@link org.apache.jackrabbit.core.data.TestCaseBase}.
 */
public abstract class AbstractDataStoreTest {

    /**
     * Logger
     */
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractDataStoreTest.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    /**
     * length of record to be added
     */
    protected int dataLength = 123456;

    /**
     * datastore directory path
     */
    protected String dataStoreDir;

    protected DataStore ds;

    /**
     * Random number generator to populate data
     */
    protected Random randomGen = new Random();

    /**
     * Delete temporary directory.
     */
    @Before
    public void setUp() throws Exception {
        dataStoreDir = folder.newFolder().getAbsolutePath();
        ds = createDataStore();
    }

    @After
    public void tearDown() {
        try {
            ds.close();
        } catch (DataStoreException e) {
            LOG.info("Error in close ds", e);
        }
    }

    /**
     * Testcase to validate {@link DataStore#addRecord(InputStream)} API.
     */
    @Test
    public void testAddRecord() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#addRecord, testDir=" + dataStoreDir);
            doAddRecordTest();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#addRecord finished, time taken = ["
                + (System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error:", e);
            fail(e.getMessage());
        }
    }

    /**
     * Testcase to validate {@link DataStore#getRecord(DataIdentifier)} API.
     */
    @Test
    public void testGetRecord() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testGetRecord, testDir=" + dataStoreDir);
            doGetRecordTest();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testGetRecord finished, time taken = ["
                + (System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error:", e);
        }
    }
    
    /**
     * Testcase to validate {@link DataStore#getAllIdentifiers()} API.
     */
    @Test
    public void testGetAllIdentifiers() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testGetAllIdentifiers, testDir=" + dataStoreDir);
            doGetAllIdentifiersTest();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testGetAllIdentifiers finished, time taken = ["
                + (System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error:", e);
            fail(e.getMessage());
        }
    }

    /**
     * Testcase to validate {@link DataStore#updateModifiedDateOnAccess(long)}
     * API.
     */
    @Test
    public void testUpdateLastModifiedOnAccess() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testUpdateLastModifiedOnAccess, testDir=" + dataStoreDir);
            doUpdateLastModifiedOnAccessTest();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testUpdateLastModifiedOnAccess finished, time taken = ["
                + (System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error:", e);
            fail(e.getMessage());
        }
    }

    /**
     * Testcase to validate
     * {@link MultiDataStoreAware#deleteRecord(DataIdentifier)}.API.
     */
    @Test
    public void testDeleteRecord() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testDeleteRecord, testDir=" + dataStoreDir);
            doDeleteRecordTest();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testDeleteRecord finished, time taken = ["
                + (System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error:", e);
            fail(e.getMessage());
        }
    }

    /**
     * Testcase to validate {@link DataStore#deleteAllOlderThan(long)} API.
     */
    @Test
    public void testDeleteAllOlderThan() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testDeleteAllOlderThan, testDir=" + dataStoreDir);
            doDeleteAllOlderThan();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testDeleteAllOlderThan finished, time taken = ["
                + (System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error:", e);
            fail(e.getMessage());
        }
    }

    /**
     * Testcase to validate {@link DataStore#getRecordFromReference(String)}
     */
    @Test
    public void testReference() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testReference, testDir=" + dataStoreDir);
            doReferenceTest();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testReference finished, time taken = ["
                + (System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error:", e);
            fail(e.getMessage());
        }
    }

    /**
     * Testcase to validate mixed scenario use of {@link DataStore}.
     */
    @Test
    public void testSingleThread() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testSingleThread, testDir=" + dataStoreDir);
            doTestSingleThread();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testSingleThread finished, time taken = ["
                + (System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error:", e);
            fail(e.getMessage());
        }
    }

    /**
     * Testcase to validate mixed scenario use of {@link DataStore} in
     * multi-threaded concurrent environment.
     */
    @Test
    public void testMultiThreaded() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testMultiThreaded, testDir=" + dataStoreDir);
            doTestMultiThreaded();
            LOG.info("Testcase: " + this.getClass().getName()
                + "#testMultiThreaded finished, time taken = ["
                + (System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error:", e);
            fail(e.getMessage());
        }

    }

    protected abstract DataStore createDataStore() throws RepositoryException ;

    /**
     * Test {@link DataStore#addRecord(InputStream)} and assert length of added
     * record.
     */
    protected void doAddRecordTest() throws Exception {
        byte[] data = new byte[dataLength];
        randomGen.nextBytes(data);
        DataRecord rec = ds.addRecord(new ByteArrayInputStream(data));
        Assert.assertEquals(data.length, rec.getLength());
        assertRecord(data, rec);
    }

    /**
     * Test {@link DataStore#getRecord(DataIdentifier)} and assert length and
     * inputstream.
     */
    protected void doGetRecordTest() throws Exception {
        byte[] data = new byte[dataLength];
        randomGen.nextBytes(data);
        DataRecord rec = ds.addRecord(new ByteArrayInputStream(data));
        rec = ds.getRecord(rec.getIdentifier());
        Assert.assertEquals(data.length, rec.getLength());
        assertRecord(data, rec);
    }

    /**
     * Test {@link MultiDataStoreAware#deleteRecord(DataIdentifier)}.
     */
    protected void doDeleteRecordTest() throws Exception {
        Random random = randomGen;
        byte[] data1 = new byte[dataLength];
        random.nextBytes(data1);
        DataRecord rec1 = ds.addRecord(new ByteArrayInputStream(data1));

        byte[] data2 = new byte[dataLength];
        random.nextBytes(data2);
        DataRecord rec2 = ds.addRecord(new ByteArrayInputStream(data2));

        byte[] data3 = new byte[dataLength];
        random.nextBytes(data3);
        DataRecord rec3 = ds.addRecord(new ByteArrayInputStream(data3));

        ((MultiDataStoreAware)ds).deleteRecord(rec1.getIdentifier());

        assertNull("rec1 should be null",
            ds.getRecordIfStored(rec1.getIdentifier()));
        assertEquals(new ByteArrayInputStream(data2),
            ds.getRecord(rec2.getIdentifier()).getStream());
        assertEquals(new ByteArrayInputStream(data3),
            ds.getRecord(rec3.getIdentifier()).getStream());
    }

    /**
     * Test {@link DataStore#getAllIdentifiers()} and asserts all identifiers
     * are returned.
     */
    protected void doGetAllIdentifiersTest() throws Exception {
        List<DataIdentifier> list = new ArrayList<DataIdentifier>();
        Random random = randomGen;
        byte[] data = new byte[dataLength];
        random.nextBytes(data);
        DataRecord rec = ds.addRecord(new ByteArrayInputStream(data));
        list.add(rec.getIdentifier());

        data = new byte[dataLength];
        random.nextBytes(data);
        rec = ds.addRecord(new ByteArrayInputStream(data));
        list.add(rec.getIdentifier());

        data = new byte[dataLength];
        random.nextBytes(data);
        rec = ds.addRecord(new ByteArrayInputStream(data));
        list.add(rec.getIdentifier());

        Iterator<DataIdentifier> itr = Sets.newHashSet(ds.getAllIdentifiers()).iterator();
        while (itr.hasNext()) {
            assertTrue("record found on list", list.remove(itr.next()));
        }
        Assert.assertEquals(0, list.size());
    }

    /**
     * Asserts that timestamp of all records accessed after
     * {@link DataStore#updateModifiedDateOnAccess(long)} invocation.
     */
    protected void doUpdateLastModifiedOnAccessTest() throws Exception {
        Random random = randomGen;
        byte[] data = new byte[dataLength];
        random.nextBytes(data);
        DataRecord rec1 = ds.addRecord(new ByteArrayInputStream(data));

        data = new byte[dataLength];
        random.nextBytes(data);
        DataRecord rec2 = ds.addRecord(new ByteArrayInputStream(data));
        LOG.debug("rec2 timestamp=" + rec2.getLastModified());

        // sleep for some time to ensure that async upload completes in backend.
        sleep(6000);
        long updateTime = System.currentTimeMillis();
        LOG.debug("updateTime=" + updateTime);
        ds.updateModifiedDateOnAccess(updateTime);

        // sleep to workaround System.currentTimeMillis granularity.
        sleep(3000);
        data = new byte[dataLength];
        random.nextBytes(data);
        DataRecord rec3 = ds.addRecord(new ByteArrayInputStream(data));

        data = new byte[dataLength];
        random.nextBytes(data);
        DataRecord rec4 = ds.addRecord(new ByteArrayInputStream(data));

        rec1 = ds.getRecord(rec1.getIdentifier());

        Assert.assertEquals("rec1 touched", true, rec1.getLastModified() > updateTime);
        LOG.debug("rec2 timestamp=" + rec2.getLastModified());
        Assert.assertEquals("rec2 not touched", true,
            rec2.getLastModified() < updateTime);
        Assert.assertEquals("rec3 touched", true, rec3.getLastModified() > updateTime);
        Assert.assertEquals("rec4 touched", true, rec4.getLastModified() > updateTime);
    }

    /**
     * Asserts that {@link DataStore#deleteAllOlderThan(long)} only deleted
     * records older than argument passed.
     */
    protected void doDeleteAllOlderThan() throws Exception {
        Random random = randomGen;
        byte[] data = new byte[dataLength];
        random.nextBytes(data);
        DataRecord rec1 = ds.addRecord(new ByteArrayInputStream(data));

        data = new byte[dataLength];
        random.nextBytes(data);
        DataRecord rec2 = ds.addRecord(new ByteArrayInputStream(data));

        // sleep for some time to ensure that async upload completes in backend.
        sleep(10000);
        long updateTime = System.currentTimeMillis();
        ds.updateModifiedDateOnAccess(updateTime);
        
        // sleep to workaround System.currentTimeMillis granularity.
        sleep(3000);
        data = new byte[dataLength];
        random.nextBytes(data);
        DataRecord rec3 = ds.addRecord(new ByteArrayInputStream(data));

        data = new byte[dataLength];
        random.nextBytes(data);
        DataRecord rec4 = ds.addRecord(new ByteArrayInputStream(data));

        rec1 = ds.getRecord(rec1.getIdentifier());
        ds.clearInUse();
        Assert.assertEquals("only rec2 should be deleted", 1,
            ds.deleteAllOlderThan(updateTime));
        assertNull("rec2 should be null",
            ds.getRecordIfStored(rec2.getIdentifier()));

        Iterator<DataIdentifier> itr = ds.getAllIdentifiers();
        List<DataIdentifier> list = new ArrayList<DataIdentifier>();
        list.add(rec1.getIdentifier());
        list.add(rec3.getIdentifier());
        list.add(rec4.getIdentifier());
        while (itr.hasNext()) {
            assertTrue("record found on list", list.remove(itr.next()));
        }

        Assert.assertEquals("touched records found", 0, list.size());
        Assert.assertEquals("rec1 touched", true, rec1.getLastModified() > updateTime);
        Assert.assertEquals("rec3 touched", true, rec3.getLastModified() > updateTime);
        Assert.assertEquals("rec4 touched", true, rec4.getLastModified() > updateTime);
    }

    /**
     * Test if record can be accessed via
     * {@link DataStore#getRecordFromReference(String)}
     */
    protected void doReferenceTest() throws Exception {
        byte[] data = new byte[dataLength];
        randomGen.nextBytes(data);
        String reference;
        DataRecord record = ds.addRecord(new ByteArrayInputStream(data));
        reference = record.getReference();
        assertReference(data, reference, ds);
    }

    /**
     * Method to validate mixed scenario use of {@link DataStore}.
     */
    protected void doTestSingleThread() throws Exception {
        doTestMultiThreaded(ds, 1);
    }

    /**
     * Method to validate mixed scenario use of {@link DataStore} in
     * multi-threaded concurrent environment.
     */
    protected void doTestMultiThreaded() throws Exception {
        doTestMultiThreaded(ds, 4);
    }

    /**
     * Method to assert record with byte array.
     */
    protected void assertRecord(byte[] expected, DataRecord record)
            throws DataStoreException, IOException {
        InputStream stream = record.getStream();
        try {
            for (int i = 0; i < expected.length; i++) {
                Assert.assertEquals(expected[i] & 0xff, stream.read());
            }
            Assert.assertEquals(-1, stream.read());
        } finally {
            stream.close();
        }
    }

    /**
     * Method to run {@link AbstractDataStoreTest#doTest(DataStore, int)} in multiple
     * concurrent threads.
     */
    protected void doTestMultiThreaded(final DataStore ds, int threadCount)
            throws Exception {
        final Exception[] exception = new Exception[1];
        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int x = i;
            Thread t = new Thread() {
                public void run() {
                    try {
                        doTest(ds, x);
                    } catch (Exception e) {
                        exception[0] = e;
                    }
                }
            };
            threads[i] = t;
            t.start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        if (exception[0] != null) {
            throw exception[0];
        }
    }

    /**
     * Assert randomly read stream from record.
     */
    void doTest(DataStore ds, int offset) throws Exception {
        ArrayList<DataRecord> list = new ArrayList<DataRecord>();
        HashMap<DataRecord, Integer> map = new HashMap<DataRecord, Integer>();
        for (int i = 0; i < 10; i++) {
            int size = 100000 - (i * 100);
            RandomInputStream in = new RandomInputStream(size + offset, size);
            DataRecord rec = ds.addRecord(in);
            list.add(rec);
            map.put(rec, size);
        }
        Random random = new Random(1);
        for (int i = 0; i < list.size(); i++) {
            int pos = random.nextInt(list.size());
            DataRecord rec = list.get(pos);
            int size = map.get(rec);
            rec = ds.getRecord(rec.getIdentifier());
            Assert.assertEquals(size, rec.getLength());
            RandomInputStream expected = new RandomInputStream(size + offset,
                size);
            InputStream in = rec.getStream();
            // Workaround for race condition that can happen with low cache size relative to the test
            // read immediately
            byte[] buffer = new byte[1];
            in.read(buffer);
            in = new SequenceInputStream(new ByteArrayInputStream(buffer), in);

            if (random.nextBoolean()) {
                in = readInputStreamRandomly(in, random);
            }
            assertEquals(expected, in);
        }
    }

    InputStream readInputStreamRandomly(InputStream in, Random random)
            throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8000];
        while (true) {
            if (random.nextBoolean()) {
                int x = in.read();
                if (x < 0) {
                    break;
                }
                out.write(x);
            } else {
                if (random.nextBoolean()) {
                    int l = in.read(buffer);
                    if (l < 0) {
                        break;
                    }
                    out.write(buffer, 0, l);
                } else {
                    int offset = random.nextInt(buffer.length / 2);
                    int len = random.nextInt(buffer.length / 2);
                    int l = in.read(buffer, offset, len);
                    if (l < 0) {
                        break;
                    }
                    out.write(buffer, offset, l);
                }
            }
        }
        in.close();
        return new ByteArrayInputStream(out.toByteArray());
    }

    /**
     * Assert two inputstream
     */
    protected void assertEquals(InputStream a, InputStream b)
            throws IOException {
        try {
            assertTrue("binary not equal",
                org.apache.commons.io.IOUtils.contentEquals(a, b));
        } finally {
            try {
                a.close();
            } catch (Exception ignore) {
            }
            try {
                b.close();
            } catch (Exception ignore) {
            }
        }
    }

    /**
     * Assert inputstream read from reference.
     */
    protected void assertReference(byte[] expected, String reference,
            DataStore store) throws Exception {
        DataRecord record = store.getRecordFromReference(reference);
        assertNotNull(record);
        Assert.assertEquals(expected.length, record.getLength());

        InputStream stream = record.getStream();
        try {
            assertTrue("binary not equal",
                org.apache.commons.io.IOUtils.contentEquals(
                    new ByteArrayInputStream(expected), stream));
        } finally {
            stream.close();
        }
    }

    /**
     * Utility method to stop execution for duration time.
     * 
     * @param duration
     *            time in milli seconds
     */
    protected void sleep(long duration) {
        long expected = System.currentTimeMillis() + duration;
        while (System.currentTimeMillis() < expected) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException ie) {

            }
        }
    }
}
