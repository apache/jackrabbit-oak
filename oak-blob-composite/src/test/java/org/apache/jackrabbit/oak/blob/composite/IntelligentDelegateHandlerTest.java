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

package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.RepositoryException;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.addTestRecord;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createDataStoreProvider;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createDelegate;
import static org.junit.Assert.assertNotNull;

public class IntelligentDelegateHandlerTest {
    private DelegateHandler sut;
    private Map<String, Object> readOnlyDelegateConfig = Maps.newHashMap();

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void setup() {
        sut = new IntelligentDelegateHandler();
        readOnlyDelegateConfig.put("readOnly", true);
    }

    private DataStore getFileDataStore() throws RepositoryException, IOException {
        return getFileDataStore(folder.newFolder());
    }

    private DataStore getFileDataStore(File dsPath) throws RepositoryException {
        DataStore ds = new OakFileDataStore();
        ds.init(dsPath.getAbsolutePath());
        return ds;
    }

    private void verifyDelegates(DelegateHandler sut, Class<? extends DataStore> delegateType) {
        verifyDelegates(sut, 1, Sets.newHashSet(delegateType));
    }

    private void verifyDelegates(DelegateHandler sut) {
        verifyDelegates(sut, 0, Sets.newHashSet());
    }

    private void verifyDelegates(DelegateHandler sut, int nDelegates, Set<Class<? extends DataStore>> delegateTypes) {
        if (delegateTypes.isEmpty()) {
            assertFalse(sut.hasDelegate());
        }
        else {
            assertTrue(sut.hasDelegate());
            Iterator<DataStore> iter = sut.getAllDelegatesIterator();
            while (iter.hasNext()) {
                DataStore ds = iter.next();
                assertTrue(delegateTypes.contains(ds.getClass()));
                nDelegates--;
            }
            assertEquals(0, nDelegates);
        }
    }

    @Test
    public void testAddWritableDelegate() {
        sut.addDelegateDataStore(createDelegate("role1"));
        verifyDelegates(sut, OakFileDataStore.class);
    }

    @Test
    public void testAddReadonlyDelegate() {
        sut.addDelegateDataStore(createDelegate(createDataStoreProvider("role1"), readOnlyDelegateConfig));
        verifyDelegates(sut, OakFileDataStore.class);
    }

    @Test
    public void testAddTwoDelegatesOfSameType() {
        sut.addDelegateDataStore(createDelegate("role1"));
        sut.addDelegateDataStore(createDelegate("role2"));
        verifyDelegates(sut, 2, Sets.newHashSet(OakFileDataStore.class));
    }

    @Test
    public void testAddTwoDelegatesOfDifferentTypes() {
        sut.addDelegateDataStore(createDelegate("role1"));
        sut.addDelegateDataStore(createDelegate(createDataStoreProvider(new OtherFileDataStore(), "role2")));
        verifyDelegates(sut, 2, Sets.newHashSet(OakFileDataStore.class, OtherFileDataStore.class));
    }

    @Test
    public void testAddSameDelegateInstanceTwice() {
        DelegateDataStore delegate = createDelegate("role1");
        sut.addDelegateDataStore(delegate);
        verifyDelegates(sut, 1, Sets.newHashSet(OakFileDataStore.class));

        sut.addDelegateDataStore(delegate);
        verifyDelegates(sut, 1, Sets.newHashSet(OakFileDataStore.class));
    }

    @Test
    public void testRemoveDelegate() {
        DataStoreProvider dsp = createDataStoreProvider("role1");
        sut.addDelegateDataStore(createDelegate(dsp));
        verifyDelegates(sut, OakFileDataStore.class);

        assertTrue(sut.removeDelegateDataStore(dsp));
        verifyDelegates(sut);
    }

    @Test
    public void testRemoveDelegateRemovesMatchingDelegatesOnly() {
        DataStoreProvider dsp1 = createDataStoreProvider("role1");
        DelegateDataStore delegate = createDelegate(dsp1);
        sut.addDelegateDataStore(delegate);
        DataStoreProvider dsp2 = createDataStoreProvider(new OtherFileDataStore(), "role2");
        sut.addDelegateDataStore(createDelegate(dsp2));

        verifyDelegates(sut, 2, Sets.newHashSet(OakFileDataStore.class, OtherFileDataStore.class));

        assertTrue(sut.removeDelegateDataStore(dsp1));
        verifyDelegates(sut, OtherFileDataStore.class);

        sut.addDelegateDataStore(delegate);
        assertTrue(sut.removeDelegateDataStore(dsp2));
        verifyDelegates(sut, OakFileDataStore.class);

        assertTrue(sut.removeDelegateDataStore(dsp1));
        verifyDelegates(sut);
    }

    @Test
    public void testRemoveDelegateRemovesOnlyMatchingDelegateOfSameClass() {
        DataStoreProvider dsp1 = createDataStoreProvider("role1");
        sut.addDelegateDataStore(createDelegate(dsp1));
        DataStoreProvider dsp2 = createDataStoreProvider("role2");
        sut.addDelegateDataStore(createDelegate(dsp2));

        verifyDelegates(sut, 2, Sets.newHashSet(OakFileDataStore.class));

        assertTrue(sut.removeDelegateDataStore(dsp1));
        verifyDelegates(sut, OakFileDataStore.class);

        assertTrue(sut.removeDelegateDataStore(dsp2));
        verifyDelegates(sut);
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testSelectWritableDelegateSingleDelegateWithMatchingId() throws RepositoryException, IOException {
        DataStore ds = getFileDataStore();
        DataStoreProvider dsp = createDataStoreProvider(ds, "role1");
        sut.addDelegateDataStore(createDelegate(dsp));
        DataRecord record = addTestRecord(ds, sut);
        DataIdentifier id = record.getIdentifier();

        assertEquals(ds, sut.getWritableDelegatesIterator(id).next());
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testSelectWritableDelegateSingleDelegateWithoutMatchingId() throws RepositoryException, IOException {
        DataStore ds = getFileDataStore();
        DataRecord record = addTestRecord(ds, sut);
        DataIdentifier id = new DataIdentifier("otherIdentifier");
        DataStoreProvider dsp = createDataStoreProvider(ds, "role1");
        sut.addDelegateDataStore(createDelegate(dsp));

        assertFalse(sut.getAllDelegatesIterator(id).hasNext());
        assertEquals(ds, sut.getWritableDelegatesIterator().next());
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testSelectWritableDelegateTwoDelegatesFirstDelegateMatches() throws RepositoryException, IOException {
        DataStore ds1 = getFileDataStore();
        DataStoreProvider dsp1 = createDataStoreProvider(ds1, "role1");
        sut.addDelegateDataStore(createDelegate(dsp1));
        DataRecord record = addTestRecord(ds1, sut);
        DataIdentifier id = record.getIdentifier();

        DataStore ds2 = new OakFileDataStore();
        DataStoreProvider dsp2 = createDataStoreProvider(ds2, "role2");
        sut.addDelegateDataStore(createDelegate(dsp2));

        assertEquals(ds1, sut.getWritableDelegatesIterator(id).next());
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testSelectWritableDelegateTwoDelegatesSecondDelegateMatches() throws RepositoryException, IOException {
        DataStore ds1 = getFileDataStore();
        DataStoreProvider dsp1 = createDataStoreProvider(ds1, "role1");
        sut.addDelegateDataStore(createDelegate(dsp1));

        DataStore ds2 = getFileDataStore();
        DataStoreProvider dsp2 = createDataStoreProvider(ds2, "role2");
        sut.addDelegateDataStore(createDelegate(dsp2));
        DataRecord record = addTestRecord(ds2, sut);
        DataIdentifier id = record.getIdentifier();

        assertEquals(ds2, sut.getWritableDelegatesIterator(id).next());
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testSelectWritableDelegateTwoDelegatesWithoutMatchingId() throws RepositoryException, IOException {
        DataStore ds1 = getFileDataStore();
        DataStoreProvider dsp1 = createDataStoreProvider(ds1, "role1");
        sut.addDelegateDataStore(createDelegate(dsp1));
        DataRecord record1 = addTestRecord(ds1, sut, "record1");
        DataIdentifier id1 = record1.getIdentifier();

        DataStore ds2 = getFileDataStore();
        DataStoreProvider dsp2 = createDataStoreProvider(ds2, "role2");
        sut.addDelegateDataStore(createDelegate(dsp2));
        DataRecord record2 = addTestRecord(ds2, sut, "record2");
        DataIdentifier id2 = record2.getIdentifier();

        DataIdentifier otherId = new DataIdentifier("otherIdentifier");

        assertFalse(sut.getWritableDelegatesIterator(otherId).hasNext());
        assertEquals(ds1, sut.getWritableDelegatesIterator().next());
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testSelectWritableDelegateMultipleDelegatesMultipleMatches() throws RepositoryException, IOException {
        DataStore ds1 = getFileDataStore();
        addTestRecord(ds1, sut);
        DataStoreProvider dsp1 = createDataStoreProvider(ds1, "role1");
        sut.addDelegateDataStore(createDelegate(dsp1));

        String recordContent = "record2 same as record3";
        DataStore ds2 = getFileDataStore();
        DataStoreProvider dsp2 = createDataStoreProvider(ds2, "role2");
        sut.addDelegateDataStore(createDelegate(dsp2));
        DataRecord record2 = addTestRecord(ds2, sut, recordContent);
        DataIdentifier id2 = record2.getIdentifier();

        DataStore ds3 = getFileDataStore();
        DataStoreProvider dsp3 = createDataStoreProvider(ds3, "role3");
        sut.addDelegateDataStore(createDelegate(dsp3));
        DataRecord record3 = addTestRecord(ds3, sut, recordContent);
        DataIdentifier id3 = record3.getIdentifier();

        assertEquals(id2, id3);

        DataIdentifier otherId = new DataIdentifier("otherIdentifier");

        assertFalse(sut.getAllDelegatesIterator(otherId).hasNext());
        assertEquals(ds1, sut.getWritableDelegatesIterator().next());
        assertEquals(ds2, sut.getWritableDelegatesIterator(id2).next());
        assertEquals(ds2, sut.getWritableDelegatesIterator(id3).next());
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testSelectWritableDelegateWithReadonlyDelegateReturnsEmptyIterator() throws RepositoryException, IOException {
        DataStore ds1 = getFileDataStore();
        DataStoreProvider dsp1 = createDataStoreProvider(ds1, "role1");
        sut.addDelegateDataStore(createDelegate(dsp1, readOnlyDelegateConfig));

        DataStore ds2 = getFileDataStore();
        DataStoreProvider dsp2 = createDataStoreProvider(ds2, "role2");
        sut.addDelegateDataStore(createDelegate(dsp2));

        assertFalse(sut.getWritableDelegatesIterator(new DataIdentifier("otherIdentifier")).hasNext());
        assertEquals(ds2, sut.getWritableDelegatesIterator().next());
    }

    @Test
    public void testSelectWritableDelegateWithOnlyReadonlyDelegateReturnsEmptyIterator() throws RepositoryException, IOException {
        DataStore ds = getFileDataStore();
        DataIdentifier id = addTestRecord(ds, sut).getIdentifier();
        DataStoreProvider dsp = createDataStoreProvider(ds, "role1");
        sut.addDelegateDataStore(createDelegate(dsp, readOnlyDelegateConfig));

        assertFalse(sut.getWritableDelegatesIterator(id).hasNext());
    }

    @Test
    public void testGetDelegateIteratorSingleDelegateReturnsOneDelegate() {
        sut.addDelegateDataStore(createDelegate("role1"));

        Iterator<DataStore> i = sut.getAllDelegatesIterator();
        assertNotNull(i);
        assertTrue(i.hasNext());
        assertNotNull(i.next());
        assertFalse(i.hasNext());
    }

    @Test
    public void testGetDelegateIteratorMultipleDelegatesReturnsAllDelegatesInOrder() throws RepositoryException, IOException {
        List<DataStore> dataStores = Lists.newArrayList();
        for (int i=0; i<5; ++i) {
            DataStore ds = getFileDataStore();
            sut.addDelegateDataStore(createDelegate(createDataStoreProvider(ds, String.format("role%d", i+1))));
            dataStores.add(ds);
        }

        Iterator<DataStore> iter = sut.getAllDelegatesIterator();
        int i = 0;
        while (iter.hasNext()) {
            assertEquals(dataStores.get(i), iter.next());
            i++;
        }
        assertEquals(5, i);
    }

    @Test
    public void testGetDelegateIteratorWithReadonlyDelegateReturnsWritableDelegatesFirst() throws RepositoryException, IOException {
        int roCount = 2;
        int rwCount = 3;
        List<DataStore> readOnlyDataStores = Lists.newArrayList();
        for (int i=0; i<roCount; ++i) {
            DataStore ds = getFileDataStore();
            sut.addDelegateDataStore(createDelegate(createDataStoreProvider(ds, String.format("role%d", i+1)), readOnlyDelegateConfig));
            readOnlyDataStores.add(ds);
        }

        List<DataStore> readWriteDataStores = Lists.newArrayList();
        for (int i=0; i<rwCount; ++i) {
            DataStore ds = getFileDataStore();
            sut.addDelegateDataStore(createDelegate(createDataStoreProvider(ds, String.format("role%d", i+1))));
            readWriteDataStores.add(ds);
        }

        Iterator<DataStore> iter = sut.getAllDelegatesIterator();
        int i = 0;
        while (iter.hasNext()) {
            if (i < rwCount) {
                assertEquals(readWriteDataStores.get(i), iter.next());
            }
            else {
                assertEquals(readOnlyDataStores.get(i-rwCount), iter.next());
            }
            i++;
        }
        assertEquals(roCount+rwCount, i);
    }

    @Test
    public void testGetWritableDelegateIteratorReturnsOnlyWritableDelegates() throws RepositoryException, IOException {
        DataStore rods = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(rods,"rwRole"), readOnlyDelegateConfig));
        DataStore rwds = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(rwds,"roRole")));

        Iterator<DataStore> iter = sut.getWritableDelegatesIterator();
        assertTrue(iter.hasNext());
        assertEquals(rwds, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testGetWritableDelegateIteratorWithNoWritableDelegatesReturnsEmptyIterator() {
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider("roRole"), readOnlyDelegateConfig));

        Iterator<DataStore> iter = sut.getWritableDelegatesIterator();
        assertFalse(iter.hasNext());
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testGetDelegateIteratorWithIdentifierReturnsMatchingDataStoreOnly() throws RepositoryException, IOException {
        DataStore ds1 = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds1, "role1")));
        DataStore ds2 = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds2, "role2")));
        DataIdentifier id2 = addTestRecord(ds2, sut).getIdentifier();

        Iterator<DataStore> iter = sut.getAllDelegatesIterator(id2);
        assertTrue(iter.hasNext());
        assertEquals(ds2, iter.next());
        assertFalse(iter.hasNext());
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testGetDelegateIteratorWithIdentifierReturnsAllMatchingDataStores() throws RepositoryException, IOException {
        DataStore ds1 = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds1, "role1")));
        DataStore ds2 = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds2, "role2")));
        DataIdentifier id2 = addTestRecord(ds2, sut).getIdentifier();
        DataStore ds3 = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds3, "role3")));
        DataIdentifier id3 = addTestRecord(ds3, sut).getIdentifier();

        assertEquals(id2, id3);

        Set<DataStore> matchingDataStores = Sets.newHashSet(ds2, ds3);
        Iterator<DataStore> iter = sut.getAllDelegatesIterator(id2);
        assertTrue(iter.hasNext());
        int i = 0;
        while (iter.hasNext()) {
            assertTrue(matchingDataStores.contains(iter.next()));
            i++;
        }
        assertEquals(2, i);

        iter = sut.getAllDelegatesIterator(id3);
        assertTrue(iter.hasNext());
        i = 0;
        while (iter.hasNext()) {
            assertTrue(matchingDataStores.contains(iter.next()));
            i++;
        }
        assertEquals(2, i);
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testGetDelegateIteratorWithIdentifierReturnsEmptyIteratorIfNoMatches() throws RepositoryException, IOException {
        DataStore ds1 = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds1, "role1")));

        Iterator<DataStore> iter = sut.getAllDelegatesIterator(new DataIdentifier("otherIdentifier"));
        assertFalse(iter.hasNext());
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testGetWriteableDelegateIteratorWithIdentifier() throws RepositoryException, IOException {
        DataStore ds1 = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds1, "role1")));
        DataStore ds2 = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds2, "role2")));
        DataIdentifier id2 = addTestRecord(ds2, sut).getIdentifier();

        Iterator<DataStore> iter = sut.getWritableDelegatesIterator(id2);
        assertTrue(iter.hasNext());
        assertEquals(ds2, iter.next());
        assertFalse(iter.hasNext());
    }

    // TODO:  Enable this test when the blob ID mapping capability is added.
    // TODO:  See https://issues.apache.org/jira/browse/OAK-7090
    @Ignore
    @Test
    public void testGetWritableDelegateIteratorWithIdentifierFiltersReadonly() throws RepositoryException, IOException {
        DataStore ds1 = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds1, "role1")));
        DataStore ds2 = getFileDataStore();
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds2, "role2"), readOnlyDelegateConfig));
        DataIdentifier id2 = addTestRecord(ds2, sut).getIdentifier();

        Iterator<DataStore> iter = sut.getWritableDelegatesIterator(id2);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testGetMinRecordLengthReturnsLowest() throws RepositoryException, IOException {
        DataStore ds1 = getFileDataStore();
        ((OakFileDataStore)ds1).setMinRecordLength(1024*8);
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds1, "role1")
                )
        );

        DataStore ds2 = getFileDataStore();
        ((OakFileDataStore)ds2).setMinRecordLength(1024*4);
        sut.addDelegateDataStore(
                createDelegate(
                        createDataStoreProvider(ds2, "role2")
                )
        );

        assertEquals(1024*4, sut.getMinRecordLength());
    }

    private static class OtherFileDataStore extends OakFileDataStore { }
}
