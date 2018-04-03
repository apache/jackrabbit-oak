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
import com.google.common.collect.Sets;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.MultiDataStoreAware;
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.jcr.RepositoryException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNull;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.addTestRecord;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createCompositeDataStore;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createEmptyCompositeDataStore;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createSharedDataStoreSpyDelegate;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.extractRecordData;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.randomDataRecordStream;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.threeRoles;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.twoRoles;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.verifyRecord;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.verifyRecordCount;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.verifyRecordIdCount;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.verifyRecordIds;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.verifyRecords;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// Test the data store capabilities of CompositeDataStore.
public class CompositeDataStoreDSTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private String homedir;

    private DelegateHandler delegateHandler = new IntelligentDelegateHandler();

    @Before
    public void setup() throws IOException {
        homedir = folder.newFolder().getAbsolutePath();
    }

    private DataStore createSpyDelegate(String role, CompositeDataStore cds)
            throws RepositoryException, IOException {
        return createSharedDataStoreSpyDelegate(folder.newFolder(), role, cds);
    }

    private List<DataStore> createAndAddDelegates(CompositeDataStore cds, List<String> roles)
            throws RepositoryException, IOException {
        List<DataStore> dataStores = Lists.newArrayList();
        for (String role : roles) {
            DataStore ds = createSpyDelegate(role, cds);
            dataStores.add(ds);
        }
        return dataStores;
    }

    private List<DataRecord> createRecordsForAllDataStores(
            CompositeDataStore cds,
            List<DataStore> dataStores,
            int numberOfRecords)
            throws DataStoreException {
        List<DataRecord> records = Lists.newArrayList();
        for (DataStore ds : dataStores) {
            for (int i=0; i<numberOfRecords; i++) {
                DataRecord record = ds.addRecord(randomDataRecordStream());
                cds.mapIdToDelegate(record.getIdentifier(), ds);
                //cds.delegateHandler.mapIdentifierToDelegate(record.getIdentifier(), ds);
                records.add(record);
            }
        }
        return records;
    }

    @Test
    public void testInit() throws RepositoryException, IOException {
        List<DataStore> dataStores = Lists.newArrayList();
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);

        for (String role : twoRoles) {
            dataStores.add(CompositeDataStoreTestUtils.createSpyDelegate(folder.newFolder(), role, cds));
        }

        cds.init(folder.newFolder().getAbsolutePath());

        for (DataStore ds : dataStores) {
            verify(ds, times(1)).init(anyString());
        }
    }

    @Test
    public void testInitNullHomeDir() throws RepositoryException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);
        try {
            cds.init(null);
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testAddRecord() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, twoRoles);
        createRecordsForAllDataStores(cds, dataStores, 1);

        cds.init(homedir);

        assertEquals(2, verifyRecordCount(cds));

        cds.addRecord(randomDataRecordStream());

        assertEquals(3, verifyRecordCount(cds));
    }

    // This test was written with possible future functionality in mind, where there may be
    // multiple delegate data stores that can be written to, and we should select the one
    // that already contains a matching data identifier for the record.
    // This functionality requires pre-reading the stream to compute the identifier before
    // handing the stream off to the delegate data store, which is additional complexity not
    // required for the first composite data store use case which is 1 read-only and 1
    // writable delegate.
    // See:  https://issues.apache.org/jira/browse/OAK-7090
    // -MR
    @Ignore
    @Test
    public void testAddRecordAddsToDelegateWithMatchingId() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, twoRoles);

        List<String> records = Lists.newArrayList();
        List<DataIdentifier> ids = Lists.newArrayList();
        int i = 0;
        for (DataStore ds : dataStores) {
            String recordData = String.format("test record %d", ++i);
            records.add(recordData);
            DataIdentifier id = addTestRecord(ds, delegateHandler, recordData)
                    .getIdentifier();
            ids.add(id);
            cds.mapIdToDelegate(id, ds);
            //cds.delegateHandler.mapIdentifierToDelegate(id, ds);
        }

        for (i = 0; i<records.size(); i++) {
            cds.addRecord(new ByteArrayInputStream(records.get(i).getBytes()));
            verify(dataStores.get(i), times(2))
                    .addRecord(any(InputStream.class));
        }
    }

    @Test
    public void testAddRecordAddsToFirstDelegate() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);

        DataStore ds1 = createSpyDelegate(twoRoles.get(0), cds);
        DataStore ds2 = createSpyDelegate(twoRoles.get(1), cds);

        cds.addRecord(randomDataRecordStream());
        cds.addRecord(randomDataRecordStream());
        verify(ds1, times(2)).addRecord(any(InputStream.class));
        verify(ds2, times(0)).addRecord(any(InputStream.class));
    }

    @Test
    public void testAddRecordNullInputStream() throws DataStoreException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);

        try {
            cds.addRecord(null);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testGetRecord() throws DataStoreException, IOException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles,
                folder.newFolder().getAbsolutePath());
        String recordData = "recordData";
        DataIdentifier id = cds.addRecord(new ByteArrayInputStream(recordData.getBytes())).getIdentifier();

        verifyRecord(cds.getRecord(id), recordData);
    }

    @Test
    public void testGetNonexistentRecordThrowsDataStoreException() throws DataStoreException, IOException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles,
                folder.newFolder().getAbsolutePath());
        cds.addRecord(randomDataRecordStream());
        try {
            cds.getRecord(new DataIdentifier("invalidIdentifier"));
            fail();
        }
        catch (DataStoreException e) { }
        catch (Exception e) {
            fail(String.format("Expected DataStoreException but caught %s instead", e.getClass().getSimpleName()));
        }
    }

    @Test
    public void testGetRecordFindsRecordInAnyDelegate() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, twoRoles);
        List<String> records = Lists.newArrayList("recordContent1", "recordContent2");
        List<DataIdentifier> ids = Lists.newArrayList();
        int ctr = 0;
        for (DataStore ds : dataStores) {
            ids.add(ds.addRecord(new ByteArrayInputStream(records.get(ctr).getBytes())).getIdentifier());
            ctr++;
        }

        for (int i=0; i<dataStores.size(); ++i) {
            cds.mapIdToDelegate(ids.get(i), dataStores.get(i));
            //cds.delegateHandler.mapIdentifierToDelegate(ids.get(i), dataStores.get(i));
        }

        for (int i=0; i<ids.size(); ++i) {
            verifyRecord(cds.getRecord(ids.get(i)), records.get(i));
        }

        for (DataStore ds : dataStores) {
            verify(ds, atLeast(1))
                    .getRecordIfStored(any(DataIdentifier.class));
        }
    }

    @Test
    public void testGetRecordFindsRecordIfIdNotMapped() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(threeRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, threeRoles);
        DataStore ds1 = dataStores.get(0);
        DataStore ds2 = dataStores.get(1);
        DataRecord record1 = ds1.addRecord(randomDataRecordStream());
        DataRecord record2 = ds2.addRecord(randomDataRecordStream());
        DataIdentifier id1 = record1.getIdentifier();
        DataIdentifier id2 = record2.getIdentifier();

        cds.mapIdToDelegate(id1, ds1);
        //cds.delegateHandler.mapIdentifierToDelegate(id1, ds1);
        // Do not map record id 2

        cds.init(homedir);

        verifyRecord(cds.getRecord(id2), extractRecordData(record2));
    }

    @Test
    public void testGetRecordNullId() throws DataStoreException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);

        try {
            cds.getRecord(null);
            fail();
        }
        catch (NullPointerException e) { }
    }

    @Test
    public void testGetRecordFromReference() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, twoRoles);
        List<DataRecord> records = createRecordsForAllDataStores(cds, dataStores, 2);
        List<String> refs = Lists.newArrayList();
        for (DataRecord record : records) {
            for (DataStore ds : dataStores) {
                if (null != ds.getRecordIfStored(record.getIdentifier())) {
                    refs.add(((CompositeDataStoreTestUtils.TestableFileDataStore)ds).getReferenceFromIdentifier(record.getIdentifier()));
                    break;
                }
            }
        }
        cds.init(homedir);

        for (String ref : refs) {
            assertTrue(records.contains(cds.getRecordFromReference(ref)));
        }
    }

    @Test
    public void testGetRecordFromInvalidReference() throws DataStoreException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);

        for (String ref : Lists.newArrayList("", "invalid", null)) {
            assertNull(cds.getRecordFromReference(ref));
        }
    }

    @Test
    public void testUpdateModifiedOnAccess() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(threeRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, threeRoles);
        cds.init(homedir);

        cds.updateModifiedDateOnAccess(0);

        for (DataStore ds : dataStores) {
            verify(ds, times(1)).updateModifiedDateOnAccess(0);
        }
    }

    @Test
    public void testDeleteAllOlderThan() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(threeRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, threeRoles);
        cds.init(homedir);

        cds.deleteAllOlderThan(1000L);

        for (DataStore ds : dataStores) {
            verify(ds, times(1)).deleteAllOlderThan(anyLong());
        }
    }

    @Test
    public void testGetAllIdentifiers() throws RepositoryException {
        CompositeDataStore cds = createCompositeDataStore(threeRoles, homedir);
        Set<DataIdentifier> ids = Sets.newHashSet();

        Iterator<DataIdentifier> iter = cds.getAllIdentifiers();
        assertFalse(iter.hasNext());

        for (int i=0; i<10; ++i) {
            ids.add(cds.addRecord(randomDataRecordStream()).getIdentifier());
        }

        assertEquals(10, verifyRecordIds(cds, ids));
    }

    @Test
    public void testGetAllIdentifiersWithNoRecordsReturnsEmptyIterator()
            throws DataStoreException {
        CompositeDataStore cds = createCompositeDataStore(threeRoles, homedir);
        Iterator<DataIdentifier> iter = cds.getAllIdentifiers();
        assertNotNull(iter);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testGetAllIdentifiersReturnsIdsFromAllDelegates()
            throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(threeRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, threeRoles);
        cds.init(homedir);

        List<DataRecord> records = createRecordsForAllDataStores(cds, dataStores, 5);
        Set<DataIdentifier> ids = Sets.newHashSet();
        for (DataRecord record : records) {
            ids.add(record.getIdentifier());
        }

        assertEquals(15, verifyRecordIds(cds, ids));
    }

    @Test
    public void testGetMinRecordLength() throws RepositoryException, IOException {
        List<String> roles = Lists.newArrayList("role1", "role2", "role3", "role4");
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);

        int i = 1024*16;
        for (String role : roles) {
            DataStore ds = createSpyDelegate(role, cds);
            when(ds.getMinRecordLength()).thenReturn(i);
            i -= 1024*4;
        }

        assertEquals(1024*4, cds.getMinRecordLength());
    }

    @Test
    public void testCloseClosesAllDelegates() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, twoRoles);
        cds.init(homedir);

        cds.close();

        for (DataStore ds : dataStores) {
            verify(ds, times(1)).close();
        }
    }

    @Test
    public void testClearInUse() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(threeRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, threeRoles);
        cds.init(homedir);

        cds.clearInUse();
        for (DataStore ds : dataStores) {
            verify(ds, times(1)).clearInUse();
        }
    }

    @Test
    public void testDelete() throws RepositoryException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);
        cds.init(homedir);

        Set<DataIdentifier> ids = Sets.newHashSet();
        for (int i=0; i<10; ++i) {
            DataIdentifier id = cds.addRecord(randomDataRecordStream()).getIdentifier();
            ids.add(id);
        }

        assertEquals(10, verifyRecordIds(cds, ids));

        for (DataIdentifier id : ids) {
            cds.deleteRecord(id);
        }

        assertFalse(cds.getAllIdentifiers().hasNext());
    }

    @Test
    public void testDeleteRemovesFromAnyDelegate() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, twoRoles);
        List<DataRecord> records = createRecordsForAllDataStores(cds, dataStores, 5);
        Set<DataIdentifier> ids = Sets.newHashSet();
        for (DataRecord record : records) {
            ids.add(record.getIdentifier());
        }
        cds.init(homedir);

        assertEquals(10, verifyRecordIds(cds, ids));

        for (DataIdentifier id : ids) {
            cds.deleteRecord(id);
        }

        assertFalse(cds.getAllIdentifiers().hasNext());
    }

    @Test
    public void testDeleteDeletesAllMatchingRecords() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(threeRoles);
        Set<DataStore> dataStores = Sets.newHashSet();
        DataIdentifier id = null;
        for (String role : threeRoles) {
            DataStore ds = createSpyDelegate(role, cds);
            id = ds.addRecord(new ByteArrayInputStream("recordData".getBytes())).getIdentifier();
            cds.mapIdToDelegate(id, ds);
            //cds.delegateHandler.mapIdentifierToDelegate(id, ds);
            dataStores.add(ds);
        }
        cds.init(homedir);

        assertEquals(3, verifyRecordIdCount(cds));

        cds.deleteRecord(id);

        for (DataStore ds : dataStores) {
            verify((MultiDataStoreAware) ds, times(1)).deleteRecord(id);
        }

        assertFalse(cds.getAllIdentifiers().hasNext());
    }

    @Test
    public void testDeleteInvalidId() throws RepositoryException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);
        cds.init(homedir);

        cds.addRecord(randomDataRecordStream());

        try {
            cds.deleteRecord(new DataIdentifier("invalid"));
            fail();
        }
        catch (DataStoreException e) { }
    }

    @Test
    public void testDeleteNullId() throws DataStoreException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);

        try {
            cds.deleteRecord(null);
            fail();
        }
        catch (NullPointerException e) { }
    }

    @Test
    public void testGetMetadataRecord() throws RepositoryException, IOException{
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        List<DataStore> dataStores = Lists.newArrayList();
        List<String> names = Lists.newArrayList();

        int recnum = 1;
        String prefix = "testWriterRecord";
        for (String role : twoRoles) {
            DataStore ds = createSpyDelegate(role, cds);
            String name = String.format("%sName%d", prefix, recnum);
            names.add(name);
            ((SharedDataStore) ds).addMetadataRecord(
                    new ByteArrayInputStream(String.format("%s%s", prefix, role).getBytes()),
                    name
            );
            dataStores.add(ds);
            recnum++;
        }

        cds.init(homedir);

        Set<DataRecord> records = Sets.newHashSet();
        for (String name : names) {
            DataRecord rec = cds.getMetadataRecord(name);
            verifyRecord(rec, prefix, false);
            records.add(rec);
        }

        List<DataRecord> recList = cds.getAllMetadataRecords(prefix);
        assertEquals(records.size(), recList.size());
        for (DataRecord rec : recList) {
            assertTrue(records.contains(rec));
        }
    }

    @Test
    public void testGetMetadataRecordWithNullName() {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);
        try {
            cds.getMetadataRecord(null);
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testGetMetadataRecordWithInvalidName() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        createAndAddDelegates(cds, twoRoles);
        cds.init(homedir);

        List<String> metaRecordNames = Lists.newArrayList("first", "second", "third");
        for (String name : metaRecordNames) {
            cds.addMetadataRecord(
                    randomDataRecordStream(),
                    name
            );
        }

        for (String name : Lists.newArrayList("invalid", "")) {
            assertNull(cds.getMetadataRecord(name));
        }
    }

    @Test
    public void testAddMetadataRecord() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        createAndAddDelegates(cds, twoRoles);
        cds.init(homedir);

        String name = "testRecordName";
        String prefix = "testRecordPrefix";
        String data = "data";
        assertNull(cds.getMetadataRecord(name));

        // Not sure this is correct.  addMetadataRecord adds the record
        // to every delegate.  This results in duplicate records.  -MR
        cds.addMetadataRecord(new ByteArrayInputStream(
                String.format("%s%s", prefix, data).getBytes()
        ), name);

        verifyRecord(cds.getMetadataRecord(name), String.format("%s%s", prefix, data));
    }

    @Test
    public void testAddMetadataRecordWithNullRecord() throws DataStoreException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);
        try {
            cds.addMetadataRecord((InputStream) null, "recordName");
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testAddMetadataRecordWithNullName() throws DataStoreException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);
        try {
            cds.addMetadataRecord(randomDataRecordStream(), null);
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testAddMetadataRecordWithEmptyName() throws DataStoreException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);
        try {
            cds.addMetadataRecord(randomDataRecordStream(), "");
            fail();
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testAddMetadataRecordThrowsAggregateException() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        for (String role : twoRoles) {
            SharedDataStore ds = (SharedDataStore) createSpyDelegate(role, cds);
            doThrow(new DataStoreException("")).when(ds).addMetadataRecord(any(InputStream.class), anyString());
            doThrow(new DataStoreException("")).when(ds).addMetadataRecord(any(File.class), anyString());
        }
        cds.init(homedir);

        try {
            cds.addMetadataRecord(randomDataRecordStream(), "testname");
            fail();
        }
        catch (DataStoreException e) {
            assertEquals(2, e.getSuppressed().length);
        }
    }

    @Test
    public void testDeleteMetadataRecord() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        createAndAddDelegates(cds, twoRoles);
        cds.init(homedir);

        for (int i=0; i<10; ++i) {
            cds.addMetadataRecord(randomDataRecordStream(), String.format("testName%d", i));
        }

        assertTrue(cds.deleteMetadataRecord("testName1"));

        cds.deleteAllMetadataRecords("testName");

        assertTrue(cds.getAllMetadataRecords("testName").isEmpty());
    }

    @Test
    public void testDeleteMetadataRecordWithInvalidIds() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        createAndAddDelegates(cds, twoRoles);
        cds.init(homedir);

        cds.addMetadataRecord(randomDataRecordStream(), "name");

        for (String id : Lists.newArrayList("", "invalid")) {
            assertFalse(cds.deleteMetadataRecord(id));
        }
        try {
            cds.deleteMetadataRecord(null);
            fail();
        } catch (NullPointerException | IllegalArgumentException e) {
        }
    }

    @Test
    public void testGetAllRecords() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        createAndAddDelegates(cds, twoRoles);
        cds.init(homedir);

        assertFalse(cds.getAllRecords().hasNext());

        for (int i=0; i<10; ++i) {
            cds.addRecord(randomDataRecordStream());
        }

        assertEquals(10, verifyRecordCount(cds));
    }

    @Test
    public void testGetAllRecordsReturnsFromAllDelegates() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        List<DataStore> dataStores = createAndAddDelegates(cds, twoRoles);
        List<DataRecord> records = createRecordsForAllDataStores(cds, dataStores, 5);
        cds.init(homedir);

        assertEquals(10, verifyRecords(cds, records));
    }

    @Test
    public void testGetAllRecordsNoRecordsReturnsEmptyIterator() throws DataStoreException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);
        Iterator<DataRecord> iter = cds.getAllRecords();
        assertNotNull(iter);
        assertFalse(iter.hasNext());
    }

    @Test
    public void testGetRecordForId() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        createAndAddDelegates(cds, twoRoles);
        cds.init(homedir);

        cds.addRecord(new ByteArrayInputStream("testRecord".getBytes()));

        DataIdentifier id = cds.getAllRecords().next().getIdentifier();

        verifyRecord(cds.getRecordForId(id), "testRecord");
    }

    @Test
    public void testGetRecordForIdWithNullId() throws DataStoreException {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);
        try {
            cds.getRecordForId(null);
            fail();
        }
        catch (NullPointerException | IllegalArgumentException e) { }
    }

    @Test
    public void testGetRecordForIdWithInvalidId() {
        CompositeDataStore cds = createCompositeDataStore(twoRoles, homedir);
        try {
            cds.getRecordForId(new DataIdentifier("invalid"));
            fail();
        }
        catch (DataStoreException e) { }
    }

    @Test
    public void testGetType() {
        assertEquals(
                SharedDataStore.Type.SHARED,
                createCompositeDataStore(twoRoles, homedir).getType()
        );
    }
}
