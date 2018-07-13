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
import org.apache.jackrabbit.oak.plugins.blob.SharedDataStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.jcr.RepositoryException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createEmptyCompositeDataStore;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createReadOnlyDelegate;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createReadOnlySharedDataStoreSpyDelegate;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createSharedDataStoreSpyDelegate;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.createSpyDelegate;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.randomDataRecordStream;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.threeRoles;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.twoRoles;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.verifyRecord;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.verifyRecordIds;
import static org.apache.jackrabbit.oak.blob.composite.CompositeDataStoreTestUtils.verifyRecords;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CompositeDataStoreReadOnlyDelegatesTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));
    private String homedir;

    @Before
    public void setup() throws IOException {
        homedir = folder.newFolder().getAbsolutePath();
    }

    private DataStore getFileDataStore() throws RepositoryException, IOException {
        return CompositeDataStoreTestUtils.getFileDataStore(folder.newFolder());
    }

    private DataStore createReadOnlySpyDelegate(String role, CompositeDataStore cds)
            throws RepositoryException, IOException {
        return CompositeDataStoreTestUtils.createReadOnlySpyDelegate(folder.newFolder(), role, cds);
    }

    @Test
    public void testAddRecordDoesntAddToReadonlyDelegate() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);

        DataStore ds1 = createReadOnlySpyDelegate(twoRoles.get(0), cds);
        DataStore ds2 = createSpyDelegate(folder.newFolder(), twoRoles.get(1), cds);

        cds.addRecord(randomDataRecordStream());
        cds.addRecord(randomDataRecordStream());
        verify(ds1, times(0)).addRecord(any(InputStream.class));
        verify(ds2, times(2)).addRecord(any(InputStream.class));
    }

    @Test
    public void testAddRecordNoWritableDelegates() throws RepositoryException, IOException {
        List<DataStore> dataStores = Lists.newArrayList();
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);

        for (String role : twoRoles) {
            DataStore ds = spy(getFileDataStore());
            dataStores.add(ds);
            cds.addDelegate(createReadOnlyDelegate(role, ds));
        }
        cds.init(homedir);

        for (int i=0; i<dataStores.size(); i++) {
            assertNull(cds.addRecord(new ByteArrayInputStream(
                    String.format("test record %d", i).getBytes()
            )));
            verify(dataStores.get(i), times(0))
                    .addRecord(any(InputStream.class));
        }
    }

    @Test
    public void testGetRecordFromReadonlyDelegate() throws RepositoryException, IOException {
        List<String> roles = Lists.newArrayList("role1", "role2");

        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        DataStore ds = createSpyDelegate(folder.newFolder(), twoRoles.get(0), cds);
        DataStore rods = createReadOnlySpyDelegate(twoRoles.get(1), cds);
        cds.init(homedir);

        String recordData = "recordData";
        DataIdentifier id =
                rods.addRecord(new ByteArrayInputStream(recordData.getBytes()))
                        .getIdentifier();

        cds.mapIdToDelegate(id, rods);

        verifyRecord(cds.getRecord(id), recordData);
        verify(rods, times(1))
                .getRecordIfStored(any(DataIdentifier.class));
    }

    @Test
    public void testDeleteAllOlderThanDoesntDeleteFromReadonlyDelegates()
            throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        DataStore ds = createSpyDelegate(folder.newFolder(), twoRoles.get(0), cds);
        DataStore rods = spy(getFileDataStore());
        cds.addDelegate(createReadOnlyDelegate(twoRoles.get(1), rods));
        cds.init(homedir);

        cds.deleteAllOlderThan(0L);

        verify(ds, times(1)).deleteAllOlderThan(anyLong());
        verify(rods, times(0)).deleteAllOlderThan(anyLong());
    }

    @Test
    public void testGetAllIdentifiersReturnsIdsFromReadOnlyDelegates()
            throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(threeRoles);
        List<DataStore> dataStores = Lists.newArrayList();
        for (String role : threeRoles) {
            if ("role1".equals(role)) {
                dataStores.add(createReadOnlySpyDelegate(role, cds));
            }
            else {
                dataStores.add(createSpyDelegate(folder.newFolder(), role, cds));
            }
        }
        cds.init(homedir);
        Set<DataIdentifier> ids = Sets.newHashSet();
        for (DataStore ds : dataStores) {
            for (int recnum=0; recnum<5; ++recnum) {
                ids.add(ds.addRecord(randomDataRecordStream()).getIdentifier());
            }
        }

        assertEquals(15, verifyRecordIds(cds, ids));
    }

    @Test
    public void testGetMinRecordLengthIgnoresReadOnlyDelegates() throws RepositoryException, IOException {
        List<String> roles = Lists.newArrayList("role1", "role2", "role3", "role4");
        CompositeDataStore cds = createEmptyCompositeDataStore(roles);

        int i = 1024*4;
        for (String role : roles) {
            DataStore ds;
            if ("role1".equals(role)) {
                ds = createReadOnlySpyDelegate(role, cds);
            }
            else {
                ds = createSpyDelegate(folder.newFolder(), role, cds);
            }
            Mockito.when(ds.getMinRecordLength()).thenReturn(i);
            i += 1024*4;
        }

        assertEquals(1024*8, cds.getMinRecordLength());
    }

    @Test
    public void testCloseClosesReadonlyDelegates() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        List<DataStore> dataStores = Lists.newArrayList(
                createReadOnlySpyDelegate(twoRoles.get(0), cds),
                createSpyDelegate(folder.newFolder(), twoRoles.get(1), cds)
        );
        cds.init(homedir);

        cds.close();

        for (DataStore ds : dataStores) {
            verify(ds, times(1)).close();
        }
    }

    @Test
    public void testDeleteDoesntRemoveFromReadonlyDelegate() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        List<DataStore> dataStores = Lists.newArrayList();
        for (String role : twoRoles) {
            if ("role1".equals(role)) {
                dataStores.add(createReadOnlySpyDelegate(role, cds));
            }
            else {
                dataStores.add(createSpyDelegate(folder.newFolder(), role, cds));
            }
        }
        Set<DataIdentifier> ids = Sets.newHashSet();
        for (DataStore ds : dataStores) {
            for (int recnum=0; recnum<5; ++recnum) {
                DataIdentifier id = ds.addRecord(randomDataRecordStream()).getIdentifier();
                cds.mapIdToDelegate(id, ds);
                ids.add(id);
            }
        }
        cds.init(homedir);

        assertEquals(10, verifyRecordIds(cds, ids));

        for (DataIdentifier id : ids) {
            cds.deleteRecord(id);
        }

        assertEquals(5, verifyRecordIds(cds, ids));
    }

    @Test
    public void testGetMetadataRecordFromReadonlyDelegate() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        List<DataStore> dataStores = Lists.newArrayList();
        List<String> names = Lists.newArrayList();

        int recnum = 1;
        String prefix = "testWriterRecord";
        for (String role : twoRoles) {
            DataStore ds;
            if ("role1".equals(role)) {
                ds = createSharedDataStoreSpyDelegate(folder.newFolder(), role, cds);
            }
            else {
                ds = createReadOnlySharedDataStoreSpyDelegate(folder.newFolder(), role, cds);
            }
            String name = String.format("%sName%d", prefix, recnum);
            names.add(name);
            ((SharedDataStore) ds).addMetadataRecord(
                    new ByteArrayInputStream(String.format("%s%d", prefix, recnum).getBytes()),
                    name
            );
            dataStores.add(ds);
            recnum++;
        }

        cds.init(homedir);

        Set<DataRecord> records = Sets.newHashSet();
        for (String name : names) {
            DataRecord rec = cds.getMetadataRecord(name);
            verifyRecord(cds.getMetadataRecord(name), prefix, false);
            records.add(rec);
        }

        List<DataRecord> recList = cds.getAllMetadataRecords(prefix);
        assertEquals(records.size(), recList.size());
        for (DataRecord rec : recList) {
            assertTrue(records.contains(rec));
        }
    }

    @Test
    public void testAddMetadataRecordAddsToReadonlyDelegate() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        for (String role : twoRoles) {
            if ("role1".equals(role)) {
                createSharedDataStoreSpyDelegate(folder.newFolder(), role, cds);
            }
            else {
                createReadOnlySharedDataStoreSpyDelegate(folder.newFolder(), role, cds);
            }
        }
        cds.init(homedir);

        String name = "testRecordName";
        String prefix = "testRecordPrefix";
        String data = "data";
        assertNull(cds.getMetadataRecord(name));

        cds.addMetadataRecord(
                new ByteArrayInputStream(
                        String.format("%s%s", prefix, data).getBytes()
                ), name);

        verifyRecord(cds.getMetadataRecord(name), String.format("%s%s", prefix, data));
    }

    @Test
    public void testDeleteMetadataRecordFromReadonlyDelegate() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        for (String role : twoRoles) {
            if ("role1".equals(role)) {
                createSharedDataStoreSpyDelegate(folder.newFolder(), role, cds);
            }
            else {
                createReadOnlySharedDataStoreSpyDelegate(folder.newFolder(), role, cds);
            }
        }
        cds.init(homedir);

        for (int i=0; i<10; ++i) {
            cds.addMetadataRecord(new ByteArrayInputStream(String.format("testRecord%d", i).getBytes()),
                    String.format("testName%d", i));
        }

        assertTrue(cds.deleteMetadataRecord("testName1"));

        cds.deleteAllMetadataRecords("testName");

        assertTrue(cds.getAllMetadataRecords("testName").isEmpty());
    }

    @Test
    public void testGetAllRecordsWithReadonlyDelegate() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        Set<DataRecord> records = Sets.newHashSet();
        for (String role : twoRoles) {
            DataStore ds;
            if ("role1".equals(role)) {
                ds = createSharedDataStoreSpyDelegate(folder.newFolder(), role, cds);
            }
            else {
                ds = createReadOnlySharedDataStoreSpyDelegate(folder.newFolder(), role, cds);
            }
            DataRecord record = ds.addRecord(new ByteArrayInputStream(String.format("testRecord%s", role).getBytes()));
            cds.mapIdToDelegate(record.getIdentifier(), ds);
            records.add(record);
        }
        cds.init(homedir);

        assertEquals(twoRoles.size(), verifyRecords(cds, records));
    }

    @Test
    public void testGetRecordForIdFromReadonlyDelegate() throws RepositoryException, IOException {
        CompositeDataStore cds = createEmptyCompositeDataStore(twoRoles);
        Set<DataIdentifier> ids = Sets.newHashSet();
        for (String role : twoRoles) {
            DataStore ds;
            if ("role1".equals(role)) {
                ds = createSharedDataStoreSpyDelegate(folder.newFolder(), role, cds);
            }
            else {
                ds = createReadOnlySharedDataStoreSpyDelegate(folder.newFolder(), role, cds);
            }
            DataRecord record = ds.addRecord(new ByteArrayInputStream(String.format("testRecord%s", role).getBytes()));
            cds.mapIdToDelegate(record.getIdentifier(), ds);
            ids.add(record.getIdentifier());
        }
        cds.init(homedir);

        for (DataIdentifier id : ids) {
            assertNotNull(cds.getRecordForId(id));
        }
    }
}
