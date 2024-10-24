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
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for SharedDataUtils to test addition, retrieval and deletion of root records.
 */
public class SharedDataStoreUtilsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    protected DataStoreBlobStore dataStore;

    protected DataStoreBlobStore getBlobStore(File root) throws Exception {
        return DataStoreUtils.getBlobStore(root);
    }

    @Test
    public void test() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        String repoId1 = UUID.randomUUID().toString();
        String repoId2 = UUID.randomUUID().toString();

        // Add repository records
        dataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId1));
        DataRecord repo1 = dataStore.getMetadataRecord(SharedStoreRecordType.REPOSITORY.getNameFromId(repoId1));
        dataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId2));
        DataRecord repo2 = dataStore.getMetadataRecord(SharedStoreRecordType.REPOSITORY.getNameFromId(repoId2));
        // Add reference marker record for repo1
        dataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
                                       SharedStoreRecordType.MARKED_START_MARKER.getNameFromId(repoId1));
        DataRecord markerRec1 = dataStore.getMetadataRecord(SharedStoreRecordType.MARKED_START_MARKER.getNameFromId(repoId1));
        assertEquals(
               SharedStoreRecordType.MARKED_START_MARKER.getIdFromName(markerRec1.getIdentifier().toString()),
               repoId1);
        long lastModifiedMarkerRec1 = markerRec1.getLastModified();
        TimeUnit.MILLISECONDS.sleep(100);
        
        // Add reference records
        dataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REFERENCES.getNameFromId(repoId1));
        DataRecord rec1 = dataStore.getMetadataRecord(SharedStoreRecordType.REFERENCES.getNameFromId(repoId1));
        long lastModifiedRec1 = rec1.getLastModified();
        TimeUnit.MILLISECONDS.sleep(25);
        dataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REFERENCES.getNameFromId(repoId2));
        DataRecord rec2 = dataStore.getMetadataRecord(SharedStoreRecordType.REFERENCES.getNameFromId(repoId2));
        long lastModifiedRec2 = rec2.getLastModified();

        assertEquals(SharedStoreRecordType.REPOSITORY.getIdFromName(repo1.getIdentifier().toString()), repoId1);
        assertEquals(
                SharedStoreRecordType.REPOSITORY.getIdFromName(repo2.getIdentifier().toString()),
                repoId2);
        assertEquals(
                SharedStoreRecordType.REFERENCES.getIdFromName(rec1.getIdentifier().toString()),
                repoId1);
        assertEquals(
                SharedStoreRecordType.REFERENCES.getIdFromName(rec2.getIdentifier().toString()),
                repoId2);

        // All the references from registered repositories are available
        Assert.assertTrue(SharedDataStoreUtils
                .refsNotAvailableFromRepos(dataStore.getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType()),
                    dataStore.getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType())).isEmpty());

        // Since, we don't care about which file specifically but only the earliest timestamped record
        // Earliest time should be the min timestamp from the 2 reference files
        long minRefTime = (lastModifiedRec1 <= lastModifiedRec2 ? lastModifiedRec1 : lastModifiedRec2);
        assertEquals(
               SharedDataStoreUtils.getEarliestRecord(
                        dataStore.getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType())).getLastModified(), 
                        minRefTime);
        
        // the marker timestamp should be the minimum
        long minMarkerTime = 
            SharedDataStoreUtils.getEarliestRecord(
                    dataStore.getAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType()))
                        .getLastModified();
        Assert.assertTrue(minRefTime >= minMarkerTime);
        
        // Delete references and check back if deleted
        dataStore.deleteAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());
        Assert.assertTrue(dataStore.getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType()).isEmpty());
        
        // Delete markers and check back if deleted
        dataStore.deleteAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType());
        Assert.assertTrue(dataStore.getAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType()).isEmpty());
    
        // Repository ids should still be available
        assertEquals(2,
            dataStore.getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType()).size());
    }

    @Test
    public void testAddMetadata() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        String repoId = UUID.randomUUID().toString();
        Set<String> refs = Set.of("1_1", "1_2");
        File f = folder.newFile();
        FileIOUtils.writeStrings(refs.iterator(), f, false);

        dataStore.addMetadataRecord(new FileInputStream(f),
            SharedStoreRecordType.REFERENCES.getNameFromId(repoId));
        assertTrue(dataStore.metadataRecordExists(SharedStoreRecordType.REFERENCES.getNameFromId(repoId)));
        DataRecord rec = dataStore.getMetadataRecord(SharedStoreRecordType.REFERENCES.getNameFromId(repoId));
        Set<String> refsReturned = FileIOUtils.readStringsAsSet(rec.getStream(), false);
        assertEquals(refs, refsReturned);
        assertEquals(
            SharedStoreRecordType.REFERENCES.getIdFromName(rec.getIdentifier().toString()),
            repoId);
        dataStore.deleteAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());
    }

    @Test
    public void testAddMetadataWithExtraSuffix() throws Exception {
        addMultipleMetadata(true);
    }

    @Test
    public void testAddMetadataWithConditionalExtraSuffix() throws Exception {
        addMultipleMetadata(false);
    }

    @Test
    public void testRefsAvailableAllRepos() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);

        loadData(true);

        // check if All the references from registered repositories are available
        Assert.assertTrue(SharedDataStoreUtils
            .refsNotAvailableFromRepos(dataStore.getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType()),
                dataStore.getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType())).isEmpty());
    }

    @Test
    public void testRefsNotAvailableAllRepos() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);

        Data data = loadData(true);
        // Delete one of the references file
        String expectedMissingRepoId = data.repoIds.get(data.repoIds.size() - 1);
        dataStore.deleteAllMetadataRecords(
            SharedStoreRecordType.REFERENCES.getNameFromId(expectedMissingRepoId));

        // check if All the references from registered repositories are available
        Set<String> missingRepoIds = SharedDataStoreUtils
            .refsNotAvailableFromRepos(dataStore.getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType()),
                dataStore.getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType()));
        assertEquals(Set.of(expectedMissingRepoId), missingRepoIds);
    }

    @Test
    public void testRefsOld() throws Exception {
        Clock clock = new Clock.Virtual();
        TimeLapsedDataStore ds = new TimeLapsedDataStore(clock);
        Data data = new Data();

        String repoId1 = UUID.randomUUID().toString();

        data.repoIds.add(repoId1);
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId1));
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.MARKED_START_MARKER.getNameFromId(repoId1));

        String repoId2 = UUID.randomUUID().toString();

        data.repoIds.add(repoId2);
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId2));
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.MARKED_START_MARKER.getNameFromId(repoId2));

        clock.waitUntil(10);
        // All the references from registered repositories are available
        Set<String> repos = SharedDataStoreUtils
            .refsNotOld(ds.getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType()),
                ds.getAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType()), 5);
        assertEquals(Collections.EMPTY_SET, repos);
    }

    @Test
    public void testRefsNotOldOne() throws Exception {
        Clock clock = new Clock.Virtual();
        TimeLapsedDataStore ds = new TimeLapsedDataStore(clock);
        Data data = new Data();

        String repoId1 = UUID.randomUUID().toString();

        data.repoIds.add(repoId1);
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId1));
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.MARKED_START_MARKER.getNameFromId(repoId1));

        String repoId2 = UUID.randomUUID().toString();

        data.repoIds.add(repoId2);
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId2));
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.MARKED_START_MARKER.getNameFromId(repoId2));

        clock.waitUntil(10);
        // Only references from first registered repository is available
        Set<String> repos = SharedDataStoreUtils
            .refsNotOld(ds.getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType()),
                ds.getAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType()), 3);
        assertEquals(CollectionUtils.toSet(repoId2), repos);
    }

    @Test
    public void testRefsNotOldAll() throws Exception {
        Clock clock = new Clock.Virtual();
        TimeLapsedDataStore ds = new TimeLapsedDataStore(clock);
        Data data = new Data();

        String repoId1 = UUID.randomUUID().toString();

        data.repoIds.add(repoId1);
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId1));
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.MARKED_START_MARKER.getNameFromId(repoId1));

        String repoId2 = UUID.randomUUID().toString();

        data.repoIds.add(repoId2);
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId2));
        ds.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.MARKED_START_MARKER.getNameFromId(repoId2));

        clock.waitUntil(5);
        // none of the references from registered repositories are available
        Set<String> repos = SharedDataStoreUtils
            .refsNotOld(ds.getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType()),
                ds.getAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType()), 2);
        assertEquals(CollectionUtils.toSet(repoId1, repoId2), repos);
    }

    @Test
    public void repoMarkerExistOnClose() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        String repoId = UUID.randomUUID().toString();
        dataStore.setRepositoryId(repoId);
        assertEquals(repoId, dataStore.getRepositoryId());
        assertNotNull(dataStore.getMetadataRecord(SharedStoreRecordType.REPOSITORY.getNameFromId(repoId)));

        assertNotNull(FileUtils.getFile(new File(rootFolder, "repository/datastore"),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId)));
        dataStore.close();

        dataStore = getBlobStore(rootFolder);
        assertNotNull(dataStore.getMetadataRecord(SharedStoreRecordType.REPOSITORY.getNameFromId(repoId)));
    }

    @Test
    public void transientRepoMarkerDeleteOnClose() throws Exception {
        System.setProperty("oak.datastore.sharedTransient", "true");
        try {
            File rootFolder = folder.newFolder();
            dataStore = getBlobStore(rootFolder);
            String repoId = UUID.randomUUID().toString();
            dataStore.setRepositoryId(repoId);
            assertEquals(repoId, dataStore.getRepositoryId());
            assertNotNull(dataStore.getMetadataRecord(SharedStoreRecordType.REPOSITORY.getNameFromId(repoId)));

            assertNotNull(FileUtils.getFile(new File(rootFolder, "repository/datastore"),
                SharedStoreRecordType.REPOSITORY.getNameFromId(repoId)));
            dataStore.close();

            dataStore = getBlobStore(rootFolder);
            assertNull(dataStore.getMetadataRecord(SharedStoreRecordType.REPOSITORY.getNameFromId(repoId)));
        } finally {
            System.clearProperty("oak.datastore.sharedTransient");
        }
    }

    private void addMultipleMetadata(boolean extended) throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);

        Data data = loadData(extended);

        // Retrieve all records
        List<DataRecord> recs =
            dataStore.getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());

        Set<String> returnedRefs = new HashSet<>();
        for (DataRecord retRec : recs) {
            assertTrue(data.repoIds.contains(SharedStoreRecordType.REFERENCES.getIdFromName(retRec.getIdentifier().toString())));
            returnedRefs.addAll(FileIOUtils.readStringsAsSet(retRec.getStream(), false));
        }
        assertEquals(data.refs, returnedRefs);

        // Delete all references records
        dataStore.deleteAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());
        for (int i = 0; i < data.repoIds.size(); i++) {
            assertFalse(
                dataStore.metadataRecordExists(getName(extended, data.repoIds.get(i), data.suffixes.get(i + 1))));

            if (i == 0) {
                assertFalse(
                    dataStore.metadataRecordExists(getName(extended, data.repoIds.get(i), data.suffixes.get(i))));
            }
        }
    }

    private Data loadData(boolean extended) throws Exception {
        Data data = new Data();
        String repoId1 = UUID.randomUUID().toString();
        data.repoIds.add(repoId1);
        dataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId1));

        Set<String> refs = Set.of("1_1", "1_2");
        data.refs.addAll(refs);
        File f = folder.newFile();
        FileIOUtils.writeStrings(refs.iterator(), f, false);

        Set<String> refs2 = Set.of("2_1", "2_2");
        data.refs.addAll(refs2);
        File f2 = folder.newFile();
        FileIOUtils.writeStrings(refs2.iterator(), f2, false);

        String repoId2 = UUID.randomUUID().toString();
        data.repoIds.add(repoId2);
        dataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            SharedStoreRecordType.REPOSITORY.getNameFromId(repoId2));

        Set<String> refs3 = Set.of("3_1", "3_2");
        data.refs.addAll(refs3);
        File f3 = folder.newFile();
        FileIOUtils.writeStrings(refs3.iterator(), f3, false);

        String suffix1 = String.valueOf(System.currentTimeMillis());
        data.suffixes.add(suffix1);
        dataStore.addMetadataRecord(new FileInputStream(f), getName(extended, repoId1, suffix1));

        // Checks for the presence of existing record
        assertTrue(dataStore.metadataRecordExists(getName(extended, repoId1, suffix1)));

        DataRecord rec = dataStore.getMetadataRecord(getName(extended, repoId1, suffix1));
        assertEquals(refs, FileIOUtils.readStringsAsSet(rec.getStream(), false));

        // Add a duplicate
        TimeUnit.MILLISECONDS.sleep(100);
        String suffix2 = String.valueOf(System.currentTimeMillis());
        data.suffixes.add(suffix2);
        dataStore.addMetadataRecord(f2, getName(true, repoId1, suffix2));

        // Check presence of the duplicate
        assertTrue(
            dataStore.metadataRecordExists(getName(extended, repoId1, suffix2)));

        // Add a separate record
        TimeUnit.MILLISECONDS.sleep(100);
        String suffix3 = String.valueOf(System.currentTimeMillis());
        data.suffixes.add(suffix3);
        dataStore.addMetadataRecord(f3, getName(extended, repoId2, suffix3));

        // Check presence of the separate
        assertTrue(
            dataStore.metadataRecordExists(getName(extended, repoId2, suffix3)));

        return data;
    }

    class Data {
        List<String> suffixes = new ArrayList<>();
        List<String> repoIds = new ArrayList<>();
        Set<String> refs = new HashSet<>();
    }

    private static String getName(boolean extended, String repoId, String suffix) {
        if (!extended) {
            return SharedStoreRecordType.REFERENCES.getNameFromId(repoId);
        } else {
            return SharedStoreRecordType.REFERENCES.getNameFromIdPrefix(repoId, suffix);
        }
    }

    @Test
    public void testGetAllChunkIds() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        int number = 10;
        Set<String> added = new HashSet<>();
        for (int i = 0; i < number; i++) {
            String rec = dataStore.writeBlob(randomStream(i, 16516));
            added.add(rec);
        }

        Set<String> retrieved = CollectionUtils.toSet(dataStore.getAllChunkIds(0));
        assertEquals(added, retrieved);
    }

    @Test
    public void testGetAllRecords() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        int number = 10;
        Set<String> added = new HashSet<>();
        for (int i = 0; i < number; i++) {
            String rec = dataStore.addRecord(randomStream(i, 16516))
                .getIdentifier().toString();
            added.add(rec);
        }

        Set<String> retrieved = CollectionUtils.toSet(Iterables.transform(CollectionUtils.toSet(dataStore.getAllRecords()),
                input -> input.getIdentifier().toString()));

        assertEquals(added, retrieved);
    }

    @Test
    public void testGetAllRecordsWithMeta() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        int number = 2;
        Set<String> added = new HashSet<>();
        for (int i = 0; i < number; i++) {
            String rec = dataStore.addRecord(randomStream(i, 16516))
                .getIdentifier().toString();
            added.add(rec);
        }

        if (dataStore.getType() == SharedDataStore.Type.SHARED) {
            dataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]), "1meta");
            dataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]), "2meta");
            dataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]), "ameta1");
            dataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]), "bmeta2");
        }

        for (int i = 0; i < number; i++) {
            String rec = dataStore.addRecord(randomStream(100+i, 16516))
                .getIdentifier().toString();
            added.add(rec);
        }

        Set<String> retrieved = CollectionUtils.toSet(Iterables.transform(CollectionUtils.toSet(dataStore.getAllRecords()),
                input -> input.getIdentifier().toString()));

        assertEquals(added, retrieved);
    }

    @Test
    public void testStreamFromGetAllRecords() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        int number = 10;
        Set<DataRecord> added = new HashSet<>();
        for (int i = 0; i < number; i++) {
            added.add(dataStore.addRecord(randomStream(i, 16516)));
        }

        Set<DataRecord> retrieved = CollectionUtils.toSet((dataStore.getAllRecords()));
        assertRecords(added, retrieved);
    }

    @Test
    public void testGetRecordForId() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        int number = 10;
        Set<DataRecord> added = new HashSet<>();
        for (int i = 0; i < number; i++) {
            added.add(dataStore.addRecord(randomStream(i, 16516)));
        }

        Set<DataRecord> retrieved = new HashSet<>();
        for (DataRecord rec : added) {
            retrieved.add(dataStore.getRecordForId(rec.getIdentifier()));
        }
        assertRecords(added, retrieved);
    }

    private static void assertRecords(Set<DataRecord> expected, Set<DataRecord> retrieved)
        throws DataStoreException, IOException {
        //assert streams
        Map<DataIdentifier, DataRecord> retMap = Maps.newHashMap();
        for (DataRecord ret : retrieved) {
            retMap.put(ret.getIdentifier(), ret);
        }

        for (DataRecord rec : expected) {
            assertEquals("Record id different for " + rec.getIdentifier(),
                rec.getIdentifier(), retMap.get(rec.getIdentifier()).getIdentifier());
            assertEquals("Record length different for " + rec.getIdentifier(),
                rec.getLength(), retMap.get(rec.getIdentifier()).getLength());
            assertEquals("Record lastModified different for " + rec.getIdentifier(),
                rec.getLastModified(), retMap.get(rec.getIdentifier()).getLastModified());
            assertTrue("Record steam different for " + rec.getIdentifier(),
                IOUtils.contentEquals(rec.getStream(), retMap.get(rec.getIdentifier()).getStream()));
        }
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }
}

