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

import static com.google.common.collect.Sets.newHashSet;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import junit.framework.Assert;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test for SharedDataUtils to test addition, retrieval and deletion of root records.
 */
public class SharedDataStoreUtilsTest {
    private static final Logger log = LoggerFactory.getLogger(SharedDataStoreUtilsTest.class);

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
        Set<String> refs = Sets.newHashSet("1_1", "1_2");
        File f = folder.newFile();
        FileIOUtils.writeStrings(refs.iterator(), f, false);

        dataStore.addMetadataRecord(new FileInputStream(f),
            SharedStoreRecordType.REFERENCES.getNameFromId(repoId));
        DataRecord rec = dataStore.getMetadataRecord(SharedStoreRecordType.REFERENCES.getNameFromId(repoId));
        Set<String> refsReturned = FileIOUtils.readStringsAsSet(rec.getStream(), false);
        Assert.assertEquals(refs, refsReturned);
        dataStore.deleteAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());

        dataStore.addMetadataRecord(f,
            SharedStoreRecordType.REFERENCES.getNameFromId(repoId));
        rec = dataStore.getMetadataRecord(SharedStoreRecordType.REFERENCES.getNameFromId(repoId));
        refsReturned = FileIOUtils.readStringsAsSet(rec.getStream(), false);
        Assert.assertEquals(refs, refsReturned);
        assertEquals(
            SharedStoreRecordType.REFERENCES.getIdFromName(rec.getIdentifier().toString()),
            repoId);
        dataStore.deleteAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());
    }

    @Test
    public void testGetAllChunkIds() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        int number = 10;
        Set<String> added = newHashSet();
        for (int i = 0; i < number; i++) {
            String rec = dataStore.writeBlob(randomStream(i, 16516));
            added.add(rec);
        }

        Set<String> retrieved = newHashSet(dataStore.getAllChunkIds(0));
        assertEquals(added, retrieved);
    }

    @Test
    public void testGetAllRecords() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        int number = 10;
        Set<String> added = newHashSet();
        for (int i = 0; i < number; i++) {
            String rec = dataStore.addRecord(randomStream(i, 16516))
                .getIdentifier().toString();
            added.add(rec);
        }

        Set<String> retrieved = newHashSet(Iterables.transform(newHashSet(dataStore.getAllRecords()),
            new Function<DataRecord, String>() {
                @Nullable @Override public String apply(@Nullable DataRecord input) {
                    return input.getIdentifier().toString();
                }
            }));
        assertEquals(added, retrieved);
    }

    @Test
    public void testStreamFromGetAllRecords() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        int number = 10;
        Set<DataRecord> added = newHashSet();
        for (int i = 0; i < number; i++) {
            added.add(dataStore.addRecord(randomStream(i, 16516)));
        }

        Set<DataRecord> retrieved = newHashSet((dataStore.getAllRecords()));
        assertRecords(added, retrieved);
    }

    @Test
    public void testGetRecordForId() throws Exception {
        File rootFolder = folder.newFolder();
        dataStore = getBlobStore(rootFolder);
        int number = 10;
        Set<DataRecord> added = newHashSet();
        for (int i = 0; i < number; i++) {
            added.add(dataStore.addRecord(randomStream(i, 16516)));
        }

        Set<DataRecord> retrieved = newHashSet();
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

