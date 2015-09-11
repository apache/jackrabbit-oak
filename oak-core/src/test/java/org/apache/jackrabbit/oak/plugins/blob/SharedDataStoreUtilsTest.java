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

import static org.hamcrest.CoreMatchers.instanceOf;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test for SharedDataUtils to test addition, retrieval and deletion of root records.
 */
public class SharedDataStoreUtilsTest {
    SharedDataStore dataStore;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        try {
            Assume.assumeThat(DataStoreUtils.getBlobStore(), instanceOf(SharedDataStore.class));
        } catch (Exception e) {
            Assume.assumeNoException(e);
        }
    }

    @Test
    public void test() throws Exception {
        dataStore = DataStoreUtils.getBlobStore();
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
        Assert.assertEquals(
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

        Assert.assertEquals(SharedStoreRecordType.REPOSITORY.getIdFromName(repo1.getIdentifier().toString()), repoId1);
        Assert.assertEquals(
                SharedStoreRecordType.REPOSITORY.getIdFromName(repo2.getIdentifier().toString()),
                repoId2);
        Assert.assertEquals(
                SharedStoreRecordType.REFERENCES.getIdFromName(rec1.getIdentifier().toString()),
                repoId1);
        Assert.assertEquals(
                SharedStoreRecordType.REFERENCES.getIdFromName(rec2.getIdentifier().toString()),
                repoId2);

        // All the references from registered repositories are available
        Assert.assertTrue(SharedDataStoreUtils
                .refsNotAvailableFromRepos(dataStore.getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType()),
                    dataStore.getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType())).isEmpty());

        // Since, we don't care about which file specifically but only the earliest timestamped record
        // Earliest time should be the min timestamp from the 2 reference files
        long minRefTime = (lastModifiedRec1 <= lastModifiedRec2 ? lastModifiedRec1 : lastModifiedRec2);
        Assert.assertEquals(
               SharedDataStoreUtils.getEarliestRecord(
                        dataStore.getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType())).getLastModified(), 
                        minRefTime);
        
        // the marker timestamp should be the minimum
        long minMarkerTime = 
            SharedDataStoreUtils.getEarliestRecord(
                    dataStore.getAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType()))
                        .getLastModified();
        Assert.assertTrue(minRefTime > minMarkerTime);
        
        // Delete references and check back if deleted
        dataStore.deleteAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType());
        Assert.assertTrue(dataStore.getAllMetadataRecords(SharedStoreRecordType.REFERENCES.getType()).isEmpty());
        
        // Delete markers and check back if deleted
        dataStore.deleteAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType());
        Assert.assertTrue(dataStore.getAllMetadataRecords(SharedStoreRecordType.MARKED_START_MARKER.getType()).isEmpty());
    
        // Repository ids should still be available
        Assert.assertEquals(2,
            dataStore.getAllMetadataRecords(SharedStoreRecordType.REPOSITORY.getType()).size());
    }

    @After
    public void close() throws IOException {
        FileUtils.cleanDirectory(new File(DataStoreUtils.getHomeDir()));
    }
}

