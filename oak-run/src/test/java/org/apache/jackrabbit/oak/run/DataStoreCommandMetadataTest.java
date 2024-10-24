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
package org.apache.jackrabbit.oak.run;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.jackrabbit.guava.common.base.Joiner;
import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.plugins.blob.MemoryBlobStoreNodeStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.run.DataStoreCommandTest.DataStoreFixture;
import org.apache.jackrabbit.oak.run.DataStoreCommandTest.StoreFixture;
import org.apache.jackrabbit.oak.run.cli.BlobStoreOptions.Type;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.MARKED_START_MARKER;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.REFERENCES;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY;
import static org.junit.Assume.assumeFalse;

/**
 * Tests for {@link DataStoreCommand} metadata command
 */
@RunWith(Parameterized.class)
public class DataStoreCommandMetadataTest {
    private static Logger log = LoggerFactory.getLogger(DataStoreCommandMetadataTest.class);

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target")) {
        @Override public void delete() {

        }
    };

    private DataStoreFixture blobFixture;

    private StoreFixture storeFixture;


    private DataStoreBlobStore setupDataStore;

    private NodeStore store;


    public DataStoreCommandMetadataTest(StoreFixture storeFixture, DataStoreFixture blobFixture) {
        this.storeFixture = storeFixture;
        this.blobFixture = blobFixture;
    }

    @Parameterized.Parameters(name="{index}: ({0} : {1})")
    public static List<Object[]> fixtures() {
        return DataStoreCommandTest.FixtureHelper.get();
    }

    @Before
    public void setup() throws Exception {
        if (storeFixture instanceof StoreFixture.AzureSegmentStoreFixture) {
            assumeFalse("Environment variable \"AZURE_SECRET_KEY\" must be set to run Azure Segment fixture",
                    Strings.isNullOrEmpty(System.getenv("AZURE_SECRET_KEY")));
        }

        setupDataStore = blobFixture.init(temporaryFolder);
        store = storeFixture.init(setupDataStore, temporaryFolder.newFolder());
    }

    @After
    public void tearDown() {
        storeFixture.after();
        blobFixture.after();
    }

    private List<String> setupData() throws DataStoreException {
        /** Create records for 1st aux repository **/
        MemoryBlobStoreNodeStore memNodeStore = new MemoryBlobStoreNodeStore(setupDataStore);
        String rep2Id = ClusterRepositoryInfo.getOrCreateId(memNodeStore);
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            REPOSITORY.getNameFromId(rep2Id));

        String sessionId = UUID.randomUUID().toString();
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            MARKED_START_MARKER.getNameFromIdPrefix(rep2Id, sessionId));
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            REFERENCES.getNameFromIdPrefix(rep2Id, sessionId));

        DataRecord expectAuxMetadataRecord = setupDataStore
            .getMetadataRecord(REFERENCES.getNameFromIdPrefix(rep2Id, sessionId));
        DataRecord expectAuxMarkerMetadataRecord = setupDataStore
            .getMetadataRecord(MARKED_START_MARKER.getNameFromIdPrefix(rep2Id, sessionId));

        /** Create records for the 2nd repository **/
        MemoryBlobStoreNodeStore mem2NodeStore = new MemoryBlobStoreNodeStore(setupDataStore);
        String rep3Id = ClusterRepositoryInfo.getOrCreateId(mem2NodeStore);

        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            REPOSITORY.getNameFromId(rep3Id));

        sessionId = UUID.randomUUID().toString();
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            MARKED_START_MARKER.getNameFromIdPrefix(rep3Id, sessionId));
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            REFERENCES.getNameFromIdPrefix(rep3Id, sessionId));
        DataRecord expectAux2MetadataRecord = setupDataStore
            .getMetadataRecord(REFERENCES.getNameFromIdPrefix(rep3Id, sessionId));
        DataRecord expectAux2MarkerMetadataRecord = setupDataStore
            .getMetadataRecord(MARKED_START_MARKER.getNameFromIdPrefix(rep3Id, sessionId));

        /** Create records for the main repository **/
        String repoId = ClusterRepositoryInfo.getOrCreateId(store);
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            REPOSITORY.getNameFromId(repoId));

        sessionId = UUID.randomUUID().toString();
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            MARKED_START_MARKER.getNameFromIdPrefix(repoId, sessionId));
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            REFERENCES.getNameFromIdPrefix(repoId, sessionId));
        DataRecord expectMainMetadataRecord = setupDataStore
            .getMetadataRecord(REFERENCES.getNameFromIdPrefix(repoId, sessionId));
        DataRecord expectMainMarkerMetadataRecord = setupDataStore
            .getMetadataRecord(MARKED_START_MARKER.getNameFromIdPrefix(repoId, sessionId));

        /** Create extra record for the simulated remote repository **/
        sessionId = UUID.randomUUID().toString();
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            MARKED_START_MARKER.getNameFromIdPrefix(rep2Id, sessionId));
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            REFERENCES.getNameFromIdPrefix(rep2Id, sessionId));

        List<String> expectations = new ArrayList<>();
        expectations.add(Joiner.on("|").join(rep2Id, MILLISECONDS.toSeconds(expectAuxMarkerMetadataRecord.getLastModified()),
            MILLISECONDS.toSeconds(expectAuxMetadataRecord.getLastModified()), "-"));
        expectations.add(Joiner.on("|").join(repoId, MILLISECONDS.toSeconds(expectMainMarkerMetadataRecord.getLastModified()),
            MILLISECONDS.toSeconds(expectMainMetadataRecord.getLastModified()), "*"));
        expectations.add(Joiner.on("|").join(rep3Id, MILLISECONDS.toSeconds(expectAux2MarkerMetadataRecord.getLastModified()),
            MILLISECONDS.toSeconds(expectAux2MetadataRecord.getLastModified()), "-"));

        storeFixture.close();
        return expectations;
    }

    @Test
    public void getMetadata() throws Exception {
        List<String> expectations = setupData();

        File dump = temporaryFolder.newFolder();

        List<String> argsList = Lists
            .newArrayList("--get-metadata", "--" + getOption(blobFixture.getType()), blobFixture.getConfigPath(),
                storeFixture.getConnectionString(), "--out-dir", dump.getAbsolutePath(), "--work-dir",
                temporaryFolder.newFolder().getAbsolutePath());

        DataStoreCommand cmd = new DataStoreCommand();
        cmd.execute(argsList.toArray(new String[0]));

        File f = new File(dump, "metadata");
        Set<String> actuals = FileIOUtils.readStringsAsSet(new FileInputStream(f), false);
        Assert.assertEquals(new HashSet<>(expectations), actuals);
    }

    protected static String getOption(Type dsOption) {
        if (dsOption == Type.FDS) {
            return "fds";
        } else if (dsOption == Type.S3) {
            return "s3ds";
        } else if (dsOption == Type.AZURE) {
            return "azureds";
        } else {
            return "fake-ds-path";
        }
    }
}
