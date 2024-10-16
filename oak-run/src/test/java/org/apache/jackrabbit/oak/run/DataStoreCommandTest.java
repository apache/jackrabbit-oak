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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import ch.qos.logback.classic.Level;
import org.apache.jackrabbit.guava.common.base.Joiner;
import org.apache.jackrabbit.guava.common.base.Splitter;
import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.guava.common.collect.Sets;
import joptsimple.OptionException;
import org.apache.commons.io.FileUtils;
import org.apache.felix.cm.file.ConfigurationHandler;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStoreUtils;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStoreUtils;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.plugins.blob.MemoryBlobStoreNodeStore;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MongoUtils;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.OakDirectory;
import org.apache.jackrabbit.oak.run.cli.BlobStoreOptions.Type;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureStorageCredentialManagerV8;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.sort;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.writeStrings;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.REFERENCES;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY;
import static org.apache.jackrabbit.oak.run.DataStoreCommand.VerboseIdLogger.DASH;
import static org.apache.jackrabbit.oak.run.DataStoreCommand.VerboseIdLogger.HASH;
import static org.apache.jackrabbit.oak.run.DataStoreCommand.VerboseIdLogger.filterFiles;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

/**
 * Tests for {@link DataStoreCommand}
 */
@RunWith(Parameterized.class)
public class DataStoreCommandTest {
    private static Logger log = LoggerFactory.getLogger(DataStoreCommandTest.class);

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private DataStoreFixture blobFixture;

    private StoreFixture storeFixture;

    private String additionalParams;

    private DataStoreBlobStore setupDataStore;

    private NodeStore store;

    public DataStoreCommandTest(StoreFixture storeFixture, DataStoreFixture blobFixture) {
        this.storeFixture = storeFixture;
        this.blobFixture = blobFixture;
    }

    @Parameterized.Parameters(name="{index}: ({0} : {1})")
    public static List<Object[]> fixtures() {
        return FixtureHelper.get();
    }

    @Before
    public void setup() throws Exception {
        if (storeFixture instanceof StoreFixture.AzureSegmentStoreFixture) {
            assumeFalse("Environment variable \"AZURE_SECRET_KEY\" must be set to run Azure Segment fixture",
                    Strings.isNullOrEmpty(System.getenv("AZURE_SECRET_KEY")));
        }

        setupDataStore = blobFixture.init(temporaryFolder);
        store = storeFixture.init(setupDataStore, temporaryFolder.newFolder());
        additionalParams = "--ds-read-write";

        String repoId = ClusterRepositoryInfo.getOrCreateId(store);
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            REPOSITORY.getNameFromId(repoId));
    }

    private static Data prepareData(StoreFixture storeFixture, DataStoreFixture blobFixture, int numBlobs,
                                    int numMaxDeletions, int missingDataStore) throws Exception{
        return prepareData(storeFixture, blobFixture, numBlobs, numMaxDeletions, missingDataStore, false);
    }

    private static Data prepareData(StoreFixture storeFixture, DataStoreFixture blobFixture, int numBlobs,
        int numMaxDeletions, int missingDataStore, boolean createMultiLevelNodes) throws Exception {
        return prepareData(storeFixture, blobFixture, numBlobs, numMaxDeletions, missingDataStore, createMultiLevelNodes, false);
    }

    private static Data prepareData(StoreFixture storeFixture, DataStoreFixture blobFixture, int numBlobs,
        int numMaxDeletions, int missingDataStore, boolean createMultiLevelNodes, boolean useDirListing) throws Exception {

        DataStoreBlobStore blobStore = blobFixture.getDataStore();
        NodeStore store = storeFixture.getNodeStore();
        storeFixture.preDataPrepare();

        Data data = new Data();

        List<Integer> toBeDeleted = Lists.newArrayList();
        Random rand = new Random();
        for (int i = 0; i < numMaxDeletions; i++) {
            int n = rand.nextInt(numBlobs);
            if (!toBeDeleted.contains(n)) {
                toBeDeleted.add(n);
            }
        }

        NodeBuilder a = store.getRoot().builder();
        Map<String, NodeBuilder> map = new HashMap<String, NodeBuilder>();
        map.put("/",a);
        if (createMultiLevelNodes) {
            NodeBuilder a1 = a.child("foo");
            NodeBuilder a2 = a.child("bar");

            for (int j = 0 ; j < 5 ; j ++) {
                NodeBuilder a11 = a1.child("foo" + j).child("test");
                map.put("/foo/foo" + j + "/test", a11);
                NodeBuilder a22 = a2.child("bar" + j).child("test");
                map.put("/bar/bar" + j + "/test", a22);
            }
        }
        for (int i = 0; i < numBlobs; i++) {
            List<String> valuesList = new ArrayList<String>(map.keySet());
            int randomIndex = new Random().nextInt(valuesList.size());


            String pathRoot = valuesList.get(randomIndex);

            Blob b = store.createBlob(randomStream(i, 18342));
            Iterator<String> idIter = blobStore.resolveChunks(b.getContentIdentity());
            while (idIter.hasNext()) {
                String chunk = idIter.next();
                data.added.add(chunk);
                data.idToPath.put(chunk, (createMultiLevelNodes ? pathRoot : "") + "/c" + i);
                if (!createMultiLevelNodes && toBeDeleted.contains(i)) {
                    data.deleted.add(chunk);
                }
            }
            NodeBuilder parent = a;
            if (createMultiLevelNodes) {
                parent = map.get(pathRoot);
            }
            parent.child("c" + i).setProperty("x", b);
            if (useDirListing) {
                setDirListing(parent);
            }
        }

        store.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        log.info("Created Data : {}", data);

        // Not deleting nodes in case of multi level nodes
        // Probably  change/improve this in future.
        if (!createMultiLevelNodes) {
            for (int id : toBeDeleted) {
                delete("c" + id, store);
            }
            log.info("Deleted nodes : {}", toBeDeleted.size());
        }

        int missing = 0;
        Iterator<String> iterator = data.added.iterator();
        while (iterator.hasNext()) {
            if (missing < missingDataStore) {
                String id = iterator.next();
                if (!data.deleted.contains(id)) {
                    data.missingDataStore.add(id);
                    missing++;
                }
            } else {
                break;
            }
        }

        for (String id : data.missingDataStore) {
            long count = blobStore.countDeleteChunks(ImmutableList.of(id), 0);
            assertEquals(1, count);
        }

        // Sleep a little to make eligible for cleanup
        TimeUnit.MILLISECONDS.sleep(10);

        storeFixture.postDataPrepare();

        return data;
    }

    private static void setDirListing(NodeBuilder parent) {
        List<String> names = StreamSupport.stream(parent.getChildNodeNames().spliterator(), false)
                .collect(Collectors.toList());
        parent.setProperty(OakDirectory.PROP_DIR_LISTING, names, STRINGS);
    }

    protected static void delete(String nodeId, NodeStore nodeStore) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        builder.child(nodeId).remove();

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @After
    public void tearDown() {
        storeFixture.after();
        blobFixture.after();
    }

    @Test
    public void testNoSensitiveParams() throws Exception {
        storeFixture.close();
        File dump = temporaryFolder.newFolder();
        List<String> argsList = Lists
            .newArrayList("--check-consistency", "--" + getOption(blobFixture.getType()), blobFixture.getConfigPath(), "--out-dir",
                dump.getAbsolutePath(), storeFixture.getConnectionString(), "--reset-log-config", "false", "--work-dir",
                temporaryFolder.newFolder().getAbsolutePath());
        if (!Strings.isNullOrEmpty(additionalParams)) {
            argsList.add(additionalParams);
        }

        log.info("Running testNoSensitiveParams: {}", argsList);
        LogCustomizer customLogs = LogCustomizer
            .forLogger(DataStoreCommand.class.getName())
            .enable(Level.INFO)
            .filter(Level.INFO)
            .matchesRegex(".*mongodb:.*|.*az:.*")
            .create();
        customLogs.starting();

        DataStoreCommand cmd = new DataStoreCommand();
        try {
            cmd.execute(argsList.toArray(new String[0]));
        } catch (Exception e) {
            log.error("", e);
        }

        Assert.assertEquals(0, customLogs.getLogs().size());
        customLogs.finished();
    }

    @Test
    public void testMissingOpParams() throws Exception {
        storeFixture.close();
        File dump = temporaryFolder.newFolder();
        List<String> argsList = Lists
            .newArrayList("--" + getOption(blobFixture.getType()), blobFixture.getConfigPath(), "--out-dir",
                dump.getAbsolutePath(), storeFixture.getConnectionString(), "--reset-log-config", "false", "--work-dir",
                temporaryFolder.newFolder().getAbsolutePath());
        if (!Strings.isNullOrEmpty(additionalParams)) {
            argsList.add(additionalParams);
        }

        log.info("Running testMissingOpParams: {}", argsList);
        testIncorrectParams(argsList, Lists.newArrayList("No actions specified"), DataStoreCommand.class);
    }

    /**
     * Only for Segment/Tar
     * @throws Exception
     */
    @Test
    public void testTarNoDS() throws Exception {
        storeFixture.close();
        Assume.assumeTrue(storeFixture instanceof StoreFixture.SegmentStoreFixture);

        File dump = temporaryFolder.newFolder();
        List<String> argsList = Lists
            .newArrayList(storeFixture.getConnectionString(),
                "--out-dir", dump.getAbsolutePath(), "--reset-log-config", "false", "--work-dir",
                temporaryFolder.newFolder().getAbsolutePath(), "--check-consistency");
        if (!Strings.isNullOrEmpty(additionalParams)) {
            argsList.add(additionalParams);
        }

        testIncorrectParams(argsList, Lists.newArrayList("No BlobStore specified"), DataStoreCommand.class);
    }

    @Test
    public void testConsistencyMissing() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 1);
        storeFixture.close();

        testConsistency(dump, data, false, false);
    }

    @Test
    public void testConsistencyVerbose() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 1);
        storeFixture.close();

        testConsistency(dump, data, true, false);
    }

    @Test
    public void testConsistencyVerboseRootPathNoMissing() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 0, 0);
        storeFixture.close();
        additionalParams += " --verboseRootPath /c1,/c2";


        for (String id : data.idToPath.keySet()) {
            if (data.idToPath.get(id).equals("/c1") || data.idToPath.get(id).equals("/c2")) {
                data.addedSubset.add(id);
            }
        }
        testConsistency(dump, data, true, true);
    }

    @Test
    public void testConsistencyVerboseRootPathWithDeleted() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 4, 1);
        storeFixture.close();
        additionalParams += " --verboseRootPath /c1,/c2";


        for (String id : data.idToPath.keySet()) {
            if (data.idToPath.get(id).equals("/c1") || data.idToPath.get(id).equals("/c2")) {
                data.addedSubset.add(id);
            }
        }
        testConsistency(dump, data, true, true);
    }

    @Test
    public void testConsistencyVerboseRootPath() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 0, 2);
        storeFixture.close();
        additionalParams += " --verboseRootPath /c1,/c2";
        for (String id : data.idToPath.keySet()) {
            if (data.idToPath.get(id).equals("/c1") || data.idToPath.get(id).equals("/c2")) {
                data.addedSubset.add(id);
            }
        }
        testConsistency(dump, data, true, true);
    }

    @Test
    public void testConsistencyVerboseRootPathWithoutVerbose() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 1);
        storeFixture.close();

        additionalParams += " --verboseRootPath /a";
        try {
            // Create exec command without --verbose - It should throw an exception
            // since we used --verboseRootPath without --verbose
            testConsistency(dump, data, false, true);
        } catch (OptionException e) {
            assertTrue(e.getMessage().equals("Option(s) [verboseRootPath] are unavailable " +
                    "given other options on the command line"));
            return;
        }
        assertFalse(true);
    }

    @Test
    public void testDumpRefWithVerboseRootPath() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 4, 1);
        storeFixture.close();
        additionalParams += " --verboseRootPath /c1,/c2";


        for (String id : data.idToPath.keySet()) {
            if (data.idToPath.get(id).equals("/c1") || data.idToPath.get(id).equals("/c2")) {
                data.addedSubset.add(id);
            }
        }
        testDumpRef(dump, data, true, true);
    }

    @Test
    public void testDumpRef() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 4, 1);
        storeFixture.close();

        testDumpRef(dump, data, true, false);
    }

    @Test
    public void testDumpRefWithoutVerbose() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 4, 1);
        storeFixture.close();

        testDumpRef(dump, data, false, false);
    }

    @Test
    public void testDumpRefVerboseRootPathRegex() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 4, 1, true);
        storeFixture.close();

        additionalParams += " --verboseRootPath /foo --verbosePathInclusionRegex /*/test";

        for (String id : data.idToPath.keySet()) {
            if (data.idToPath.get(id).contains("/foo")) {
                data.addedSubset.add(id);
            }
        }

        testDumpRef(dump, data, true, true);
    }

    @Test
    public void testDumpRefVerboseRegexWithoutVerboseRootPath() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 4, 1);
        storeFixture.close();

        additionalParams += " --verbosePathInclusionRegex /*/*";

        try {
            // Create exec command without --verboseRootPath - It should throw an exception
            // since we used --verbosePathInclusionRegex without --verboseRootPath
            testDumpRef(dump, data, true, false);
        } catch (OptionException e) {
            assertTrue(e.getMessage().equals("Option(s) [verbosePathInclusionRegex] are unavailable " +
                    "given other options on the command line"));
            return;
        }
        assertFalse(true);
    }

    @Test
    public void testDumpRefWithUseDirListing() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 4, 1, true, true);
        storeFixture.close();

        additionalParams += " --useDirListing --verboseRootPath /foo --verbosePathInclusionRegex /*/test";

        for (String id : data.idToPath.keySet()) {
            if (data.idToPath.get(id).contains("/foo")) {
                data.addedSubset.add(id);
            }
        }

        testDumpRef(dump, data, true, true);
    }

    @Test
    public void testDumpId() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 4, 1);
        storeFixture.close();

        testDumpIds(dump, data, true);
    }

    @Test
    public void testDumpIdWithoutVerbose() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 4, 1);
        storeFixture.close();

        testDumpIds(dump, data, false);
    }

    @Test
    public void testConsistencyNoMissing() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 0);
        storeFixture.close();

        testConsistency(dump, data, false, false);
    }

    @Test
    public void testConsistencyMarkOnly() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 0);
        storeFixture.close();

        testConsistency(dump, data, false, false, true);
    }

    @Test
    public void gcWithConsistency() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 1);
        storeFixture.close();
        additionalParams += " --check-consistency-gc true";
        testGc(dump, data, 0, false, false);

        assertFileEquals(dump, "avail-", Sets.difference(data.added, data.missingDataStore));

        // Verbose would have paths as well as ids changed but normally only DocumentNS would have paths suffixed
        assertFileEquals(dump, "consistencyCandidatesAfterGC", data.missingDataStore);
    }

    @Test
    public void gcSweepRefsOld() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 0);
        storeFixture.close();
        additionalParams += " --check-consistency-gc true --sweep-only-refs-past-retention true";
        testGc(dump, data, 0, false, false);
    }

    @Test
    public void gcSweepRefsNotOld() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 0);
        storeFixture.close();
        additionalParams += " --check-consistency-gc true --sweep-only-refs-past-retention true";
        testGc(dump, data, 1000, false, true);
    }

    @Test
    public void gc() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 1);
        storeFixture.close();

        testGc(dump, data, 0, false, false);
    }
    /*
    Command should throw and exception if --verboseRootPath specified
    with --collect-garbage
     */
    @Test
    public void gcWithVerboseRootPathOption() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 1);
        storeFixture.close();

        additionalParams += " --verboseRootPath /a";
        try {
            testGc(dump, data, 0, false, false);
        } catch (OptionException e) {
            assertTrue(e.getMessage().equals("Option(s) [verboseRootPath] are unavailable " +
                    "given other options on the command line"));
            return;
        }
        assertFalse(true);
    }

    @Test
    public void gcNoDeletion() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 0, 1);
        storeFixture.close();

        testGc(dump, data, 0, false, false);
    }

    @Test
    public void gcNoneOld() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 1);
        storeFixture.close();

        testGc(dump, data, 10000, false, false);
    }

    @Test
    public void gcOnlyMark() throws Exception {
        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 1);
        storeFixture.close();

        testGc(dump, data, 10000, true, false);
    }

    @Test
    public void gcMarkOnRemote() throws Exception {
        MemoryBlobStoreNodeStore memNodeStore = new MemoryBlobStoreNodeStore(setupDataStore);
        String rep2Id = ClusterRepositoryInfo.getOrCreateId(memNodeStore);
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            REPOSITORY.getNameFromId(rep2Id));
        Map<String, String> idMapping =
            dummyData(memNodeStore, rep2Id, store, setupDataStore, temporaryFolder.newFile());

        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 1);
        data.added.addAll(idMapping.keySet());
        data.idToPath.putAll(idMapping);

        storeFixture.close();

        testGc(dump, data, 0, false, false);
    }

    @Test
    public void gcNoMarkOnRemote() throws Exception {
        MemoryBlobStoreNodeStore memNodeStore = new MemoryBlobStoreNodeStore(setupDataStore);
        String rep2Id = ClusterRepositoryInfo.getOrCreateId(memNodeStore);
        setupDataStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
            REPOSITORY.getNameFromId(rep2Id));

        File dump = temporaryFolder.newFolder();
        Data data = prepareData(storeFixture, blobFixture, 10, 5, 1);
        storeFixture.close();

        List<String> argsList = Lists
            .newArrayList("--collect-garbage", "--max-age", String.valueOf(0), "--" + getOption(blobFixture.getType()),
                blobFixture.getConfigPath(), storeFixture.getConnectionString(), "--out-dir", dump.getAbsolutePath(),
                "--reset-log-config", "false", "--work-dir", temporaryFolder.newFolder().getAbsolutePath());
        if (!Strings.isNullOrEmpty(additionalParams)) {
            argsList.add(additionalParams);
        }

        testIncorrectParams(argsList, Lists.newArrayList("Not all repositories have marked references available : "),
            MarkSweepGarbageCollector.class);
    }

    /**
     * Only for Segment/Tar
     * @throws Exception
     */
    @Test
    public void testConsistencyFakeDS() throws Exception {
        Assume.assumeTrue(storeFixture instanceof StoreFixture.SegmentStoreFixture);
        File dump = temporaryFolder.newFolder();
        File dsPath = temporaryFolder.newFolder();

        Data data = prepareData(storeFixture, blobFixture, 10, 5, 0);
        storeFixture.close();

        List<String> argsList = Lists
            .newArrayList("--check-consistency", "--fake-ds-path", dsPath.getAbsolutePath(),
                storeFixture.getConnectionString(), "--out-dir", dump.getAbsolutePath(), "--work-dir",
                temporaryFolder.newFolder().getAbsolutePath());
        if (!Strings.isNullOrEmpty(additionalParams)) {
            argsList.add(additionalParams);
        }
        DataStoreCommand cmd = new DataStoreCommand();

        cmd.execute(argsList.toArray(new String[0]));
        assertFileEquals(dump, "avail-", new HashSet<>());
        assertFileEquals(dump, "marked-", Sets.difference(data.added, data.deleted));
    }


    private void testConsistency(File dump, Data data, boolean verbose, boolean verboseRootPath) throws Exception {
        testConsistency(dump, data, verbose, verboseRootPath, false);
    }

    private void testConsistency(File dump, Data data, boolean verbose, boolean verboseRootPath, boolean markOnly) throws Exception {
        List<String> argsList = Lists
            .newArrayList("--check-consistency", String.valueOf(markOnly), "--" + getOption(blobFixture.getType()), blobFixture.getConfigPath(),
                storeFixture.getConnectionString(), "--out-dir", dump.getAbsolutePath(), "--work-dir",
                temporaryFolder.newFolder().getAbsolutePath());
        if (!Strings.isNullOrEmpty(additionalParams)) {
            argsList.addAll(Splitter.on(" ").splitToList(additionalParams));
        }

        if (verbose) {
            argsList.add("--verbose");
        }
        DataStoreCommand cmd = new DataStoreCommand();
        cmd.execute(argsList.toArray(new String[0]));
        
        if (!markOnly) {
            assertFileEquals(dump, "avail-", Sets.difference(data.added, data.missingDataStore));
        } else {
            assertFileNull(dump, "avail-");
        }

        // Verbose would have paths as well as ids changed but normally only DocumentNS would have paths suffixed
        assertFileEquals(dump, "marked-", verbose ?
                encodedIdsAndPath(Sets.difference(verboseRootPath ? data.addedSubset :
                        data.added, data.deleted), blobFixture.getType(), data.idToPath, true) :
                (storeFixture instanceof StoreFixture.MongoStoreFixture) ?
                        encodedIdsAndPath(Sets.difference(data.added, data.deleted), blobFixture.getType(), data.idToPath, false) :
                        Sets.difference(data.added, data.deleted));
        
        if (!markOnly) {
            // Verbose would have paths as well as ids changed but normally only DocumentNS would have paths suffixed
            assertFileEquals(dump, "gccand-", verbose ?
                    encodedIdsAndPath(verboseRootPath ? Sets.intersection(data.addedSubset, data.missingDataStore) :
                            data.missingDataStore, blobFixture.getType(), data.idToPath, true) :
                    (storeFixture instanceof StoreFixture.MongoStoreFixture) ?
                            encodedIdsAndPath(data.missingDataStore, blobFixture.getType(), data.idToPath, false) :
                            data.missingDataStore);
        } else {
            assertFileNull(dump, "gccand-");
        }
    }

    private void testDumpRef(File dump, Data data, boolean verbose, boolean verboseRootPath) throws Exception {
        List<String> argsList = Lists
                .newArrayList("--dump-ref", "--" + getOption(blobFixture.getType()), blobFixture.getConfigPath(),
                        storeFixture.getConnectionString(), "--out-dir", dump.getAbsolutePath(), "--work-dir",
                        temporaryFolder.newFolder().getAbsolutePath());
        if (!Strings.isNullOrEmpty(additionalParams)) {
            argsList.addAll(Splitter.on(" ").splitToList(additionalParams));
        }

        if (verbose) {
            argsList.add("--verbose");
        }
        DataStoreCommand cmd = new DataStoreCommand();
        cmd.execute(argsList.toArray(new String[0]));

        // Verbose would have paths as well as ids changed, otherwise only ids would be listed as it is.
        assertFileEquals(dump, "dump-ref-", verbose ?
                encodedIdsAndPath(Sets.difference(verboseRootPath ? data.addedSubset :
                        data.added, data.deleted), blobFixture.getType(), data.idToPath, true) :
                Sets.difference(data.added, data.deleted), "dump");

    }

    private void testDumpIds(File dump, Data data, boolean verbose) throws Exception {
        List<String> argsList = Lists
                .newArrayList("--dump-id", "--" + getOption(blobFixture.getType()), blobFixture.getConfigPath(),
                        storeFixture.getConnectionString(), "--out-dir", dump.getAbsolutePath(), "--work-dir",
                        temporaryFolder.newFolder().getAbsolutePath());
        if (!Strings.isNullOrEmpty(additionalParams)) {
            argsList.addAll(Splitter.on(" ").splitToList(additionalParams));
        }

        if (verbose) {
            argsList.add("--verbose");
        }
        DataStoreCommand cmd = new DataStoreCommand();
        cmd.execute(argsList.toArray(new String[0]));

        // Verbose would have backend friendly encoded ids
        assertFileEquals(dump, "dump-id-", verbose ?
                encodeIds(Sets.difference(data.added, data.missingDataStore), blobFixture.getType()) :
                        Sets.difference(data.added, data.missingDataStore), "dump");

    }

    private void testGc(File dump, Data data, long maxAge, boolean markOnly, boolean refsOld) throws Exception {
        List<String> argsList = Lists
            .newArrayList("--collect-garbage", String.valueOf(markOnly), "--max-age", String.valueOf(maxAge),
                "--" + getOption(blobFixture.getType()), blobFixture.getConfigPath(),
                storeFixture.getConnectionString(), "--out-dir", dump.getAbsolutePath(), "--work-dir",
                temporaryFolder.newFolder().getAbsolutePath());
        if (!Strings.isNullOrEmpty(additionalParams)) {
            argsList.addAll(Splitter.on(" ").splitToList(additionalParams));
        }

        DataStoreCommand cmd = new DataStoreCommand();
        cmd.execute(argsList.toArray(new String[0]));

        if (!markOnly && !refsOld) {
            assertFileEquals(dump, "avail-", Sets.difference(data.added, data.missingDataStore));
        } else {
            assertFileNull(dump, "avail-");
        }

        assertFileEquals(dump, "marked-", Sets.difference(data.added, data.deleted));
        if (!markOnly && !refsOld) {
            assertFileEquals(dump, "gccand-", data.deleted);
        } else {
            assertFileNull(dump, "gccand-");
        }

        Sets.SetView<String> blobsBeforeGc = Sets.difference(data.added, data.missingDataStore);
        if (maxAge <= 0) {
            assertEquals(Sets.difference(blobsBeforeGc, data.deleted), blobs(setupDataStore));
        } else {
            assertEquals(blobsBeforeGc, blobs(setupDataStore));
        }
    }

    public static void testIncorrectParams(List<String> argList, ArrayList<String> assertMsg, Class logger) {
        LogCustomizer customLogs = LogCustomizer
            .forLogger(logger.getName())
            .enable(Level.INFO)
            .filter(Level.INFO)
            .contains(assertMsg.get(0))
            .create();
        customLogs.starting();

        DataStoreCommand cmd = new DataStoreCommand();
        try {
            cmd.execute(argList.toArray(new String[0]));
        } catch (Exception e) {
            log.error("", e);
        }

        Assert.assertNotNull(customLogs.getLogs().get(0));
        customLogs.finished();
    }

    private static Map<String, String> dummyData(MemoryBlobStoreNodeStore memNodeStore, String rep2Id, NodeStore store,
        DataStoreBlobStore setupDataStore, File f)
        throws IOException, CommitFailedException, DataStoreException {
        List<String> list = Lists.newArrayList();
        Map<String, String> idMapping = Maps.newHashMap();
        NodeBuilder a = memNodeStore.getRoot().builder();
        for (int i = 0; i < 2; i++) {
            Blob b = store.createBlob(randomStream(i+100, 18342));
            Iterator<String> idIter = setupDataStore.resolveChunks(b.getContentIdentity());
            while (idIter.hasNext()) {
                String id = idIter.next();
                list.add(id);
                idMapping.put(id, "/d" + i);
            }
            a.child("d" + i).setProperty("x", b);
        }
        memNodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        writeStrings(list.iterator(), f, false);
        sort(f);
        setupDataStore.addMetadataRecord(f, REFERENCES.getNameFromId(rep2Id));
        return idMapping;
    }

    private static void assertFileEquals(File dump, String prefix, Set<String> blobsAdded) throws IOException{
        assertFileEquals(dump, prefix, blobsAdded, null);
    }

    private static void assertFileEquals(File dump, String prefix, Set<String> blobsAdded, String dirPrefix)
        throws IOException {
        File file = (dirPrefix == null) ? filterFiles(dump, prefix) : filterFiles(dump, dirPrefix, prefix);
        Assert.assertNotNull(file);
        Assert.assertTrue(file.exists());
        assertEquals(blobsAdded,
            FileIOUtils.readStringsAsSet(new FileInputStream(file), true));
    }

    private static void assertFileNull(File dump, String prefix) {
        File file = filterFiles(dump, prefix);
        Assert.assertNull(file);
    }

    private static Set<String> blobs(GarbageCollectableBlobStore blobStore) throws Exception {
        Iterator<String> cur = blobStore.getAllChunkIds(0);

        Set<String> existing = new HashSet<>();
        while (cur.hasNext()) {
            existing.add(cur.next());
        }
        return existing;
    }

    static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    private static String createTempConfig(File cfgFile, Properties props) throws IOException {
        FileOutputStream fos = FileUtils.openOutputStream(cfgFile);
        ConfigurationHandler.write(fos, props);
        return cfgFile.getAbsolutePath();
    }

    private static Set<String> encodedIdsAndPath(Set<String> ids, Type dsOption, Map<String, String> idToNodes,
        boolean encodeId) {
        return CollectionUtils.toSet(Iterators.transform(ids.iterator(), input -> Joiner.on(",").join(encodeId ? encodeId(input, dsOption) : input, idToNodes.get(input))));
    }

    private static Set<String> encodeIds(Set<String> ids, Type dsOption) {
        return CollectionUtils.toSet(Iterators.transform(ids.iterator(), input -> encodeId(input, dsOption)));
    }


    static String encodeId(String id, Type dsType) {
        List<String> idLengthSepList = Splitter.on(HASH).trimResults().omitEmptyStrings().splitToList(id);
        String blobId = idLengthSepList.get(0);

        if (dsType == Type.FDS) {
            return String.join(System.getProperty("file.separator"), blobId.substring(0, 2), blobId.substring(2, 4),
                    blobId.substring(4, 6), blobId);
        } else if (dsType == Type.S3 || dsType == Type.AZURE) {
            return (blobId.substring(0, 4) + DASH + blobId.substring(4));
        }
        return id;
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

    static class Data {
        // Set of blob ids created
        private Set<String> added;
        // Mapping of id to the path on which their ref exists
        private Map<String, String> idToPath;
        // Set of deleted paths
        private Set<String> deleted;
        // Set of blobs that will be deleted from datastore
        private Set<String> missingDataStore;
        // A subset of added blobs - use this to test the --verboseRootPath option
        // (In case of getting the blob refs only under a specific path)
        private Set<String> addedSubset;

        public Data() {
            added = new HashSet<>();
            idToPath = Maps.newHashMap();
            deleted = new HashSet<>();
            missingDataStore = new HashSet<>();
            addedSubset = new HashSet<>();
        }
    }

    interface StoreFixture {
        NodeStore init(DataStoreBlobStore blobStore, File storeFile) throws Exception;

        NodeStore getNodeStore() throws Exception;

        String getConnectionString();

        boolean isAvailable();

        void preDataPrepare() throws Exception;

        void postDataPrepare() throws Exception;

        void close();

        void after();

        StoreFixture MONGO = new MongoStoreFixture();
        StoreFixture SEGMENT = new SegmentStoreFixture();
        StoreFixture SEGMENT_AZURE = new AzureSegmentStoreFixture();

        class MongoStoreFixture implements StoreFixture {
            private final Clock.Virtual clock;
            MongoConnection c;
            DocumentMK.Builder builder;
            private DocumentNodeStore nodeStore;

            public MongoStoreFixture() {
                c = MongoUtils.getConnection();
                if (c != null) {
                    MongoUtils.dropCollections(c.getDBName());
                }
                clock = new Clock.Virtual();
            }

            @Override public NodeStore init(DataStoreBlobStore blobStore, File storeFile) {
                c = MongoUtils.getConnection();
                if (c != null) {
                    MongoUtils.dropCollections(c.getDBName());
                }
                clock.waitUntil(Revision.getCurrentTimestamp());
                builder = new DocumentMK.Builder().clock(clock).setMongoDB(c.getMongoClient(), c.getDBName());
                nodeStore = builder.setBlobStore(blobStore).getNodeStore();

                return nodeStore;
            }

            @Override public NodeStore getNodeStore() {
                return nodeStore;
            }

            @Override public String getConnectionString() {
                return MongoUtils.URL;
            }

            @Override public void postDataPrepare() throws Exception {
                long maxAge = 20; // hours
                // 1. Go past GC age and check no GC done as nothing deleted
                clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(maxAge));
                VersionGarbageCollector vGC = nodeStore.getVersionGarbageCollector();
                VersionGarbageCollector.VersionGCStats stats = vGC.gc(0, TimeUnit.MILLISECONDS);
            }

            @Override public void close() {
                nodeStore.dispose();
            }

            @Override public boolean isAvailable() {
                return c != null;
            }

            @Override public void preDataPrepare() {
            }

            @Override public void after() {
                String dbName = c.getDBName();
                try {
                    nodeStore.dispose();
                } catch (Exception e) {
                    log.warn("dispose failed with exception", e);
                }
                MongoUtils.dropCollections(dbName);
            }
        }

        class SegmentStoreFixture implements StoreFixture {
            protected FileStore fileStore;
            protected SegmentNodeStore store;
            protected SegmentGCOptions gcOptions = defaultGCOptions();
            protected String storePath;

            @Override public NodeStore init(DataStoreBlobStore blobStore, File storeFile)
                throws Exception {
                storePath = storeFile.getAbsolutePath();
                FileStoreBuilder fileStoreBuilder =
                    FileStoreBuilder.fileStoreBuilder(storeFile);

                fileStore = fileStoreBuilder.withBlobStore(blobStore).withMaxFileSize(256).withSegmentCacheSize(64).build();
                store = SegmentNodeStoreBuilders.builder(fileStore).build();
                return store;
            }

            @Override public NodeStore getNodeStore() {
                return store;
            }

            @Override public String getConnectionString() {
                return storePath;
            }

            @Override public void postDataPrepare() throws Exception {
                for (int k = 0; k < gcOptions.getRetainedGenerations(); k++) {
                    fileStore.compactFull();
                }
                fileStore.cleanup();
            }

            @Override public void close() {
                fileStore.close();
            }

            @Override public void after() {
            }

            @Override public boolean isAvailable() {
                return true;
            }

            @Override public void preDataPrepare() throws Exception {
                NodeBuilder a = store.getRoot().builder();

                /* Create garbage by creating in-lined blobs (size < 16KB) */
                int number = 500;
                NodeBuilder content = a.child("content");
                for (int i = 0; i < number; i++) {
                    NodeBuilder c = content.child("x" + i);
                    for (int j = 0; j < 5; j++) {
                        c.setProperty("p" + j, store.createBlob(randomStream(j, 16384)));
                    }
                }
                store.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }
        }


        /**
         * Requires 'AZURE_SECRET_KEY' to be set as an environment variable as well
         */
        class AzureSegmentStoreFixture extends SegmentStoreFixture {
            private static final String AZURE_DIR = "repository";
            private String container;
            private final AzureStorageCredentialManagerV8 azureStorageCredentialManagerV8 = new AzureStorageCredentialManagerV8();

            @Override public NodeStore init(DataStoreBlobStore blobStore, File storeFile) throws Exception {
                Properties props = AzureDataStoreUtils.getAzureConfig();
                String accessKey = props.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME);
                String secretKey = props.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY);
                container = props.getProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME);
                container = container + System.currentTimeMillis();
                // Create the azure segment container
                String connectionString = getAzureConnectionString(accessKey, secretKey, container, AZURE_DIR);
                AzureUtilitiesV8.cloudBlobDirectoryFrom(connectionString, container, AZURE_DIR);

                // get the azure uri expected by the command
                storePath = getAzureUri(accessKey, container, AZURE_DIR);

                // initialize azure segment for test setup
                SegmentNodeStorePersistence segmentNodeStorePersistence =
                    ToolUtils.newSegmentNodeStorePersistence(ToolUtils.SegmentStoreType.AZURE, storePath, azureStorageCredentialManagerV8);
                fileStore = fileStoreBuilder(storeFile).withBlobStore(blobStore)
                    .withCustomPersistence(segmentNodeStorePersistence).build();

                store = SegmentNodeStoreBuilders.builder(fileStore).build();

                return store;
            }

            protected String getAzureUri(String accountName, String container, String directory) {
                StringBuilder uri = new StringBuilder("az:");
                uri.append("https://").append(accountName).append(".blob.core.windows.net/");
                uri.append(container).append("/");
                uri.append(directory);

                return uri.toString();
            }

            protected String getAzureConnectionString(String accountName, String secret, String container, String directory) {
                StringBuilder builder = new StringBuilder();
                builder.append("AccountName=").append(accountName).append(";");
                builder.append("DefaultEndpointsProtocol=https;");
                builder.append("BlobEndpoint=https://").append(accountName).append(".blob.core.windows.net").append(";");
                builder.append("AccountKey=").append(secret);

                return builder.toString();
            }

            @Override
            public void after() {
                try {
                    AzureDataStoreUtils.deleteContainer(container);
                    azureStorageCredentialManagerV8.close();
                } catch(Exception e) {
                    log.error("Error in cleaning the container {}", container, e);
                }
            }

            @Override public boolean isAvailable() {
                return AzureDataStoreUtils.isAzureConfigured();
            }
        }
    }

    interface DataStoreFixture {
        boolean isAvailable();

        DataStoreBlobStore init(TemporaryFolder folder) throws Exception;

        DataStoreBlobStore getDataStore();

        String getConfigPath();

        Type getType();

        void after();

        DataStoreFixture S3 = new S3DataStoreFixture();
        DataStoreFixture AZURE = new AzureDataStoreFixture();
        DataStoreFixture FDS = new FileDataStoreFixture();

        class S3DataStoreFixture implements DataStoreFixture {
            DataStoreBlobStore blobStore;
            String cfgFilePath;
            String container;

            @Override public boolean isAvailable() {
                return S3DataStoreUtils.isS3Configured();
            }

            @Override public DataStoreBlobStore init(TemporaryFolder folder) throws Exception {
                Properties props = S3DataStoreUtils.getS3Config();
                props.setProperty("cacheSize", "0");
                container = props.getProperty(S3Constants.S3_BUCKET);
                container = container + System.currentTimeMillis();
                props.setProperty(S3Constants.S3_BUCKET, container);
                DataStore ds = S3DataStoreUtils.getS3DataStore(S3DataStoreUtils.getFixtures().get(0), props,
                    folder.newFolder().getAbsolutePath());
                blobStore = new DataStoreBlobStore(ds);
                cfgFilePath = createTempConfig(
                    folder.newFile(getType().name() + String.valueOf(System.currentTimeMillis()) + ".config"), props);
                return blobStore;
            }

            @Override public DataStoreBlobStore getDataStore() {
                return blobStore;
            }

            @Override public String getConfigPath() {
                return cfgFilePath;
            }

            @Override public Type getType() {
                return Type.S3;
            }

            @Override public void after() {
                try {
                    S3DataStoreUtils.deleteBucket(container, new Date());
                } catch (Exception e) {
                    log.error("Error in cleaning the container {}", container, e);
                }
            }
        }

        class AzureDataStoreFixture implements DataStoreFixture {
            DataStoreBlobStore blobStore;
            String cfgFilePath;
            String container;

            @Override public boolean isAvailable() {
                return AzureDataStoreUtils.isAzureConfigured();
            }

            @Override public DataStoreBlobStore init(TemporaryFolder folder) throws Exception {
                Properties props = AzureDataStoreUtils.getAzureConfig();
                props.setProperty("cacheSize", "0");
                container = props.getProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME);
                container = container + System.currentTimeMillis();
                props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, container);
                DataStore ds = AzureDataStoreUtils.getAzureDataStore(props, folder.newFolder().getAbsolutePath());
                blobStore = new DataStoreBlobStore(ds);
                cfgFilePath = createTempConfig(
                    folder.newFile(getType().name() + String.valueOf(System.currentTimeMillis()) + ".config"), props);
                return blobStore;
            }

            @Override public DataStoreBlobStore getDataStore() {
                return blobStore;
            }

            @Override public String getConfigPath() {
                return cfgFilePath;
            }

            @Override public Type getType() {
                return Type.AZURE;
            }

            @Override public void after() {
                try {
                    AzureDataStoreUtils.deleteContainer(container);
                } catch (Exception e) {
                    log.error("Error in cleaning the container {}", container, e);
                }
            }
        }

        class FileDataStoreFixture implements DataStoreFixture {
            DataStoreBlobStore blobStore;
            String cfgFilePath;
            String container;

            @Override public boolean isAvailable() {
                return true;
            }

            @Override public DataStoreBlobStore init(TemporaryFolder folder) throws Exception {
                OakFileDataStore delegate = new OakFileDataStore();
                container = folder.newFolder().getAbsolutePath();
                delegate.setPath(container);
                delegate.init(null);
                blobStore = new DataStoreBlobStore(delegate);

                File cfgFile = folder.newFile();
                Properties props = new Properties();
                props.put("path", container);
                props.put("minRecordLength", new Long(4096));
                cfgFilePath = createTempConfig(cfgFile, props);

                return blobStore;
            }

            @Override public DataStoreBlobStore getDataStore() {
                return blobStore;
            }

            @Override public String getConfigPath() {
                return cfgFilePath;
            }

            @Override public Type getType() {
                return Type.FDS;
            }

            @Override public void after() {
            }
        }
    }

    static class FixtureHelper {
        static List<StoreFixture> getStoreFixtures() {
            return ImmutableList.of(StoreFixture.MONGO, StoreFixture.SEGMENT, StoreFixture.SEGMENT_AZURE);
        }

        static List<DataStoreFixture> getDataStoreFixtures() {
            return ImmutableList.of(DataStoreFixture.S3, DataStoreFixture.AZURE, DataStoreFixture.FDS);
        }

        static List<Object[]> get() {
            List<Object[]> fixtures = Lists.newArrayList();
            for (StoreFixture storeFixture : getStoreFixtures()) {
                if (storeFixture.isAvailable()) {
                    for (DataStoreFixture dsFixture : getDataStoreFixtures()) {
                        if (dsFixture.isAvailable()) {
                            fixtures.add(new Object[] {storeFixture, dsFixture});
                        }
                    }
                }
            }
            return fixtures;
        }
    }
}
