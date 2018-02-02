package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.segment.SegmentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.defaultGCOptions;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertNotNull;

/**
 * Performs integration-level tests to test the functionality of the read-only/read-write
 * delegate use case for CompositeDataStore.
 * This use case is comprised of two Oak repositories.  The first repository is a standard
 * Oak repository - in this case, using SegmentNodeStore and FileDataStore.  The second
 * repository is created by cloning node store of the first.  It uses CompositeDataStore
 * with two FileDataStore delegates.  One delegate refers to the same location as the
 * first repository's FileDataStore, but is marked as "readOnly".  The second delegate
 * refers to a second FileDataStore that is writable.  The second repository can service
 * read-only requests to data that was in the original first repository by reading from
 * the shared FileDataStore.  Write requests to the second repository should always go
 * to the second, non-shared FileDataStore.  The first repository, knowing nothing about
 * the second repository, services read and write requests as normal.
 */
public class CompositeDataStoreRORWIT {
    private static final Logger log = LoggerFactory.getLogger(CompositeDataStoreRORWIT.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private static final String CONTENT_ROOT = "testContent";
    private static final String CONTENT_ENTRY = "entry";
    private static final String CONTENT_BINARY_PROP = "binaryContent";
    private static final String ITERATION_FLAG_PROP = "iterationFlag";
    private static final int BLOB_SIZE = 1024*32;

    private static SegmentGCOptions gcOptions = defaultGCOptions();

    private static Closer closer = Closer.create();

    @After
    public void tearDown() {
        IOUtils.closeQuietly(closer);
    }

    @Ignore
    @Test
    public void testReadonlyAndReadwriteDelegateSupport() throws Exception {
        // Set up the primary node store and file data store, and
        // prepopulate the primary repo with some number of blobs
        PrimaryRepo primaryRepo = PrimaryRepo.builder(folder)
                .withRepoSubTreeName("first")
                .withIterationFlag("iteration1")
                .withChildNodeCount(10)
                .build();
        primaryRepo.verifySubTree("first", 10);

        // Set up secondary repo using a composite data store, with
        //  - production file data store read-only delegate
        //  - new staging file data store writable delegate
        // Then clone secondary node store from primary node store
        SecondaryRepo secondaryRepo = SecondaryRepo.builder(folder, primaryRepo).build();
        secondaryRepo.verifyProdSubTree("first", 10);

        // Make updates to primary repo.
        // Verify the updates to primary don't show up in the secondary repo.
        primaryRepo.addSubTree("second", "iteration2", 10);
        primaryRepo.verifySubTree("second", 10);
        secondaryRepo.verifySubTreeAbsent("second");

        // Make new updates to the secondary repo.
        // Verify the updates to secondary don't show up in the primary repo.
        secondaryRepo.addSubTree("third", "iteration3", 10);
        secondaryRepo.verifyStgSubTree("third", 10);
        primaryRepo.verifySubTreeAbsent("third");

        // On the secondary repo, save a new change to a node that was created
        // originally in the primary repo.
        // Verify the update doesn't show up in the primary repo.
        String blobReference = secondaryRepo.addNode("first", "iteration4", 2);
        primaryRepo.verifySubTree("first", 10);
        primaryRepo.verifyBlobNotInSubTree("first", blobReference);
        secondaryRepo.verifyBlobInSubTree("first", blobReference);

        // Verify that each repo's view is correct
        NodeState p11 = primaryRepo.getNode("first", 1);
        NodeState p12 = primaryRepo.getNode("first", 2);
        NodeState s11 = secondaryRepo.getNode("first", 1);
        NodeState s12 = secondaryRepo.getNode("first", 2);
        NodeState p21 = primaryRepo.getNode("second", 1);
        NodeState s21 = secondaryRepo.getNode("second", 1);
        NodeState p31 = primaryRepo.getNode("third", 1);
        NodeState s31 = secondaryRepo.getNode("third", 1);

        assertTrue(p11.exists());
        assertTrue(p12.exists());
        assertTrue(s11.exists());
        assertTrue(s12.exists());
        assertTrue(p21.exists());
        assertTrue(s31.exists());

        assertFalse(s21.exists());
        assertFalse(p31.exists());

        assertTrue(nodesAreEquivalent(p11, s11));
        assertFalse(nodesAreEquivalent(p12, s12));
    }

    @Ignore
    @Test
    public void testCompareReferences() throws Exception {
        PrimaryRepo primaryRepo = PrimaryRepo.builder(folder)
                .withRepoSubTreeName("content")
                .withIterationFlag("first")
                .withChildNodeCount(10)
                .build();
        primaryRepo.verifySubTree("content", 10);

        SecondaryRepo secondaryRepo = SecondaryRepo.builder(folder, primaryRepo).build();
        secondaryRepo.verifyProdSubTree("content", 10);

        for (int i=0; i<10; i++) {
            NodeState primaryChild = primaryRepo.getNode("content", i);
            assertTrue(primaryChild.exists());

            NodeState secondaryChild = secondaryRepo.getNode("content", i);
            assertTrue(secondaryChild.exists());

            PropertyState content = primaryChild.getProperty(CONTENT_BINARY_PROP);
            assertNotNull(content);
            Blob b = content.getValue(Type.BINARY);
            assertNotNull(b);
            String pChildRef = b.getReference();
            assertNotNull(pChildRef);

            content = secondaryChild.getProperty(CONTENT_BINARY_PROP);
            assertNotNull(content);
            b = content.getValue(Type.BINARY);
            assertNotNull(b);
            String sChildRef = b.getReference();
            assertNotNull(sChildRef);

            assertEquals(pChildRef, sChildRef);
        }
    }

    @Test
    public void testDSGC() throws Exception {
        // Step 1:  Set up the primary repo and secondary repo as shown.
        // Primary repo creates four trees of 10 nodes, each with a different purpose:
        //  - P_perm1 - Nothing deleted by either repository.
        //  - P_del1 - Nodes will be deleted via primary repo (details below).
        //  - S_del1 - Nodes will be deleted via secondary repo (details below).
        //  - P_shared1 - Nodes will be deleted from both repos (details below).
        // Secondary repo is cloned from primary.
        //
        //        PrimaryRepo  SecondaryRepo
        //             |             |
        //             +-- P_perm1 --+
        //             |             |
        //             +-- P_del1  --+
        //             |             |
        //             +-- S_del1  --+
        //             |             |
        //             +- P_shared1 -+

        int childNodeCount = 10;
        String iterationFlag = "first";

        PrimaryRepo primaryRepo = PrimaryRepo.builder(folder).build();
        primaryRepo.addSubTree("P_perm1", iterationFlag, childNodeCount);
        primaryRepo.addSubTree("P_del1", iterationFlag, childNodeCount);
        primaryRepo.addSubTree("S_del1", iterationFlag, childNodeCount);
//        primaryRepo.addSubTree("P_shared1", iterationFlag, childNodeCount);

        SecondaryRepo secondaryRepo = SecondaryRepo.builder(folder, primaryRepo).build();


        // Step 2:  Create additional trees (10 blobs each) in both repos that are NOT shared.
        // On primary:
        //  - P_perm2 - Nothing will be deleted.
        //  - P_del2 - Nodes will be deleted via primary repo (details below).
        //  - P_shared2 - Tree with common name in both repos, but not actually "shared".
        //                Some common nodes will be deleted via both, some only from one or the other.
        //                Details below.
        // On secondary:
        //  - S_perm2 - Nothing will be deleted.
        //  - S_del2 - Nodes will be deleted via secondary repo (details below).
        //  - P_shared2 - Tree with common name as primary as mentioned above.
        //
        //        PrimaryRepo  SecondaryRepo
        //             |             |
        //             +-- P_perm1 --+
        //             |             |
        //             +-- P_del1  --+
        //             |             |
        //             +-- S_del1  --+
        //             |             |
        //             +- P_shared1 -+
        //             |             |
        //    P_perm2 -+             +- S_perm2
        //             |             |
        //     P_del2 -+             +- S_del2
        //             |             |
        //  P_shared2 -+             +- P_shared2

        iterationFlag = "second";

        primaryRepo.addSubTree("P_perm2", iterationFlag, childNodeCount);
        primaryRepo.addSubTree("P_del2", iterationFlag, childNodeCount);
//        primaryRepo.addSubTree("P_shared2", iterationFlag, childNodeCount);

        secondaryRepo.addSubTree("S_perm2", iterationFlag, childNodeCount);
        secondaryRepo.addSubTree("S_del2", iterationFlag, childNodeCount);
//        secondaryRepo.addSubTree("P_shared2", iterationFlag, childNodeCount);


        // Step 3:  Delete the nodes as specified:
        //  - P_perm1 - Nothing deleted by either repository.
        //  - P_del1 - 4 nodes will be deleted via primary repo.
        //  - S_del1 - 6 nodes will be deleted via secondary repo.
        //  - P_shared1 - Nodes will be deleted from both repos:
        //    - 3 deleted from primary only
        //    - 4 deleted from secondary only
        //    - 2 deleted from both
        //  - P_perm2 - Nothing will be deleted.
        //  - P_del2 - 4 nodes will be deleted via primary repo.
        //  - P_shared2 - Nodes deleted as follows:
        //    - 3 deleted from primary only
        //    - 4 deleted from secondary only
        //    - 2 deleted from both
        //  - S_perm2 - Nothing will be deleted.
        //  - S_del2 - 6 nodes will be deleted via secondary repo.
        //
        //        PrimaryRepo  SecondaryRepo
        //             |             |
        //             +-- P_perm1 --+
        //             |             |
        //             | -4          |
        //             +-- P_del1  --+
        //             |             |
        //             |          -6 |
        //             +-- S_del1  --+
        //             |             |
        //             | -3  -2   -4 |
        //             +- P_shared1 -+
        //             |             |
        //    P_perm2 -+             +- S_perm2
        //             |             |
        //          -4 |             | -6
        //     P_del2 -+             +- S_del2
        //          -3 |     -2      | -4
        //  P_shared2 -+             +- P_shared2

        int prDel1DeleteCount = 4;
        int srDel1DeleteCount = 6;
        int prShared1DeleteCountFromPrimary = 3;
        int srShared1DeleteCountFromSecondary = 4;
        int prShared1DeleteCountFromBoth = 2;
        int prDel2DeleteCount = 4;
        int prShared2DeleteCountFromPrimary = 3;
        int srShared2DeleteCountFromSecondary = 4;
        int prShared2DeleteCountFromBoth = 2;
        int srDel2DeleteCount = 6;

        Map<String, Integer> prExpectedBlobCounts = Maps.newHashMap();  // Counts of blobs expected from primary repo view.
        Map<String, Integer> prExpectedNodeCounts = Maps.newHashMap();  // Counts of nodes expected from primary repo view.
        prExpectedNodeCounts.put("P_perm1", childNodeCount);
        prExpectedBlobCounts.put("P_perm1", childNodeCount);
        prExpectedNodeCounts.put("P_del1", childNodeCount-prDel1DeleteCount); // 6
        prExpectedBlobCounts.put("P_del1", childNodeCount);
        prExpectedNodeCounts.put("S_del1", childNodeCount);
        prExpectedBlobCounts.put("S_del1", childNodeCount);
//        prExpectedNodeCounts.put("P_shared1", childNodeCount-prShared1DeleteCountFromBoth-prShared1DeleteCountFromPrimary); // 5
//        prExpectedBlobCounts.put("P_shared1", childNodeCount-prShared1DeleteCountFromBoth); // 8
        prExpectedNodeCounts.put("P_perm2", childNodeCount);
        prExpectedBlobCounts.put("P_perm2", childNodeCount);
        prExpectedNodeCounts.put("P_del2", childNodeCount-prDel2DeleteCount); // 6
        prExpectedBlobCounts.put("P_del2", childNodeCount-prDel2DeleteCount); // 6
//        prExpectedNodeCounts.put("P_shared2", childNodeCount-prShared2DeleteCountFromBoth-prShared2DeleteCountFromPrimary); // 5
//        prExpectedBlobCounts.put("P_shared2", childNodeCount-prShared2DeleteCountFromBoth-prShared2DeleteCountFromPrimary); // 5

        Map<String, Integer> srExpectedBlobCounts = Maps.newHashMap();  // Counts of blobs expected from secondary repo view.
        Map<String, Integer> srExpectedNodeCounts = Maps.newHashMap();  // Counts of nodes expected from secondary repo view.
        srExpectedNodeCounts.put("P_perm1", childNodeCount);
        srExpectedBlobCounts.put("P_perm1", childNodeCount);
        srExpectedNodeCounts.put("P_del1", childNodeCount);
        srExpectedBlobCounts.put("P_del1", childNodeCount);
        srExpectedNodeCounts.put("S_del1", childNodeCount-srDel1DeleteCount);  // 4
        srExpectedBlobCounts.put("S_del1", childNodeCount);
//        srExpectedNodeCounts.put("P_shared1", childNodeCount-prShared1DeleteCountFromBoth-srShared1DeleteCountFromSecondary);  // 4
//        srExpectedBlobCounts.put("P_shared1", childNodeCount-prShared1DeleteCountFromBoth);  // 8
        srExpectedNodeCounts.put("S_perm2", childNodeCount);
        srExpectedBlobCounts.put("S_perm2", childNodeCount);
        srExpectedNodeCounts.put("S_del2", childNodeCount-srDel2DeleteCount);  // 4
        srExpectedBlobCounts.put("S_del2", childNodeCount-srDel2DeleteCount);  // 4
//        srExpectedNodeCounts.put("P_shared2", childNodeCount-prShared2DeleteCountFromBoth-srShared2DeleteCountFromSecondary);  // 4
//        srExpectedBlobCounts.put("P_shared2", childNodeCount-prShared2DeleteCountFromBoth-srShared2DeleteCountFromSecondary);  // 4

        // Delete some number of blobs, remembering which are deleted
        Map<String, Map<Integer, String>> primaryRepoDeletedNodes = Maps.newHashMap();
        primaryRepoDeletedNodes.put("P_del1",
                deleteRandomNodesFromRepo(primaryRepo, "P_del1", childNodeCount, prDel1DeleteCount));
//        primaryRepoDeletedNodes.put("P_shared1",
//                deleteRandomNodesFromRepo(primaryRepo, "P_shared1", childNodeCount,
//                        prShared1DeleteCountFromPrimary+prShared1DeleteCountFromBoth));
        primaryRepoDeletedNodes.put("P_del2",
                deleteRandomNodesFromRepo(primaryRepo, "P_del2", childNodeCount, prDel2DeleteCount));
//        primaryRepoDeletedNodes.put("P_shared2",
//                deleteRandomNodesFromRepo(primaryRepo, "P_shared2", childNodeCount,
//                        prShared2DeleteCountFromPrimary+prShared2DeleteCountFromBoth));

        int sharedDelCtr = 0;
//        for (int nodeNum : primaryRepoDeletedNodes.get("P_shared1").keySet()) {
//            secondaryRepo.deleteNode("P_shared1", nodeNum);
//            if (++sharedDelCtr == prShared1DeleteCountFromBoth) {
//                break;
//            }
//        }
//        sharedDelCtr = 0;
//        for (int nodeNum : primaryRepoDeletedNodes.get("P_shared2").keySet()) {
//            secondaryRepo.deleteNode("P_shared2", nodeNum);
//            if (++sharedDelCtr == prShared2DeleteCountFromBoth) {
//                break;
//            }
//        }

        Map<String, Map<Integer, String>> secondaryRepoDeletedNodes = Maps.newHashMap();
        secondaryRepoDeletedNodes.put("S_del1",
                deleteRandomNodesFromRepo(secondaryRepo, "S_del1", childNodeCount, srDel1DeleteCount));
//        secondaryRepoDeletedNodes.put("P_shared1",
//                deleteRandomNodesFromRepo(secondaryRepo, "P_shared1",
//                        childNodeCount-prShared1DeleteCountFromBoth,
//                        srShared1DeleteCountFromSecondary));
        secondaryRepoDeletedNodes.put("S_del2",
                deleteRandomNodesFromRepo(secondaryRepo, "S_del2", childNodeCount, srDel2DeleteCount));
//        secondaryRepoDeletedNodes.put("P_shared2",
//                deleteRandomNodesFromRepo(secondaryRepo, "P_shared2",
//                        childNodeCount-prShared2DeleteCountFromBoth,
//                        srShared2DeleteCountFromSecondary));


        // Step 4:  Run GC on both repos.

        TimeUnit.MILLISECONDS.sleep(5); // Make eligible for GC

        // Invoke DGSC
        primaryRepo.mark();
        secondaryRepo.mark();
        primaryRepo.sweep();
        secondaryRepo.sweep();


        // Step 5:  Inspect the results.
        //
        // Reading from primary - Primary deleted a total of 18 nodes and should see 18 fewer nodes.  Not
        // all the blobs will be deleted however.  Details:
        //  - P_perm1 should have 10 nodes and 10 blobs.  (-0)
        //  - P_del1 should have 6 nodes and 10 blobs.  (-4)
        //    - GC did not collect the deleted blobs because they are still referenced by secondary.
        //  - S_del1 should have 10 nodes and 10 blobs.  (-0)
        //    - GC did not collect the deleted blobs because they are still referenced by primary.
        //  - P_shared1 should have 5 nodes and 8 blobs.  (-5)
        //    - GC did not collect 2 blobs because they are still referenced by secondary.
        //  - P_perm2 should have 10 nodes and 10 blobs.  (-0)
        //  - P_del2 should have 6 nodes and 6 blobs.  (-4)
        //  - P_shared2 should have 5 nodes and 5 blobs.  (-5)
        // End state for primary repo:  52 nodes, 59 blobs
        //
        // Reading from secondary - Secondary deleted a total of 24 nodes and should see 24 fewer nodes.  Not
        // all the blobs will be deleted however.  Details:
        //  - P_perm1 should have 10 nodes and 10 blobs.  (-0)
        //  - P_del1 should have 10 nodes and 10 blobs.  (-0)
        //  - S_del1 should have 4 nodes and 10 blobs.  (-6)
        //  - P_shared1 should have 4 nodes and 8 blobs.  (-6)
        //  - S_perm2 should have 10 nodes and 10 blobs.
        //  - S_del2 should have 4 nodes and 4 blobs.  (-6)
        //  - P_shared2 should have 4 nodes and 4 blobs.  (-6)
        // End state for secondary repo:  46 nodes, 56 blobs
        //
        //        PrimaryRepo  SecondaryRepo
        //             |             |
        //             +-- P_perm1 --+
        //             | 10n     10n |
        //             | 10b     10b |
        //             |             |
        //             +-- P_del1  --+
        //             | 6n      10b |
        //             | 10b     10b |
        //             |             |
        //             +-- S_del1  --+
        //             | 10n      4n |
        //             | 10b     10b |
        //             |             |
        //             +- P_shared1 -+
        //             | 5n       4n |
        //             | 8b       8b |
        //             |             |
        //    P_perm2 -+             +- S_perm2
        //         10n |             | 10n
        //         10b |             | 10b
        //             |             |
        //     P_del2 -+             +- S_del2
        //          6n |             | 4n
        //          6b |             | 4b
        //             |             |
        //  P_shared2 -+             +- P_shared2
        //          5n |             | 4n
        //          5b |             | 4b


        int prActualBlobCount = primaryRepo.getAllChunkIds().size();
        int prExpectedBlobCount = prExpectedBlobCounts.values().stream().mapToInt(Integer::intValue).sum();
        assertEquals(prExpectedBlobCount, prActualBlobCount);

        long prActualNodeCount = primaryRepo.countLeaves();
        int prExpectedNodeCount = prExpectedNodeCounts.values().stream().mapToInt(Integer::intValue).sum();
        assertEquals(prExpectedNodeCount, prActualNodeCount);

        // getAllChunkIds returns a count of all the blobs in the store (raw file count).
        // So the actual count from the composite data store's point of view is a complete count of all
        // blobs in both primary and secondary repos.  So we have to count them both.
        int srActualBlobCount = secondaryRepo.getAllChunkIds().size() - prActualBlobCount;
        int srExpectedBlobCount = srExpectedBlobCounts.values().stream().mapToInt(Integer::intValue).sum();
//        int srExpectedBlobCount = Sets.union(
//                Sets.newHashSet(srExpectedBlobCounts.values()),
//                Sets.newHashSet(prExpectedBlobCounts.values()))
//                .stream().mapToInt(Integer::intValue).sum();
        assertEquals(srExpectedBlobCount, srActualBlobCount);

        long srActualNodeCount = secondaryRepo.countLeaves();
        int srExpectedNodeCount = srExpectedNodeCounts.values().stream().mapToInt(Integer::intValue).sum();
        assertEquals(srExpectedNodeCount, srActualNodeCount);

        // Verify:
        //  - Every blob in P_perm1 remains.
        //  - Deleted blobs in P_del1 are removed from blob store and from both node stores.
        //  - Every blob in P_shared1 remains, from production's view.
        //  - Every blob in S_perm1 remains.
        //  - Deleted blobs from S_del1 are removed from the blob store.
        //  - Deleted blobs from P_shared1 are removed from staging's view.
        //  - Every blob in P_perm2 remains.
        //  - Deleted blobs in P_del2 are removed from blob store.
        //  - Deleted blobs in P_shared2, deleted by production, are removed from production's view.
        //    Blobs deleted by staging remain visible from production's view.
        //  - Every blob in S_perm2 remains.
        //  - Deleted blobs in S_del2 are removed from blob store.
        //  - Deleted blobs in P_shared2, deleted by staging, are removed from staging's view.
        //    Blobs deleted by production are also removed from staging's view.
        //primaryRepo.verifySubTree("P_perm1", childNodeCount);
        //primaryRepo.verifySubTree("P_del1", childNodeCount, primaryRepoDeletedNodes.get("P_del1"));
        // what about the P_del1 blobs in the secondary tree?
        //primaryRepo.verifySubTree("P_shared1", childNodeCount);
        //secondaryRepo.verifyStgSubTree("S_perm1", childNodeCount);
        //secondaryRepo.verifyProdSubTree("P_shared1", childNodeCount, secondaryRepoDeletedNodes.get("P_shared1"));
        //primaryRepo.verifySubTree("P_perm2", childNodeCount);
        //primaryRepo.verifySubTree("P_shared2", childNodeCount, primaryRepoDeletedNodes.get("P_shared2"));
        //secondaryRepo.verifyStgSubTree("S_perm2", childNodeCount);
        //secondaryRepo.verifyStgSubTree("S_del2", childNodeCount, secondaryRepoDeletedNodes.get("S_del2"));
        //secondaryRepo.verifyProdSubTree("P_shared2", childNodeCount, secondaryRepoDeletedNodes.get("P_shared2"));
    }

    private static Closeable asCloseable(DataStoreBlobStore ds) {
        return new Closeable() {
            @Override
            public void close() throws IOException {
                try {
                    ds.close();
                }
                catch (DataStoreException e) {
                    throw new IOException(e);
                }
            }
        };
    }

    static abstract class OakRepo {
        private SegmentNodeStore nodeStore;
        private FileStore fileStore;
        private DataStoreBlobStore blobStore;
        private File blobDir;

        private String repositoryId;
        private MarkSweepGarbageCollector garbageCollector = null;
        private ThreadPoolExecutor executor = null;

        private boolean commitHappened;
        private CommitHook commitHook = new CommitHook() {
            @Nonnull
            @Override
            public NodeState processCommit(NodeState before, NodeState after, CommitInfo info) throws CommitFailedException {
                commitHappened = true;

                return after;
            }
        };

        public OakRepo(SegmentNodeStore nodeStore, FileStore fileStore, DataStoreBlobStore blobStore, File blobDir)
                throws IOException, DataStoreException {
            this.nodeStore = nodeStore;
            this.fileStore = fileStore;
            this.blobStore = blobStore;
            this.blobDir = blobDir;

            assertTrue(SharedDataStoreUtils.isShared(blobStore));
            repositoryId = ClusterRepositoryInfo.getOrCreateId(nodeStore);
            blobStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
                    REPOSITORY.getNameFromId(repositoryId));
            if (null == executor) {
                executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
            }
            garbageCollector = new MarkSweepGarbageCollector(
                    new SegmentBlobReferenceRetriever(fileStore),
                    blobStore,
                    executor,
                    blobDir.getAbsolutePath(),
                    2048,
                    0,
                    repositoryId
            );

        }

        protected SegmentNodeStore getNodeStore() {
            return nodeStore;
        }

        protected FileStore getFileStore() { return fileStore; }

        protected DataStoreBlobStore getBlobStore() {
            return blobStore;
        }

        protected File getBlobDir() {
            return blobDir;
        }

        protected String nodeNameFromNodeNumber(int nodeNumber) {
            return String.format("%s%d", CONTENT_ENTRY, nodeNumber);
        }

        protected void verifySubTree(String subTreeName,
                                     int count,
                                     File dsFolder,
                                     Map<Integer, String> exceptions) {
            NodeState n = nodeStore.getRoot().getChildNode(CONTENT_ROOT);
            assertTrue(n.exists());
            n = n.getChildNode(subTreeName);
            assertTrue(n.exists());
            for (int i=0; i<count; i++) {
                String nodeName = nodeNameFromNodeNumber(i);
                NodeState child = n.getChildNode(nodeName);
                if (exceptions.containsKey(i)) {
                    assertFalse(child.exists());
                    assertFalse(getBlobPathForBlobId(exceptions.get(i), dsFolder).toFile().exists());
                }
                else {
                    assertTrue(child.exists());
                    PropertyState content = child.getProperty(CONTENT_BINARY_PROP);
                    assertNotNull(content);
                    assertFalse(child.getChildNodeNames().iterator().hasNext());
                    Blob b = content.getValue(Type.BINARY);
                    assertNotNull(b);
                    String id = identifierFromBlob(b);
                    assertNotNull(id);

                    assertTrue(getBlobPathForBlobId(id, dsFolder).toFile().exists());
                }
            }
        }

        public void verifySubTreeAbsent(String subTreeName) {
            NodeState n = nodeStore.getRoot().getChildNode(CONTENT_ROOT);
            assertTrue(n.exists());
            assertFalse(n.hasChildNode(subTreeName));
        }

        public NodeState getNode(String subTreeName, int nodeNumber) {
            NodeState subTreeNode = nodeStore.getRoot().getChildNode(CONTENT_ROOT).getChildNode(subTreeName);
            if (subTreeNode.exists()) {
                return subTreeNode.getChildNode(nodeNameFromNodeNumber(nodeNumber));
            }
            return subTreeNode;
        }

        private NodeState findChildNodeWithBlobByReference(NodeState subTreeRoot, String reference) {
            for (String childNodeName : subTreeRoot.getChildNodeNames()) {
                NodeState childNode = subTreeRoot.getChildNode(childNodeName);
                PropertyState content = childNode.getProperty(CONTENT_BINARY_PROP);
                Blob b = content.getValue(Type.BINARY);
                String thisReference = b.getReference();
                if (thisReference.equals(reference)) {
                    return childNode;
                }
            }
            return null;
        }

        public void verifyBlobInSubTree(String subTreeName, String reference) {
            NodeState nodeWithBlob = findChildNodeWithBlobByReference(
                    nodeStore.getRoot().getChildNode(CONTENT_ROOT).getChildNode(subTreeName),
                    reference);
            assertNotNull(nodeWithBlob);
        }

        public void verifyBlobNotInSubTree(String subTreeName, String reference) {
            NodeState nodeWithBlob = findChildNodeWithBlobByReference(
                    nodeStore.getRoot().getChildNode(CONTENT_ROOT).getChildNode(subTreeName),
                    reference);
            assertNull(nodeWithBlob);
        }

        public long countLeaves() {
            NodeState contentRoot = nodeStore.getRoot().getChildNode(CONTENT_ROOT);
            long leafCount = 0;
            for (String childNodeName : contentRoot.getChildNodeNames()) {
                leafCount += countLeaves(childNodeName);
            }
            return leafCount;
        }

        public long countLeaves(String subTreeName) {
            NodeState subTreeNode = nodeStore.getRoot().getChildNode(CONTENT_ROOT).getChildNode(subTreeName);
            long leafCount = 0;
            if (subTreeNode.exists()) {
                leafCount = subTreeNode.getChildNodeCount(Long.MAX_VALUE);
            }
            return leafCount;
        }

        protected void mergeAndWait(NodeStore nodeStore, NodeBuilder nb) throws CommitFailedException {
            commitHappened = false;
            nodeStore.merge(nb, commitHook, CommitInfo.EMPTY);
            try {
                while (! commitHappened) {
                    Thread.sleep(100);
                }
            }
            catch (InterruptedException e) { }
        }

        public String addNode(String subTreeName, String iterationFlag, int nodeNumber) throws IOException, CommitFailedException {
            NodeBuilder nb = nodeStore.getRoot().builder();
            String nodeName = nodeNameFromNodeNumber(nodeNumber);
            NodeBuilder child = nb.getChildNode(CONTENT_ROOT).getChildNode(subTreeName).child(nodeName);
            Blob blob = nodeStore.createBlob(randomStream(BLOB_SIZE));
            child.setProperty(ITERATION_FLAG_PROP, iterationFlag);
            child.setProperty(CONTENT_BINARY_PROP, blob);

            mergeAndWait(nodeStore, nb);

            return blob.getReference();
        }

        public void addSubTree(String subTreeName, String iterationFlag, int childNodeCount) throws IOException, CommitFailedException {
            NodeBuilder nb = nodeStore.getRoot().builder();
            NodeBuilder subTreeRoot = nb.child(CONTENT_ROOT);
            populateSubTree(subTreeName, nodeStore, subTreeRoot, iterationFlag, childNodeCount);

            mergeAndWait(nodeStore, nb);
        }

        protected void populateSubTree(String subTreeName,
                                       SegmentNodeStore nodeStore,
                                       NodeBuilder parent,
                                       String iterationFlag,
                                       int count) throws IOException {
            NodeBuilder nb = parent.child(subTreeName);
            for (int i=0; i<count; i++) {
                String nodeName = nodeNameFromNodeNumber(i);
                NodeBuilder newChild = nb.child(nodeName);
                newChild.setProperty(ITERATION_FLAG_PROP, iterationFlag);
                newChild.setProperty(CONTENT_BINARY_PROP, nodeStore.createBlob(randomStream(BLOB_SIZE)));
            }
        }

        public boolean deleteNode(String subTreeName, int nodeNumber) throws CommitFailedException {
            boolean wasDeleted = false;
            NodeBuilder nb = nodeStore.getRoot().builder();
            String nodeName = nodeNameFromNodeNumber(nodeNumber);
            NodeBuilder child = nb.getChildNode(CONTENT_ROOT).getChildNode(subTreeName).child(nodeName);
            if (child.exists()) {
                for (PropertyState p : child.getProperties()) {
                    child.removeProperty(p.getName());
                }
                child.removeProperty(CONTENT_BINARY_PROP);
                wasDeleted = child.remove();
                mergeAndWait(nodeStore, nb);
            }
            return wasDeleted;
        }

        private MarkSweepGarbageCollector getGarbageCollector(long gcMaxAge) throws DataStoreException, IOException {
            if (null == garbageCollector) {
                assertTrue(SharedDataStoreUtils.isShared(blobStore));
                String repoId = ClusterRepositoryInfo.getOrCreateId(nodeStore);
                blobStore.addMetadataRecord(new ByteArrayInputStream(new byte[0]),
                        REPOSITORY.getNameFromId(repoId));
                if (null == executor) {
                    executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
                }
                garbageCollector = new MarkSweepGarbageCollector(
                        new SegmentBlobReferenceRetriever(fileStore),
                        blobStore,
                        executor,
                        blobDir.getAbsolutePath(),
                        2048,
                        gcMaxAge,
                        repoId
                );
            }
            return garbageCollector;
        }

        public Set<String> getAllChunkIds() throws Exception {
            Iterator<String> i = blobStore.getAllChunkIds(0);
            Set<String> chunks = Sets.newHashSet();
            while (i.hasNext()) {
                chunks.add(i.next());
            }
            return chunks;
        }

        public void mark() throws Exception {
            garbageCollector.collectGarbage(true);
        }

        public void sweep() throws Exception {
            doFileStoreCompaction();

            garbageCollector.collectGarbage(false);
        }

        protected abstract void doFileStoreCompaction() throws Exception;

        protected void compactFileStore(FileStore fileStore, SegmentGCOptions gcOptions) throws Exception {
            for (int i=0; i<gcOptions.getRetainedGenerations(); i++) {
                fileStore.compactFull();
            }
            fileStore.cleanup();
        }
    }

    static class PrimaryRepo extends OakRepo {
        private PrimaryRepo(SegmentNodeStore nodeStore, FileStore fileStore, DataStoreBlobStore blobStore, File blobDir)
                throws IOException, DataStoreException {
            super(nodeStore, fileStore, blobStore, blobDir);
        }

        public static PrimaryRepoBuilder builder(TemporaryFolder parentFolder) {
            return new PrimaryRepoBuilder(parentFolder);
        }

        public static class PrimaryRepoBuilder {
            private TemporaryFolder parentFolder;
            private String repoSubTreeName = null;
            private String iterationFlag = null;
            private int childNodeCount = 10;

            public PrimaryRepoBuilder(TemporaryFolder parentFolder) {
                this.parentFolder = parentFolder;
            }

            public PrimaryRepoBuilder withRepoSubTreeName(String repoSubTreeName) {
                this.repoSubTreeName = repoSubTreeName;
                return this;
            }

            public PrimaryRepoBuilder withIterationFlag(String iterationFlag) {
                this.iterationFlag = iterationFlag;
                return this;
            }

            public PrimaryRepoBuilder withChildNodeCount(int childNodeCount) {
                this.childNodeCount = childNodeCount;
                return this;
            }

            public PrimaryRepo build() throws Exception {
                File blobDir = parentFolder.newFolder();
                DataStoreBlobStore blobStore = createBlobStore(blobDir);
                FileStore fileStore = createFileStore(blobStore, blobDir);
                SegmentNodeStore nodeStore = createNodeStore(fileStore);

                PrimaryRepo prodRepo = new PrimaryRepo(nodeStore, fileStore, blobStore, blobDir);

                if (null != repoSubTreeName && null != iterationFlag && 0 != childNodeCount) {
                    prodRepo.addSubTree(repoSubTreeName, iterationFlag, childNodeCount);
                }

                return prodRepo;
            }
        }

        public File getBlobDir() {
            return super.getBlobDir();
        }

        public SegmentNodeStore getNodeStore() {
            return super.getNodeStore();
        }

        public DataStoreBlobStore getBlobStore() {
            return super.getBlobStore();
        }

        public void verifySubTree(String subTreeName, int nodeCount) {
            verifySubTree(subTreeName, nodeCount, Maps.newHashMap());
        }

        public void verifySubTree(String subTreeName, int nodeCount, Map<Integer, String> exceptions) {
            super.verifySubTree(subTreeName, nodeCount, getBlobDir(), exceptions);
        }

        @Override
        protected void doFileStoreCompaction() throws Exception {
            compactFileStore(getFileStore(), gcOptions);
        }
    }

    static class SecondaryRepo extends OakRepo {
        private static final String PROD_ROLE_KEY = "prodRole";
        private static final String STG_ROLE_KEY = "stgRole";
        private PrimaryRepo primaryRepo;
        private File compositeBlobDir;
        private CompositeDataStore compositeDataStore;

        public SecondaryRepo(File blobDir,
                             File compositeBlobDir,
                             SegmentNodeStore nodeStore,
                             FileStore fileStore,
                             DataStoreBlobStore blobStore,
                             CompositeDataStore compositeDataStore,
                             PrimaryRepo primaryRepo) throws Exception {
            super(nodeStore, fileStore, blobStore, blobDir);
            this.compositeBlobDir = compositeBlobDir;
            this.compositeDataStore = compositeDataStore;
            this.primaryRepo = primaryRepo;

            // Clone "staging" node store from production node store
            cloneNodeStore(primaryRepo.getNodeStore(), nodeStore);
        }

        private void cloneNodeStore(final SegmentNodeStore source, SegmentNodeStore dest) throws CommitFailedException {
            NodeState sourceRoot = source.getRoot();
            NodeState destRoot = dest.getRoot();
            NodeBuilder destRootBuilder = destRoot.builder();
            for (PropertyState p : sourceRoot.getProperties()) {
                destRootBuilder.setProperty(p);
            }
            for (String childNodeName : sourceRoot.getChildNodeNames()) {
                cloneSubTree(childNodeName, sourceRoot, destRootBuilder);
            }

            if (destRootBuilder.isModified()) {
                mergeAndWait(dest, destRootBuilder);
            }
        }

        private void cloneSubTree(final String nodeName, final NodeState sourceRoot, NodeBuilder destRootBuilder) {
            NodeState sourceChild = sourceRoot.getChildNode(nodeName);
            NodeBuilder destChildBuilder = destRootBuilder.child(nodeName);
            for (PropertyState p : sourceChild.getProperties()) {
                destChildBuilder.setProperty(p);
            }
            for (String childNodeName : sourceChild.getChildNodeNames()) {
                cloneSubTree(childNodeName, sourceChild, destChildBuilder);
            }
        }

        public void verifyProdSubTree(String subTreeName, int nodeCount) {
            verifyProdSubTree(subTreeName, nodeCount, Maps.newHashMap());
        }

        public void verifyProdSubTree(String subTreeName, int nodeCount, Map<Integer, String> exceptions) {
            verifySubTree(subTreeName, nodeCount, primaryRepo.getBlobDir(), exceptions);
        }

        public void verifyStgSubTree(String subTreeName, int nodeCount) {
            verifyStgSubTree(subTreeName, nodeCount, Maps.newHashMap());
        }

        public void verifyStgSubTree(String subTreeName, int nodeCount, Map<Integer, String> exceptions) {
            verifySubTree(subTreeName, nodeCount, getSecondaryBlobDir(), exceptions);
        }

        public PrimaryRepo getPrimaryRepo() {
            return primaryRepo;
        }

        public File getSecondaryBlobDir() {
            return super.getBlobDir();
        }

        public File getCompositeBlobDir() {
            return compositeBlobDir;
        }

        public SegmentNodeStore getNodeStore() {
            return super.getNodeStore();
        }

        public DataStoreBlobStore getSecondaryBlobStore() {
            return super.getBlobStore();
        }

        public CompositeDataStore getCompositeDataStore() {
            return compositeDataStore;
        }

        @Override
        protected void doFileStoreCompaction() throws Exception {
            primaryRepo.doFileStoreCompaction();
            compactFileStore(getFileStore(), gcOptions);
        }

        public static SecondaryRepoBuilder builder(TemporaryFolder parentFolder, PrimaryRepo primaryRepo) {
            return new SecondaryRepoBuilder(parentFolder, primaryRepo);
        }

        public static class SecondaryRepoBuilder {
            private TemporaryFolder parentFolder;
            private PrimaryRepo primaryRepo;
            private Map<String, String> roles = Maps.newHashMap();

            public SecondaryRepoBuilder(TemporaryFolder parentFolder, PrimaryRepo primaryRepo) {
                this.parentFolder = parentFolder;
                this.primaryRepo = primaryRepo;
                roles.put(PROD_ROLE_KEY, "prod");
                roles.put(STG_ROLE_KEY, "stg");
            }

            public SecondaryRepoBuilder withRoles(String prodRole, String stgRole) {
                if (!Strings.isNullOrEmpty(prodRole)) {
                    roles.put(PROD_ROLE_KEY, prodRole);
                }
                if (!Strings.isNullOrEmpty(stgRole)) {
                    roles.put(STG_ROLE_KEY, stgRole);
                }
                return this;
            }

            public SecondaryRepo build() throws Exception {
                File blobDir = parentFolder.newFolder();
                File compositeBlobDir = parentFolder.newFolder();
                CompositeDataStore compositeDataStore = createCompositeDataStore(
                        primaryRepo.getBlobDir(),
                        blobDir,
                        roles.get(PROD_ROLE_KEY),
                        roles.get(STG_ROLE_KEY)
                );
                DataStoreBlobStore secondaryDataStore = new DataStoreBlobStore(compositeDataStore);
                FileStore fileStore = createFileStore(secondaryDataStore, compositeBlobDir);
                SegmentNodeStore nodeStore = createNodeStore(fileStore);
                SecondaryRepo secondaryRepo = new SecondaryRepo(
                        blobDir,
                        compositeBlobDir,
                        nodeStore,
                        fileStore,
                        secondaryDataStore,
                        compositeDataStore,
                        primaryRepo
                );

                return secondaryRepo;
            }
        }

        private static CompositeDataStore createCompositeDataStore(File prodFolder, File stgFolder, String prodRole, String stgRole) throws Exception {
            DataStoreBlobStore prodDelegate = createBlobStore(prodFolder);
            DataStoreBlobStore stgDelegate = createBlobStore(stgFolder);

            Properties props = new Properties();
            CompositeDataStore cds = new CompositeDataStore(props, Lists.newArrayList(prodRole, stgRole));

            Map<String, Object> prodConfig = Maps.newHashMap();
            prodConfig.put("path", prodFolder.getAbsolutePath());
            prodConfig.put("role", prodRole);
            prodConfig.put("readOnly", "true");

            Map<String, Object> stgConfig = Maps.newHashMap();
            stgConfig.put("path", stgFolder.getAbsolutePath());
            stgConfig.put("role", stgRole);

            cds.addDelegate(new DelegateDataStore(new DataStoreProvider() {
                @Override
                public DataStore getDataStore() {
                    return prodDelegate;
                }

                @Override
                public String getRole() {
                    return prodRole;
                }}, prodConfig)
            );

            cds.addDelegate(new DelegateDataStore(new DataStoreProvider() {
                @Override
                public DataStore getDataStore() {
                    return stgDelegate;
                }

                @Override
                public String getRole() {
                    return stgRole;
                }}, stgConfig)
            );

            return cds;
        }
    }

    private static DataStoreBlobStore createBlobStore(File homeDir) throws Exception {
        DataStoreBlobStore ds = DataStoreUtils.getBlobStore(homeDir);
        closer.register(asCloseable(ds));
        return ds;
    }

    private static FileStore createFileStore(BlobStore blobStore, File workDir) throws IOException, InvalidFileStoreVersionException {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        FileStoreBuilder builder = fileStoreBuilder(workDir)
                .withNodeDeduplicationCacheSize(16384)
                .withBlobStore(blobStore)
                .withMaxFileSize(256)
                .withMemoryMapping(false)
                .withStatisticsProvider(new DefaultStatisticsProvider(executor))
                .withGCOptions(gcOptions);
        FileStore store = builder.build();
        closer.register(store);
        return store;
    }

    private static SegmentNodeStore createNodeStore(FileStore store) throws IOException, InvalidFileStoreVersionException {
        return SegmentNodeStoreBuilders.builder(store).build();
    }

    private static String identifierFromBlob(Blob blob) {
        return blob.getContentIdentity().split("#")[0];
    }

    private static byte[] randomBytes(int size) {
        Random r = new Random(LocalTime.now().toNanoOfDay());
        byte[] data = new byte[size];
        r.nextBytes(data);
        return data;
    }

    private static String randomString(int size) {
        return new String(randomBytes(size));
    }

    private static InputStream randomStream(int size) {
        return new ByteArrayInputStream(randomBytes(size));
    }

    private static Path getBlobPathForBlobId(String id, File dsFolder) {
        // This part is admittedly very implementation-specific and therefore brittle. -MR
        return Paths.get(dsFolder.getAbsolutePath(),
                "repository",
                "datastore",
                id.substring(0, 2),
                id.substring(2, 4),
                id.substring(4, 6),
                id);
    }

    private static boolean nodesAreEquivalent(NodeState n1, NodeState n2) throws IOException {
        for (NodeState lhs : Lists.newArrayList(n1, n2)) {
            NodeState rhs = n1 == lhs ? n2 : n1;
            for (String name : lhs.getChildNodeNames()) {
                if (!rhs.hasChildNode(name)) {
                    return false;
                }
            }
            for (PropertyState p1 : lhs.getProperties()) {
                if (!rhs.hasProperty(p1.getName())) {
                    return false;
                }
                PropertyState p2 = rhs.getProperty(p1.getName());
                if (p1.getType() != p2.getType()) {
                    return false;
                }
                if (Type.STRING == p1.getType()) {
                    String s1 = p1.getValue(Type.STRING);
                    String s2 = p2.getValue(Type.STRING);
                    if (!s1.equals(s2)) {
                        return false;
                    }
                } else if (Type.BINARY == p1.getType()) {
                    Blob b1 = p1.getValue(Type.BINARY);
                    Blob b2 = p2.getValue(Type.BINARY);
                    if (b1.length() != b2.length()) {
                        return false;
                    }
                    byte[] ba1 = new byte[(int)b1.length()];
                    byte[] ba2 = new byte[(int)b2.length()];
                    b1.getNewStream().read(ba1, 0, (int)b1.length());
                    b2.getNewStream().read(ba2, 0, (int)b2.length());
                    for (int i=0; i<b1.length(); i++) {
                        if (ba1[i] != ba2[i]) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    private static Map<Integer, String> deleteRandomNodesFromRepo(
            OakRepo repo,
            String subTreeName,
            int nodesPerSubtree,
            int numberOfNodesToDelete)
            throws CommitFailedException {
        Map<Integer, String> deletedNodes = Maps.newHashMap();
        Random r = new Random(LocalTime.now().toNanoOfDay());
        Set<Integer> alreadyDeletedNodeNumbers = Sets.newHashSet();
        while (numberOfNodesToDelete > deletedNodes.size()) {
            int nodeNumber;
            do {
                nodeNumber = r.nextInt(nodesPerSubtree);
            }
            while (alreadyDeletedNodeNumbers.contains(nodeNumber));
            alreadyDeletedNodeNumbers.add(nodeNumber);

            NodeState node = repo.getNode(subTreeName, nodeNumber);
            if (! node.exists()) {
                continue;
            }
            PropertyState prop = node.getProperty(CONTENT_BINARY_PROP);
            Blob b = prop.getValue(Type.BINARY);
            String blobId = identifierFromBlob(b);
            assertTrue(repo.deleteNode(subTreeName, nodeNumber));
            deletedNodes.put(nodeNumber, blobId);
        }
        return deletedNodes;
    }
}
