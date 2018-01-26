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
import org.apache.jackrabbit.oak.segment.SegmentBlob;
import org.apache.jackrabbit.oak.segment.SegmentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
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
import java.util.List;
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
    public void testDSGCManually() throws Exception {
        File otherBlobDir = folder.newFolder();
        DataStoreBlobStore otherBlobStore = createBlobStore(otherBlobDir);
        FileStore otherFileStore = createFileStore(otherBlobStore, otherBlobDir);
        SegmentNodeStore otherNodeStore = createNodeStore(otherFileStore);
        // My version
        PrimaryRepo pr = PrimaryRepo.builder(folder).build();

        // Put stuff in
        int numBlobs = 10;
        NodeBuilder a = otherNodeStore.getRoot().builder();
        List<Integer> processed = Lists.newArrayList();

        for (int i = 0; i < numBlobs; i++) {
            SegmentBlob b = (SegmentBlob) otherNodeStore.createBlob(randomStream(BLOB_SIZE));
            a.child("c" + i).setProperty("x", b);
        }

        otherNodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // My version
        pr.addSubTree("P_del1", "first", numBlobs);


        // Delete some of the stuff
        int maxDeleted  = 5;
        Random rand = new Random();
        for (int i = 0; i < maxDeleted; i++) {
            int n = rand.nextInt(numBlobs);
            if (!processed.contains(n)) {
                processed.add(n);
            }
        }
        for (int id : processed) {
            a.child("c" + id).remove();
        }
        otherNodeStore.merge(a, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // My version
        //for (int id : processed) {
        //    assertTrue(pr.deleteNode("content", id));
        //}
        Map<String, Map<Integer, String>> deletedNodes =
                deleteRandomNodesFromRepo(pr, Lists.newArrayList("P_del1"), numBlobs);

        log.info("Deleted nodes : {}", processed.size());

        // Sleep a little to make eligible for cleanup
        TimeUnit.MILLISECONDS.sleep(5);

        // Ensure cleanup is efficient by surpassing the number of
        // retained generations
        for (int k = 0; k < gcOptions.getRetainedGenerations(); k++) {
            otherFileStore.compactFull();
        }
        otherFileStore.cleanup();

        // Now try gc
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        String repoId = null;
        if (SharedDataStoreUtils.isShared(otherBlobStore)) {
            repoId = ClusterRepositoryInfo.getOrCreateId(otherNodeStore);
            otherBlobStore.addMetadataRecord(
                    new ByteArrayInputStream(new byte[0]),
                    REPOSITORY.getNameFromId(repoId));
        }
        MarkSweepGarbageCollector gc = new MarkSweepGarbageCollector(
                new SegmentBlobReferenceRetriever(otherFileStore),
                (GarbageCollectableBlobStore) otherFileStore.getBlobStore(),
                executor,
                otherBlobDir.getAbsolutePath(),
                2048,
                0,
                repoId
        );
        gc.collectGarbage(false);
        // My version
        pr.sweep(0);

        Set<String> existingAfterGC = Sets.newHashSet();
        Iterator<String> cur = otherBlobStore.getAllChunkIds(0);
        while (cur.hasNext()) {
            existingAfterGC.add(cur.next());
        }
        // My version
        Set<String> myExistingAfterGC = pr.getAllChunkIds();

        assertEquals(numBlobs - processed.size(), existingAfterGC.size());

        // My version
        assertEquals(numBlobs - deletedNodes.get("P_del1").keySet().size(), myExistingAfterGC.size());
    }

    @Test
    public void testDSGC() throws Exception {
        // Set up the "production" node store and file data store
        // Prepopulate the production node store with several sets of blobs
        //  - Set P_perm1 - will never be deleted
        //  - Set P_del1 - will be deleted on production
        //  - Set P_shared1 - will be deleted on staging
        int childNodeCount = 2;
        String iterationFlag = "first";
        PrimaryRepo primaryRepo = PrimaryRepo.builder(folder).build();
//        primaryRepo.addSubTree("P_perm1", iterationFlag, childNodeCount);
        //primaryRepo.addSubTree("P_del1", iterationFlag, childNodeCount);
        //primaryRepo.addSubTree("P_shared1", iterationFlag, childNodeCount);

        // Clone "staging" node store from production node store
        // Prepopulate the staging node store with several sets of blobs
        //  - Set S_perm1 - will never be deleted
        //  - Set S_del1 - will be deleted (from staging)
        //  - Set P_shared1 - the same set that was included in production,
        //    to be deleted by staging
        iterationFlag = "second";
        SecondaryRepo secondaryRepo = SecondaryRepo.builder(folder, primaryRepo).build();
        //secondaryRepo.addSubTree("S_perm1", iterationFlag, childNodeCount);
        secondaryRepo.addSubTree("S_del1", iterationFlag, childNodeCount);

        // Add to production:
        //  - Set P_perm2 - will not be deleted
        //  - Set P_del2 - will be deleted by production
        //  - Set P_shared2 - overlaps staging, part of which will be deleted by production
        //iterationFlag = "third";
        //primaryRepo.addSubTree("P_perm2", iterationFlag, childNodeCount);
//        primaryRepo.addSubTree("P_del2", iterationFlag, childNodeCount);
//        primaryRepo.addSubTree("P_shared2", iterationFlag, childNodeCount);
        // Add to staging:
        //  - Set S_perm2 - will not be deleted
        //  - Set S_del2 - will be deleted by staging
        //  - Set P_shared2 - production, part of which will be deleted by staging
        //secondaryRepo.addSubTree("S_perm2", iterationFlag, childNodeCount);
        //secondaryRepo.addSubTree("S_del2", iterationFlag, childNodeCount);
        //secondaryRepo.addSubTree("P_shared2", iterationFlag, childNodeCount);

        //Set<String> prBefore = primaryRepo.getAllChunkIds();
        Set<String> srBefore = secondaryRepo.getAllChunkIds();
        // Delete some number of blobs, remembering which are deleted:
        //  - From P_del1 via production
        //  - From S_del1 via staging
        //  - From P_shared1 via staging
        //  - From P_del2 via production
        //  - From P_shared2 via production
        //  - From S_del2 via staging
        //  - From P_shared2 via staging
//        Map<String, Map<Integer, String>> primaryRepoDeletedNodes =
//                deleteRandomNodesFromRepo(
//                        primaryRepo,
//                        Lists.newArrayList("P_del1"), // , "P_del2", "P_shared2"),
//                        childNodeCount
//                );
        Map<String, Map<Integer, String>> secondaryRepoDeletedNodes =
                deleteRandomNodesFromRepo(
                        secondaryRepo,
                        Lists.newArrayList("S_del1"), //, "S_del2"), // , "P_shared1", "P_shared2"),
                        childNodeCount
                );
        TimeUnit.MILLISECONDS.sleep(5); // Make eligible for GC

        // Invoke DGSC
        //primaryRepo.mark(0L);
        //secondaryRepo.mark(0L);
        //primaryRepo.sweep(0L);
        secondaryRepo.sweep(0L);

        //Set<String> prAfter = primaryRepo.getAllChunkIds();
        Set<String> srAfter = secondaryRepo.getAllChunkIds();

//        int prExpectedDeletedNodeCount = 0;
//        for (String subTreeName : primaryRepoDeletedNodes.keySet()) {
//            prExpectedDeletedNodeCount += primaryRepoDeletedNodes.get(subTreeName).keySet().size();
//        }
//        assertEquals(prExpectedDeletedNodeCount, prBefore.size() - prAfter.size());

        int srExpectedDeletedNodeCount = 0;
        for (String subTreeName : secondaryRepoDeletedNodes.keySet()) {
            srExpectedDeletedNodeCount += secondaryRepoDeletedNodes.get(subTreeName).keySet().size();
        }
        assertEquals(srExpectedDeletedNodeCount, srBefore.size() - srAfter.size());

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

        public OakRepo(SegmentNodeStore nodeStore, FileStore fileStore, DataStoreBlobStore blobStore, File blobDir) {
            this.nodeStore = nodeStore;
            this.fileStore = fileStore;
            this.blobStore = blobStore;
            this.blobDir = blobDir;
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

        public void mark(long gcMaxAge) throws Exception {
            MarkSweepGarbageCollector gc = getGarbageCollector(gcMaxAge);
            gc.collectGarbage(true);
        }

        public void sweep(long gcMaxAge) throws Exception {
            MarkSweepGarbageCollector gc = getGarbageCollector(gcMaxAge);

            doFileStoreCompaction();

            gc.collectGarbage(false);
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
        private PrimaryRepo(SegmentNodeStore nodeStore, FileStore fileStore, DataStoreBlobStore blobStore, File blobDir) {
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

    private static Map<String, Map<Integer, String>> deleteRandomNodesFromRepo(OakRepo repo, List<String> subTrees, int nodesPerSubtree) throws CommitFailedException {
        Map<String, Map<Integer, String>> deletedNodes = Maps.newHashMap();
        Random r = new Random(LocalTime.now().toNanoOfDay());
        for (String subTreeName : subTrees) {
            int numberOfNodesToDelete = r.nextInt(Integer.max(nodesPerSubtree/2, 1))+1;
            Set<Integer> alreadyDeletedNodeNumbers = Sets.newHashSet();
            for (int i=0; i<numberOfNodesToDelete; i++) {
                int nodeNumber;
                do {
                    nodeNumber = r.nextInt(nodesPerSubtree);
                }
                while (alreadyDeletedNodeNumbers.contains(nodeNumber));
                alreadyDeletedNodeNumbers.add(nodeNumber);

                NodeState node = repo.getNode(subTreeName, nodeNumber);
                PropertyState prop = node.getProperty(CONTENT_BINARY_PROP);
                Blob b = prop.getValue(Type.BINARY);
                String blobId = identifierFromBlob(b);
                assertTrue(repo.deleteNode(subTreeName, nodeNumber));
                if (! deletedNodes.containsKey(subTreeName)) {
                    deletedNodes.put(subTreeName, Maps.newHashMap());
                }
                Map<Integer, String> deletedNodeNumbers = deletedNodes.get(subTreeName);
                deletedNodeNumbers.put(i, blobId);
                deletedNodes.put(subTreeName, deletedNodeNumbers);
            }
        }
        return deletedNodes;
    }
}
