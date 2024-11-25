/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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
package org.apache.jackrabbit.oak.jcr.binary;

import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.getBinary;
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.httpGet;
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.isSuccessfulHttpPut;
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.putBinary;
import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.storeBinaryAndRetrieve;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.api.JackrabbitValueFactory;
import org.apache.jackrabbit.api.binary.BinaryDownload;
import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;
import org.apache.jackrabbit.api.binary.BinaryUpload;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore.AzureDataStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.datastore.S3DataStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore.DocumentMongoNodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.fixtures.nodestore.SegmentMemoryNodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessDSGCFixture;
import org.apache.jackrabbit.oak.jcr.binary.util.Content;
import org.apache.jackrabbit.oak.plugins.blob.BlobReferenceRetriever;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import org.apache.jackrabbit.guava.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BinaryAccessDSGCIT extends AbstractBinaryAccessIT {
    private static Logger LOG = LoggerFactory.getLogger(BinaryAccessDSGCIT.class);

    private static final String TEST_ROOT = "testroot";
    private static final long BINARY_SIZE = 1024*1024;

    private static final String TRADITIONAL_UPLOAD_1 = "tu1";
    private static final String TRADITIONAL_UPLOAD_2 = "tu2";
    private static final String DIRECT_UPLOAD_1 = "du1";
    private static final String DIRECT_UPLOAD_2 = "du2";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<?> dataStoreFixtures() {
        Collection<NodeStoreFixture> fixtures = new ArrayList<>();
        fixtures.add(new SegmentMemoryNodeStoreFixture(new S3DataStoreFixture()));
        fixtures.add(new DocumentMongoNodeStoreFixture(new S3DataStoreFixture()));

        fixtures.add(new SegmentMemoryNodeStoreFixture(new AzureDataStoreFixture()));
        fixtures.add(new DocumentMongoNodeStoreFixture(new AzureDataStoreFixture()));

        return fixtures;
    }

    public BinaryAccessDSGCIT(NodeStoreFixture fixture) {
        // reuse NodeStore (and DataStore) across all tests in this class
        super(fixture, false);
    }

    private Session session;
    private JackrabbitValueFactory directUploader;

    private MarkSweepGarbageCollector garbageCollector = null;
    private ThreadPoolExecutor executor = null;

    @Before
    public void setup() throws RepositoryException {
        session = getAdminSession();
        directUploader = (JackrabbitValueFactory) session.getValueFactory();

        if (session.getNode("/").hasNode(TEST_ROOT)) {
            session.getNode("/" + TEST_ROOT).remove();
            session.save();
        }
        session.getNode("/").addNode(TEST_ROOT);

        getConfigurableHttpDataRecordProvider().setDirectUploadURIExpirySeconds(60*5);
        getConfigurableHttpDataRecordProvider().setDirectDownloadURIExpirySeconds(60*5);
    }

    // For debugging.
    private void printTree(Node root) throws RepositoryException {
        printTree(root, 0);
    }

    // For debugging.
    private void printTree(Node root, int level) throws RepositoryException {
        for (int i=0; i<level; i++) {
            System.out.print(" ");
        }
        System.out.println(root.getName());

        NodeIterator iter = root.getNodes();
        while (iter.hasNext()) {
            printTree(iter.nextNode(), level+1);
        }
    }

    private String toAbsolutePath(String leaf) {
        return "/" + TEST_ROOT + "/" + leaf;
    }

    private Binary createDirectBinary(String path, Content content) throws RepositoryException, IOException {
        BinaryUpload upload = directUploader.initiateBinaryUpload(content.size(), 1); // multi-part not needed for this
        assertNotNull(upload);

        int code = content.httpPUT(upload.getUploadURIs().iterator().next());
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        Binary binary = directUploader.completeBinaryUpload(upload.getUploadToken());
        putBinary(session, path, binary);

        return getBinary(session, path);
    }

    private void verifyBinariesExistViaSession(Session session,
                                               Map<String, Binary> binaries,
                                               Map<String, Content> binaryContent)
            throws RepositoryException, IOException {
        for (Map.Entry<String, Binary> entry : binaries.entrySet()) {
            Binary b = getBinary(session, toAbsolutePath(entry.getKey()));
            assertEquals(b, entry.getValue());
            binaryContent.get(entry.getKey()).assertEqualsWith(b.getStream());
        }
    }

    private void verifyBinariesExistDirectly(Map<String, Binary> binaries, Map<String, Content> binaryContent)
            throws RepositoryException, IOException {
        for (Map.Entry<String, Binary> entry : binaries.entrySet()) {
            assertTrue(entry.getValue() instanceof BinaryDownload);
            URI uri = ((BinaryDownload) entry.getValue()).getURI(BinaryDownloadOptions.DEFAULT);
            binaryContent.get(entry.getKey()).assertEqualsWith(httpGet(uri));
        }
    }

    private void verifyBinariesDoNotExistDirectly(Map<String, Binary> deletedBinaries) throws RepositoryException {
        for (Map.Entry<String, Binary> entry : deletedBinaries.entrySet()) {
            assertTrue(entry.getValue() instanceof BinaryDownload);
            URI uri = ((BinaryDownload) entry.getValue()).getURI(BinaryDownloadOptions.DEFAULT);
            assertNull(uri);
        }
    }

    private void compactFileStore() {
        FileStore fileStore = getNodeStoreComponent(FileStore.class);
        for (int i=0; i<SegmentGCOptions.defaultGCOptions().getRetainedGenerations(); i++) {
            fileStore.compactFull();
        }
    }

    private MarkSweepGarbageCollector getGarbageCollector()
            throws DataStoreException, IOException {
        DataStoreBlobStore blobStore = (DataStoreBlobStore) getNodeStoreComponent(BlobStore.class);
        
        if (null == garbageCollector) {
            String repoId = ClusterRepositoryInfo.getOrCreateId(getNodeStore());
            blobStore.setRepositoryId(repoId);
            if (null == executor) {
                executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
            }
            BlobReferenceRetriever referenceRetriever = ((BinaryAccessDSGCFixture) fixture).getBlobReferenceRetriever(getNodeStore());
            garbageCollector = new MarkSweepGarbageCollector(
                    referenceRetriever,
                    blobStore,
                    executor,
                    folder.newFolder().getAbsolutePath(),
                    2048,
                    0,
                    repoId
            );
        }
        return garbageCollector;
    }

    private int getBlobCount() throws Exception {
        GarbageCollectableBlobStore ds = (GarbageCollectableBlobStore) getNodeStoreComponent(BlobStore.class);
        Set<String> chunks = new HashSet<>();
        Iterator<String> chunkIds = ds.getAllChunkIds(0);
        while (chunkIds.hasNext()) {
            chunks.add(chunkIds.next());
        }
        return chunks.size();
    }

    @Test
    public void testGC() throws Exception {
        LOG.info("Starting testGC [{}]", fixture);

        Map<String, Content> binaryContent = new HashMap<>();
        Map<String, Binary> binaries = new HashMap<>();

        for (String key : List.of(TRADITIONAL_UPLOAD_1, TRADITIONAL_UPLOAD_2)) {
            Content content = Content.createRandom(BINARY_SIZE);
            binaryContent.put(key, content);
            binaries.put(key, storeBinaryAndRetrieve(session, toAbsolutePath(key), content));
        }
        for (String key : List.of(DIRECT_UPLOAD_1, DIRECT_UPLOAD_2)) {
            Content content = Content.createRandom(BINARY_SIZE);
            binaryContent.put(key, content);
            binaries.put(key, createDirectBinary(toAbsolutePath(key), content));
        }
        session.save();

        // Test that all four binaries can be accessed
        assertEquals(4, getBlobCount());
        //  - Download all four via repo
        verifyBinariesExistViaSession(session, binaries, binaryContent);
        //  - Download directly
        verifyBinariesExistDirectly(binaries, binaryContent);

        // Delete one of the binaries uploaded via repo and one uploaded directly
        Node testRoot = session.getNode("/" + TEST_ROOT);
        List<String> deletedBinaryPaths = List.of(TRADITIONAL_UPLOAD_2, DIRECT_UPLOAD_2);
        for (String path : deletedBinaryPaths) {
            Node toRemove = testRoot.getNode(path);
            toRemove.remove();
        }
        session.save();

        // Verify that they are deleted from repo
        for (String path : deletedBinaryPaths) {
            assertFalse(session.nodeExists(toAbsolutePath(path)));
        }

        // Verify that all four binaries are still in data store
        assertEquals(4, getBlobCount());


        // Run DSGC
        ((BinaryAccessDSGCFixture) fixture).compactStore(getNodeStore());
        MarkSweepGarbageCollector garbageCollector = getGarbageCollector();
        garbageCollector.collectGarbage(false);

        // Verify that only two binaries remain in data store
        assertEquals(2, getBlobCount());

        // Verify that the two binaries remaining can still be accessed
        Map<String, Binary> deletedBinaries = new HashMap<>();
        for (String deletedPath : List.of(TRADITIONAL_UPLOAD_2, DIRECT_UPLOAD_2)) {
            deletedBinaries.put(deletedPath, binaries.get(deletedPath));
            binaries.remove(deletedPath);
            binaryContent.remove(deletedPath);
        }

        verifyBinariesExistViaSession(session, binaries, binaryContent);
        verifyBinariesExistDirectly(binaries, binaryContent);

        verifyBinariesDoNotExistDirectly(deletedBinaries);
    }
}
