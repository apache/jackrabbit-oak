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

import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.createFileWithBinary;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.getBinary;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.getRandomString;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.httpGet;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.httpPut;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.isSuccessfulHttpPut;
import static org.apache.jackrabbit.oak.jcr.binary.BinaryAccessTestUtils.putBinary;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.api.JackrabbitValueFactory;
import org.apache.jackrabbit.api.binary.BinaryDownload;
import org.apache.jackrabbit.api.binary.BinaryDownloadOptions;
import org.apache.jackrabbit.api.binary.BinaryUpload;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.plugins.blob.MarkSweepGarbageCollector;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.segment.SegmentBlobReferenceRetriever;
import org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.cluster.ClusterRepositoryInfo;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

public class BinaryAccessDSGCIT extends AbstractHttpBinaryIT {
    private static final String TEST_ROOT = "testroot";
    private static long BINARY_SIZE = 1024*1024;

    private static final String TRADITIONAL_UPLOAD_1 = "tu1";
    private static final String TRADITIONAL_UPLOAD_2 = "tu2";
    private static final String DIRECT_UPLOAD_1 = "du1";
    private static final String DIRECT_UPLOAD_2 = "du2";

    public BinaryAccessDSGCIT(NodeStoreFixture fixture) { super(fixture); }

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

    @Before
    public void ignoreIfUsingDocumentNodeStore() {
        assumeFalse(fixture instanceof DocumentMemoryNodeStoreFixture);
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

    private Binary createSessionBinary(String nodeName, String content) throws RepositoryException {
        return createFileWithBinary(
                session, nodeName, new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
        );
    }

    private Binary createDirectBinary(String nodeName, String content) throws RepositoryException, IOException {
        byte[] contentBytes = content.getBytes(StandardCharsets.UTF_8);
        long size = contentBytes.length;

        BinaryUpload upload = directUploader.initiateBinaryUpload(size, 1); // multi-part not needed for this
        assertNotNull(upload);

        int code = httpPut(upload.getUploadURIs().iterator().next(), size, new ByteArrayInputStream(contentBytes));
        assertTrue(isSuccessfulHttpPut(code, getConfigurableHttpDataRecordProvider()));

        Binary binary = directUploader.completeBinaryUpload(upload.getUploadToken());
        putBinary(session, nodeName, binary);

        return binary;
    }

    private void verifyBinariesExistViaSession(Session session,
                                               Map<String, Binary> binaries,
                                               Map<String, String> binaryContent)
            throws RepositoryException, IOException {
        for (Map.Entry<String, Binary> entry : binaries.entrySet()) {
            Binary b = getBinary(session, toAbsolutePath(entry.getKey()));
            assertEquals(b, entry.getValue());
            StringWriter writer = new StringWriter();
            IOUtils.copy(b.getStream(), writer, StandardCharsets.UTF_8);
            assertEquals(binaryContent.get(entry.getKey()), writer.toString());

        }
    }

    private void verifyBinariesExistDirectly(Map<String, Binary> binaries, Map<String, String> binaryContent)
            throws RepositoryException, IOException {
        for (Map.Entry<String, Binary> entry : binaries.entrySet()) {
            assertTrue(entry.getValue() instanceof BinaryDownload);
            URI uri = ((BinaryDownload) entry.getValue()).getURI(BinaryDownloadOptions.DEFAULT);
            StringWriter writer = new StringWriter();
            IOUtils.copy(httpGet(uri), writer, StandardCharsets.UTF_8);
            assertEquals(binaryContent.get(entry.getKey()), writer.toString());
        }
    }

    private void verifyBinariesDoNotExistDirectly(Map<String, Binary> deletedBinaries) throws RepositoryException {
        for (Map.Entry<String, Binary> entry : deletedBinaries.entrySet()) {
            assertTrue(entry.getValue() instanceof BinaryDownload);
            URI uri = ((BinaryDownload) entry.getValue()).getURI(BinaryDownloadOptions.DEFAULT);
            assertNull(uri);
        }
    }

    private void compactFileStore(NodeStoreFixture fixture) throws IOException {
        FileStore fileStore = ((FileStoreHolder) fixture).getFileStore();
        for (int i=0; i<SegmentGCOptions.defaultGCOptions().getRetainedGenerations(); i++) {
            fileStore.compactFull();
        }
    }

    private MarkSweepGarbageCollector getGarbageCollector(NodeStoreFixture fixture)
            throws DataStoreException, IOException {
        DataStoreBlobStore blobStore = (DataStoreBlobStore) ((BlobStoreHolder) fixture).getBlobStore();
        NodeStore nodeStore = ((NodeStoreHolder) fixture).getNodeStore();
        FileStore fileStore = ((FileStoreHolder) fixture).getFileStore();
        File fileStoreRoot = ((FileStoreHolder) fixture).getFileStoreRoot();

        if (null == garbageCollector) {
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
                    fileStoreRoot.getAbsolutePath(),
                    2048,
                    0,
                    repoId
            );
        }
        return garbageCollector;
    }

    private int getBlobCount(NodeStoreFixture fixture) throws Exception {
        DataStoreBlobStore ds = (DataStoreBlobStore) ((BlobStoreHolder) fixture).getBlobStore();
        Set<String> chunks = Sets.newHashSet();
        Iterator<String> chunkIds = ds.getAllChunkIds(0);
        while (chunkIds.hasNext()) {
            chunks.add(chunkIds.next());
        }
        return chunks.size();
    }

    @Test
    public void testGC() throws Exception {
        Map<String, String> binaryContent = Maps.newHashMap();
        Map<String, Binary> binaries = Maps.newHashMap();

        for (String key : Lists.newArrayList(TRADITIONAL_UPLOAD_1, TRADITIONAL_UPLOAD_2)) {
            String content = getRandomString(BINARY_SIZE);
            binaryContent.put(key, content);
            binaries.put(key, createSessionBinary(toAbsolutePath(key), content));
        }
        for (String key : Lists.newArrayList(DIRECT_UPLOAD_1, DIRECT_UPLOAD_2)) {
            String content = getRandomString(BINARY_SIZE);
            binaryContent.put(key, content);
            binaries.put(key, createDirectBinary(toAbsolutePath(key), content));
        }
        session.save();

        // Test that all four binaries can be accessed
        assertEquals(4, getBlobCount(fixture));
        //  - Download all four via repo
        verifyBinariesExistViaSession(session, binaries, binaryContent);
        //  - Download directly
        verifyBinariesExistDirectly(binaries, binaryContent);

        // Delete one of the binaries uploaded via repo and one uploaded directly
        Node testRoot = session.getNode("/" + TEST_ROOT);
        List<String> deletedBinaryPaths = Lists.newArrayList(TRADITIONAL_UPLOAD_2, DIRECT_UPLOAD_2);
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
        assertEquals(4, getBlobCount(fixture));


        // Run DSGC
        compactFileStore(fixture);
        MarkSweepGarbageCollector garbageCollector = getGarbageCollector(fixture);
        garbageCollector.collectGarbage(false);

        // Verify that only two binaries remain in data store
        assertEquals(2, getBlobCount(fixture));

        // Verify that the two binaries remaining can still be accessed
        Map<String, Binary> deletedBinaries = Maps.newHashMap();
        for (String deletedPath : Lists.newArrayList(TRADITIONAL_UPLOAD_2, DIRECT_UPLOAD_2)) {
            deletedBinaries.put(deletedPath, binaries.get(deletedPath));
            binaries.remove(deletedPath);
            binaryContent.remove(deletedPath);
        }

        verifyBinariesExistViaSession(session, binaries, binaryContent);
        verifyBinariesExistDirectly(binaries, binaryContent);

        verifyBinariesDoNotExistDirectly(deletedBinaries);
    }
}
