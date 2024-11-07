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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile;


import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateIndexer;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DefaultAheadOfTimeBlobDownloaderTest {
    public final static String BINARY_BLOB_SUFFIX = "metadata/jcr:content";

    private static final List<String> FFS_PATHS = List.of(
            "/include1/asset1.pdf/metadata/jcr:content",
            "/include1/asset1.pdf/jcr:content",
            "/include2/asset2.pdf/metadata/jcr:content",
            "/include2/asset2.pdf/jcr:content",
            "/tmp/asset3.pdf/jcr:content/metadata",
            "/tmp/asset3.pdf/jcr:content/metadata/jcr:content",
            "/var/a/b"
    );

    private static void generateTestFFS(Path ffs, List<String> pathNames, MemoryBlobStore memoryBlobStore) throws IOException {
        try (var bw = Files.newBufferedWriter(ffs, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            for (String pathName : pathNames) {
                String blobContent = "This is the blob for path " + pathName;
                String id = memoryBlobStore.writeBlob(new ByteArrayInputStream(blobContent.getBytes()));
                bw.write(pathName);
                bw.write("|{\"jcr:primaryType\":\"nam:oak:Resource\",\"jcr:data\":\":blobId:");
                bw.write(id);
                bw.write("\"}\n");
            }
        }
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testSingleIndexIncludeAllPaths() throws IOException {
        doTestSingleIndexIncludeAllPaths(List.of(new TestNodeStateIndexer(List.of("/"))), BINARY_BLOB_SUFFIX, 3);
    }

    @Test
    public void testSingleIndexIncludeOnePath() throws IOException {
        doTestSingleIndexIncludeAllPaths(List.of(new TestNodeStateIndexer(List.of("/include1"))), BINARY_BLOB_SUFFIX, 1);
    }

    @Test
    public void testSingleIndexIncludePathDoesNotExist() throws IOException {
        doTestSingleIndexIncludeAllPaths(List.of(new TestNodeStateIndexer(List.of("/doesnotexist"))), BINARY_BLOB_SUFFIX, 0);
    }

    @Test
    public void testSingleIndexPathDoesNotContainMatches() throws IOException {
        doTestSingleIndexIncludeAllPaths(List.of(new TestNodeStateIndexer(List.of("/var"))), BINARY_BLOB_SUFFIX, 0);
    }

    @Test
    public void testMultipleIndexes() throws IOException {
        doTestSingleIndexIncludeAllPaths(List.of(
                        new TestNodeStateIndexer(List.of("/include2")),
                        new TestNodeStateIndexer(List.of("/include1"))),
                BINARY_BLOB_SUFFIX, 2);
    }

    @Test
    public void testMultipleIncludePaths() throws IOException {
        doTestSingleIndexIncludeAllPaths(List.of(new TestNodeStateIndexer(List.of("/include2", "/include1"))),
                BINARY_BLOB_SUFFIX, 2);
    }

    private void doTestSingleIndexIncludeAllPaths(List<NodeStateIndexer> indexers, String prefetchBinaryBlobSuffix, int expectedBlobsDownloaded) throws IOException {
        Path ffsPath = folder.newFile("ffs.json").toPath();
        try (MemoryBlobStore blobStore = new MemoryBlobStore()) {
            generateTestFFS(ffsPath, FFS_PATHS, blobStore);

            DefaultAheadOfTimeBlobDownloader defaultAheadOfTimeBlobDownloader = new DefaultAheadOfTimeBlobDownloader(
                    prefetchBinaryBlobSuffix, ffsPath.toFile(), Compression.NONE,
                    blobStore, indexers,
                    2, 128, 1);

            defaultAheadOfTimeBlobDownloader.start();
            defaultAheadOfTimeBlobDownloader.join();

            assertEquals(FFS_PATHS.size(), defaultAheadOfTimeBlobDownloader.getLinesScanned());
            assertEquals(expectedBlobsDownloaded, defaultAheadOfTimeBlobDownloader.getBlobsEnqueuedForDownload());
            assertEquals(expectedBlobsDownloaded, defaultAheadOfTimeBlobDownloader.getTotalBlobsDownloaded());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}