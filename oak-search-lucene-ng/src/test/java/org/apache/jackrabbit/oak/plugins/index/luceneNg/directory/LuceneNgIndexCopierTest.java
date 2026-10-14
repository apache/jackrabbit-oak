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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.directory;

import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LuceneNgIndexCopierTest {

    @TempDir
    public Path tempDir;

    private final Executor sameThreadExecutor = Runnable::run;

    @Test
    public void wrapForRead_copiesFileToLocalDisk() throws Exception {
        // OakDirectory, not a generic Directory stand-in: wrapForRead's remote parameter is
        // typed OakDirectory (see Task B1's Interfaces line and Step 5's rationale) because
        // in this module remote is never anything else - there is no mount/multiplexing
        // support here unlike legacy, so the type can say what's actually true instead of
        // being checked at runtime.
        NodeBuilder storageBuilder = INITIAL_CONTENT.builder();
        OakDirectory remote = new OakDirectory(storageBuilder, "testIndex", false);
        try (IndexOutput out = remote.createOutput("segments_1", IOContext.DEFAULT)) {
            out.writeString("hello-luceneNg");
        }

        LuceneNgIndexCopier copier = new LuceneNgIndexCopier(sameThreadExecutor, Files.createTempDirectory(tempDir, "folder").toFile(), false);
        LuceneNgIndexDefinition definition = testDefinition("uid-1");

        try (Directory wrapped = copier.wrapForRead("/oak:index/test", definition, remote, "luceneNg")) {
            byte[] expected;
            try (var in = remote.openInput("segments_1", IOContext.DEFAULT)) {
                expected = new byte[(int) in.length()];
                in.readBytes(expected, 0, expected.length);
            }
            byte[] actual;
            try (var in = wrapped.openInput("segments_1", IOContext.DEFAULT)) {
                actual = new byte[(int) in.length()];
                in.readBytes(actual, 0, actual.length);
            }
            assertArrayEquals(expected, actual);
        }

        assertTrue(copier.getDownloadCount() >= 1, "expected at least one file copied locally");
        copier.close();
    }

    @Test
    public void existsLocally_and_localFileLength_reflectFilePresenceAndAbsence() throws Exception {
        File dir = Files.createTempDirectory(tempDir, "folder").toFile();
        File file = new File(dir, "segments_1");
        byte[] content = {1, 2, 3, 4, 5};
        Files.write(file.toPath(), content);

        assertTrue(LuceneNgIndexCopier.existsLocally(dir, "segments_1"));
        assertEquals(content.length, LuceneNgIndexCopier.localFileLength(dir, "segments_1"));

        assertFalse(LuceneNgIndexCopier.existsLocally(dir, "missing"));
        assertEquals(-1, LuceneNgIndexCopier.localFileLength(dir, "missing"));
    }

    @Test
    public void deleteFile_removesExistingLocalFile() throws Exception {
        File dir = Files.createTempDirectory(tempDir, "folder").toFile();
        LuceneNgIndexCopier copier = new LuceneNgIndexCopier(sameThreadExecutor, Files.createTempDirectory(tempDir, "folder").toFile(), false);
        try (Directory local = FSDirectory.open(dir.toPath())) {
            try (IndexOutput out = local.createOutput("segments_1", IOContext.DEFAULT)) {
                out.writeString("content");
            }
            assertTrue(LuceneNgIndexCopier.existsLocally(dir, "segments_1"));

            boolean deleted = copier.deleteFile(local, dir, "segments_1", true);

            assertTrue(deleted, "deleteFile should report success");
            assertFalse(LuceneNgIndexCopier.existsLocally(dir, "segments_1"), "file should be removed from disk");
        } finally {
            copier.close();
        }
    }

    @Test
    public void deleteFile_whenFileAlreadyGone_stillReportsSuccessWithoutThrowing() throws Exception {
        File dir = Files.createTempDirectory(tempDir, "folder").toFile();
        LuceneNgIndexCopier copier = new LuceneNgIndexCopier(sameThreadExecutor, Files.createTempDirectory(tempDir, "folder").toFile(), false);
        try (Directory local = FSDirectory.open(dir.toPath())) {
            boolean deleted = copier.deleteFile(local, dir, "never-existed", true);
            assertTrue(deleted, "deleteFile has nothing to do but should still report success");
        } finally {
            copier.close();
        }
    }

    @Test
    public void indexSanityChecker_purgesLocalOnSizeMismatch() throws Exception {
        File localDir = Files.createTempDirectory(tempDir, "folder").toFile();
        NodeBuilder storageBuilder = INITIAL_CONTENT.builder();
        OakDirectory remote = new OakDirectory(storageBuilder, "sanityIndex", false);
        try (IndexOutput out = remote.createOutput("segments_1", IOContext.DEFAULT)) {
            out.writeBytes(new byte[20], 20);
        }

        try (Directory local = FSDirectory.open(localDir.toPath())) {
            try (IndexOutput out = local.createOutput("segments_1", IOContext.DEFAULT)) {
                out.writeBytes(new byte[5], 5);
            }

            IndexSanityChecker checker = new IndexSanityChecker("/oak:index/sanity", local, localDir, remote);
            boolean allFine = checker.check(new IndexSanityChecker.IndexSanityStatistics());

            assertFalse(allFine, "size mismatch between local and remote should be flagged");
            assertEquals(0, local.listAll().length, "local should be fully purged on mismatch");
        }
    }

    @Test
    public void indexSanityChecker_removesStaleLocalFileNotPresentOnRemote() throws Exception {
        File localDir = Files.createTempDirectory(tempDir, "folder").toFile();
        NodeBuilder storageBuilder = INITIAL_CONTENT.builder();
        OakDirectory remote = new OakDirectory(storageBuilder, "sanityIndex2", false);
        try (IndexOutput out = remote.createOutput("onBoth", IOContext.DEFAULT)) {
            out.writeBytes(new byte[10], 10);
        }

        try (Directory local = FSDirectory.open(localDir.toPath())) {
            try (IndexOutput out = local.createOutput("onBoth", IOContext.DEFAULT)) {
                out.writeBytes(new byte[10], 10);
            }
            try (IndexOutput out = local.createOutput("onlyLocal", IOContext.DEFAULT)) {
                out.writeBytes(new byte[3], 3);
            }

            IndexSanityChecker checker = new IndexSanityChecker("/oak:index/sanity2", local, localDir, remote);
            boolean allFine = checker.check(new IndexSanityChecker.IndexSanityStatistics());

            assertTrue(allFine, "matching sizes on the shared file should not trigger a purge");
            List<String> remaining = Arrays.asList(local.listAll());
            assertTrue(remaining.contains("onBoth"), "file present on both sides with matching size should survive");
            assertFalse(remaining.contains("onlyLocal"), "file only present locally should be removed");
        }
    }

    /**
     * Follows LuceneNgIndexDefinitionTest's construction pattern: an INITIAL_CONTENT-backed
     * NodeBuilder with the luceneNg type property set, fed through
     * LuceneNgIndexDefinition.Builder (which exposes .uid(...) via the shared
     * IndexDefinition.Builder) so getUniqueId() returns the requested value.
     */
    private LuceneNgIndexDefinition testDefinition(String uniqueId) {
        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();
        builder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE_NG);
        NodeState defnState = builder.getNodeState();

        return new LuceneNgIndexDefinition.Builder()
                .root(root)
                .defn(defnState)
                .indexPath("/oak:index/test")
                .uid(uniqueId)
                .build();
    }
}
