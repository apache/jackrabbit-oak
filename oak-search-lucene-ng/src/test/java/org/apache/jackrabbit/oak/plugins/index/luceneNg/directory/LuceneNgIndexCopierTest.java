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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LuceneNgIndexCopierTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
            out.writeString("hello-lucene9");
        }

        LuceneNgIndexCopier copier = new LuceneNgIndexCopier(sameThreadExecutor, temporaryFolder.newFolder(), false);
        LuceneNgIndexDefinition definition = testDefinition("uid-1");

        try (Directory wrapped = copier.wrapForRead("/oak:index/test", definition, remote, "lucene9")) {
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

        assertTrue("expected at least one file copied locally", copier.getDownloadCount() >= 1);
        copier.close();
    }

    @Test
    public void existsLocally_and_localFileLength_reflectFilePresenceAndAbsence() throws Exception {
        File dir = temporaryFolder.newFolder();
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
        File dir = temporaryFolder.newFolder();
        LuceneNgIndexCopier copier = new LuceneNgIndexCopier(sameThreadExecutor, temporaryFolder.newFolder(), false);
        try (Directory local = FSDirectory.open(dir.toPath())) {
            try (IndexOutput out = local.createOutput("segments_1", IOContext.DEFAULT)) {
                out.writeString("content");
            }
            assertTrue(LuceneNgIndexCopier.existsLocally(dir, "segments_1"));

            boolean deleted = copier.deleteFile(local, dir, "segments_1", true);

            assertTrue("deleteFile should report success", deleted);
            assertFalse("file should be removed from disk", LuceneNgIndexCopier.existsLocally(dir, "segments_1"));
        } finally {
            copier.close();
        }
    }

    @Test
    public void deleteFile_whenFileAlreadyGone_stillReportsSuccessWithoutThrowing() throws Exception {
        File dir = temporaryFolder.newFolder();
        LuceneNgIndexCopier copier = new LuceneNgIndexCopier(sameThreadExecutor, temporaryFolder.newFolder(), false);
        try (Directory local = FSDirectory.open(dir.toPath())) {
            boolean deleted = copier.deleteFile(local, dir, "never-existed", true);
            assertTrue("deleteFile has nothing to do but should still report success", deleted);
        } finally {
            copier.close();
        }
    }

    @Test
    public void indexSanityChecker_purgesLocalOnSizeMismatch() throws Exception {
        File localDir = temporaryFolder.newFolder();
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

            assertFalse("size mismatch between local and remote should be flagged", allFine);
            assertEquals("local should be fully purged on mismatch", 0, local.listAll().length);
        }
    }

    @Test
    public void indexSanityChecker_removesStaleLocalFileNotPresentOnRemote() throws Exception {
        File localDir = temporaryFolder.newFolder();
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

            assertTrue("matching sizes on the shared file should not trigger a purge", allFine);
            List<String> remaining = Arrays.asList(local.listAll());
            assertTrue("file present on both sides with matching size should survive", remaining.contains("onBoth"));
            assertFalse("file only present locally should be removed", remaining.contains("onlyLocal"));
        }
    }

    /**
     * Follows LuceneNgIndexDefinitionTest's construction pattern: an INITIAL_CONTENT-backed
     * NodeBuilder with the lucene9 type property set, fed through
     * LuceneNgIndexDefinition.Builder (which exposes .uid(...) via the shared
     * IndexDefinition.Builder) so getUniqueId() returns the requested value.
     */
    private LuceneNgIndexDefinition testDefinition(String uniqueId) {
        NodeState root = INITIAL_CONTENT;
        NodeBuilder builder = root.builder();
        builder.setProperty("type", LuceneNgIndexConstants.TYPE_LUCENE9);
        NodeState defnState = builder.getNodeState();

        return new LuceneNgIndexDefinition.Builder()
                .root(root)
                .defn(defnState)
                .indexPath("/oak:index/test")
                .uid(uniqueId)
                .build();
    }
}
