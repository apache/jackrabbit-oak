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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.lucene.directory.CopyOnReadDirectory.DELETE_MARGIN_MILLIS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexCopierCleanupTest {
    private Random rnd = new Random();
    private static final int maxFileSize = 7896;

    private static final long SAFE_MARGIN_FOR_DELETION = TimeUnit.SECONDS.toMillis(5);
    private static final long MARGIN_BUFFER_FOR_FS_GRANULARITY = TimeUnit.SECONDS.toMillis(1);

    private NodeState root = INITIAL_CONTENT;

    private static final Clock CLOCK = new Clock.Virtual();
    static {
        try {
            CLOCK.waitUntil(Clock.SIMPLE.getTime());
        } catch (InterruptedException e) {
            // ignored
        }
    }

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private NodeBuilder builder = root.builder();

    private String indexPath = "/oak:index/test";

    private final Closer closer = Closer.create();

    private LuceneIndexDefinition defn = null;

    private CloseSafeRemoteRAMDirectory remote = null;

    private File localFSDir = null;

    private RAMIndexCopier copier = null;

    @Before
    public void setUp() throws IOException {
        System.setProperty(DELETE_MARGIN_MILLIS_NAME, String.valueOf(SAFE_MARGIN_FOR_DELETION));
        LuceneIndexEditorContext.configureUniqueId(builder);

        defn = new LuceneIndexDefinition(root, builder.getNodeState(), indexPath);
        remote = new CloseSafeRemoteRAMDirectory(closer);

        localFSDir = temporaryFolder.newFolder();

        copier = new RAMIndexCopier(localFSDir, sameThreadExecutor(), temporaryFolder.getRoot(), true);

        // convince copier that local FS dir is ok (avoid integrity check doing the cleanup)
        copier.getCoRDir().close();
    }

    @After
    public void tearDown() throws IOException {
        closer.close();
        System.clearProperty(DELETE_MARGIN_MILLIS_NAME);
    }

    @Test
    public void basicOperationSameNodeIndexing() throws Exception {
        Directory cow = copier.getCoWDir();
        writeFile(cow, "a");
        cow.close();

        Directory cor1 = copier.getCoRDir();

        cow = copier.getCoWDir();
        cow.deleteFile("a");
        writeFile(cow, "b");
        cow.close();

        Directory cor2 = copier.getCoRDir();
        cor1.close();

        //CoR1 saw "a" and everything else is newer. Nothing should get deleted
        assertTrue(existsLocally("a"));
        assertTrue(existsLocally("b"));

        cow = copier.getCoWDir();
        cow.deleteFile("b");
        writeFile(cow, "c");
        cow.close();

        Directory cor3 = copier.getCoRDir();
        cor2.close();
        assertFalse(existsLocally("a"));
        assertTrue(existsLocally("b"));
        assertTrue(existsLocally("c"));

        cor3.close();
        assertFalse(existsLocally("a"));
        assertFalse(existsLocally("b"));
        assertTrue(existsLocally("c"));
    }

    @Test
    public void basicOperationRemoteNodeIndexing() throws Exception {
        writeFile(remote, "a");
        remote.close();

        Directory cor1 = copier.getCoRDir();

        remote.deleteFile("a");
        writeFile(remote, "b");
        remote.close();

        Directory cor2 = copier.getCoRDir();
        cor1.close();

        //CoR1 saw "a" and everything else ("b" due to CoR2) is newer. Nothing should get deleted
        assertTrue(existsLocally("a"));
        assertTrue(existsLocally("b"));

        remote.deleteFile("b");
        writeFile(remote, "c");
        remote.close();

        Directory cor3 = copier.getCoRDir();
        cor2.close();
        assertFalse(existsLocally("a"));
        assertTrue(existsLocally("b"));
        assertTrue(existsLocally("c"));

        cor3.close();
        assertFalse(existsLocally("a"));
        assertFalse(existsLocally("b"));
        assertTrue(existsLocally("c"));
    }

    @Test
    public void oak7246Description() throws Exception {
        // Step 1
        Directory cow1 = copier.getCoWDir();
        writeFile(cow1, "a");
        writeFile(cow1, "b");
        cow1.close();

        Directory remoteSnapshowCow1 = remote.snapshot();

        // Step 2
        Directory cow2 = copier.getCoWDir();
        cow2.deleteFile("a");
        cow2.deleteFile("b");
        writeFile(cow2, "c");
        writeFile(cow2, "d");

        // Step 3
        Directory cor1 = copier.getCoRDir(remoteSnapshowCow1);
        // local listing
        assertEquals(Sets.newHashSet("a", "b", "c", "d"),
                Sets.newHashSet(new SimpleFSDirectory(localFSDir).listAll()));
        // reader listing
        assertEquals(Sets.newHashSet("a", "b"),
                Sets.newHashSet(cor1.listAll()));

        // Step 4
        cow2.close();

        Directory remoteSnapshotCow2 = remote.snapshot();

        // Step 5
        Directory cow3 = copier.getCoWDir();
        cow3.deleteFile("c");
        cow3.deleteFile("d");
        writeFile(cow3, "e");
        writeFile(cow3, "f");

        // Step 6
        Directory cor2 = copier.getCoRDir(remoteSnapshotCow2);
        // local listing
        assertEquals(Sets.newHashSet("a", "b", "c", "d", "e", "f"),
                Sets.newHashSet(new SimpleFSDirectory(localFSDir).listAll()));
        // reader listing
        assertEquals(Sets.newHashSet("c", "d"),
                Sets.newHashSet(cor2.listAll()));

        // Step 7
        cor1.close();

        // nothing should get deleted as CoR1 sees "a", "b" and everything else is newer
        assertEquals(Sets.newHashSet("a", "b", "c", "d", "e", "f"),
                Sets.newHashSet(new SimpleFSDirectory(localFSDir).listAll()));
    }

    @Test
    public void newlyWrittenFileMustNotBeDeletedDueToLateObservation() throws Exception {
        Directory cow1 = copier.getCoWDir();
        writeFile(cow1, "a");
        cow1.close();

        Directory snap1 = remote.snapshot();

        Directory cow2 = copier.getCoWDir();
        writeFile(cow2, "fileX");
        cow2.close();

        Directory cor1 = copier.getCoRDir(snap1);
        cor1.close();

        assertTrue(existsLocally("fileX"));
    }

    @Test
    public void newlyWrittenFileMustNotBeDeletedDueToLateClose() throws Exception {
        Directory cow1 = copier.getCoWDir();
        writeFile(cow1, "a");
        cow1.close();

        Directory cor1 = copier.getCoRDir();

        Directory cow2 = copier.getCoWDir();
        writeFile(cow2, "fileX");
        cow2.close();

        cor1.close();

        assertTrue(existsLocally("fileX"));
    }

    @Test
    public void failedWritesGetCleanedUp() throws Exception {
        CloseSafeRemoteRAMDirectory oldRemote = remote.snapshot();

        Directory failedWriter = copier.getCoWDir();
        writeFile(failedWriter, "a");
        failedWriter.close();
        // actually, everything would've worked for 'failedWriter', but we restore 'remote' to old state
        // to fake failed remote update
        remote = oldRemote;

        assertTrue(existsLocally("a"));

        // Create some files that get sent to remote
        Directory cow = copier.getCoWDir();
        writeFile(cow, "b");
        cow.close();

        // reader that would invoke cleanup according to its view on close
        Directory cor = copier.getCoRDir();
        cor.close();

        assertFalse(existsLocally("a"));
    }

    @Test
    public void strayFilesGetRemoved() throws Exception {
        DelayCopyingSimpleFSDirectory strayDir = new DelayCopyingSimpleFSDirectory(localFSDir);

        writeFile(strayDir, "oldestStray");

        // add "a" directly to remote
        writeFile(remote, "a");

        copier.getCoRDir().close();

        // "a" is added to remote and hence local FS gets when CoR is opened
        assertFalse(existsLocally("oldestStray"));

        // "b" gets created locally by CoW
        Directory cow = copier.getCoWDir();
        writeFile(cow, "b");

        writeFile(strayDir, "oldStray");

        copier.getCoRDir().close();

        // "oldStray" is newer than "b"
        // hence, doesn't get removed yet
        assertTrue(existsLocally("oldStray"));

        // "c" gets created locally
        cow = copier.getCoWDir();
        writeFile(cow, "c");

        // "newStray" is newer than "c"
        writeFile(strayDir, "newStray");

        copier.getCoRDir().close();

        assertFalse(existsLocally("oldStray"));
        assertTrue(existsLocally("newStray"));
    }

    @Test
    public void marginIsRespected() throws Exception {
        writeFile(remote, "a");

        FileUtils.write(new File(localFSDir, "beyond-margin"), "beyond-margin-data", (Charset) null);
        DelayCopyingSimpleFSDirectory.updateLastModified(localFSDir, "beyond-margin");
        // Delay 1 more second to avoid FS time granularity
        CLOCK.waitUntil(CLOCK.getTime() + SAFE_MARGIN_FOR_DELETION + MARGIN_BUFFER_FOR_FS_GRANULARITY);

        FileUtils.write(new File(localFSDir, "within-margin"), "within-margin-data", (Charset) null);
        DelayCopyingSimpleFSDirectory.updateLastModified(localFSDir, "within-margin");

        copier.getCoRDir().close();

        assertEquals(Sets.newHashSet("within-margin", "a"),
                Sets.newHashSet(new SimpleFSDirectory(localFSDir).listAll()));
    }

    @Test
    public void remoteOnlyFilesDontAvoidDeletion() throws Exception {
        writeFile(remote, "a");
        for (String name : IndexCopier.REMOTE_ONLY) {
            writeFile(remote, name);
        }
        remote.close();

        // get files to get locally copied
        copier.getCoRDir().close();

        remote.deleteFile("a");
        writeFile(remote, "b");
        remote.close();

        assertTrue(existsLocally("a"));

        copier.getCoRDir().close();

        assertFalse(existsLocally("a"));
        assertTrue(existsLocally("b"));
    }

    @Test
    public void remoteOnlyFilesIfExistingGetDeleted() throws Exception {
        Directory cow = copier.getCoWDir();
        writeFile(cow, "a");
        for (String name : IndexCopier.REMOTE_ONLY) {
            writeFile(cow, name);
        }
        cow.close();

        remote.deleteFile("a");
        writeFile(remote, "b");
        remote.close();

        assertTrue(existsLocally("a"));
        for (String name : IndexCopier.REMOTE_ONLY) {
            assertTrue(existsLocally(name));
        }

        copier.getCoRDir().close();

        assertFalse(existsLocally("a"));
        for (String name : IndexCopier.REMOTE_ONLY) {
            assertFalse(existsLocally(name));
        }
        assertTrue(existsLocally("b"));
    }

    @Test
    public void remoteOnlyFilesNotCleanedIfUpdatedRecently() throws Exception {
        // Create remote_only files (and a normal one)
        Directory cow = copier.getCoWDir();
        writeFile(cow, "a");
        for (String name : IndexCopier.REMOTE_ONLY) {
            writeFile(cow, name);
        }
        cow.close();

        // delete all existing files and create a new normal one
        remote.deleteFile("a");
        for (String name : IndexCopier.REMOTE_ONLY) {
            remote.deleteFile(name);
        }
        writeFile(remote, "b");
        remote.close();

        // get a CoR at this state (only sees "b" in list of files)
        Directory cor = copier.getCoRDir();

        // re-create remote_only files
        cow = copier.getCoWDir();
        for (String name : IndexCopier.REMOTE_ONLY) {
            writeFile(cow, name);
        }
        cow.close();

        assertTrue(existsLocally("a"));
        for (String name : IndexCopier.REMOTE_ONLY) {
            assertTrue(existsLocally(name));
        }

        cor.close();

        assertFalse(existsLocally("a"));
        for (String name : IndexCopier.REMOTE_ONLY) {
            assertTrue(existsLocally(name));
        }
        assertTrue(existsLocally("b"));
    }

    private boolean existsLocally(String fileName) {
        return new File(localFSDir, fileName).exists();
    }

    private void writeFile(Directory dir, String name) throws IOException {
        byte[] data = new byte[(rnd.nextInt(maxFileSize) + 1)];
        rnd.nextBytes(data);

        IndexOutput o = dir.createOutput(name, IOContext.DEFAULT);
        o.writeBytes(data, data.length);
        o.close();

        DelayCopyingSimpleFSDirectory.updateLastModified(dir, name);
    }

    private class RAMIndexCopier extends IndexCopier {
        final File baseFSDir;

        RAMIndexCopier(File baseFSDir, Executor executor, File indexRootDir,
                       boolean prefetchEnabled) throws IOException {
            super(executor, indexRootDir, prefetchEnabled);
            this.baseFSDir = baseFSDir;
        }

        @Override
        protected Directory createLocalDirForIndexReader(String indexPath, LuceneIndexDefinition definition, String dirName) throws IOException {
            return new DelayCopyingSimpleFSDirectory(baseFSDir);
        }

        @Override
        protected Directory createLocalDirForIndexWriter(LuceneIndexDefinition definition, String dirName,
                                                         boolean reindexMode,
                                                         COWDirectoryTracker cowDirectoryTracker) throws IOException {
            return new DelayCopyingSimpleFSDirectory(baseFSDir);
        }

        Directory getCoRDir() throws IOException {
            return getCoRDir(remote.snapshot());
        }

        Directory getCoRDir(Directory remoteSnapshot) throws IOException {
            return wrapForRead(indexPath, defn, remoteSnapshot, INDEX_DATA_CHILD_NAME);
        }

        Directory getCoWDir() throws IOException {
            return wrapForWrite(defn, remote, false, INDEX_DATA_CHILD_NAME, COWDirectoryTracker.NOOP);
        }
    }

    private static class DelayCopyingSimpleFSDirectory extends SimpleFSDirectory {
        DelayCopyingSimpleFSDirectory(File dir) throws IOException {
            super(dir);
        }

        static void updateLastModified(Directory dir, String name) throws IOException {
            DelayCopyingSimpleFSDirectory d = null;
            if (dir instanceof DelayCopyingSimpleFSDirectory) {
                d = (DelayCopyingSimpleFSDirectory)dir;
            } else if (dir instanceof FilterDirectory) {
                Directory delegate = ((FilterDirectory)dir).getDelegate();
                if (delegate instanceof DelayCopyingSimpleFSDirectory) {
                    d = (DelayCopyingSimpleFSDirectory)delegate;
                }
            }

            if (d != null) {
                d.updateLastModified(name);
            }
        }

        void updateLastModified(String name) throws IOException {
            try {
                updateLastModified(directory, name);

                CLOCK.waitUntil(CLOCK.getTime() + SAFE_MARGIN_FOR_DELETION + MARGIN_BUFFER_FOR_FS_GRANULARITY);
            } catch (InterruptedException ie) {
                // ignored
            }
        }

        static void updateLastModified(File fsDirectory, String name) throws IOException {
            // Update file timestamp manually to mimic last updated time updates without sleeping
            File f = new File(fsDirectory, name);
            if (!f.setLastModified(CLOCK.getTime())) {
                throw new IOException("Failed to update last modified for " + name);
            }
        }
    }

    private static class CloseSafeRemoteRAMDirectory extends RAMDirectory {
        private final Closer closer;

        CloseSafeRemoteRAMDirectory(Closer closer) {
            super();
            this.closer = closer;
            closer.register(this::close0);
        }

        CloseSafeRemoteRAMDirectory(CloseSafeRemoteRAMDirectory that) throws IOException {
            super(that, IOContext.READ);
            this.closer = that.closer;
            closer.register(this::close0);
        }

        @Override
        public void close() {
        }

        @Override
        public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
            super.copy(to, src, dest, context);

            if (to instanceof DelayCopyingSimpleFSDirectory) {
                ((DelayCopyingSimpleFSDirectory)to).updateLastModified(dest);
            }
        }

        CloseSafeRemoteRAMDirectory snapshot() throws IOException {
            return new CloseSafeRemoteRAMDirectory(this);
        }

        private void close0() {
            super.close();
        }
    }
}
