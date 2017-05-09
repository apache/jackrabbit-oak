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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.openmbean.TabularData;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexFile;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class IndexCopierTest {
    private Random rnd = new Random();
    private int maxFileSize = 7896;

    private NodeState root = INITIAL_CONTENT;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private NodeBuilder builder = root.builder();

    private String indexPath = "/oak:index/test";

    @Before
    public void setUp(){
        LuceneIndexEditorContext.configureUniqueId(builder);
    }

    @Test
    public void basicTest() throws Exception{
        Directory baseDir = new RAMDirectory();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        Directory remote = new RAMDirectory();
        Directory wrapped = c1.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);

        byte[] t1 = writeFile(remote , "t1");
        byte[] t2 = writeFile(remote , "t2");

        assertEquals(2, wrapped.listAll().length);

        assertTrue(wrapped.fileExists("t1"));
        assertTrue(wrapped.fileExists("t2"));

        assertEquals(t1.length, wrapped.fileLength("t1"));
        assertEquals(t2.length, wrapped.fileLength("t2"));

        readAndAssert(wrapped, "t1", t1);

        //t1 should now be added to testDir
        assertTrue(baseDir.fileExists("t1"));
    }

    @Test
    public void basicTestWithPrefetch() throws Exception{
        final List<String> syncedFiles = Lists.newArrayList();
        Directory baseDir = new RAMDirectory(){
            @Override
            public void sync(Collection<String> names) throws IOException {
                syncedFiles.addAll(names);
                super.sync(names);
            }
        };
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir(), true);

        Directory remote = new RAMDirectory();

        byte[] t1 = writeFile(remote, "t1");
        byte[] t2 = writeFile(remote , "t2");

        Directory wrapped = c1.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);
        assertEquals(2, wrapped.listAll().length);
        assertThat(syncedFiles, containsInAnyOrder("t1", "t2"));

        assertTrue(wrapped.fileExists("t1"));
        assertTrue(wrapped.fileExists("t2"));

        assertTrue(baseDir.fileExists("t1"));
        assertTrue(baseDir.fileExists("t2"));

        assertEquals(t1.length, wrapped.fileLength("t1"));
        assertEquals(t2.length, wrapped.fileLength("t2"));

        readAndAssert(wrapped, "t1", t1);

    }

    @Test
    public void nonExistentFile() throws Exception{
        Directory baseDir = new RAMDirectory();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        CollectingExecutor executor = new CollectingExecutor();
        IndexCopier c1 = new RAMIndexCopier(baseDir, executor, getWorkDir(), true);

        Directory remote = new RAMDirectory();
        Directory wrapped = c1.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);

        try {
            wrapped.openInput("foo.txt", IOContext.DEFAULT);
            fail();
        } catch(FileNotFoundException ignore){

        }

        assertEquals(0, executor.commands.size());
    }

    @Test
    public void basicTestWithFS() throws Exception{
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier c1 = new IndexCopier(sameThreadExecutor(), getWorkDir());

        Directory remote = new RAMDirectory();
        Directory wrapped = c1.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);

        byte[] t1 = writeFile(remote, "t1");
        byte[] t2 = writeFile(remote , "t2");

        assertEquals(2, wrapped.listAll().length);

        assertTrue(wrapped.fileExists("t1"));
        assertTrue(wrapped.fileExists("t2"));

        assertEquals(t1.length, wrapped.fileLength("t1"));
        assertEquals(t2.length, wrapped.fileLength("t2"));

        readAndAssert(wrapped, "t1", t1);

        //t1 should now be added to testDir
        File indexDir = c1.getIndexDir(defn, "/foo", INDEX_DATA_CHILD_NAME);
        assertTrue(new File(indexDir, "t1").exists());

        TabularData td = c1.getIndexPathMapping();
        assertEquals(1, td.size());
    }


    @Test
    public void multiDirNames() throws Exception{
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier c1 = new IndexCopier(sameThreadExecutor(), getWorkDir());

        Directory remote = new CloseSafeDir();
        byte[] t1 = writeFile(remote, "t1");
        byte[] t2 = writeFile(remote , "t2");

        Directory w1 = c1.wrapForRead(indexPath, defn, remote, ":data");

        readAndAssert(w1, "t1", t1);

        Directory w2 = c1.wrapForRead(indexPath, defn, remote, ":private-data");
        w2.close();

        readAndAssert(w1, "t1", t1);
    }

    @Test
    public void deleteOldPostReindex() throws Exception{
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier c1 = new IndexCopier(sameThreadExecutor(), getWorkDir());

        Directory remote = new CloseSafeDir();
        Directory w1 = c1.wrapForRead(indexPath, defn, remote, INDEX_DATA_CHILD_NAME);

        byte[] t1 = writeFile(remote, "t1");
        byte[] t2 = writeFile(remote , "t2");

        readAndAssert(w1, "t1", t1);
        readAndAssert(w1, "t2", t2);

        //t1 should now be added to testDir
        File indexDir = c1.getIndexDir(defn, indexPath, INDEX_DATA_CHILD_NAME);
        assertTrue(new File(indexDir, "t1").exists());

        doReindex(builder);
        defn = new IndexDefinition(root, builder.getNodeState(), "/foo");

        //Close old version
        w1.close();
        //Get a new one with updated reindexCount
        Directory w2 = c1.wrapForRead(indexPath, defn, remote, INDEX_DATA_CHILD_NAME);

        readAndAssert(w2, "t1", t1);

        w2.close();
        assertFalse("Old index directory should have been removed", indexDir.exists());

        //Assert that new index file do exist and not get removed
        File indexDir2 = c1.getIndexDir(defn, indexPath, INDEX_DATA_CHILD_NAME);
        assertTrue(new File(indexDir2, "t1").exists());

        //Check if parent directory is also removed i.e.
        //index count should be 1 now
        assertEquals(1, c1.getIndexRootDirectory().getLocalIndexes(indexPath).size());
    }

    @Test
    public void concurrentRead() throws Exception{
        Directory baseDir = new RAMDirectory();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        CollectingExecutor executor = new CollectingExecutor();

        IndexCopier c1 = new RAMIndexCopier(baseDir, executor, getWorkDir());

        TestRAMDirectory remote = new TestRAMDirectory();
        Directory wrapped = c1.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);

        byte[] t1 = writeFile(remote , "t1");

        //1. Trigger a read which should go to remote
        readAndAssert(wrapped, "t1", t1);
        assertEquals(1, c1.getScheduledForCopyCount());
        assertEquals(1, remote.openedFiles.size());
        assertEquals(1, executor.commands.size());

        //2. Trigger another read and this should also be
        //served from remote
        readAndAssert(wrapped, "t1", t1);
        assertEquals(1, c1.getScheduledForCopyCount());
        assertEquals(2, remote.openedFiles.size());
        //Second read should not add a new copy task
        assertEquals(1, executor.commands.size());

        //3. Perform copy
        executor.executeAll();
        remote.reset();

        //4. Now read again after copy is done
        readAndAssert(wrapped, "t1", t1);
        // Now read should be served from local and not from remote
        assertEquals(0, remote.openedFiles.size());
        assertEquals(0, c1.getScheduledForCopyCount());
    }

    @Test
    public void copyInProgressStats() throws Exception{
        Directory baseDir = new RAMDirectory();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");

        final List<ListenableFuture<?>> submittedTasks = Lists.newArrayList();
        ExecutorService executor = new ForwardingListeningExecutorService() {
            @Override
            protected ListeningExecutorService delegate() {
                return MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
            }

            @Override
            public void execute(Runnable command) {
                submittedTasks.add(super.submit(command));
            }
        };

        IndexCopier c1 = new RAMIndexCopier(baseDir, executor, getWorkDir());

        final CountDownLatch copyProceed = new CountDownLatch(1);
        final CountDownLatch copyRequestArrived = new CountDownLatch(1);
        TestRAMDirectory remote = new TestRAMDirectory(){
            @Override
            public void copy(Directory to, String src, String dest, IOContext context) throws IOException {
                copyRequestArrived.countDown();
                try {
                    copyProceed.await();
                } catch (InterruptedException e) {

                }
                super.copy(to, src, dest, context);
            }
        };
        Directory wrapped = c1.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);

        byte[] t1 = writeFile(remote , "t1");

        //1. Trigger a read which should go to remote
        readAndAssert(wrapped, "t1", t1);
        copyRequestArrived.await();
        assertEquals(1, c1.getCopyInProgressCount());
        assertEquals(1, remote.openedFiles.size());

        //2. Trigger another read and this should also be
        //served from remote
        readAndAssert(wrapped, "t1", t1);
        assertEquals(1, c1.getCopyInProgressCount());
        assertEquals(IOUtils.humanReadableByteCount(t1.length), c1.getCopyInProgressSize());
        assertEquals(1, c1.getCopyInProgressDetails().length);
        System.out.println(Arrays.toString(c1.getCopyInProgressDetails()));
        assertEquals(2, remote.openedFiles.size());

        //3. Perform copy
        copyProceed.countDown();
        Futures.allAsList(submittedTasks).get();
        remote.reset();

        //4. Now read again after copy is done
        readAndAssert(wrapped, "t1", t1);
        // Now read should be served from local and not from remote
        assertEquals(0, remote.openedFiles.size());
        assertEquals(0, c1.getCopyInProgressCount());

        executor.shutdown();
    }

    /**
     * Test for the case where local directory is opened already contains
     * the index files and in such a case file should not be read from remote
     */
    @Test
    public void reuseLocalDir() throws Exception{
        Directory baseDir = new RAMDirectory();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        TestRAMDirectory remote = new TestRAMDirectory();
        Directory wrapped = c1.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);

        byte[] t1 = writeFile(remote, "t1");

        //1. Read for the first time should be served from remote
        readAndAssert(wrapped, "t1", t1);
        assertEquals(1, remote.openedFiles.size());

        //2. Reuse the testDir and read again
        Directory wrapped2 = c1.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);
        remote.reset();

        //3. Now read should be served from local
        readAndAssert(wrapped2, "t1", t1);
        assertEquals(0, remote.openedFiles.size());

        //Now check if local file gets corrupted then read from remote
        Directory wrapped3 = c1.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);
        remote.reset();

        //4. Corrupt the local copy
        writeFile(baseDir, "t1");

        //Now read would be done from remote
        readAndAssert(wrapped3, "t1", t1);
        assertEquals(1, remote.openedFiles.size());
    }

    @Test
    public void deleteCorruptedFile() throws Exception{
        Directory baseDir = new RAMDirectory();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        RAMIndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        Directory remote = new RAMDirectory(){
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                throw new IllegalStateException("boom");
            }
        };

        String fileName = "failed.txt";
        Directory wrapped = c1.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);

        byte[] t1 = writeFile(remote , fileName);

        try {
            readAndAssert(wrapped, fileName, t1);
            fail("Read of file should have failed");
        } catch (IllegalStateException ignore){

        }

        assertFalse(c1.baseDir.fileExists(fileName));
    }

    @Test
    public void deletesOnClose() throws Exception{
        //Use a close safe dir. In actual case the FSDir would
        //be opened on same file system hence it can retain memory
        //but RAMDirectory does not retain memory hence we simulate
        //that by not closing the RAMDir and reuse it
        Directory baseDir = new CloseSafeDir();


        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        Directory r1 = new RAMDirectory();

        byte[] t1 = writeFile(r1, "t1");
        byte[] t2 = writeFile(r1 , "t2");

        Directory w1 = c1.wrapForRead("/foo", defn, r1, INDEX_DATA_CHILD_NAME);
        readAndAssert(w1, "t1", t1);
        readAndAssert(w1, "t2", t2);

        // t1 and t2 should now be present in local (base dir which back local)
        assertTrue(baseDir.fileExists("t1"));
        assertTrue(baseDir.fileExists("t2"));

        Directory r2 = new RAMDirectory();
        copy(r1, r2);
        r2.deleteFile("t1");

        Directory w2 = c1.wrapForRead("/foo", defn, r2, INDEX_DATA_CHILD_NAME);

        //Close would trigger removal of file which are not present in remote
        w2.close();

        assertFalse("t1 should have been deleted", baseDir.fileExists("t1"));
        assertTrue(baseDir.fileExists("t2"));
    }


    @Test
    public void failureInDelete() throws Exception{
        final Set<String> testFiles = new HashSet<String>();
        Directory baseDir = new CloseSafeDir() {
            @Override
            public void deleteFile(String name) throws IOException {
                if (testFiles.contains(name)){
                    throw new IOException("Not allowed to delete " + name);
                }
                super.deleteFile(name);
            }
        };

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        Directory r1 = new RAMDirectory();

        byte[] t1 = writeFile(r1, "t1");
        byte[] t2 = writeFile(r1 , "t2");

        Directory w1 = c1.wrapForRead("/foo", defn, r1, INDEX_DATA_CHILD_NAME);
        readAndAssert(w1, "t1", t1);
        readAndAssert(w1, "t2", t2);

        // t1 and t2 should now be present in local (base dir which back local)
        assertTrue(baseDir.fileExists("t1"));
        assertTrue(baseDir.fileExists("t2"));

        Directory r2 = new CloseSafeDir();
        copy(r1, r2);
        r2.deleteFile("t1");

        Directory w2 = c1.wrapForRead("/foo", defn, r2, INDEX_DATA_CHILD_NAME);

        //Close would trigger removal of file which are not present in remote
        testFiles.add("t1");
        w2.close();

        assertEquals(1, c1.getFailedToDeleteFiles().size());
        LocalIndexFile testFile = c1.getFailedToDeleteFiles().values().iterator().next();

        assertEquals(1, testFile.getDeleteAttemptCount());
        assertEquals(IOUtils.humanReadableByteCount(t1.length), c1.getGarbageSize());
        assertEquals(1, c1.getGarbageDetails().length);

        Directory w3 = c1.wrapForRead("/foo", defn, r2, INDEX_DATA_CHILD_NAME);
        w3.close();
        assertEquals(2, testFile.getDeleteAttemptCount());

        //Now let the file to be deleted
        testFiles.clear();

        Directory w4 = c1.wrapForRead("/foo", defn, r2, INDEX_DATA_CHILD_NAME);
        w4.close();

        //No pending deletes left
        assertEquals(0, c1.getFailedToDeleteFiles().size());
    }

    @Test
    public void deletedOnlyFilesForOlderVersion() throws Exception{
        Directory baseDir = new CloseSafeDir();

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier copier = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        //1. Open a local and read t1 from remote
        Directory remote1 = new RAMDirectory();
        byte[] t1 = writeFile(remote1, "t1");

        Directory local1 = copier.wrapForRead("/foo", defn, remote1, INDEX_DATA_CHILD_NAME);
        readAndAssert(local1, "t1", t1);

        //While local1 is open , open another local2 and read t2
        Directory remote2 = new RAMDirectory();
        byte[] t2 = writeFile(remote2, "t2");

        Directory local2 = copier.wrapForRead("/foo", defn, remote2, INDEX_DATA_CHILD_NAME);
        readAndAssert(local2, "t2", t2);

        //Close local1
        local1.close();

        //t2 should still be readable
        readAndAssert(local2, "t2", t2);
    }

    @Test
    public void wrapForWriteWithoutIndexPath() throws Exception{
        Directory remote = new CloseSafeDir();

        IndexCopier copier = new IndexCopier(sameThreadExecutor(), getWorkDir());

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        Directory dir = copier.wrapForWrite(defn, remote, false, INDEX_DATA_CHILD_NAME);

        byte[] t1 = writeFile(dir, "t1");

        dir.close();

        readAndAssert(remote, "t1", t1);
        //Work dir must be empty post close
        assertArrayEquals(FileUtils.EMPTY_FILE_ARRAY, copier.getIndexWorkDir().listFiles());
    }

    @Test
    public void wrapForWriteWithIndexPath() throws Exception{
        Directory remote = new CloseSafeDir();

        IndexCopier copier = new IndexCopier(sameThreadExecutor(), getWorkDir());

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        Directory dir = copier.wrapForWrite(defn, remote, false, INDEX_DATA_CHILD_NAME);

        byte[] t1 = writeFile(dir, "t1");

        dir.close();

        readAndAssert(remote, "t1", t1);
        //Work dir must be empty post close
        File indexDir = copier.getIndexDir(defn, "foo", INDEX_DATA_CHILD_NAME);
        List<File> files = new ArrayList<File>(FileUtils.listFiles(indexDir, null, true));
        Set<String> fileNames = Sets.newHashSet();
        for (File f : files){
            fileNames.add(f.getName());
        }
        assertThat(fileNames, contains("t1"));
    }

    @Test
    public void copyOnWriteBasics() throws Exception{
        Directory baseDir = new CloseSafeDir();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier copier = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        Directory remote = new RAMDirectory();
        byte[] t1 = writeFile(remote, "t1");

        //State of remote directory should set before wrapping as later
        //additions would not be picked up given COW assume remote directory
        //to be read only
        Directory local = copier.wrapForWrite(defn, remote, false, INDEX_DATA_CHILD_NAME);

        assertEquals(newHashSet("t1"), newHashSet(local.listAll()));
        assertEquals(t1.length, local.fileLength("t1"));

        byte[] t2 = writeFile(local, "t2");
        assertEquals(newHashSet("t1", "t2"), newHashSet(local.listAll()));
        assertEquals(t2.length, local.fileLength("t2"));

        assertTrue(local.fileExists("t1"));
        assertTrue(local.fileExists("t2"));

        assertTrue("t2 should be copied to remote", remote.fileExists("t2"));

        readAndAssert(local, "t1", t1);
        readAndAssert(local, "t2", t2);

        local.deleteFile("t1");
        assertEquals(newHashSet("t2"), newHashSet(local.listAll()));

        local.deleteFile("t2");
        assertEquals(newHashSet(), newHashSet(local.listAll()));


        try {
            local.fileLength("nonExistentFile");
            fail();
        } catch (FileNotFoundException ignore) {

        }

        try {
            local.openInput("nonExistentFile", IOContext.DEFAULT);
            fail();
        } catch (FileNotFoundException ignore) {

        }

        local.close();
        assertFalse(baseDir.fileExists("t2"));
    }

    /**
     * Checks for the case where if the file exist local before writer starts
     * then those files do not get deleted even if deleted by writer via
     * indexing process from 'baseDir' as they might be in use by existing open
     * indexes
     */
    @Test
    public void cowExistingLocalFileNotDeleted() throws Exception{
        Directory baseDir = new CloseSafeDir();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier copier = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        Directory remote = new CloseSafeDir();
        byte[] t1 = writeFile(remote, "t1");
        byte[] t2 = writeFile(remote, "t2");
        Directory local = copier.wrapForWrite(defn, remote, false, INDEX_DATA_CHILD_NAME);
        assertEquals(newHashSet("t1", "t2"), newHashSet(local.listAll()));

        byte[] t3 = writeFile(local, "t3");

        //Now pull in the file t1 via CopyOnRead in baseDir
        Directory localForRead = copier.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);
        readAndAssert(localForRead, "t1", t1);

        //File which was copied from remote should not be deleted from baseDir
        //upon delete from local
        assertTrue(baseDir.fileExists("t1"));
        local.deleteFile("t1");
        assertFalse("t1 should be deleted from remote", remote.fileExists("t1"));
        assertFalse("t1 should be deleted from 'local' view also", local.fileExists("t1"));
        assertTrue("t1 should not be deleted from baseDir", baseDir.fileExists("t1"));

        //File which was created only via local SHOULD get removed from
        //baseDir only upon close
        assertTrue(baseDir.fileExists("t3"));
        local.deleteFile("t3");
        assertFalse("t1 should be deleted from remote", local.fileExists("t3"));
        assertTrue("t1 should NOT be deleted from remote", baseDir.fileExists("t3"));

        local.close();
        assertFalse("t3 should also be deleted from local", baseDir.fileExists("t3"));
    }

    @Test
    public void cowReadDoneFromLocalIfFileExist() throws Exception{
        final Set<String> readLocal = newHashSet();
        Directory baseDir = new CloseSafeDir(){
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                readLocal.add(name);
                return super.openInput(name, context);
            }
        };
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier copier = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        final Set<String> readRemotes = newHashSet();
        Directory remote = new RAMDirectory() {
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                readRemotes.add(name);
                return super.openInput(name, context);
            }
        };
        byte[] t1 = writeFile(remote, "t1");
        Directory local = copier.wrapForWrite(defn, remote, false, INDEX_DATA_CHILD_NAME);

        //Read should be served from remote
        readRemotes.clear();readLocal.clear();
        readAndAssert(local, "t1", t1);
        assertEquals(newHashSet("t1"), readRemotes);
        assertEquals(newHashSet(), readLocal);

        //Now pull in the file t1 via CopyOnRead in baseDir
        Directory localForRead = copier.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);
        readAndAssert(localForRead, "t1", t1);

        //Read should be served from local
        readRemotes.clear();readLocal.clear();
        readAndAssert(local, "t1", t1);
        assertEquals(newHashSet(), readRemotes);
        assertEquals(newHashSet("t1"), readLocal);

        local.close();
    }

    @Test
    public void cowCopyDoneOnClose() throws Exception{
        final CollectingExecutor executor = new CollectingExecutor();
        Directory baseDir = new CloseSafeDir();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier copier = new RAMIndexCopier(baseDir, executor, getWorkDir());

        Directory remote = new CloseSafeDir();

        final Directory local = copier.wrapForWrite(defn, remote, false, INDEX_DATA_CHILD_NAME);
        byte[] t1 = writeFile(local, "t1");

        assertTrue(local.fileExists("t1"));
        assertFalse("t1 should NOT be copied to remote", remote.fileExists("t1"));

        //Execute all job
        executor.executeAll();

        assertTrue("t1 should now be copied to remote", remote.fileExists("t1"));

        byte[] t2 = writeFile(local, "t2");
        assertFalse("t2 should NOT be copied to remote", remote.fileExists("t2"));

        final ExecutorService executorService = Executors.newFixedThreadPool(4);
        final CountDownLatch copyLatch = new CountDownLatch(1);
        Future<?> copyTasks = executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                copyLatch.await();
                //the executor to a proper one as it might happen that
                //STOP task is added post CountingExecutor has executed. Then there
                //would be none to process the STOP. Having a proper executor would
                //handle that case
                executor.setForwardingExecutor(executorService);
                executor.executeAll();
                return null;
            }
        });

        final CountDownLatch closeLatch = new CountDownLatch(1);
        Future<?> closeTasks = executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                closeLatch.await();
                local.close();
                return null;
            }
        });

        closeLatch.countDown();
        assertFalse("t2 should NOT be copied to remote", remote.fileExists("t2"));

        //Let copy to proceed
        copyLatch.countDown();

        //Now wait for close to finish
        closeTasks.get();
        assertTrue("t2 should now be copied to remote", remote.fileExists("t2"));

        executorService.shutdown();
    }

    @Test
    public void cowCopyDoneOnCloseExceptionHandling() throws Exception{
        final CollectingExecutor executor = new CollectingExecutor();
        Directory baseDir = new CloseSafeDir();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier copier = new RAMIndexCopier(baseDir, executor, getWorkDir());

        Directory remote = new CloseSafeDir();

        final Directory local = copier.wrapForWrite(defn, remote, false, INDEX_DATA_CHILD_NAME);
        byte[] t1 = writeFile(local, "t1");

        assertTrue(local.fileExists("t1"));
        assertFalse("t1 should NOT be copied to remote", remote.fileExists("t1"));

        //Execute all job
        executor.executeAll();

        assertTrue("t1 should now be copied to remote", remote.fileExists("t1"));

        byte[] t2 = writeFile(local, "t2");
        assertFalse("t2 should NOT be copied to remote", remote.fileExists("t2"));

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        final CountDownLatch copyLatch = new CountDownLatch(1);
        Future<?> copyTasks = executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                copyLatch.await();
                executor.executeAll();
                executor.enableImmediateExecution();
                return null;
            }
        });

        final CountDownLatch closeLatch = new CountDownLatch(1);
        Future<?> closeTasks = executorService.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                closeLatch.await();
                local.close();
                return null;
            }
        });

        closeLatch.countDown();
        assertFalse("t2 should NOT be copied to remote", remote.fileExists("t2"));

        //Let copy to proceed
        copyLatch.countDown();
        copyTasks.get();

        //Now wait for close to finish
        closeTasks.get();
        assertTrue("t2 should now be copied to remote", remote.fileExists("t2"));

        executorService.shutdown();
    }

    @Test
    public void cowFailureInCopy() throws Exception{
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Directory baseDir = new CloseSafeDir();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier copier = new RAMIndexCopier(baseDir, executorService, getWorkDir());

        final Set<String> toFail = Sets.newHashSet();
        Directory remote = new CloseSafeDir() {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                if (toFail.contains(name)){
                    throw new RuntimeException("Failing copy for "+name);
                }
                return super.createOutput(name, context);
            }
        };

        final Directory local = copier.wrapForWrite(defn, remote, false, INDEX_DATA_CHILD_NAME);
        toFail.add("t2");
        byte[] t1 = writeFile(local, "t1");
        byte[] t2 = writeFile(local, "t2");

        try {
            local.close();
            fail();
        } catch (IOException ignore){

        }

        executorService.shutdown();
    }

    @Test
    public void cowPoolClosedWithTaskInQueue() throws Exception{
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Directory baseDir = new CloseSafeDir();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier copier = new RAMIndexCopier(baseDir, executorService, getWorkDir());

        final Set<String> toPause = Sets.newHashSet();
        final CountDownLatch pauseCopyLatch = new CountDownLatch(1);
        Directory remote = new CloseSafeDir() {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                if (toPause.contains(name)){
                    try {
                        pauseCopyLatch.await();
                    } catch (InterruptedException ignore) {

                    }
                }
                return super.createOutput(name, context);
            }
        };

        final Directory local = copier.wrapForWrite(defn, remote, false, INDEX_DATA_CHILD_NAME);
        toPause.add("t2");
        byte[] t1 = writeFile(local, "t1");
        byte[] t2 = writeFile(local, "t2");
        byte[] t3 = writeFile(local, "t3");
        byte[] t4 = writeFile(local, "t4");

        final AtomicReference<Throwable> error =
                new AtomicReference<Throwable>();
        Thread closer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    local.close();
                } catch (Throwable e) {
                    e.printStackTrace();
                    error.set(e);
                }
            }
        });

        closer.start();

        copier.close();
        executorService.shutdown();
        executorService.awaitTermination(100, TimeUnit.MILLISECONDS);

        pauseCopyLatch.countDown();
        closer.join();
        assertNotNull("Close should have thrown an exception", error.get());
    }

    /**
     * Test the interaction between COR and COW using same underlying directory
     */
    @Test
    public void cowConcurrentAccess() throws Exception{
        CollectingExecutor executor = new CollectingExecutor();
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executor.setForwardingExecutor(executorService);

        Directory baseDir = new CloseSafeDir();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), indexPath);
        IndexCopier copier = new RAMIndexCopier(baseDir, executor, getWorkDir(), true);

        Directory remote = new CloseSafeDir();
        byte[] f1 = writeFile(remote, "f1");

        Directory cor1 = copier.wrapForRead(indexPath, defn, remote, INDEX_DATA_CHILD_NAME);
        readAndAssert(cor1, "f1", f1);
        cor1.close();

        final CountDownLatch pauseCopyLatch = new CountDownLatch(1);
        Directory remote2 = new FilterDirectory(remote) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                try {
                    pauseCopyLatch.await();
                } catch (InterruptedException ignore) {

                }
                return super.createOutput(name, context);
            }
        };

        //Start copying a file to remote via COW
        Directory cow1 = copier.wrapForWrite(defn, remote2, false, INDEX_DATA_CHILD_NAME);
        byte[] f2 = writeFile(cow1, "f2");

        //Before copy is done to remote lets delete f1 from remote and
        //open a COR and close it such that it triggers delete of f1
        remote.deleteFile("f1");
        Directory cor2 = copier.wrapForRead(indexPath, defn, remote, INDEX_DATA_CHILD_NAME);

        //Ensure that deletion task submitted to executor get processed immediately
        executor.enableImmediateExecution();
        cor2.close();
        executor.enableDelayedExecution();

        assertFalse(baseDir.fileExists("f1"));
        assertFalse("f2 should not have been copied to remote so far", remote.fileExists("f2"));
        assertTrue("f2 should exist", baseDir.fileExists("f2"));

        pauseCopyLatch.countDown();
        cow1.close();
        assertTrue("f2 should exist", remote.fileExists("f2"));

        executorService.shutdown();
    }

    @Test
    public void directoryContentMismatch_COR() throws Exception{
        Directory baseDir = new CloseSafeDir();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState(), "/foo");
        IndexCopier copier = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir(), true);

        Directory remote = new RAMDirectory();
        byte[] t1 = writeFile(remote, "t1");
        byte[] t2 = writeFile(remote, "t2");

        //State of remote directory should set before wrapping as later
        //additions would not be picked up given COW assume remote directory
        //to be read only
        Directory local = copier.wrapForRead("/foo", defn, remote, INDEX_DATA_CHILD_NAME);

        readAndAssert(local, "t1", t1);
        readAndAssert(local, "t2", t2);

        copier.close();

        //2. Modify the same file in remote directory simulating rollback scenario
        Directory remoteModified = new RAMDirectory();
        t1 = writeFile(remoteModified, "t1");

        //3. Reopen the copier
        copier = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir(), true);

        //4. Post opening local the content should be in sync with remote
        //So t1 should be recreated matching remote
        //t2 should be removed
        local = copier.wrapForRead("/foo", defn, remoteModified, INDEX_DATA_CHILD_NAME);
        readAndAssert(baseDir, "t1", t1);
        assertFalse(baseDir.fileExists("t2"));
    }

    private static void doReindex(NodeBuilder builder) {
        builder.child(IndexDefinition.STATUS_NODE).remove();
        LuceneIndexEditorContext.configureUniqueId(builder);
    }

    private byte[] writeFile(Directory dir, String name) throws IOException {
        byte[] data = randomBytes(rnd.nextInt(maxFileSize) + 1);
        IndexOutput o = dir.createOutput(name, IOContext.DEFAULT);
        o.writeBytes(data, data.length);
        o.close();
        return data;
    }

    private byte[] randomBytes(int size) {
        byte[] data = new byte[size];
        rnd.nextBytes(data);
        return data;
    }

    private File getWorkDir(){
        return temporaryFolder.getRoot();
    }

    private static void readAndAssert(Directory wrapped, String fileName, byte[] expectedData) throws IOException {
        IndexInput i = wrapped.openInput(fileName, IOContext.DEFAULT);
        byte[] result = new byte[(int)wrapped.fileLength(fileName)];
        i.readBytes(result, 0, result.length);
        assertTrue(Arrays.equals(expectedData, result));
        i.close();
    }

    private static void copy(Directory source, Directory dest) throws IOException {
        for (String file : source.listAll()) {
            source.copy(dest, file, file, IOContext.DEFAULT);
        }
    }

    private class RAMIndexCopier extends IndexCopier {
        final Directory baseDir;

        public RAMIndexCopier(Directory baseDir, Executor executor, File indexRootDir,
                              boolean prefetchEnabled) throws IOException {
            super(executor, indexRootDir, prefetchEnabled);
            this.baseDir = baseDir;
        }

        public RAMIndexCopier(Directory baseDir, Executor executor, File indexRootDir) throws IOException {
            this(baseDir, executor, indexRootDir, false);
        }

        @Override
        protected Directory createLocalDirForIndexReader(String indexPath, IndexDefinition definition, String dirName) throws IOException {
            return baseDir;
        }

        @Override
        protected Directory createLocalDirForIndexWriter(IndexDefinition definition, String dirName) throws IOException {
            return baseDir;
        }
    }

    private static class TestRAMDirectory extends RAMDirectory {
        final List<String> openedFiles = newArrayList();

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            openedFiles.add(name);
            return super.openInput(name, context);
        }

        public void reset(){
            openedFiles.clear();
        }
    }

    private static class CloseSafeDir extends RAMDirectory {
        @Override
        public void close() {

        }
    }

    private static class CollectingExecutor implements Executor {
        final BlockingQueue<Runnable> commands = new LinkedBlockingQueue<Runnable>();
        private volatile boolean immediateExecution = false;
        private volatile Executor forwardingExecutor;

        @Override
        public void execute(Runnable command) {
            if (immediateExecution){
                command.run();
                return;
            }

            if (forwardingExecutor != null){
                forwardingExecutor.execute(command);
                return;
            }

            commands.add(command);
        }

        void executeAll(){
            Runnable c;
            while ((c = commands.poll()) != null){
                c.run();
            }
        }

        void enableImmediateExecution(){
            immediateExecution = true;
        }

        void enableDelayedExecution(){
            immediateExecution = false;
        }

        void setForwardingExecutor(Executor forwardingExecutor){
            this.forwardingExecutor = forwardingExecutor;
        }
    }

}
