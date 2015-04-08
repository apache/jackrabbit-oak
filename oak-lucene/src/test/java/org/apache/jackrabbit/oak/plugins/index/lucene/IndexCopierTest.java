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
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.management.openmbean.TabularData;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_COUNT;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IndexCopierTest {
    private Random rnd = new Random();
    private int maxFileSize = 7896;

    private NodeState root = INITIAL_CONTENT;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private NodeBuilder builder = root.builder();

    @Test
    public void basicTest() throws Exception{
        Directory baseDir = new RAMDirectory();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        IndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        Directory remote = new RAMDirectory();
        Directory wrapped = c1.wrap("/foo" , defn, remote);

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
    public void basicTestWithFS() throws Exception{
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        IndexCopier c1 = new IndexCopier(sameThreadExecutor(), getWorkDir());

        Directory remote = new RAMDirectory();
        Directory wrapped = c1.wrap("/foo" , defn, remote);

        byte[] t1 = writeFile(remote, "t1");
        byte[] t2 = writeFile(remote , "t2");

        assertEquals(2, wrapped.listAll().length);

        assertTrue(wrapped.fileExists("t1"));
        assertTrue(wrapped.fileExists("t2"));

        assertEquals(t1.length, wrapped.fileLength("t1"));
        assertEquals(t2.length, wrapped.fileLength("t2"));

        readAndAssert(wrapped, "t1", t1);

        //t1 should now be added to testDir
        File indexBaseDir = c1.getIndexDir("/foo");
        File indexDir = new File(indexBaseDir, "0");
        assertTrue(new File(indexDir, "t1").exists());

        TabularData td = c1.getIndexPathMapping();
        assertEquals(1, td.size());
    }

    @Ignore("OAK-2722") //FIXME test fails on windows
    @Test
    public void deleteOldPostReindex() throws Exception{
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        IndexCopier c1 = new IndexCopier(sameThreadExecutor(), getWorkDir());

        Directory remote = new CloseSafeDir();
        Directory w1 = c1.wrap("/foo" , defn, remote);

        byte[] t1 = writeFile(remote , "t1");
        byte[] t2 = writeFile(remote , "t2");

        readAndAssert(w1, "t1", t1);
        readAndAssert(w1, "t2", t2);

        //t1 should now be added to testDir
        File indexBaseDir = c1.getIndexDir("/foo");
        File indexDir = new File(indexBaseDir, "0");
        assertTrue(new File(indexDir, "t1").exists());

        builder.setProperty(REINDEX_COUNT, 1);
        defn = new IndexDefinition(root, builder.getNodeState());

        //Close old version
        w1.close();
        //Get a new one with updated reindexCount
        Directory w2 = c1.wrap("/foo" , defn, remote);

        readAndAssert(w2, "t1", t1);

        w2.close();
        assertFalse("Old index directory should have been removed", indexDir.exists());

        File indexDir2 = new File(indexBaseDir, "1");
        assertTrue(new File(indexDir2, "t1").exists());
    }

    @Test
    public void concurrentRead() throws Exception{
        Directory baseDir = new RAMDirectory();
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        CollectingExecutor executor = new CollectingExecutor();

        IndexCopier c1 = new RAMIndexCopier(baseDir, executor, getWorkDir());

        TestRAMDirectory remote = new TestRAMDirectory();
        Directory wrapped = c1.wrap("/foo", defn, remote);

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
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());

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
        Directory wrapped = c1.wrap("/foo", defn, remote);

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
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        IndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        TestRAMDirectory remote = new TestRAMDirectory();
        Directory wrapped = c1.wrap("/foo" , defn, remote);

        byte[] t1 = writeFile(remote, "t1");

        //1. Read for the first time should be served from remote
        readAndAssert(wrapped, "t1", t1);
        assertEquals(1, remote.openedFiles.size());

        //2. Reuse the testDir and read again
        Directory wrapped2 = c1.wrap("/foo", defn, remote);
        remote.reset();

        //3. Now read should be served from local
        readAndAssert(wrapped2, "t1", t1);
        assertEquals(0, remote.openedFiles.size());

        //Now check if local file gets corrupted then read from remote
        Directory wrapped3 = c1.wrap("/foo" , defn, remote);
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
        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        RAMIndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        Directory remote = new RAMDirectory(){
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
                throw new IllegalStateException("boom");
            }
        };

        String fileName = "failed.txt";
        Directory wrapped = c1.wrap("/foo" , defn, remote);

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


        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        IndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        Directory r1 = new RAMDirectory();

        byte[] t1 = writeFile(r1 , "t1");
        byte[] t2 = writeFile(r1 , "t2");

        Directory w1 = c1.wrap("/foo" , defn, r1);
        readAndAssert(w1, "t1", t1);
        readAndAssert(w1, "t2", t2);

        // t1 and t2 should now be present in local (base dir which back local)
        assertTrue(baseDir.fileExists("t1"));
        assertTrue(baseDir.fileExists("t2"));

        Directory r2 = new RAMDirectory();
        copy(r1, r2);
        r2.deleteFile("t1");

        Directory w2 = c1.wrap("/foo" , defn, r2);

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

        IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
        IndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        Directory r1 = new RAMDirectory();

        byte[] t1 = writeFile(r1, "t1");
        byte[] t2 = writeFile(r1 , "t2");

        Directory w1 = c1.wrap("/foo" , defn, r1);
        readAndAssert(w1, "t1", t1);
        readAndAssert(w1, "t2", t2);

        // t1 and t2 should now be present in local (base dir which back local)
        assertTrue(baseDir.fileExists("t1"));
        assertTrue(baseDir.fileExists("t2"));

        Directory r2 = new CloseSafeDir();
        copy(r1, r2);
        r2.deleteFile("t1");

        Directory w2 = c1.wrap("/foo" , defn, r2);

        //Close would trigger removal of file which are not present in remote
        testFiles.add("t1");
        w2.close();

        assertEquals(1, c1.getFailedToDeleteFiles().size());
        IndexCopier.LocalIndexFile testFile = c1.getFailedToDeleteFiles().values().iterator().next();

        assertEquals(1, testFile.getDeleteAttemptCount());
        assertEquals(IOUtils.humanReadableByteCount(t1.length), c1.getGarbageSize());
        assertEquals(1, c1.getGarbageDetails().length);

        Directory w3 = c1.wrap("/foo" , defn, r2);
        w3.close();
        assertEquals(2, testFile.getDeleteAttemptCount());

        //Now let the file to be deleted
        testFiles.clear();

        Directory w4 = c1.wrap("/foo" , defn, r2);
        w4.close();

        //No pending deletes left
        assertEquals(0, c1.getFailedToDeleteFiles().size());
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
    }

    private static void copy(Directory source, Directory dest) throws IOException {
        for (String file : source.listAll()) {
            source.copy(dest, file, file, IOContext.DEFAULT);
        }
    }

    private class RAMIndexCopier extends IndexCopier {
        final Directory baseDir;

        public RAMIndexCopier(Directory baseDir, Executor executor, File indexRootDir) {
            super(executor, indexRootDir);
            this.baseDir = baseDir;
        }

        @Override
        protected Directory createLocalDir(String indexPath, IndexDefinition definition) throws IOException {
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
        final List<Runnable> commands = newArrayList();

        @Override
        public void execute(Runnable command) {
            commands.add(command);
        }

        void executeAll(){
            for (Runnable c : commands) {
                c.run();
            }
        }
    }

}
