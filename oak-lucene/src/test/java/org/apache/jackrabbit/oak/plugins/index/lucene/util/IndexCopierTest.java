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

package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.junit.After;
import org.junit.Test;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexCopierTest {
    private Random rnd = new Random();
    private int maxFileSize = 7896;

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

    @Test
    public void basicTest() throws Exception{
        Directory baseDir = new RAMDirectory();
        IndexDefinition defn = new IndexDefinition(builder);
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
    public void concurrentRead() throws Exception{
        Directory baseDir = new RAMDirectory();
        IndexDefinition defn = new IndexDefinition(builder);
        CollectingExecutor executor = new CollectingExecutor();

        IndexCopier c1 = new RAMIndexCopier(baseDir, executor, getWorkDir());

        TestRAMDirectory remote = new TestRAMDirectory();
        Directory wrapped = c1.wrap("/foo" , defn, remote);

        byte[] t1 = writeFile(remote , "t1");

        //1. Trigger a read which should go to remote
        readAndAssert(wrapped, "t1", t1);
        assertEquals(1, remote.openedFiles.size());
        assertEquals(1, executor.commands.size());

        //2. Trigger another read and this should also be
        //served from remote
        readAndAssert(wrapped, "t1", t1);
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
    }

    /**
     * Test for the case where local directory is opened already contains
     * the index files and in such a case file should not be read from remote
     */
    @Test
    public void reuseLocalDir() throws Exception{
        Directory baseDir = new RAMDirectory();
        IndexDefinition defn = new IndexDefinition(builder);
        IndexCopier c1 = new RAMIndexCopier(baseDir, sameThreadExecutor(), getWorkDir());

        TestRAMDirectory remote = new TestRAMDirectory();
        Directory wrapped = c1.wrap("/foo" , defn, remote);

        byte[] t1 = writeFile(remote , "t1");

        //1. Read for the first time should be served from remote
        readAndAssert(wrapped, "t1", t1);
        assertEquals(1, remote.openedFiles.size());

        //2. Reuse the testDir and read again
        Directory wrapped2 = c1.wrap("/foo" , defn, remote);
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
    public void deletesOnClose() throws Exception{
        //Use a close safe dir. In actual case the FSDir would
        //be opened on same file system hence it can retain memory
        //but RAMDirectory does not retain memory hence we simulate
        //that by not closing the RAMDir and reuse it
        Directory baseDir = new CloseSafeDir();


        IndexDefinition defn = new IndexDefinition(builder);
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

    @After
    public void close() throws IOException {
        FileUtils.deleteQuietly(getWorkDir());
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
        return new File("target", "IndexClonerTest");
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
