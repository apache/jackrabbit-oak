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

package org.apache.jackrabbit.oak.plugins.index.lucene.directory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.OakAnalyzer;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker.Level;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexConsistencyChecker.Result;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexersLucene;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.index.lucene.FieldFactory.newPathField;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IndexConsistencyCheckerTest {

    private NodeState rootState = InitialContent.INITIAL_CONTENT;
    private NodeBuilder idx = new IndexDefinitionBuilder().build().builder();


    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void emptyIndex() throws Exception{
        IndexConsistencyChecker checker = new IndexConsistencyChecker(EMPTY_NODE, "/foo", temporaryFolder.getRoot());
        Result result = checker.check(Level.BLOBS_ONLY);
        assertFalse(result.clean);
        assertTrue(result.typeMismatch);
        assertEquals(result.indexPath, "/foo");
    }

    @Test
    public void blobsWithError() throws Exception{
        FailingBlob failingBlob = new FailingBlob("foo");

        idx.setProperty("foo", failingBlob);
        idx.child(":index").setProperty("foo", failingBlob);
        idx.child("b").setProperty("foo", Lists.newArrayList(failingBlob, failingBlob), Type.BINARIES);

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("a", idx.getNodeState());

        IndexConsistencyChecker checker = new IndexConsistencyChecker(builder.getNodeState(), "/a", temporaryFolder.getRoot());
        Result result = checker.check(Level.BLOBS_ONLY);

        assertFalse(result.clean);
        assertTrue(result.missingBlobs);
        assertFalse(result.blobSizeMismatch);
        assertEquals(4, result.missingBlobIds.size());

        dumpResult(result);
    }

    @Test
    public void blobsWithSizeMismatch() throws Exception{
        FailingBlob failingBlob = new FailingBlob("foo", true);

        idx.child(":index").setProperty("foo", failingBlob);

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("a", idx.getNodeState());

        IndexConsistencyChecker checker = new IndexConsistencyChecker(builder.getNodeState(), "/a", temporaryFolder.getRoot());
        Result result = checker.check(Level.BLOBS_ONLY);

        assertFalse(result.clean);
        assertFalse(result.missingBlobs);
        assertTrue(result.blobSizeMismatch);
        assertEquals(1, result.invalidBlobIds.size());

        dumpResult(result);
    }

    @Test
    public void validIndexTest() throws Exception{
        IndexDefinition defn = IndexDefinition.newBuilder(rootState, idx.getNodeState(), "/fooIndex").build();
        Directory dir = new OakDirectory(idx, ":data", defn, false);
        createIndex(dir, 10);

        dir = new OakDirectory(idx, ":data2"+ MultiplexersLucene.INDEX_DIR_SUFFIX, defn, false);
        createIndex(dir, 10);

        NodeBuilder builder = rootState.builder();
        builder.setChildNode("fooIndex", idx.getNodeState());
        NodeState indexState = builder.getNodeState();

        IndexConsistencyChecker checker = new IndexConsistencyChecker(indexState, "/fooIndex", temporaryFolder.getRoot());
        Result result = checker.check(Level.BLOBS_ONLY);
        assertTrue(result.clean);

        checker = new IndexConsistencyChecker(indexState, "/fooIndex", temporaryFolder.getRoot());
        result = checker.check(Level.FULL);
        assertTrue(result.clean);
        assertEquals(2, result.dirStatus.size());

        dumpResult(result);
    }

    @Test
    public void missingFile() throws Exception{
        IndexDefinition defn = IndexDefinition.newBuilder(rootState, idx.getNodeState(), "/fooIndex").build();
        Directory dir = new OakDirectory(idx, ":data", defn, false);
        createIndex(dir, 10);

        NodeBuilder builder = rootState.builder();

        idx.getChildNode(":data").getChildNode("segments.gen").remove();

        builder.setChildNode("fooIndex", idx.getNodeState());
        NodeState indexState = builder.getNodeState();

        IndexConsistencyChecker checker = new IndexConsistencyChecker(indexState, "/fooIndex", temporaryFolder.getRoot());
        Result result = checker.check(Level.FULL);
        assertFalse(result.clean);
        assertEquals(1, result.dirStatus.get(0).missingFiles.size());
        assertNull(result.dirStatus.get(0).status);

        dumpResult(result);
    }

    @Test
    public void badFile() throws Exception{
        IndexDefinition defn = IndexDefinition.newBuilder(rootState, idx.getNodeState(), "/fooIndex").build();
        Directory dir = new OakDirectory(idx, ":data", defn, false);
        createIndex(dir, 10);

        NodeBuilder builder = rootState.builder();

        NodeBuilder file = idx.getChildNode(":data").getChildNode("_0.cfe");
        List<Blob> blobs = Lists.newArrayList(file.getProperty("jcr:data").getValue(Type.BINARIES));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(blobs.get(0).getNewStream(), baos);
        byte[] bytes = baos.toByteArray();
        bytes[0] = (byte)(bytes[0] ^ (1 << 3)); //Flip the 3rd bit to make it corrupt
        blobs.set(0, new ArrayBasedBlob(bytes));
        file.setProperty("jcr:data", blobs, Type.BINARIES);

        builder.setChildNode("fooIndex", idx.getNodeState());
        NodeState indexState = builder.getNodeState();


        IndexConsistencyChecker checker = new IndexConsistencyChecker(indexState, "/fooIndex", temporaryFolder.getRoot());
        Result result = checker.check(Level.FULL);
        assertFalse(result.clean);
        assertEquals(0, result.dirStatus.get(0).missingFiles.size());
        assertFalse(result.dirStatus.get(0).status.clean);
    }

    private void createIndex(Directory dir, int numOfDocs) throws IOException {
        IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(Version.LUCENE_47, new OakAnalyzer(Version.LUCENE_47)));
        for (int i = 0; i < numOfDocs; i++) {
            Document d1 = new Document();
            d1.add(newPathField("/a/b"));
            w.addDocument(d1);
        }
        w.close();
        dir.close();
    }

    private static void dumpResult(Result result) {
        System.out.println(result);
    }

    private static class FailingBlob extends ArrayBasedBlob {
        static int count;
        private final String id;
        private final boolean corruptLength;

        public FailingBlob(String s) {
           this(s, false);
        }

        public FailingBlob(String s, boolean corruptLength) {
            super(s.getBytes());
            this.id = String.valueOf(++count);
            this.corruptLength = corruptLength;
        }

        @Nonnull
        @Override
        public InputStream getNewStream() {
            if (corruptLength){
                return super.getNewStream();
            }
            throw new UnsupportedOperationException();
        }

        @Override
        public String getContentIdentity() {
            return id;
        }

        @Override
        public long length() {
            return corruptLength ? super.length() + 1 : super.length();
        }
    }


}