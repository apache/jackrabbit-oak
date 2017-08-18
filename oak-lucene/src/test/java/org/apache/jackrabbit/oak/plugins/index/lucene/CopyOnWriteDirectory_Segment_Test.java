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
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.fail;

public class CopyOnWriteDirectory_Segment_Test {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    private Random rnd = new Random();

    private IndexCopier copier;

    private ExecutorService executor;

    private NodeStore ns;

    private FileStore fileStore;

    @Before
    public void before() throws Exception {
        executor = Executors.newCachedThreadPool(new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("IndexCopier-" + counter.incrementAndGet());
                return t;
            }
        });
        copier = new IndexCopier(executor, tempFolder.newFolder());

        OakFileDataStore delegate = new OakFileDataStore();
        delegate.setPath(tempFolder.newFolder().getAbsolutePath());
        delegate.init(null);

        DataStoreBlobStore dsbs = new DataStoreBlobStore(delegate);

        fileStore = FileStore.newFileStore(tempFolder.newFolder()).withBlobStore(dsbs).create();
        ns = new SegmentNodeStore(fileStore);
    }

    @After
    public void after() throws Exception {
        fileStore.close();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    // OAK-5238
    @Test
    public void copyOnWrite() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo").setProperty(LuceneIndexConstants.INDEX_PATH, "/foo");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexDefinition def = new IndexDefinition(ns.getRoot(), ns.getRoot().getChildNode("foo"));
        builder = ns.getRoot().builder();
        Directory remote = LuceneIndexEditorContext.newIndexDirectory(def, builder.child("foo"), true);
        Directory dir = copier.wrapForWrite(def, remote, false);
        addFiles(dir);

        writeTree(builder);
        dir.close();
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertIndexState();
    }

    @Test
    public void copyOnWrite_largeFile() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo").setProperty(LuceneIndexConstants.INDEX_PATH, "/foo");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        IndexDefinition def = new IndexDefinition(ns.getRoot(), ns.getRoot().getChildNode("foo"));
        builder = ns.getRoot().builder();
        Directory remote = LuceneIndexEditorContext.newIndexDirectory(def, builder.child("foo"), true);
        Directory dir = copier.wrapForWrite(def, remote, false);

        byte[] data = randomBytes();
        IndexOutput out = dir.createOutput("file-large", IOContext.DEFAULT);
        for (int i = 0; i < 1024 * 1024; i++) {
            out.writeBytes(data, data.length);
        }
        out.close();

        writeTree(builder);
        dir.close();
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertIndexState();
    }

    private void assertIndexState() {
        for (ChildNodeEntry e : ns.getRoot().getChildNode("foo").getChildNode(":data").getChildNodeEntries()){
            NodeState idxFile = e.getNodeState();
            if (!idxFile.hasProperty("blobSize")){
                fail(idxFile.toString());
            }
        }
    }

    private void writeTree(NodeBuilder builder) {
        NodeBuilder test = builder.child("test");
        for (int i = 0; i < 100; i++) {
            NodeBuilder b = test.child("child-" + i);
            for (int j = 0; j < 10; j++) {
                NodeBuilder child = b.child("child-" + j);
                child.setProperty("p", "value");
            }
        }
    }

    private void addFiles(Directory dir) throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] data = randomBytes();
            IndexOutput out = dir.createOutput("file-" + i, IOContext.DEFAULT);
            out.writeBytes(data, data.length);
            out.close();
        }
    }

    private byte[] randomBytes() {
        byte[] data = new byte[1024];
        rnd.nextBytes(data);
        return data;
    }
}
