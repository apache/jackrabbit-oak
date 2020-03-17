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

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.io.Closer;
import org.apache.jackrabbit.oak.plugins.document.DocumentMKBuilderProvider;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.IndexWriterUtils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.PROP_UID;
import static org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.STATUS_NODE;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.SUGGEST_DATA_CHILD_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class CopyOnWriteDirectoryTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Random rnd = new Random();

    private IndexCopier copier;

    private ExecutorService executor;

    private DocumentNodeStore ns;

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
        ns = builderProvider.newBuilder().getNodeStore();
    }

    @After
    public void after() throws Exception {
        ns.dispose();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    // OAK-5238
    @Test
    public void copyOnWrite() throws Exception {
        IndexDefinition def = new IndexDefinition(ns.getRoot(), ns.getRoot(), "/foo");
        NodeBuilder builder = ns.getRoot().builder();
        Directory dir = new DefaultDirectoryFactory(copier, null).newInstance(def, builder.child("foo"), INDEX_DATA_CHILD_NAME, false);
        addFiles(dir);
        writeTree(builder);
        dir.close();
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    // OAK-6775
    @Test
    public void suggestDirUseCOWOnlyWhenItGetUniqueFSFolder() throws Exception {
        Closer closer = Closer.create();
        try {
            NodeBuilder builder = ns.getRoot().builder().child("foo");

            IndexDefinition def = new IndexDefinition(ns.getRoot(), builder.getNodeState(), "/foo");
            Directory dir = new DefaultDirectoryFactory(copier, null).newInstance(def, builder.child("foo"), INDEX_DATA_CHILD_NAME, false);
            Directory suggestDir = new DefaultDirectoryFactory(copier, null).newInstance(def, builder.child("foo"), SUGGEST_DATA_CHILD_NAME, false);

            closer.register(dir);
            closer.register(suggestDir);

            assertTrue("Data directory not COW-wrapped", dir instanceof CopyOnWriteDirectory);
            assertFalse("Suggester directory COW-wrapped", suggestDir instanceof CopyOnWriteDirectory);

            builder.child(STATUS_NODE).setProperty(PROP_UID, "some_random_string");
            def = new IndexDefinition(ns.getRoot(), builder.getNodeState(), "/foo");

            assertNotNull("Synthetic UID not read by definition", def.getUniqueId());

            dir = new DefaultDirectoryFactory(copier, null).newInstance(def, builder.child("foo"), INDEX_DATA_CHILD_NAME, false);
            Directory dir1 = new DefaultDirectoryFactory(copier, null).newInstance(def, builder.child("foo"), INDEX_DATA_CHILD_NAME, false);
            suggestDir = new DefaultDirectoryFactory(copier, null).newInstance(def, builder.child("foo"), SUGGEST_DATA_CHILD_NAME, false);

            closer.register(dir);
            closer.register(suggestDir);

            assertTrue("Data directory not COW-wrapped", dir instanceof CopyOnWriteDirectory);
            assertTrue("Suggester directory not COW-wrapped", suggestDir instanceof CopyOnWriteDirectory);
        } finally {
            closer.close();
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
