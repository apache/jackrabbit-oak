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
package org.apache.jackrabbit.oak.index.indexer.document.tree;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.NodeStateEntryJson;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.TreeSortPipelinedBuildTreeTask;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.TreeSortPipelinedStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Session;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Store;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.StoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.StoreLock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TreeSortPipelinedBuildTreeTaskTest {

    public static final String TEST_DIR = "target/treeTest";

    {
        new File(TEST_DIR).mkdirs();
    }

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(new File(TEST_DIR));

    @Test
    public void test() throws Exception {
        File storeDir = tempFolder.getRoot();
        storeDir.mkdirs();
        BlockingQueue<List<NodeStateEntryJson>> queue = new ArrayBlockingQueue<>(3);

        String storeConfig = "type=file\n" +
                "cacheSizeMB=4096\n" +
                "maxFileSize=64000000\n" +
                "dir=" + storeDir.getAbsolutePath();
        Store store = StoreBuilder.build(storeConfig);
        StoreLock storeLock = StoreLock.lock(store);
        store.setWriteCompression(Compression.LZ4);
        Session session = Session.open(store);
        session.setMaxRoots(Integer.MAX_VALUE);
        
        TreeSortPipelinedBuildTreeTask task = new TreeSortPipelinedBuildTreeTask(
                storeDir, store, storeLock, session, queue);

        Random r = new Random(1);
        int blockCount = 10;
        int blockSize = 1000;
        AtomicLong counter = new AtomicLong();
        // long start = System.nanoTime();

        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    for (int i = 0; i < blockCount; i++) {
                        List<NodeStateEntryJson> list = randomList(r, blockSize);
                        Collections.sort(list);
                        counter.addAndGet(list.size());
                        // System.out.println("put " + counter + " at " + (System.nanoTime() - start) / counter.get() + " ns/item");
                        queue.put(list);
                    }
                    queue.put(TreeSortPipelinedStrategy.SENTINEL_NODE_STATE_ENTRY_JSON);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        });
        t.setName(TreeSortPipelinedBuildTreeTaskTest.class.getName());
        t.setDaemon(true);
        t.start();
        task.call();
        t.join();
    }

    private static List<NodeStateEntryJson> randomList(Random r, int blockSize) {
        ArrayList<NodeStateEntryJson> list = new ArrayList<>();
        for (int i = 0; i < blockSize; i++) {
            String path = "/n" + r.nextInt(1000) + "/a" + r.nextInt(1000) + "/b" + r.nextInt(1000) + "/c" + r.nextInt(1000);
            NodeStateEntryJson e = new NodeStateEntryJson(path, "{\"node\": \"" + i + "\"}");
            list.add(e);
        }
        return list;
    }

}
