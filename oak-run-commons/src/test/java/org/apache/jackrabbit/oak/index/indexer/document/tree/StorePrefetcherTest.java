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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StorePrefetcherTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void test() throws IOException, InterruptedException {
        BlobStore blobStore = null;
        NodeStateEntryReader entryReader = new NodeStateEntryReader(blobStore);
        File folder = temporaryFolder.newFolder();
        TreeStore indexStore = new TreeStore("index",
                folder, entryReader, 1);
        indexStore.getSession().init();
        indexStore.getSession().put("/", "{}");
        indexStore.getSession().put("/content", "{}");
        indexStore.getSession().put("/test.txt", "{}");
        indexStore.getSession().put("/test.txt/jcr:content", "{}");
        indexStore.getSession().checkpoint();
        TreeStore prefetchStore = new TreeStore(
                "prefetch", folder, entryReader, 1);
        Prefetcher prefetcher = new Prefetcher(prefetchStore, indexStore) {
            @Override
            public void sleep(String status) throws InterruptedException {
                Thread.sleep(0, 1);
            }
        };
        prefetcher.setBlobReadAheadSize(1);
        prefetcher.setNodeReadAheadCount(1);
        prefetcher.setBlobSuffix("/test.txt");
        prefetcher.start();
        Iterator<NodeStateEntry> it = indexStore.iterator();
        while (it.hasNext()) {
            Thread.sleep(100);
            NodeStateEntry e = it.next();
            assertTrue(e.getPath().compareTo(indexStore.getHighestReadKey()) <= 0);
        }
        prefetcher.shutdown();
        System.out.println(indexStore.toString());
        assertTrue("Expected shutdown after 1 second", prefetcher.shutdown());
    }
}
