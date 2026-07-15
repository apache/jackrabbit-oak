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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStore;
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ParallelIndexStoreTest {

    @ClassRule
    public static TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    @Test
    public void test() throws IOException {
        File dir = temporaryFolder.newFolder("sourceDir");
        Store store = StoreBuilder.build(
                "type=file\n" +
                "dir="+dir.getAbsolutePath()+"\n" +
                "cacheSizeMB=1\n" +
                "maxFileSizeBytes=100");
        store.setWriteCompression(Compression.LZ4);
        store.removeAll();
        TreeSession session = new TreeSession(store);
        session.init();
        int count = 1000;
        session.checkpoint();
        HashSet<String> paths = new HashSet<>();
        for (int i = 0; i < count; i++) {
            String path = "/c" + (i / 100) + "/d" + i;
            session.put(path, "{\"x\":" + i + "}");
            paths.add(path);
        }
        session.mergeRoots(Integer.MAX_VALUE);
        session.flush();
        store.close();

        TreeStore treeStore = new TreeStore("test", dir, new NodeStateEntryReader(new MemoryBlobStore()), 1);
        Collection<IndexStore> list = treeStore.buildParallelStores(10);
        ArrayList<Iterator<NodeStateEntry>> iterators = new ArrayList<>();
        for(IndexStore ps : list) {
            iterators.add(ps.iterator());
        }
        int found = 0;
        int[] countPerIterator = new int[10];
        while (true) {
            boolean oneIteratorHasNext = false;
            for (int i = 0; i < 10; i++) {
                Iterator<NodeStateEntry> it = iterators.get(i);
                if (it.hasNext()) {
                    String path = it.next().getPath();
                    countPerIterator[i]++;
                    assertTrue(paths.remove(path));
                    oneIteratorHasNext = true;
                    found++;
                }
            }
            if (!oneIteratorHasNext) {
                break;
            }
        }
        for (int i = 0; i < 10; i++) {
            assertTrue(Arrays.toString(countPerIterator),
                    countPerIterator[i] > 50);
        }
        assertEquals(count, found);
        treeStore.close();
    }

}
