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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList;

import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PersistedLinkedListV2Test extends FlatFileBufferLinkedListTest {

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void setup() throws IOException {
        list = newList(2, 1);
    }

    private PersistedLinkedListV2 newList(int cacheSize, int cacheSizeMB) throws IOException {
        String fileName = folder.newFile().getAbsolutePath();
        BlobStore blobStore = new MemoryBlobStore();
        NodeStateEntryReader reader = new NodeStateEntryReader(blobStore);
        NodeStateEntryWriter writer = new NodeStateEntryWriter(blobStore);
        return new PersistedLinkedListV2(fileName, writer, reader, cacheSize, cacheSizeMB);
    }

    @After
    public void tearDown() {
        list.close();
    }

    @Test
    public void testCacheHitsMissNumberOfEntries() throws IOException {
        PersistedLinkedListV2 persistedLinkedList = newList(2, 1);
        persistedLinkedList.add(testNode("/"));
        persistedLinkedList.add(testNode("/a"));
        persistedLinkedList.add(testNode("/a/b"));

        Assert.assertEquals(3, persistedLinkedList.size());
        ArrayList<String> names1 = extracted(persistedLinkedList.iterator());
        Assert.assertEquals(List.of("/", "/a", "/a/b"), names1);
        Assert.assertEquals(2, persistedLinkedList.getCacheHits());
        Assert.assertEquals(1, persistedLinkedList.getCacheMisses());

        ArrayList<String> names2 = extracted(persistedLinkedList.iterator());
        Assert.assertEquals(List.of("/", "/a", "/a/b"), names2);
        Assert.assertEquals(4, persistedLinkedList.getCacheHits());
        Assert.assertEquals(2, persistedLinkedList.getCacheMisses());

        // Does not count as a cache miss/hit
        Assert.assertEquals("/", persistedLinkedList.remove().getPath());

        var names3 = extracted(persistedLinkedList.iterator());
        Assert.assertEquals(List.of("/a", "/a/b"), names3);
        Assert.assertEquals(6, persistedLinkedList.getCacheHits());
        Assert.assertEquals(3, persistedLinkedList.getCacheMisses());

        Assert.assertEquals("/a", persistedLinkedList.remove().getPath());
        Assert.assertEquals("/a/b", persistedLinkedList.remove().getPath());

        var names4 = extracted(persistedLinkedList.iterator());
        Assert.assertTrue(names4.isEmpty());
        Assert.assertEquals(7, persistedLinkedList.getCacheHits());
        Assert.assertEquals(4, persistedLinkedList.getCacheMisses());
    }

    @Test
    public void testCacheHitsMissSizeOfEntries() throws IOException {
        PersistedLinkedListV2 persistedLinkedList = newList(100, 1);
        persistedLinkedList.add(testNode("/", 500 * 1024));
        persistedLinkedList.add(testNode("/a", 500 * 1024));
        persistedLinkedList.add(testNode("/a/b", 500 * 1024));

        // The first two nodes should be kept in the cache. The last node exceeds the cache size and should be stored in
        // the persistent map. So there will be 2 cache hits and one miss.
        Assert.assertEquals(3, persistedLinkedList.size());
        ArrayList<String> names1 = extracted(persistedLinkedList.iterator());
        Assert.assertEquals(List.of("/", "/a", "/a/b"), names1);
        Assert.assertEquals(2, persistedLinkedList.getCacheHits());
        Assert.assertEquals(1, persistedLinkedList.getCacheMisses());
    }

    private static ArrayList<String> extracted(Iterator<NodeStateEntry> iter) {
        ArrayList<String> names = new ArrayList<>();
        while (iter.hasNext()) {
            names.add(iter.next().getPath());
        }
        return names;
    }
}
