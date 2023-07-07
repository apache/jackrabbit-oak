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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry.NodeStateEntryBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Session;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Store;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.StoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.Cache;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class TreeStore implements Iterable<NodeStateEntry>, Closeable {

    public static void main(String... args) throws IOException {
        String dir = args[0];
        MemoryBlobStore blobStore = new MemoryBlobStore();
        NodeStateEntryReader entryReader = new NodeStateEntryReader(blobStore);
        TreeStore treeStore = new TreeStore(new File(dir), entryReader);
        Session session = treeStore.session;
        Store store = treeStore.store;
        if (store.keySet().isEmpty()) {
            session.init();
            String fileName = args[1];
            BufferedReader lineReader = new BufferedReader(
                    new FileReader(fileName, StandardCharsets.UTF_8));
            int count = 0;
            long start = System.nanoTime();
            while (true) {
                String line = lineReader.readLine();
                if (line == null) {
                    break;
                }
                count++;
                if (count % 1000000 == 0) {
                    long time = System.nanoTime() - start;
                    System.out.println(count + " " + (time / count) + " ns/entry");
                }
                int index = line.indexOf('|');
                if (index < 0) {
                    throw new IllegalArgumentException("| is missing: " + line);
                }
                String path = line.substring(0, index);
                String value = line.substring(index + 1);
                session.put(path, value);

                if (!path.equals("/")) {
                    String nodeName = PathUtils.getName(path);
                    String parentPath = PathUtils.getParentPath(path);
                    session.put(parentPath + "\t" + nodeName, "");
                }

            }
            lineReader.close();
            session.flush();
            store.close();
        }
        Iterator<NodeStateEntry> it = treeStore.iterator();
        long nodeCount = 0;
        long childNodeCount = 0;
        long start = System.nanoTime();
        while (it.hasNext()) {
            NodeStateEntry e = it.next();
            childNodeCount += e.getNodeState().getChildNodeCount(Long.MAX_VALUE);
            nodeCount++;
            if (nodeCount % 1000000 == 0) {
                long time = System.nanoTime() - start;
                System.out.println("Node count: " + nodeCount +
                        " child node count: " + childNodeCount +
                        " speed " + (time / nodeCount) + " ns/entry");
            }
        }
        System.out.println("Node count: " + nodeCount + " Child node count: " + childNodeCount);
    }

    private final Store store;
    private final Session session;
    private final NodeStateEntryReader entryReader;
    private final Cache<String, NodeState> nodeStateCache = new Cache<>(100);

    public TreeStore(File directory, NodeStateEntryReader entryReader) {
        this.entryReader = entryReader;
        String storeConfig = System.getProperty("oak.treeStoreConfig",
                "type=file\n" +
                "cacheSizeMB=4096\n" +
                "maxFileSize=64000000\n" +
                "dir=" + directory.getAbsolutePath());
        this.store = StoreBuilder.build(storeConfig);
        this.session = new Session(store);
    }

    @Override
    public void close() throws IOException {
        session.flush();
        store.close();
    }

    @Override
    public Iterator<NodeStateEntry> iterator() {
        Iterator<Entry<String, String>> it = session.iterator();
        return new Iterator<NodeStateEntry>() {

            NodeStateEntry current;

            {
                fetch();
            }

            private void fetch() {
                while (it.hasNext()) {
                    Entry<String, String> e = it.next();
                    if (e.getValue().isEmpty()) {
                        continue;
                    }
                    current = getNodeStateEntry(e.getKey(), e.getValue());
                    return;
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public NodeStateEntry next() {
                NodeStateEntry result = current;
                fetch();
                return result;
            }

        };
    }

    NodeStateEntry getNodeStateEntry(String path) {
        return new NodeStateEntryBuilder(getNodeState(path), path).build();
    }

    NodeStateEntry getNodeStateEntry(String path, String value) {
        return new NodeStateEntryBuilder(getNodeState(path, value), path).build();
    }

    NodeState getNodeState(String path) {
        NodeState result = nodeStateCache.get(path);
        if (result != null) {
            return result;
        }
        String value = session.get(path);
        if (value == null || value.isEmpty()) {
            result = EmptyNodeState.EMPTY_NODE;
        } else {
            result = getNodeState(path, value);
        }
        nodeStateCache.put(path, result);
        return result;
    }

    NodeState getNodeState(String path, String value) {
        NodeState result = nodeStateCache.get(path);
        if (result != null) {
            return result;
        }
        String line = path + "|" + value;
        NodeStateEntry entry = entryReader.read(line);
        result = new TreeStoreNodeState(entry.getNodeState(), path, this);
        nodeStateCache.put(path, result);
        return result;
    }

    /**
     * The child node entry for the given path.
     *
     * @param path the path, e.g. /hello/world
     * @return the child node entry, e.g. /hello<tab>world
     */
    public static String toChildNodeEntry(String path) {
        if (path.equals("/")) {
            return "\t";
        }
        String nodeName = PathUtils.getName(path);
        String parentPath = PathUtils.getParentPath(path);
        return parentPath + "\t" + nodeName;
    }

    /**
     * The child node entry for the given parent and child.
     *
     * @param path the parentPath, e.g. /hello
     * @param childName the name of the child node, e.g. world
     * @return the child node entry, e.g. /hello<tab>world
     */
    public static String toChildNodeEntry(String parentPath, String childName) {
        return parentPath + "\t" + childName;
    }

    public Session getSession() {
        return session;
    }

}
