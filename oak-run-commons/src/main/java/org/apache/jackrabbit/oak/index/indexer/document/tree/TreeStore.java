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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry.NodeStateEntryBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Session;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Store;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.StoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.StoreLock;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.Cache;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeStore implements Iterable<NodeStateEntry>, Closeable {

    private final Logger LOG = LoggerFactory.getLogger(TreeStore.class);

    /**
     * The latest last modified time of documents read from the document store.
     */
    public static final String LAST_MODIFIED = ":lastModified";

    /**
     * The checkpoint of the document store.
     */
    public static final String CHECKPOINT = ":checkpoint";

    /**
     * Whether node are iterated depth-first.
     */
    public static final boolean DEPTH_FIRST_TRAVERSAL = Boolean.getBoolean("treeStore.depthFirst");

    /**
     * Whether there is one additional entry per child in the key-value store.
     */
    public static final boolean CHILD_ENTRIES = Boolean.getBoolean("treeStore.childEntries");

    /**
     * Whether the preferred children (e.g. jcr:content) are listed first when
     * iterating over the children.
     */
    public static final boolean PREFERRED_CHILDREN = Boolean.getBoolean("treeStore.preferredChildren");

    /**
     * The list of preferred children (sorted alphabetically).
     */
    public static List<String> PREFERRED_CHILDREN_LIST;

    /**
     * The set of preferred children (for fast access).
     */
    public static Set<String> PREFERRED_CHILDREN_SET;

    static {
        PREFERRED_CHILDREN_LIST = Arrays.asList(System.getProperty("treeStore.preferredChildrenList", "").split("|"));
        PREFERRED_CHILDREN_LIST.sort(null);
        PREFERRED_CHILDREN_SET = new HashSet<>(PREFERRED_CHILDREN_LIST);
    }

    public static void main(String... args) throws IOException {
        String dir = args[0];
        MemoryBlobStore blobStore = new MemoryBlobStore();
        NodeStateEntryReader entryReader = new NodeStateEntryReader(blobStore);
        TreeStore treeStore = new TreeStore(new File(dir), entryReader);
        Session session = treeStore.session;
        Store store = treeStore.store;
        if (store.keySet().isEmpty()) {
            String fileName = args[1];
            try (BufferedReader lineReader = new BufferedReader(
                    new FileReader(fileName, StandardCharsets.UTF_8))) {
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
            }
            session.flush();
            store.close();
        }
        Iterator<Entry<String, String>> it0 = session.iterator();
        while (it0.hasNext()) {
            Entry<String, String> e = it0.next();
            System.out.println("key: " + e.getKey() + " values: " + e.getValue());
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
        treeStore.close();
    }

    private final Store store;
    private final StoreLock storeLock;
    private final Session session;
    private final NodeStateEntryReader entryReader;
    private final Cache<String, NodeState> nodeStateCache = new Cache<>(10000);

    public TreeStore(File directory, NodeStateEntryReader entryReader) {
        this.entryReader = entryReader;
        String storeConfig = System.getProperty("oak.treeStoreConfig",
                "type=file\n" +
                "cacheSizeMB=4096\n" +
                "maxFileSize=64000000\n" +
                "dir=" + directory.getAbsolutePath());
        this.store = StoreBuilder.build(storeConfig);
        this.storeLock = StoreLock.lock(store);
        this.session = Session.open(store);
    }

    @Override
    public void close() throws IOException {
        storeLock.close();
        store.close();
    }

    @Override
    public Iterator<NodeStateEntry> iterator() {
        if(TreeStore.DEPTH_FIRST_TRAVERSAL) {
            return depthFirstIterator();
        }
        Iterator<Entry<String, String>> it = session.iterator();
        return new Iterator<NodeStateEntry>() {

            NodeStateEntry current;

            {
                fetch();
            }

            private void fetch() {
                while (it.hasNext()) {
                    Entry<String, String> e = it.next();
                    String path = TreeStore.convertKeyToPath(e.getKey());
                    String value = e.getValue();
                    if (value.isEmpty() || path.startsWith(":")) {
                        continue;
                    }
                    current = getNodeStateEntry(path, value);
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

    private Iterator<NodeStateEntry> depthFirstIterator() {
        return new Iterator<NodeStateEntry>() {

            Stack<Iterator<? extends ChildNodeEntry>> stack = new Stack<>();
            NodeStateEntry current;

            {
                current = getNodeStateEntry("/");
            }

            private void fetch() {
                String path = current.getPath();
                Iterable<? extends ChildNodeEntry> children = current.getNodeState().getChildNodeEntries();
                Iterator<? extends ChildNodeEntry> it = children.iterator();
                while (true) {
                    if (it.hasNext()) {
                        stack.push(it);
                        ChildNodeEntry c = it.next();
                        String name = c.getName();
                        path = PathUtils.concat(path, name);
                        current = getNodeStateEntry(path);
                        break;
                    } else {
                        if (stack.isEmpty()) {
                            current = null;
                            break;
                        }
                        it = stack.pop();
                        path = PathUtils.getParentPath(path);
                    }
                }
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

    public NodeStateEntry getNodeStateEntry(String path) {
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
        LOG.debug("getNodeState uncached {}", path);
        String value = session.get(TreeStore.convertPathToKey(path));
        if (value == null || value.isEmpty()) {
            result = EmptyNodeState.MISSING_NODE;
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

    public Session getSession() {
        return session;
    }

    public static String convertPathToKey(String path) {
        if (CHILD_ENTRIES) {
            return path;
        }
        return path.replace('/', '\t');
    }

    public static String convertKeyToPath(String key) {
        if (CHILD_ENTRIES) {
            return key;
        }
        return key.replace('\t', '/');
    }

}
