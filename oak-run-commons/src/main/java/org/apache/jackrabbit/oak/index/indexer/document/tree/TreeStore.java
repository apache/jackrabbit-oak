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
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry.NodeStateEntryBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStore;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Session;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Store;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.StoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.MemoryBoundCache;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeStore implements IndexStore {

    private static final Logger LOG = LoggerFactory.getLogger(TreeStore.class);

    private static final String STORE_TYPE = "TreeStore";
    private static final String TREE_STORE_CONFIG = "oak.treeStoreConfig";

    public static final long CACHE_SIZE_NODE_MB = 128;
    private static final long CACHE_SIZE_TREE_STORE_MB = 64;

    private static final long MAX_FILE_SIZE_MB = 8;
    private static final long MB = 1024 * 1024;

    private final String name;
    private final Store store;
    private final long cacheSizeTreeStoreMB;
    private final File directory;
    private final Session session;
    private final NodeStateEntryReader entryReader;
    private final MemoryBoundCache<String, TreeStoreNodeState> nodeStateCache;
    private long entryCount;
    private volatile String highestReadKey = "";

    public TreeStore(String name, File directory, NodeStateEntryReader entryReader, boolean smallCache) {
        this.name = name;
        this.directory = directory;
        this.entryReader = entryReader;
        long cacheSizeNodeMB = CACHE_SIZE_NODE_MB;
        long cacheSizeTreeStoreMB = CACHE_SIZE_TREE_STORE_MB;
        if (smallCache) {
            cacheSizeNodeMB = 32;
            cacheSizeTreeStoreMB = 32;
        }
        this.cacheSizeTreeStoreMB = cacheSizeTreeStoreMB;
        nodeStateCache = new MemoryBoundCache<>(cacheSizeNodeMB * MB);
        String storeConfig = System.getProperty(TREE_STORE_CONFIG,
                "type=file\n" +
                Session.CACHE_SIZE_MB + "=" + cacheSizeTreeStoreMB + "\n" +
                Store.MAX_FILE_SIZE_BYTES + "=" + MAX_FILE_SIZE_MB * MB + "\n" +
                "dir=" + directory.getAbsolutePath());
        this.store = StoreBuilder.build(storeConfig);
        this.session = new Session(store);
        // we don not want to merge too early during the download
        session.setMaxRoots(1000);
        LOG.info("Open " + toString());
    }

    @Override
    public String toString() {
        return name +
                " cache " + cacheSizeTreeStoreMB +
                " at " + highestReadKey;
    }

    public Iterator<String> pathIterator() {
        Iterator<Entry<String, String>> it = session.iterator();
        return new Iterator<String>() {

            String current;

            {
                fetch();
            }

            private void fetch() {
                while (it.hasNext()) {
                    Entry<String, String> e = it.next();
                    if (e.getValue().isEmpty()) {
                        continue;
                    }
                    current = e.getKey();
                    if (current.compareTo(highestReadKey) > 0) {
                        highestReadKey = current;
                    }
                    return;
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public String next() {
                String result = current;
                fetch();
                return result;
            }

        };
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

    public String getHighestReadKey() {
        return highestReadKey;
    }

    public NodeStateEntry getNodeStateEntry(String path) {
        return new NodeStateEntryBuilder(getNodeState(path), path).build();
    }

    NodeStateEntry getNodeStateEntry(String path, String value) {
        return new NodeStateEntryBuilder(getNodeState(path, value), path).build();
    }

    NodeState getNodeState(String path) {
        TreeStoreNodeState result = nodeStateCache.get(path);
        if (result != null) {
            return result;
        }
        String value = session.get(path);
        if (value == null || value.isEmpty()) {
            result = new TreeStoreNodeState(EmptyNodeState.MISSING_NODE, path, this, path.length() * 2);
        } else {
            result = getNodeState(path, value);
        }
        if (path.compareTo(highestReadKey) > 0) {
            highestReadKey = path;
        }
        nodeStateCache.put(path, result);
        return result;
    }

    TreeStoreNodeState getNodeState(String path, String value) {
        TreeStoreNodeState result = nodeStateCache.get(path);
        if (result != null) {
            return result;
        }
        String line = path + "|" + value;
        NodeStateEntry entry = entryReader.read(line);
        result = new TreeStoreNodeState(entry.getNodeState(), path, this, path.length() * 2 + line.length() * 10);
        if (path.compareTo(highestReadKey) > 0) {
            highestReadKey = path;
        }
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

    public Store getStore() {
        return store;
    }

    @Override
    public String getStorePath() {
        return directory.getAbsolutePath();
    }

    @Override
    public long getEntryCount() {
        return entryCount;
    }

    public void setEntryCount(long entryCount) {
        this.entryCount = entryCount;
    }

    @Override
    public String getIndexStoreType() {
        return STORE_TYPE;
    }

    @Override
    public boolean isIncremental() {
        // TODO support diff and use Session.checkpoint() / flush().
        return false;
    }

}
