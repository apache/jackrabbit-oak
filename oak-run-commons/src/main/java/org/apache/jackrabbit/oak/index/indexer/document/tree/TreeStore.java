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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry.NodeStateEntryBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStore;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.TreeSession;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.Store;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.StoreBuilder;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.FilePacker;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.SieveCache;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The tree store is similar to the flat file store, but instead of storing all
 * key-value pairs in a single file, it stores the entries in multiple files
 * (except if there are very few nodes).
 */
public class TreeStore implements ParallelIndexStore {

    private static final Logger LOG = LoggerFactory.getLogger(TreeStore.class);

    public static final String DIRECTORY_NAME = "tree";

    private static final String STORE_TYPE = "TreeStore";
    private static final String TREE_STORE_CONFIG = "oak.treeStoreConfig";

    public static final long CACHE_SIZE_NODE_MB = 64;
    private static final long CACHE_SIZE_TREE_STORE_MB = 64;

    private static final long MAX_FILE_SIZE_MB = 4;
    private static final long MB = 1024 * 1024;

    private static final int SPLIT_POINT_COUNT = 512;

    private final String name;
    private final Store store;
    private final long cacheSizeTreeStoreMB;
    private final File fileOrDirectory;
    private final boolean readOnly;
    private final TreeSession session;
    private final NodeStateEntryReader entryReader;
    private final SieveCache<String, TreeStoreNodeState> nodeStateCache;
    private long entryCount;
    private volatile String highestReadKey = "";
    private final AtomicLong nodeCacheHits = new AtomicLong();
    private final AtomicLong nodeCacheMisses = new AtomicLong();
    private final AtomicLong nodeCacheFills = new AtomicLong();
    private int iterationCount;
    private PathIteratorFilter filter = new PathIteratorFilter();
    private ArrayList<String> splitPoints;

    // the prefetcher, if any. we keep a references so we can shut it down on close
    private Prefetcher prefetcher;

    public TreeStore(String name, File fileOrDirectory, NodeStateEntryReader entryReader, long cacheSizeFactor) {
        this.name = name;
        this.fileOrDirectory = fileOrDirectory;
        this.entryReader = entryReader;
        long cacheSizeNodeMB = cacheSizeFactor * CACHE_SIZE_NODE_MB;
        long cacheSizeTreeStoreMB = cacheSizeFactor * CACHE_SIZE_TREE_STORE_MB;
        this.cacheSizeTreeStoreMB = cacheSizeTreeStoreMB;
        nodeStateCache = new SieveCache<>(cacheSizeFactor * cacheSizeNodeMB * MB);
        String storeConfig;
        if (FilePacker.isPackFile(fileOrDirectory)) {
            readOnly = true;
            storeConfig = System.getProperty(TREE_STORE_CONFIG,
                    "type=pack\n" +
                    TreeSession.CACHE_SIZE_MB + "=" + cacheSizeTreeStoreMB + "\n" +
                    Store.MAX_FILE_SIZE_BYTES + "=" + MAX_FILE_SIZE_MB * MB + "\n" +
                    "file=" + fileOrDirectory.getAbsolutePath());
        } else {
            readOnly = false;
            storeConfig = System.getProperty(TREE_STORE_CONFIG,
                    "type=file\n" +
                    TreeSession.CACHE_SIZE_MB + "=" + cacheSizeTreeStoreMB + "\n" +
                    Store.MAX_FILE_SIZE_BYTES + "=" + MAX_FILE_SIZE_MB * MB + "\n" +
                    "dir=" + fileOrDirectory.getAbsolutePath());
        }
        this.store = StoreBuilder.build(storeConfig);
        store.setWriteCompression(Compression.LZ4);
        this.session = new TreeSession(store);
        // we don not want to merge too early during the download
        session.setMaxRoots(1000);
        LOG.info("Open " + toString());
    }

    public void init() {
        session.init();
    }

    public void setIndexDefinitions(Set<IndexDefinition> indexDefs) {
        if (indexDefs == null) {
            return;
        }
        List<PathFilter> pathFilters = PathIteratorFilter.extractPathFilters(indexDefs);
        SortedSet<String> includedPaths = PathIteratorFilter.getAllIncludedPaths(pathFilters);
        setIncludedPaths(includedPaths);
    }

    public void setIncludedPaths(SortedSet<String> includedPaths) {
        LOG.info("Included paths {}", includedPaths.toString());
        filter = new PathIteratorFilter(includedPaths);
    }

    @Override
    public String toString() {
        return name +
                " cache " + cacheSizeTreeStoreMB +
                " at " + highestReadKey +
                " cache-hits " + nodeCacheHits.get() +
                " cache-misses " + nodeCacheMisses.get() +
                " cache-fills " + nodeCacheFills.get();
    }

    public Iterator<String> iteratorOverPaths() {
        return iteratorOverPaths(null, null);
    }

    private Iterator<String> iteratorOverPaths(String start, String end) {
        startPrefetch();
        final Iterator<Entry<String, String>> firstIterator = session.iterator(start);
        return new Iterator<String>() {

            Iterator<Entry<String, String>> it = firstIterator;
            String current;

            {
                fetch();
            }

            private void fetch() {
                while (it.hasNext()) {
                    Entry<String, String> e = it.next();
                    String key = e.getKey();
                    if (end != null && key.compareTo(end) > 0) {
                        break;
                    }
                    String value = e.getValue();
                    // if the value is empty (not null!) this is a child node reference,
                    // without node data
                    if (value.isEmpty()) {
                        continue;
                    }
                    if (!filter.includes(key)) {
                        // if the path is not, see if there is a next included path
                        String next = filter.nextIncludedPath(key);
                        if (next == null) {
                            // it was the last one
                            break;
                        }
                        // this node itself could be a match
                        key = next;
                        value = session.get(key);
                        // for the next fetch operation, we use the new iterator
                        it = session.iterator(next);
                        if (value == null || value.isEmpty()) {
                            // no such node, or it's a child node reference
                            continue;
                        }
                    }
                    if (++iterationCount % 1_000_000 == 0) {
                        LOG.info("Fetching {} in {}", iterationCount, TreeStore.this.toString());
                    }
                    current = key;
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
    public Iterator<NodeStateEntry> iterator() {
        startPrefetch();
        return iterator(null, null);
    }

    private Iterator<NodeStateEntry> iterator(String start, String end) {
        Iterator<String> it = iteratorOverPaths(start, end);
        return new Iterator<NodeStateEntry>() {

            NodeStateEntry current;

            {
                fetch();
            }

            private void fetch() {
                current = it.hasNext() ? getNodeStateEntry(it.next()) : null;
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

    @Override
    public void close() throws IOException {
        LOG.info("Closing " + toString());
        session.flush();
        store.close();
        if (prefetcher != null) {
            prefetcher.shutdown();
        }
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
            nodeCacheHits.incrementAndGet();
            return result;
        }
        nodeCacheMisses.incrementAndGet();
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
            nodeCacheHits.incrementAndGet();
            return result;
        }
        nodeCacheMisses.incrementAndGet();
        result = buildNodeState(path, value);
        if (path.compareTo(highestReadKey) > 0) {
            highestReadKey = path;
        }
        nodeStateCache.put(path, result);
        return result;
    }

    TreeStoreNodeState buildNodeState(String path, String value) {
        String line = path + "|" + value;
        NodeStateEntry entry = entryReader.read(line);
        return new TreeStoreNodeState(entry.getNodeState(), path, this, path.length() * 2 + line.length() * 10);
    }

    public void prefillCache(String path, TreeStoreNodeState nse) {
        TreeStoreNodeState old = nodeStateCache.put(path, nse);
        if (old == null) {
            nodeCacheFills.incrementAndGet();
        }
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
     * Convert a child node entry to parent and node name.
     * This method is used for tooling and testing only.
     * It does the reverse of toChildNodeEntry(parentPath, childName)
     *
     * @param child node entry, e.g. /hello<tab>world
     * @return the parent path and the child node name, e.g. ["/hello" "world"]
     * @throws IllegalArgumentException if this is not a child node entry
     */
    public static String[] toParentAndChildNodeName(String key) {
        int index = key.lastIndexOf('\t');
        if (index < 0) {
            throw new IllegalArgumentException("Not a child node entry: " + key);
        }
        return new String[] { key.substring(0, index), key.substring(index + 1) };
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

    /**
     * Remove a node if it exists.
     *
     * @param path the path
     */
    public void removeNode(String path) {
        if (readOnly) {
            throw new IllegalStateException("Read only store");
        }
        session.put(path, null);
        if (!path.equals("/")) {
            String nodeName = PathUtils.getName(path);
            String parentPath = PathUtils.getParentPath(path);
            session.put(parentPath + "\t" + nodeName, null);
        }
    }

    /**
     * Add or update a node.
     *
     * @param path the path
     * @param json the property data
     */
    public void putNode(String path, String json) {
        if (readOnly) {
            throw new IllegalStateException("Read only store");
        }
        if (json == null) {
            throw new IllegalStateException("Value is null");
        }
        session.put(path, json);
        if (!path.equals("/")) {
            String nodeName = PathUtils.getName(path);
            String parentPath = PathUtils.getParentPath(path);
            session.put(parentPath + "\t" + nodeName, "");
        }
    }

    public TreeSession getSession() {
        return session;
    }

    public Store getStore() {
        return store;
    }

    @Override
    public String getStorePath() {
        return fileOrDirectory.getAbsolutePath();
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
        return false;
    }

    @Override
    public Collection<IndexStore> buildParallelStores(int pieces) {
        LOG.info("Building in {} pieces", pieces);
        this.splitPoints = new ArrayList<>();
        splitPoints.add(session.getMinKey());
        splitPoints.add(session.getMaxKey());
        Random r = new Random(1);
        // split in a loop, doubling the number of entries each time
        while (splitPoints.size() < SPLIT_POINT_COUNT) {
            ArrayList<String> nextSplitPoints = new ArrayList<>();
            for (int i = 1; i < splitPoints.size(); i++) {
                String min = splitPoints.get(i - 1);
                String max = splitPoints.get(i);
                String mid = session.getApproximateMedianKey(min, max, r);
                if (mid.compareTo(min) < 0) {
                    throw new IllegalStateException();
                }
                if (mid.compareTo(max) > 0) {
                    throw new IllegalStateException();
                }
                nextSplitPoints.add(min);
                nextSplitPoints.add(mid);
            }
            nextSplitPoints.add(splitPoints.get(splitPoints.size() - 1));
            splitPoints = nextSplitPoints;
        }
        // remove duplicates (in case there are few entries)
        for (int i = 1; i < splitPoints.size(); i++) {
            String prev = splitPoints.get(i - 1);
            String current = splitPoints.get(i);
            if (prev.equals(current)) {
                splitPoints.remove(i);
                i--;
            } else if (prev.compareTo(current) > 0) {
                throw new IllegalStateException();
            }
        }
        splitPoints.add(0, null);
        splitPoints.add(null);
        ArrayList<IndexStore> result = new ArrayList<>();
        AtomicInteger openCount = new AtomicInteger(pieces);
        for (int i = 0; i < pieces; i++) {
            result.add(new ParallelTreeStore(this, openCount));
        }
        return result;
    }

    public synchronized Iterator<NodeStateEntry> nextSubsetIterator() {
        if (splitPoints.size() < 2) {
            return null;
        }
        String start = splitPoints.get(0);
        String end = splitPoints.get(1);
        splitPoints.remove(0);
        return iterator(start, end);
    }

    private void startPrefetch() {
        if (prefetcher != null) {
            prefetcher.start();
        }
    }

    public void setPrefetcher(Prefetcher prefetcher) {
        this.prefetcher = prefetcher;
    }

}
