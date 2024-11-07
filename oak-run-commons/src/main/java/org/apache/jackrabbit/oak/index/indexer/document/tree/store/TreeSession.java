/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Random;

import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.ConcurrentLRUCache;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.Position;
import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.SortedStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A session that allows reading and writing keys and values in a tree store.
 */
public class TreeSession {

    private static final Logger LOG = LoggerFactory.getLogger(TreeSession.class);

    public static final String CACHE_SIZE_MB = "cacheSizeMB";
    private static final int DEFAULT_CACHE_SIZE_MB = 256;
    private static final int DEFAULT_MAX_ROOTS = 10;

    public static final String ROOT_NAME = "root";
    public static final String LEAF_PREFIX = "data_";
    static final String INNER_NODE_PREFIX = "node_";
    static final String DELETED = new String("DELETED");

    static final boolean MULTI_ROOT = true;

    private final Store store;
    private final ConcurrentLRUCache<String, PageFile> cache;
    private long updateId;
    private int maxRoots = DEFAULT_MAX_ROOTS;
    private long fileReadCount;

    public TreeSession() {
        this(new MemoryStore(new Properties()));
    }

    public static String getFileNameRegex() {
        return "(" + ROOT_NAME + ".*|" +
                LEAF_PREFIX + ".*|" +
                INNER_NODE_PREFIX + ".*)";
    }

    public TreeSession(Store store) {
        this.store = store;
        long cacheSizeMB = Long.parseLong(store.getConfig().getProperty(
                CACHE_SIZE_MB, "" + DEFAULT_CACHE_SIZE_MB));
        long cacheSizeBytes = cacheSizeMB * 1024 * 1024;
        LOG.info("Cache size {} bytes", cacheSizeBytes);
        this.cache = new ConcurrentLRUCache<>(cacheSizeBytes)  {

            private static final long serialVersionUID = 1L;

            @Override
            public void entryWasRemoved(String key, PageFile value) {
                if (value.isModified()) {
                    store.put(key, value);
                    // not strictly needed as it's no longer referenced
                    value.setModified(false);
                }
            }
        };
    }

    /**
     * Set the maximum number of roots.
     *
     * @param maxRoots the new value
     */
    public void setMaxRoots(int maxRoots) {
        this.maxRoots = maxRoots;
    }

    public int getMaxRoots() {
        return maxRoots;
    }

    /**
     * Get the number of files read from the cache or storage.
     *
     * @return the result
     */
    public long getFileReadCount() {
        return fileReadCount;
    }

    private List<String> getRootFileNames() {
        LinkedHashSet<String> result = new LinkedHashSet<>();
        String nextRoot = ROOT_NAME;
        do {
            boolean isNew = result.add(nextRoot);
            if (!isNew) {
                throw new IllegalStateException("Linked list contains a loop");
            }
            PageFile root = getFile(nextRoot);
            nextRoot = root.getNextRoot();
        } while (nextRoot != null);
        return new ArrayList<>(result);
    }

    private void mergeRootsIfNeeded() {
        List<String> roots = getRootFileNames();
        if (roots.size() > maxRoots) {
            mergeRoots(Integer.MAX_VALUE);
        }
    }

    /**
     * Initialize the storage, creating a new root if needed.
     */
    public void init() {
        PageFile root = store.getIfExists(ROOT_NAME);
        if (root == null) {
            root = newPageFile(false);
            putFile(ROOT_NAME, root);
        }
    }

    private PageFile copyPageFile(PageFile old) {
        PageFile result = old.copy();
        result.setUpdate(updateId);
        return result;
    }

    private PageFile newPageFile(boolean isInternalNode) {
        PageFile result = new PageFile(isInternalNode, store.getMaxFileSizeBytes());
        result.setUpdate(updateId);
        return result;
    }

    /**
     * Get an entry.
     *
     * @param key the key
     * @return the value, or null
     */
    public String get(String key) {
        if (key == null) {
            throw new NullPointerException();
        }
        String fileName = ROOT_NAME;
        do {
            PageFile file = getFile(fileName);
            String nextRoot = file.getNextRoot();
            String result = get(file, key);
            if (result != null) {
                return result == DELETED ? null : result;
            }
            fileName = nextRoot;
        } while (fileName != null);
        return null;
    }

    /**
     * Get the entry if it exists.
     *
     * @param root the root file
     * @param k the key
     * @return null if not found, DELETED if removed, or the value
     */
    private String get(PageFile root, String k) {
        while (true) {
            if (!root.isInnerNode()) {
                int index = root.getKeyIndex(k);
                if (index >= 0) {
                    String result = root.getValue(index);
                    return result == null ? DELETED : result;
                }
                return null;
            }
            int index = root.getKeyIndex(k);
            if (index < 0) {
                index = -index - 2;
            }
            index++;
            String fileName = root.getChildValue(index);
            root = getFile(fileName);
            // continue with the new file
        }
    }

    /**
     * Put a value.
     * To remove an entry, the value needs to be null.
     *
     * @param key the key
     * @param value the value
     */
    public void put(String key, String value) {
        if (key == null) {
            throw new NullPointerException();
        }
        if (value == null) {
            PageFile root = getFile(ROOT_NAME);
            if (root.getNextRoot() != null) {
                value = DELETED;
            }
        }
        put(ROOT_NAME, key, value);
    }

    /**
     * Put a value.
     *
     * @param rootFileName
     * @param key
     * @param value (DELETED if we need to put that, or null to remove the entry)
     * @return the file name of the root (different than the passed file name, if the file was copied)
     */
    private void put(String rootFileName, String key, String value) {
        String fileName = rootFileName;
        PageFile file = getFile(fileName);
        if (file.getUpdate() < updateId) {
            fileName = store.newFileName();
            file = copyPageFile(file);
            putFile(fileName, file);
        }
        ArrayList<String> parents = new ArrayList<>();
        String k = key;
        while (true) {
            int index = file.getKeyIndex(k);
            if (!file.isInnerNode()) {
                if (index >= 0) {
                    if (value == null) {
                        file.removeRecord(index);
                    } else {
                        file.setValue(index, value == DELETED ? null : value);
                    }
                } else {
                    // not found
                    if (value == null) {
                        // nothing to do
                        return;
                    }
                    file.insertRecord(-index - 1, k, value == DELETED ? null : value);
                }
                break;
            }
            parents.add(fileName);
            if (index < 0) {
                index = -index - 2;
            }
            index++;
            fileName = file.getChildValue(index);
            file = getFile(fileName);
            // continue with the new file
        }
        putFile(fileName, file);
        splitOrMerge(fileName, file, parents);
    }

    private void splitOrMerge(String fileName, PageFile file, ArrayList<String> parents) {
        int size = file.sizeInBytes();
        if (size > store.getMaxFileSizeBytes() && file.canSplit()) {
            split(fileName, file, parents);
        } else if (file.getKeys().size() == 0) {
            merge(fileName, file, parents);
        }
    }

    private void merge(String fileName, PageFile file, ArrayList<String> parents) {
        if (file.getValueCount() > 0) {
            return;
        }
        if (parents.isEmpty()) {
            // root: ignore
            return;
        }
        String parentFileName = parents.remove(parents.size() - 1);
        PageFile parentFile = getFile(parentFileName);
        for (int i = 0; i < parentFile.getValueCount(); i++) {
            String pf = parentFile.getChildValue(i);
            if (pf.equals(fileName)) {
                if (parentFile.getValueCount() == 1) {
                    parentFile = newPageFile(false);
                    if (!parentFileName.startsWith(ROOT_NAME)) {
                        String newParentFileName = LEAF_PREFIX + parentFileName.substring(INNER_NODE_PREFIX.length());
                        putFile(newParentFileName, parentFile);
                        updateChildFileName(parents.get(parents.size() - 1), parentFileName, newParentFileName);
                        parentFileName = newParentFileName;
                    }
                } else if (i == parentFile.getValueCount() - 1) {
                    // remove the last entry
                    parentFile.removeKey(i - 1);
                    parentFile.removeValue(i);
                } else {
                    parentFile.removeKey(i);
                    parentFile.removeValue(i);
                }
                putFile(parentFileName, parentFile);
                merge(parentFileName, parentFile, parents);
                break;
            }
        }
    }

    private void updateChildFileName(String fileName, String oldChild, String newChild) {
        PageFile file = getFile(fileName);
        for (int i = 0; i < file.getValueCount(); i++) {
            if (file.getChildValue(i).equals(oldChild)) {
                file.setValue(i, newChild);
                putFile(fileName, file);
                return;
            }
        }
    }

    private void split(String fileName, PageFile file, ArrayList<String> parents) {
        List<String> keys = new ArrayList<>(file.getKeys());
        String parentFileName, newFileName1, newFileName2;
        PageFile parentFile, newFile1, newFile2;
        boolean isInternalNode = file.isInnerNode();
        if (parents.isEmpty()) {
            // new root
            parentFileName = fileName;
            parentFile = newPageFile(true);
            parentFile.setNextRoot(file.getNextRoot());
            newFileName1 = (isInternalNode ? INNER_NODE_PREFIX : LEAF_PREFIX) +
                    store.newFileName();
            parentFile.addChild(0, null, newFileName1);
        } else {
            parentFileName = parents.remove(parents.size() - 1);
            parentFile = getFile(parentFileName);
            newFileName1 = fileName;
        }
        newFile1 = newPageFile(isInternalNode);
        newFileName2 = (isInternalNode ? INNER_NODE_PREFIX : LEAF_PREFIX) +
                store.newFileName();
        newFile2 = newPageFile(isInternalNode);
        int sentinelIndex = keys.size() / 2;
        String sentinel = keys.get(sentinelIndex);
        // shorten the sentinel if possible
        String beforeSentinal = keys.get(sentinelIndex - 1);
        while (sentinel.length() > 0 && !isInternalNode) {
            // for internal nodes, there might be other keys on the left side
            // that might be shoter than the entry before the sentinel
            String oneShorter = sentinel.substring(0, sentinel.length() - 1);
            if (beforeSentinal.compareTo(oneShorter) >= 0) {
                break;
            }
            sentinel = oneShorter;
        }
        if (!isInternalNode) {
            // leaf
            for (int i = 0; i < keys.size() / 2; i++) {
                String k = keys.get(i);
                String v = file.getValue(i);
                newFile1.appendRecord(k, v);
            }
            for (int i = keys.size() / 2; i < keys.size(); i++) {
                String k = keys.get(i);
                String v = file.getValue(i);
                newFile2.appendRecord(k, v);
            }
        } else {
            // inner node
            newFile1.addChild(0, null, file.getChildValue(0));
            for (int i = 1; i <= keys.size() / 2; i++) {
                String p = keys.get(i - 1);
                newFile1.appendRecord(p, file.getChildValue(i));
            }
            newFile2.addChild(0, null, file.getChildValue(keys.size() / 2 + 1));
            for (int i = keys.size() / 2 + 2; i <= keys.size(); i++) {
                String p = keys.get(i - 1);
                newFile2.appendRecord(p, file.getChildValue(i));
            }
        }
        // insert sentinel into parent
        int index = parentFile.getKeyIndex(sentinel);
        parentFile.addChild(-index, sentinel, newFileName2);
        putFile(newFileName1, newFile1);
        putFile(newFileName2, newFile2);
        putFile(parentFileName, parentFile);
        splitOrMerge(parentFileName, parentFile, parents);
    }

    private void putFile(String fileName, PageFile file) {
        if (!file.isModified()) {
            throw new AssertionError();
        }
        file.setFileName(fileName);
        cache.put(fileName, file);
    }

    private PageFile getFile(String key) {
        fileReadCount++;
        PageFile result = cache.get(key);
        if (result == null) {
            result = store.get(key);
            result.setFileName(key);
            cache.put(key, result);
        }
        return result;
    }

    /**
     * Merge a number of roots. Merging will create a new root, which contains the
     * data of the previous roots. If there are less roots, this method returns without merging.
     *
     * @param max the number of roots to merge (Integer.MAX_VALUE for all)
     */
    public void mergeRoots(int max) {
        List<String> list = getRootFileNames();
        if (list.size() <= 1 || (max != Integer.MAX_VALUE && list.size() < max)) {
            return;
        }
        PageFile root = getFile(ROOT_NAME);
        String rootFileCopy = ROOT_NAME + "_" + updateId;
        root = copyPageFile(root);
        root.setModified(true);
        putFile(rootFileCopy, root);
        Iterator<Entry<String, String>> it = iterator(null, max);
        PageFile newRoot = newPageFile(false);
        newRoot.setNextRoot(rootFileCopy);
        putFile(ROOT_NAME, newRoot);
        while (it.hasNext()) {
            Entry<String, String> e = it.next();
            put(e.getKey(), e.getValue());
            // we can remove files that are processed
        }
        newRoot = getFile(ROOT_NAME);
        if (max < list.size()) {
            newRoot.setNextRoot(list.get(max));
        } else {
            newRoot.setNextRoot(null);
        }
        flush();
    }

    /**
     * Get the number of roots.
     *
     * @return the number of roots
     */
    public int getRootCount() {
        return getRootFileNames().size();
    }

    /**
     * Make the current tree read-only and switch to a new root.
     * If there are already too many roots, then they will be merged.
     * All changes are flushed to storage.
     */
    public void checkpoint() {
        flush();
        mergeRootsIfNeeded();
        List<String> roots = getRootFileNames();
        if (roots.size() > 1) {
            // get the last root
            for (String s : roots) {
                int index = s.lastIndexOf('_');
                if (index >= 0) {
                    updateId = Math.max(updateId, Long.parseLong(s.substring(index + 1)));
                }
            }
            updateId++;
        }
        PageFile root = getFile(ROOT_NAME);
        // cache.remove(ROOT_NAME);
        String rootFileCopy = ROOT_NAME + "_" + updateId;
        root = copyPageFile(root);
        root.setFileName(rootFileCopy);
        putFile(rootFileCopy, root);
        updateId++;
        if (MULTI_ROOT) {
            root = newPageFile(false);
            root.setNextRoot(rootFileCopy);
            putFile(ROOT_NAME, root);
            // need to flush here
            // so that GC does not collect rootFileCopy
            flush();
            root = copyPageFile(root);
            putFile(ROOT_NAME, root);
        } else {
            flush();
            root = copyPageFile(root);
            putFile(ROOT_NAME, root);
        }
    }

    /**
     * Flush all changes to storage.
     */
    public void flush() {
        // we store all the pages except for the root, and the root at the very end
        // this is to get a smaller chance that the root is stored,
        // and points to a page that doesn't exist yet -
        // but we don't try to ensure this completely;
        // stored inner nodes might point to pages that are not stored yet
        PageFile changedRoot = null;
        for(String k : cache.keys()) {
            PageFile v = cache.get(k);
            if (v == null || !v.isModified()) {
                continue;
            }
            if (k.equals(ROOT_NAME)) {
                // don't store the changed root yet
                changedRoot = v;
            } else {
                store.put(k, v);
                // here we have to reset the flag
                v.setModified(false);
            }
        }
        // now store the changed root
        if (changedRoot != null) {
            store.put(ROOT_NAME, changedRoot);
            // here we have to reset the flag
            changedRoot.setModified(false);
        }
    }

    // ===============================================================
    // iteration over entries
    // this is fast: internally, a stack of Position object is kept

    /**
     * Get all entries. Do not add or move entries while
     * iterating.
     *
     * @return the result
     */
    public Iterator<Entry<String, String>> iterator() {
        return iterator(null);
    }

    /**
     * Get all entries. Do not add or move entries while iterating.
     *
     * @param largerThan all returned keys are larger than this; null to start at
     *                   the beginning
     * @return the result
     */
    public Iterator<Entry<String, String>> iterator(String largerThan) {
        return iterator(largerThan, Integer.MAX_VALUE);
    }

    public Iterable<Entry<String, String>> entrySet() {
        return new Iterable<Entry<String, String>>() {

            @Override
            public Iterator<Entry<String, String>> iterator() {
                return TreeSession.this.iterator();
            }

        };
    }

    private Iterator<Entry<String, String>> iterator(String largerThan, int maxRootCount) {
        ArrayList<SortedStream> streams = new ArrayList<>();
        String next = ROOT_NAME;
        for (int i = 0; i < maxRootCount; i++) {
            streams.add(new SortedStream(i, next, immutableRootIterator(next, largerThan)));
            next = getFile(next).getNextRoot();
            if (next == null) {
                break;
            }
        }
        PriorityQueue<SortedStream> pq = new PriorityQueue<>(streams);
        return new Iterator<Entry<String, String>>() {

            Entry<String, String> current;
            String lastKey;

            {
                fetchNext();
            }

            private void fetchNext() {
                while (pq.size() > 0) {
                    SortedStream s = pq.poll();
                    String key = s.currentKeyOrNull();
                    if (key == null) {
                        // if this is null, it must be the last stream
                        break;
                    }
                    if (key.equals(lastKey)) {
                        s.next();
                        pq.add(s);
                        continue;
                    }
                    String value = s.currentValue();
                    s.next();
                    pq.add(s);
                    if (value == DELETED) {
                        continue;
                    }
                    lastKey = key;
                    current = new Entry<>() {

                        @Override
                        public String getKey() {
                            return key;
                        }

                        @Override
                        public String getValue() {
                            return value;
                        }

                        @Override
                        public String setValue(String value) {
                            throw new UnsupportedOperationException();
                        }

                    };
                    return;
                }
                current = null;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public Entry<String, String> next() {
                Entry<String, String> result = current;
                fetchNext();
                return result;
            }

        };
    }

    private Iterator<Position> immutableRootIterator(String rootFileName, String largerThan) {

        return new Iterator<Position>() {
            private final ArrayList<Position> stack = new ArrayList<>();
            private Position current;

            {
                current = new Position();
                current.file = getFile(rootFileName);
                current.valuePos = index(current.file, largerThan);
                down(largerThan);
                if (current.valuePos >= current.file.getValueCount()) {
                    next();
                }
            }

            private int index(PageFile file, String largerThan) {
                if (largerThan == null) {
                    return 0;
                }
                int index = file.getKeyIndex(largerThan);
                if (file.isInnerNode()) {
                    if (index < 0) {
                        index = -index - 2;
                    }
                    index++;
                } else {
                    if (index < 0) {
                        index = -index - 1;
                    } else {
                        index++;
                    }
                }
                return index;
            }

            @Override
            public String toString() {
                return stack + " " + current;
            }

            @Override
            public boolean hasNext() {
                return current != null;
            }

            @Override
            public Position next() {
                if (current == null) {
                    throw new NoSuchElementException();
                }
                Position result = current;
                current = new Position();
                current.file = result.file;
                current.valuePos = result.valuePos + 1;
                while (true) {
                    if (!current.file.isInnerNode() && current.valuePos < result.file.getValueCount()) {
                        break;
                    }
                    if (stack.size() == 0) {
                        current = null;
                        break;
                    }
                    current = stack.remove(stack.size() - 1);
                    current.valuePos++;
                    if (current.valuePos < current.file.getValueCount()) {
                        down(null);
                        break;
                    }
                }
                return result;
            }

            private void down(String largerThan) {
                while (current.file.isInnerNode()) {
                    stack.add(current);
                    Position pos = new Position();
                    PageFile file = getFile(current.file.getChildValue(current.valuePos));
                    pos.file = file;
                    pos.valuePos = index(pos.file, largerThan);
                    current = pos;
                }
            }
        };
    }

    // ===============================================================
    // iteration over keys, over all roots
    // this is a bit slow: internally, *for each key*
    // it will traverse from all roots down to the leaf

    /**
     * Return all keys in sorted order. Roots don't need to be merged.
     *
     * @return all keys
     */
    public Iterable<String> keys() {
        return keys(null);
    }

    /**
     * Return all keys in sorted order.
     *
     * @param largerThan all returned keys are larger than this; null to start at
     *                   the beginning
     * @return the keys
     */
    public Iterable<String> keys(String largerThan) {
        final String next = getNextKey(largerThan);
        return new Iterable<String>() {

            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {

                    private String current = next;

                    @Override
                    public boolean hasNext() {
                        return current != null;
                    }

                    @Override
                    public String next() {
                        if (current == null) {
                            throw new NoSuchElementException();
                        }
                        String result = current;
                        current = getNextKey(current);
                        return result;
                    }

                };
            }

        };
    }

    private String getNextKey(String largerThan) {
        if (MULTI_ROOT) {
            String fileName = ROOT_NAME;
            String result = null;
            do {
                String next = getNextKey(largerThan, fileName);
                if (result == null) {
                    result = next;
                } else if (next != null && next.compareTo(result) < 0) {
                    result = next;
                }
                PageFile file = getFile(fileName);
                fileName = file.getNextRoot();
            } while (fileName != null);
            return result;
        } else {
            return getNextKey(largerThan, ROOT_NAME);
        }
    }

    private String getNextKey(String largerThan, String fileName) {
        PageFile file = getFile(fileName);
        if (!file.isInnerNode()) {
            String nextKey = file.getNextKey(largerThan);
            if (nextKey != null) {
                return nextKey;
            }
            return null;
        }
        int index;
        index = largerThan == null ? -1 : file.getKeyIndex(largerThan);
        if (index < 0) {
            index = -index - 2;
        }
        index++;
        for (; index < file.getValueCount(); index++) {
            fileName = file.getChildValue(index);
            String result = getNextKey(largerThan, fileName);
            if (result != null) {
                return result;
            }
        }
        return null;
    }

    // ===============================================================
    // garbage collection

    public void runGC() {
        flush();
        new GarbageCollection(store).run(getRootFileNames());
    }

    // ===============================================================
    // info

    public String getInfo() {
        StringBuilder buff = new StringBuilder();
        GarbageCollection gc = new GarbageCollection(store);
        int rootId = 0;
        for (String r : getRootFileNames()) {
            if (store.getIfExists(r) == null) {
                // not yet stored - ignore
                continue;
            }
            buff.append(String.format("root #%d contains %d files (file name %s)\n",
                    rootId, gc.mark(Collections.singletonList(r)).size(), r));
            rootId++;
        }
        buff.append(cache.toString());
        return buff.toString();
    }

    // ===============================================================
    // partitioning

    public String getMinKey() {
        return getNextKey(null);
    }

    public String getMaxKey() {
        if (getFile(ROOT_NAME).getNextRoot() != null) {
            throw new UnsupportedOperationException("Not fully merged");
        }
        String fileName = ROOT_NAME;
        while (true) {
            PageFile file = getFile(fileName);
            if (!file.isInnerNode()) {
                return file.getKey(file.getKeys().size() - 1);
            }
            fileName = file.getChildValue(file.getValueCount() - 1);
        }
    }

    public String getApproximateMedianKey(String low, String high, Random r) {
        if (getFile(ROOT_NAME).getNextRoot() != null) {
            throw new UnsupportedOperationException("Not fully merged");
        }
        String fileName = ROOT_NAME;
        if (low.compareTo(high) >= 0) {
            return low;
        }
        while (true) {
            PageFile file = getFile(fileName);
            int i1 = file.getKeyIndex(low);
            int i2 = file.getKeyIndex(high);
            if (i1 < 0) {
                i1 = -i1 - 1;
            }
            if (i2 < 0) {
                i2 = -i2 - 1;
            }
            int middle = Math.min(file.getValueCount() - 1, (i1 + i2) / 2);
            if (middle != i1 || !file.isInnerNode()) {
                return file.getKey(middle);
            }
            if (middle >= file.getValueCount() - 1) {
                fileName = file.getChildValue(middle);
            } else if (high.compareTo(file.getKey(middle)) <= 0) {
                fileName = file.getChildValue(middle);
            } else if (low.compareTo(file.getKey(middle)) >= 0) {
                fileName = file.getChildValue(middle + 1);
            } else if (r.nextInt() > 0) {
                fileName = file.getChildValue(middle);
            } else {
                fileName = file.getChildValue(middle + 1);
            }
        }
    }

}
