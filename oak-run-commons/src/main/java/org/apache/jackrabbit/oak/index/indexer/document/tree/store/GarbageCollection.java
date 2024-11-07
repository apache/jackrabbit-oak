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

import java.util.HashSet;
import java.util.List;

/**
 * Remove unreferenced files from the store.
 *
 * Only root files and inner nodes are read. This is possible because leaf pages
 * do not have references to other files.
 */
public class GarbageCollection {

    private final Store store;

    public GarbageCollection(Store store) {
        this.store = store;
    }

    /**
     * Run garbage collection.
     *
     * @param rootFiles
     * @return the result
     */
    public GarbageCollectionResult run(List<String> rootFiles) {
        HashSet<String> used = mark(rootFiles);
        return sweep(used);
    }

    HashSet<String> mark(List<String> rootFiles) {
        HashSet<String> used = new HashSet<>();
        for(String root : rootFiles) {
            mark(root, used);
        }
        return used;
    }

    private void mark(String fileName, HashSet<String> used) {
        used.add(fileName);
        if (fileName.startsWith(TreeSession.LEAF_PREFIX)) {
            return;
        }
        // root or inner node
        PageFile file = store.get(fileName);
        if (file.isInnerNode()) {
            for (int i = 0; i < file.getValueCount(); i++) {
                mark(file.getChildValue(i), used);
            }
        }
    }

    private GarbageCollectionResult sweep(HashSet<String> used) {
        GarbageCollectionResult result = new GarbageCollectionResult();
        HashSet<String> removeSet = new HashSet<String>();
        for (String key: new HashSet<>(store.keySet())) {
            if (!used.contains(key)) {
                removeSet.add(key);
                result.countRemoved++;
            } else {
                result.countKept++;
            }
        }
        store.remove(removeSet);
        return result;
    }

    /**
     * Garbage collection results.
     */
    public static class GarbageCollectionResult {
        public long sizeInBytes;
        public long countRemoved;
        public long countKept;

        public String toString() {
            return "removed: " + countRemoved + " kept: " + countKept + " size: " + sizeInBytes;
        }
    }

}
