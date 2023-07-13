package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import java.util.HashSet;
import java.util.List;

/**
 * Remove unreferenced files from the store.
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
        if (fileName.startsWith(Session.LEAF_PREFIX)) {
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
        // TODO keep files that were very recently updated
        // (don't remove files that are part of a concurrent flush)
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
