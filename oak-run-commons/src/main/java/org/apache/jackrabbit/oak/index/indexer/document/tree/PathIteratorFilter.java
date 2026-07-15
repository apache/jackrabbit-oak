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

import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;

/**
 * A utility class that allows skipping nodes that are not included in the index
 * definition.
 *
 * The use case is to speed up indexing by only traversing over the nodes that
 * are included in the set of indexes.
 *
 * The class has an efficient way to get the prefix of the _next_ included path,
 * given a path: nextIncludedPath().
 *
 * For the root node, we remember that the root node is included.
 * For all other included paths, we internally retain two prefix entries:
 * the entry itself (e.g. /content), and the entry with children (e.g. /content/).
 * This is because the alphabetically sorted list of path does not always go from parent to child.
 * As an example, the entries are sorted like this:
 *
 * - /content
 * - /content-more
 * - /content/child
 *
 * In this case, /content-more is sorted up before the direct children of /content.
 * So the next included path of /content, after /content-more, is /content/.
 */
public class PathIteratorFilter {

    private final boolean includeAll;
    private final TreeSet<String> includedPaths;

    private String cachedMatchingPrefix;

    PathIteratorFilter(SortedSet<String> includedPaths) {
        this.includedPaths = new TreeSet<>();
        for(String s : includedPaths) {
            if (PathUtils.denotesRoot(s)) {
                includedPaths.add(s);
            } else {
                this.includedPaths.add(s);
                this.includedPaths.add(s + "/");
            }
        }
        this.includeAll = includedPaths.contains(PathUtils.ROOT_PATH);
    }

    public PathIteratorFilter() {
        this.includedPaths = new TreeSet<>();
        includedPaths.add("/");
        this.includeAll = true;
    }

    /**
     * Extract all the path filters from a set of index definitions.
     *
     * @param indexDefs the index definitions
     * @return the list of path filters
     */
    public static List<PathFilter> extractPathFilters(Set<IndexDefinition> indexDefs) {
        return indexDefs.stream().map(IndexDefinition::getPathFilter).collect(Collectors.toList());
    }

    /**
     * Extract a list of included paths from a path filter. Only the top-most
     * entries are retained. Excluded path are ignored.
     *
     * @param pathFilters the path filters
     * @return the set of included path, sorted by path
     */
    public static SortedSet<String> getAllIncludedPaths(List<PathFilter> pathFilters) {
        TreeSet<String> set = new TreeSet<>();
        // convert to a flat set
        for (PathFilter f : pathFilters) {
            for (String p : f.getIncludedPaths()) {
                set.add(p);
            }
        }
        // only keep entries where the parent isn't in the set
        TreeSet<String> result = new TreeSet<>();
        for (String path : set) {
            boolean parentExists = false;
            String p = path;
            while (!PathUtils.denotesRoot(p)) {
                p = PathUtils.getParentPath(p);
                if (set.contains(p)) {
                    parentExists = true;
                    break;
                }
            }
            if (!parentExists) {
                result.add(path);
            }
        }
        return result;
    }

    public boolean includes(String path) {
        if (includeAll) {
            return true;
        }
        String cache = cachedMatchingPrefix;
        if (cache != null && path.startsWith(cache)) {
            return true;
        }
        String p = path;
        while (!PathUtils.denotesRoot(p)) {
            if (includedPaths.contains(p)) {
                // add a final slash, so that we only accept children
                String newCache = p;
                if (!newCache.endsWith("/")) {
                    newCache += "/";
                }
                cachedMatchingPrefix = newCache;
                return true;
            }
            p = PathUtils.getParentPath(p);
        }
        return false;
    }

    /**
     * Get the next higher included path, or null if none.
     *
     * @param path the path
     * @return the next included path, or null
     */
    public String nextIncludedPath(String path) {
        if (includeAll) {
            return null;
        }
        return includedPaths.higher(path);
    }

    public String toString() {
        return includedPaths.toString();
    }

}
