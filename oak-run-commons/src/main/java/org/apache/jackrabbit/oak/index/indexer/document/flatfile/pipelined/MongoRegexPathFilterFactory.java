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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

public class MongoRegexPathFilterFactory {

    public static class MongoFilterPaths {
        // Special value that means "download everything". This is used when regex path filtering is disabled or when
        // the path filters would require downloading everything. When this is returned, it is preferable to do not
        // use any filter in the Mongo query, even though using the values in this object as filters would also result
        // in downloading everything
        public static final MongoFilterPaths DOWNLOAD_ALL = new MongoFilterPaths(List.of("/"), List.of());

        final List<String> included;
        final List<String> excluded;

        public MongoFilterPaths(List<String> included, List<String> excluded) {
            this.included = included;
            this.excluded = excluded;
        }

        @Override
        public String toString() {
            return "MongoFilterPaths{" +
                    "included=" + included +
                    ", excluded=" + excluded +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MongoFilterPaths that = (MongoFilterPaths) o;
            return Objects.equals(included, that.included) && Objects.equals(excluded, that.excluded);
        }

        @Override
        public int hashCode() {
            return Objects.hash(included, excluded);
        }
    }

    private final static Logger LOG = LoggerFactory.getLogger(MongoRegexPathFilterFactory.class);

    private final int maxNumberOfPaths;

    public MongoRegexPathFilterFactory(int maxNumberOfPaths) {
        this.maxNumberOfPaths = maxNumberOfPaths;
    }

    public MongoFilterPaths buildMongoFilter(List<PathFilter> pathFilters) {
        return buildMongoFilter(pathFilters, List.of());

    }

    /**
     * @param pathFilters         Filters representing the included/excludedPaths settings of the indexes
     * @param customExcludedPaths Additional excluded paths set in the Oak configuration.
     * @return Paths to include and exclude in the Mongo query
     */
    public MongoFilterPaths buildMongoFilter(List<PathFilter> pathFilters, List<String> customExcludedPaths) {
        if (pathFilters == null || pathFilters.isEmpty()) {
            return MongoFilterPaths.DOWNLOAD_ALL;
        }
        // Merge and deduplicate the included and excluded paths of all filters.
        // The included paths will also be further de-deuplicated, by removing paths that are children of other paths in
        // the list, that is, by keeping only the top level paths. To make it easy to de-duplicate, we sort the paths
        // alphabetically.
        // The list of excluded paths does not need to be sorted, but we do it anyway for consistency when logging the
        // list of paths.
        TreeSet<String> sortedCandidateIncludedPaths = new TreeSet<>();
        TreeSet<String> candidateExcludedPaths = new TreeSet<>();
        for (PathFilter pathFilter : pathFilters) {
            sortedCandidateIncludedPaths.addAll(pathFilter.getIncludedPaths());
            candidateExcludedPaths.addAll(pathFilter.getExcludedPaths());
        }
        // Keep only unique included paths. That is, if paths "/a/b" and "/a/b/c" are both in the list, keep only "/a/b"
        List<String> finalIncludedPathsRoots = new ArrayList<>();
        LOG.debug("Candidate included paths: {}, candidate excluded paths: {}", sortedCandidateIncludedPaths, candidateExcludedPaths);
        // The quadratic algorithm below could easily be replaced by linear time algorithm, but it's not worth the added
        // complexity because the list of paths should never be more than a few tens
        // Idea for linear time algorithm: after adding a path to the includedPathsRoots, advance in the list of candidates
        // skipping all paths that are children of the path just added. Since the list is sorted alphabetically, the children
        // of a path will be next to it in the list
        for (String candidateIncludedPath : sortedCandidateIncludedPaths) {
            if (finalIncludedPathsRoots.stream().noneMatch(ancestor -> PathUtils.isAncestor(ancestor, candidateIncludedPath))) {
                finalIncludedPathsRoots.add(candidateIncludedPath);
            }
        }

        List<String> finalExcludedPaths = new ArrayList<>();
        // Keep an excluded path only if it is not needed by any filter.
        for (String candidateExcludedPath : candidateExcludedPaths) {
            boolean notNeeded = pathFilters.stream().allMatch(pathFilter -> pathFilter.filter(candidateExcludedPath) == PathFilter.Result.EXCLUDE);
            if (notNeeded) {
                finalExcludedPaths.add(candidateExcludedPath);
            }
        }

        // Do not filter if there are too many includedPaths. The official Mongo documentation recommends
        // not using more than tens of values in the $in filter to avoid performance degradation.
        // https://www.mongodb.com/docs/manual/reference/operator/query/in/
        if (finalIncludedPathsRoots.size() > maxNumberOfPaths) {
            LOG.info("Number of includedPaths ({}) exceed the maximum allowed ({}), downloading everything",
                    finalIncludedPathsRoots.size(), maxNumberOfPaths);
            return MongoFilterPaths.DOWNLOAD_ALL;
        }

        if (customExcludedPaths.stream().anyMatch(PathUtils::denotesRoot)) {
            LOG.warn("Ignoring custom excluded paths setting, root cannot be excluded: {}",  customExcludedPaths);
        } else if (!isRootPath(finalIncludedPathsRoots)) {
            LOG.info("Ignoring custom excluded paths because included paths did not resolve to root. Mongo filters: {}", finalIncludedPathsRoots);
        } else {
            finalExcludedPaths = mergeIndexAndCustomExcludePaths(finalExcludedPaths, customExcludedPaths);
        }

        // Also check the number of excluded paths and disable filtering by them if there are too many.
        // We do not need to revert to download the full repository in the case where there is a reasonable number of
        // included paths but too many excluded paths. The excluded paths are always children of the included paths, so
        // if we do not filter on excludedPaths, we simply download everything inside the included paths. This might be
        // much less than the full repository.
        if (finalExcludedPaths.size() > maxNumberOfPaths) {
            LOG.info("Number of excludedPaths ({}) exceed the maximum allowed ({}), disabling filtering of excludedPaths",
                    finalExcludedPaths.size(), maxNumberOfPaths);
            finalExcludedPaths = List.of();
        }

        return new MongoFilterPaths(finalIncludedPathsRoots, finalExcludedPaths);
    }

    private boolean isRootPath(List<String> includedPaths) {
        return includedPaths.isEmpty() || (includedPaths.size() == 1 && includedPaths.get(0).equals("/"));
    }

    /*
     * Merge the excluded paths defined by the indexes with the custom excluded paths. This is done by taking the union
     * of the two lists and then removing any path that is a subpath of any other path in the list.
     */
    static List<String> mergeIndexAndCustomExcludePaths(List<String> indexExcludedPaths, List<String> customExcludedPaths) {
        if (customExcludedPaths.isEmpty()) {
            return indexExcludedPaths;
        }

        var excludedUnion = new HashSet<>(indexExcludedPaths);
        excludedUnion.addAll(customExcludedPaths);
        var mergedExcludes = new ArrayList<String>();
        for (String testPath : excludedUnion) {
            // Add a path only if it is not a subpath of any other path in the list
            if (excludedUnion.stream().noneMatch(p -> PathUtils.isAncestor(p, testPath))) {
                mergedExcludes.add(testPath);
            }
        }
        Collections.sort(mergedExcludes);
        return mergedExcludes;
    }
}
