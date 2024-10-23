
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

package org.apache.jackrabbit.oak.spi.filter;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.commons.PathUtils.isAncestor;

/**
 * Filter which determines whether given path should be included for processing
 * or not
 */
public class PathFilter {
    private static final Set<String> INCLUDE_ROOT = Set.of("/");
    /**
     * Multi value property name used to determine list of paths to be included
     */
    public static final String PROP_INCLUDED_PATHS = "includedPaths";

    /**
     * Multi value property name used to determine list of paths to be excluded
     */
    public static final String PROP_EXCLUDED_PATHS = "excludedPaths";

    public enum Result {

        /**
         * Include the path for processing
         */
        INCLUDE,

        /**
         * Exclude the path and subtree for processing
         */
        EXCLUDE,

        /**
         * Do not process the path but just perform traversal to
         * child nodes. For IndexEditor it means that such nodes
         * should not be indexed but editor must traverse down
         */
        TRAVERSE
    }

    private static final PathFilter ALL = new PathFilter(INCLUDE_ROOT, List.of()) {
        @Override
        public Result filter(@NotNull String path) {
            return Result.INCLUDE;
        }
    };

    /**
     * Constructs the predicate based on given definition state. It looks for
     * multi value property with names {@link PathFilter#PROP_INCLUDED_PATHS}
     * and {@link PathFilter#PROP_EXCLUDED_PATHS}. Both the properties are
     * optional.
     * If the properties are defined as String instead of Strings, then they
     * are interpreted as a single-element list.
     *
     * @param defn nodestate representing the configuration. Generally it would
     *             be the nodestate representing the index definition
     * @return predicate based on the passed definition state
     */
    public static PathFilter from(@NotNull NodeBuilder defn) {
        if (!defn.hasProperty(PROP_EXCLUDED_PATHS) && !defn.hasProperty(PROP_INCLUDED_PATHS)) {
            return ALL;
        }
        return new PathFilter(
                getStrings(defn.getProperty(PROP_INCLUDED_PATHS), INCLUDE_ROOT),
                getStrings(defn.getProperty(PROP_EXCLUDED_PATHS), Set.of())
        );
    }

    /*
     * Gets the value of the given property as a set of strings. This works both if the property is of type STRING or
     * type STRINGS. If the type is STRING, then it is interpreted as a single-element list. This is the default behavior
     * of calling {@link PropertyState#getValue(Type)} with {@link Type#STRINGS}.
     */
    public static Iterable<String> getStrings(PropertyState ps, Set<String> defaultValues) {
        if (ps != null && (ps.getType() == Type.STRING || ps.getType() == Type.STRINGS)) {
            return ps.getValue(Type.STRINGS);
        }
        return defaultValues;
    }

    private final String[] includedPaths;
    private final String[] excludedPaths;

    /**
     * Constructs the predicate with given included and excluded paths
     * <p>
     * If both are empty then all paths would be considered to be included
     *
     * @param includes list of paths which should be included
     * @param excludes list of paths which should not be included
     */
    public PathFilter(Iterable<String> includes, Iterable<String> excludes) {
        Set<String> includeCopy = CollectionUtils.toSet(includes);
        Set<String> excludeCopy = CollectionUtils.toSet(excludes);
        PathUtils.unifyInExcludes(includeCopy, excludeCopy);
        Validate.checkState(!includeCopy.isEmpty(), "No valid include provided. Includes %s, " +
                "Excludes %s", includes, excludes);
        this.includedPaths = includeCopy.toArray(new String[0]);
        this.excludedPaths = excludeCopy.toArray(new String[0]);
    }

    /**
     * Determines whether given path is to be included or not
     *
     * @param path path to check
     * @return result indicating if the path needs to be included, excluded or just traversed
     */
    public Result filter(@NotNull String path) {
        for (String excludedPath : excludedPaths) {
            if (excludedPath.equals(path) || isAncestor(excludedPath, path)) {
                return Result.EXCLUDE;
            }
        }

        for (String includedPath : includedPaths) {
            if (includedPath.equals(path) || isAncestor(includedPath, path)) {
                return Result.INCLUDE;
            }
        }

        for (String includedPath : includedPaths) {
            if (isAncestor(path, includedPath)) {
                return Result.TRAVERSE;
            }
        }

        return Result.EXCLUDE;
    }

    public List<String> getIncludedPaths() {
        return List.of(includedPaths);
    }

    public List<String> getExcludedPaths() {
        return List.of(excludedPaths);
    }

    @Override
    public String toString() {
        return "PathFilter{" +
                "includedPaths=" + Arrays.toString(includedPaths) +
                ", excludedPaths=" + Arrays.toString(excludedPaths) +
                '}';
    }

    /**
     * Check whether this node and all descendants are included in this filter.
     *
     * @param path the path
     * @return true if this and all descendants of this path are included in the filter
     */
    public boolean areAllDescendantsIncluded(String path) {
        for (String excludedPath : excludedPaths) {
            if (excludedPath.equals(path) || isAncestor(excludedPath, path) || isAncestor(path, excludedPath)) {
                return false;
            }
        }
        for (String includedPath : includedPaths) {
            if (includedPath.equals(path) || isAncestor(includedPath, path)) {
                return true;
            }
        }
        return false;
    }
}
