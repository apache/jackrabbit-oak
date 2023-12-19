
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
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.guava.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAncestor;

/**
 * Filter which determines whether given path should be included for processing
 * or not
 */
public class PathFilter {
    private static final Logger LOG = LoggerFactory.getLogger(PathFilter.class);
    private static final Collection<String> INCLUDE_ROOT = List.of("/");
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
    private final String[] includedPaths;
    private final String[] excludedPaths;

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
                getStringsLenient(defn, PROP_INCLUDED_PATHS, INCLUDE_ROOT),
                getStringsLenient(defn, PROP_EXCLUDED_PATHS, List.of())
        );
    }

    /**
     * Constructs the predicate with given included and excluded paths
     * <p>
     * If both are empty then all paths would be considered to be included
     *
     * @param includes list of paths which should be included
     * @param excludes list of paths which should not be included
     */
    public PathFilter(Iterable<String> includes, Iterable<String> excludes) {
        Set<String> includeCopy = newHashSet(includes);
        Set<String> excludeCopy = newHashSet(excludes);
        PathUtils.unifyInExcludes(includeCopy, excludeCopy);
        checkState(!includeCopy.isEmpty(), "No valid include provided. Includes %s, " +
                "Excludes %s", includes, excludes);
        this.includedPaths = includeCopy.toArray(new String[includeCopy.size()]);
        this.excludedPaths = excludeCopy.toArray(new String[excludeCopy.size()]);
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
     * Gets the value of a property of expected type Strings. However, if the property is of type String, interpret
     * it as a single-element list instead of returning the default value.
     *
     * @return the value of the property if the property is set and is of type Strings or String, otherwise the default value
     */
    private static Iterable<String> getStringsLenient(NodeBuilder builder, String propertyName, Collection<String> defaultVal) {
        PropertyState property = builder.getProperty(propertyName);
        if (property != null && property.getType() == Type.STRINGS) {
            return property.getValue(Type.STRINGS);
        } else if (property != null && property.getType() == Type.STRING) {
            String value = property.getValue(Type.STRING);
            LOG.warn("Property \"{}\"=\"{}\" has type String but it should be array of String. Proceeding by treating it as a " +
                            "one-element array. Please correct the index definition by defining it instead as \"{}\" = [\"{}\"].",
                    propertyName, value, propertyName, value);
            return List.of(value);
        } else {
            return defaultVal;
        }
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
