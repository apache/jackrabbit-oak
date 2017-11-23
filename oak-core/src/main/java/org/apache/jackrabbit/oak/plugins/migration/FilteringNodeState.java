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
package org.apache.jackrabbit.oak.plugins.migration;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

/**
 * NodeState implementation that decorates another node-state instance
 * in order to hide subtrees or partial subtrees from the consumer of
 * the API.
 * <br>
 * The set of visible subtrees is defined by two parameters: include paths
 * and exclude paths, both of which are sets of absolute paths.
 * <br>
 * Any paths that are equal or are descendants of one of the
 * <b>excluded paths</b> are hidden by this implementation.
 * <br>
 * For all <b>included paths</b>, the direct ancestors, the node-state at
 * the path itself and all descendants are visible. Any siblings of the
 * defined path or its ancestors are implicitly hidden (unless made visible
 * by another include path).
 * <br>
 * The implementation delegates to the decorated node-state instance and
 * filters out hidden node-states in the following methods:
 * <ul>
 *     <li>{@link #exists()}</li>
 *     <li>{@link #hasChildNode(String)}</li>
 *     <li>{@link #getChildNodeEntries()}</li>
 * </ul>
 * Additionally, hidden node-state names are removed from the property
 * {@code :childOrder} in the following two methods:
 * <ul>
 *     <li>{@link #getProperties()}</li>
 *     <li>{@link #getProperty(String)}</li>
 * </ul>
 */
public class FilteringNodeState extends AbstractDecoratedNodeState {

    public static final Set<String> ALL = ImmutableSet.of("/");

    public static final Set<String> NONE = ImmutableSet.of();

    private final String path;

    private final Set<String> includedPaths;

    private final Set<String> excludedPaths;

    private final Set<String> fragmentPaths;

    private final Set<String> excludedFragments;

    /**
     * Factory method that conditionally decorates the given node-state
     * iff the node-state is (a) hidden itself or (b) has hidden descendants.
     *
     * @param path The path where the node-state should be assumed to be located.
     * @param delegate The node-state to decorate.
     * @param includePaths A Set of paths that should be visible. Defaults to ["/"] if {@code null}.
     * @param excludePaths A Set of paths that should be hidden. Empty if {@code null}.
     * @param fragmentPaths A Set of paths that should support the fragments (see below). Empty if {@code null}.
     * @param excludedFragments A Set of name fragments that should be hidden. Empty if {@code null}.
     * @return The decorated node-state if required, the original node-state if decoration is unnecessary.
     */
    @Nonnull
    public static NodeState wrap(
            @Nonnull final String path,
            @Nonnull final NodeState delegate,
            @Nullable final Set<String> includePaths,
            @Nullable final Set<String> excludePaths,
            @Nullable final Set<String> fragmentPaths,
            @Nullable final Set<String> excludedFragments
    ) {
        final Set<String> includes = defaultIfEmpty(includePaths, ALL);
        final Set<String> excludes = defaultIfEmpty(excludePaths, NONE);
        final Set<String> safeFragmentPaths = defaultIfEmpty(fragmentPaths, NONE);
        final Set<String> safeExcludedFragments = defaultIfEmpty(excludedFragments, NONE);
        if (hasHiddenDescendants(path, includes, excludes, safeFragmentPaths, safeExcludedFragments)) {
            return new FilteringNodeState(path, delegate, includes, excludes, fragmentPaths, safeExcludedFragments);
        }
        return delegate;
    }

    private FilteringNodeState(
            @Nonnull final String path,
            @Nonnull final NodeState delegate,
            @Nonnull final Set<String> includedPaths,
            @Nonnull final Set<String> excludedPaths,
            @Nonnull final Set<String> fragmentPaths,
            @Nonnull final Set<String> excludedFragments
    ) {
        super(delegate);
        this.path = path;
        this.includedPaths = includedPaths;
        this.excludedPaths = excludedPaths;
        this.fragmentPaths = fragmentPaths;
        this.excludedFragments = excludedFragments;
    }

    @Nonnull
    @Override
    protected NodeState decorateChild(@Nonnull final String name, @Nonnull final NodeState child) {
        final String childPath = PathUtils.concat(path, name);
        return wrap(childPath, child, includedPaths, excludedPaths, fragmentPaths, excludedFragments);
    }

    @Override
    protected boolean hideChild(@Nonnull final String name, @Nonnull final NodeState delegateChild) {
        return isHidden(PathUtils.concat(path, name), includedPaths, excludedPaths, excludedFragments);
    }

    @Override
    protected PropertyState decorateProperty(@Nonnull final PropertyState propertyState) {
        return fixChildOrderPropertyState(this, propertyState);
    }

    /**
     * Utility method to determine whether a given path should is hidden given the
     * include paths and exclude paths.
     *
     * @param path Path to be checked
     * @param includes Include paths
     * @param excludes Exclude paths
     * @param excludedFragments Exclude fragments
     * @return Whether the {@code path} is hidden or not.
     */
    private static boolean isHidden(
            @Nonnull final String path,
            @Nonnull final Set<String> includes,
            @Nonnull final Set<String> excludes,
            @Nonnull final Set<String> excludedFragments
    ) {
        return isExcluded(path, excludes, excludedFragments) || !isIncluded(path, includes);
    }

    /**
     * Utility method to determine whether the path itself or any of its descendants should
     * be hidden given the include paths and exclude paths.
     *
     * @param path Path to be checked
     * @param includePaths Include paths
     * @param excludePaths Exclude paths
     * @param excludedFragments Exclude fragments
     * @return Whether the {@code path} or any of its descendants are hidden or not.
     */
    private static boolean hasHiddenDescendants(
            @Nonnull final String path,
            @Nonnull final Set<String> includePaths,
            @Nonnull final Set<String> excludePaths,
            @Nonnull final Set<String> fragmentPaths,
            @Nonnull final Set<String> excludedFragments
    ) {
        return isHidden(path, includePaths, excludePaths, excludedFragments)
                || isAncestorOfAnyPath(path, fragmentPaths)
                || isDescendantOfAnyPath(path, fragmentPaths)
                || fragmentPaths.contains(path)
                || isAncestorOfAnyPath(path, excludePaths)
                || isAncestorOfAnyPath(path, includePaths);
    }

    /**
     * Utility method to check whether a given set of include paths cover the given
     * {@code path}. I.e. whether the path is visible or implicitly hidden due to the
     * lack of a matching include path.
     * <br>
     * Note: the ancestors of every include path are considered visible.
     *
     * @param path Path to be checked
     * @param includePaths Include paths
     * @return Whether the path is covered by the include paths or not.
     */
    private static boolean isIncluded(@Nonnull final String path, @Nonnull final Set<String> includePaths) {
        return isAncestorOfAnyPath(path, includePaths)
                || includePaths.contains(path)
                || isDescendantOfAnyPath(path, includePaths);
    }

    /**
     * Utility method to check whether a given set of exclude paths cover the given
     * {@code path}. I.e. whether the path is hidden due to the presence of a
     * matching exclude path.
     *
     * @param path Path to be checked
     * @param excludePaths Exclude paths
     * @param excludedFragments Exclude fragments
     * @return Whether the path is covered by the excldue paths or not.
     */
    private static boolean isExcluded(@Nonnull final String path, @Nonnull final Set<String> excludePaths, @Nonnull final Set<String> excludedFragments) {
        return excludePaths.contains(path) || isDescendantOfAnyPath(path, excludePaths) || containsAnyFragment(path, excludedFragments);
    }

    /**
     * Utility method to check whether any of the provided {@code paths} is a descendant
     * of the given ancestor path.
     *
     * @param ancestor Ancestor path
     * @param paths Paths that may be descendants of {@code ancestor}.
     * @return true if {@code paths} contains a descendant of {@code ancestor}, false otherwise.
     */
    private static boolean isAncestorOfAnyPath(@Nonnull final String ancestor, @Nonnull final Set<String> paths) {
        for (final String p : paths) {
            if (PathUtils.isAncestor(ancestor, p)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Utility method to check whether any of the provided {@code paths} is an ancestor
     * of the given descendant path.
     *
     * @param descendant Descendant path
     * @param paths Paths that may be ancestors of {@code descendant}.
     * @return true if {@code paths} contains an ancestor of {@code descendant}, false otherwise.
     */
    private static boolean isDescendantOfAnyPath(@Nonnull final String descendant, @Nonnull final Set<String> paths) {
        for (final String p : paths) {
            if (PathUtils.isAncestor(p, descendant)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Utility method to check whether the passed path contains any of the provided {@code fragments}.
     *
     * @param path Path
     * @param fragments Fragments, which the path may contain
     * @return true if {@code path} contains any of the {@code fragments}, false otherwise.
     */
    private static boolean containsAnyFragment(@Nonnull final String path, @Nonnull final Set<String> fragments) {
        for (final String f : fragments) {
            if (path.contains(f)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Utility method to return the given {@code Set} if it is not empty and a default Set otherwise.
     *
     * @param value Value to check for emptiness
     * @param defaultValue Default value
     * @return return the given {@code Set} if it is not empty and a default Set otherwise
     */
    @Nonnull
    private static <T> Set<T> defaultIfEmpty(@Nullable Set<T> value, @Nonnull Set<T> defaultValue) {
        return !isEmpty(value) ? value : defaultValue;
    }

    /**
     * Utility method to check whether a Set is empty, i.e. null or of size 0.
     *
     * @param set The Set to check.
     * @return true if empty, false otherwise
     */
    private static <T> boolean isEmpty(@Nullable final Set<T> set) {
        return set == null || set.isEmpty();
    }
}
