package org.apache.jackrabbit.oak.upgrade.nodestate;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

import static org.apache.jackrabbit.oak.plugins.tree.impl.TreeConstants.OAK_CHILD_ORDER;

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
public class FilteringNodeState extends AbstractNodeState {

    public static final Set<String> ALL = ImmutableSet.of("/");

    public static final Set<String> NONE = ImmutableSet.of();

    private final NodeState delegate;

    private final String path;

    private final Set<String> includedPaths;

    private final Set<String> excludedPaths;

    /**
     * Factory method that conditionally decorates the given node-state
     * iff the node-state is (a) hidden itself or (b) has hidden descendants.
     *
     * @param path The path where the node-state should be assumed to be located.
     * @param delegate The node-state to decorate.
     * @param includePaths A Set of paths that should be visible. Defaults to ["/"] if {@code null).
     * @param excludePaths A Set of paths that should be hidden. Empty if {@code null).
     * @return The decorated node-state if required, the original node-state if decoration is unnecessary.
     * @param excludePaths
     */
    @Nonnull
    public static NodeState wrap(
            @Nonnull final String path,
            @Nonnull final NodeState delegate,
            @Nullable final Set<String> includePaths,
            @Nullable final Set<String> excludePaths
    ) {
        final Set<String> includes = defaultIfEmpty(includePaths, ALL);
        final Set<String> excludes = defaultIfEmpty(excludePaths, NONE);
        if (hasHiddenDescendants(path, includes, excludes)) {
            return new FilteringNodeState(path, delegate, includes, excludes);
        }
        return delegate;
    }

    private FilteringNodeState(
            @Nonnull final String path,
            @Nonnull final NodeState delegate,
            @Nonnull final Set<String> includedPaths,
            @Nonnull final Set<String> excludedPaths
    ) {
        this.path = path;
        this.delegate = delegate;
        this.includedPaths = includedPaths;
        this.excludedPaths = excludedPaths;
    }

    @Override
    @Nonnull
    public NodeBuilder builder() {
        return new MemoryNodeBuilder(this);
    }

    @Override
    public boolean exists() {
        return !isHidden(path, includedPaths, excludedPaths) && delegate.exists();
    }

    @Override
    @Nonnull
    public NodeState getChildNode(@Nonnull final String name) throws IllegalArgumentException {
        final String childPath = PathUtils.concat(path, name);
        return wrap(childPath, delegate.getChildNode(name), includedPaths, excludedPaths);
    }

    @Override
    public boolean hasChildNode(@Nonnull final String name) {
        final String childPath = PathUtils.concat(path, name);
        return !isHidden(childPath, includedPaths, excludedPaths) && delegate.hasChildNode(name);
    }

    @Override
    @Nonnull
    public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
        final Iterable<ChildNodeEntry> transformed = Iterables.transform(
                delegate.getChildNodeEntries(),
                new Function<ChildNodeEntry, ChildNodeEntry>() {
                    @Nullable
                    @Override
                    public ChildNodeEntry apply(@Nullable final ChildNodeEntry childNodeEntry) {
                        if (childNodeEntry != null) {
                            final String name = childNodeEntry.getName();
                            final String childPath = PathUtils.concat(path, name);
                            if (!isHidden(childPath, includedPaths, excludedPaths)) {
                                final NodeState nodeState = childNodeEntry.getNodeState();
                                final NodeState state = wrap(childPath, nodeState, includedPaths, excludedPaths);
                                return new MemoryChildNodeEntry(name, state);
                            }
                        }
                        return null;
                    }
                }
        );
        return Iterables.filter(transformed, new Predicate<ChildNodeEntry>() {
            @Override
            public boolean apply(@Nullable final ChildNodeEntry childNodeEntry) {
                return childNodeEntry != null;
            }
        });
    }

    @Override
    public long getPropertyCount() {
        return delegate.getPropertyCount();
    }

    @Override
    @Nonnull
    public Iterable<? extends PropertyState> getProperties() {
        return Iterables.transform(delegate.getProperties(), new Function<PropertyState, PropertyState>() {
            @Nullable
            @Override
            public PropertyState apply(@Nullable final PropertyState propertyState) {
                return fixChildOrderPropertyState(propertyState);
            }
        });
    }

    @Override
    public PropertyState getProperty(String name) {
        return fixChildOrderPropertyState(delegate.getProperty(name));
    }

    @Override
    public boolean hasProperty(String name) {
        return delegate.getProperty(name) != null;
    }

    /**
     * Utility method to fix the PropertyState of properties called {@code :childOrder}.
     *
     * @param propertyState A property-state.
     * @return The original property-state or if the property name is {@code :childOrder}, a
     *         property-state with hidden child names removed from the value.
     */
    @CheckForNull
    private PropertyState fixChildOrderPropertyState(@Nullable final PropertyState propertyState) {
        if (propertyState != null && OAK_CHILD_ORDER.equals(propertyState.getName())) {
            final Iterable<String> values = Iterables.filter(propertyState.getValue(Type.NAMES), new Predicate<String>() {
                @Override
                public boolean apply(@Nullable final String name) {
                    if (name == null) {
                        return false;
                    }
                    final String childPath = PathUtils.concat(path, name);
                    return !isHidden(childPath, includedPaths, excludedPaths);
                }
            });
            return PropertyStates.createProperty(OAK_CHILD_ORDER, values, Type.NAMES);
        }
        return propertyState;
    }

    /**
     * Utility method to determine whether a given path should is hidden given the
     * include paths and exclude paths.
     *
     * @param path Path to be checked
     * @param includes Include paths
     * @param excludes Exclude paths
     * @return Whether the {@code path} is hidden or not.
     */
    public static boolean isHidden(
            @Nonnull final String path,
            @Nonnull final Set<String> includes,
            @Nonnull final Set<String> excludes
    ) {
        return isExcluded(path, excludes) || !isIncluded(path, includes);
    }

    /**
     * Utility method to determine whether the path itself or any of its descendants should
     * be hidden given the include paths and exclude paths.
     *
     * @param path Path to be checked
     * @param includePaths Include paths
     * @param excludePaths Exclude paths
     * @return Whether the {@code path} or any of its descendants are hidden or not.
     */
    private static boolean hasHiddenDescendants(
            @Nonnull final String path,
            @Nonnull final Set<String> includePaths,
            @Nonnull final Set<String> excludePaths
    ) {
        return isHidden(path, includePaths, excludePaths)
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
     * @return Whether the path is covered by the excldue paths or not.
     */
    private static boolean isExcluded(@Nonnull final String path, @Nonnull final Set<String> excludePaths) {
        return excludePaths.contains(path) || isDescendantOfAnyPath(path, excludePaths);
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
