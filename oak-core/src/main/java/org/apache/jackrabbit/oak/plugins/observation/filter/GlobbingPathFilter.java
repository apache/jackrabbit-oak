package org.apache.jackrabbit.oak.plugins.observation.filter;

import static com.google.common.base.Objects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;

import javax.annotation.Nonnull;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * This {@code Filter} implementation supports filtering on paths using
 * simple glob patterns. Such a pattern is a string denoting a path. Each
 * element of the pattern is matched against the corresponding element of
 * a path. Elements of the pattern are matched literally except for the special
 * elements {@code *} and {@code **} where the former matches an arbitrary
 * path element and the latter matches any number of path elements (including none).
 * <p>
 * Note: an empty path pattern matches no path.
 * <p>
 * Note: path patterns only match against the corresponding elements of the path
 * and <em>do not</em> distinguish between absolute and relative paths.
 * <p>
 * Note: there is no way to escape {@code *} and {@code **}.
 * <p>
 * Examples:
 * <pre>
 *    q matches q only
 *    * matches every path containing a single element
 *    ** matches every path
 *    a/b/c matches a/b/c only
 *    a/*&#47c matches a/x/c for every element x
 *    **&#47y/z match every path ending in y/z
 *    r/s/t&#47** matches r/s/t and all its descendants
 * </pre>
 */
public class GlobbingPathFilter implements Filter {
    public static final String STAR = "*";
    public static final String STAR_STAR = "**";

    private final ImmutableTree beforeTree;
    private final ImmutableTree afterTree;
    private final ImmutableList<String> pattern;

    private GlobbingPathFilter(
            @Nonnull ImmutableTree beforeTree,
            @Nonnull ImmutableTree afterTree,
            @Nonnull Iterable<String> pattern) {
        this.beforeTree = checkNotNull(beforeTree);
        this.afterTree = checkNotNull(afterTree);
        this.pattern = ImmutableList.copyOf(checkNotNull(pattern));
    }

    public GlobbingPathFilter(
            @Nonnull ImmutableTree beforeTree,
            @Nonnull ImmutableTree afterTree,
            @Nonnull String pattern) {
        this(beforeTree, afterTree, elements(pattern));
    }

    @Override
    public boolean includeAdd(PropertyState after) {
        return includeItem(after.getName());
    }

    @Override
    public boolean includeChange(PropertyState before, PropertyState after) {
        return includeItem(after.getName());
    }

    @Override
    public boolean includeDelete(PropertyState before) {
        return includeItem(before.getName());
    }

    @Override
    public boolean includeAdd(String name, NodeState after) {
        return includeItem(name);
    }

    @Override
    public boolean includeChange(String name, NodeState before, NodeState after) {
        return includeItem(name);
    }

    @Override
    public boolean includeDelete(String name, NodeState before) {
        return includeItem(name);
    }

    @Override
    public boolean includeMove(String sourcePath, String destPath, NodeState moved) {
        return includeItem(getName(destPath));
    }

    @Override
    public Filter create(String name, NodeState before, NodeState after) {
        if (pattern.isEmpty()) {
            return null;
        }

        String head = pattern.get(0);
        if (pattern.size() == 1 && !STAR_STAR.equals(head)) {
            // shortcut when no further matches are possible
            return null;
        }

        if (STAR.equals(head) || head.equals(name)) {
            return new GlobbingPathFilter(beforeTree.getChild(name), afterTree.getChild(name),
                    pattern.subList(1, pattern.size()));
        } else if (STAR_STAR.equals(head)) {
            if (pattern.size() >= 2 && pattern.get(1).equals(name)) {
                // ** matches empty list of elements and pattern.get(1) matches name
                // match the rest of the pattern against the rest of the path and
                // match the whole pattern against the rest of the path
                return Filters.any(
                        new GlobbingPathFilter(beforeTree.getChild(name), afterTree.getChild(name),
                                pattern.subList(2, pattern.size())),
                        new GlobbingPathFilter(beforeTree.getChild(name), afterTree.getChild(name),
                                pattern)
                );
            } else {
                // ** matches name, match the whole pattern against the rest of the path
                return new GlobbingPathFilter(beforeTree.getChild(name), afterTree.getChild(name),
                        pattern);
            }
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("path", Joiner.on('/').join(pattern))
                .toString();
    }

    //------------------------------------------------------------< private >---

    private boolean includeItem(String name) {
        if (!pattern.isEmpty() && pattern.size() <= 2) {
            String head = pattern.get(0);
            boolean headMatches = STAR.equals(head) || STAR_STAR.equals(head) || head.equals(name);
            return pattern.size() == 1
                ? headMatches
                : headMatches && STAR_STAR.equals(pattern.get(1));
        } else {
            return false;
        }
    }

}
