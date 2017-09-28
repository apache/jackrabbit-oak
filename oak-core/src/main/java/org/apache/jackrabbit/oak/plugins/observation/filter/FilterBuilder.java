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
package org.apache.jackrabbit.oak.plugins.observation.filter;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAncestor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.observation.filter.UniversalFilter.Selector;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.observation.ChangeSet;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Builder for {@link FilterProvider} instances.
 */
public final class FilterBuilder {
    private static final int ALL_EVENTS = NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED |
            PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;

    private boolean includeSessionLocal;
    private boolean includeClusterExternal;
    private boolean includeClusterLocal = true;
    private final List<String> subTrees = newArrayList();
    private final Set<String> pathsForMBean = newHashSet();
    private Condition condition = includeAll();
    private ChangeSetFilter changeSetFilter = new ChangeSetFilter() {
        
        @Override
        public boolean excludes(ChangeSet changeSet) {
            return false;
        }
    };

    private EventAggregator aggregator;

    public interface Condition {
        @Nonnull
        EventFilter createFilter(@Nonnull NodeState before, @Nonnull NodeState after);
    }
    
    @Nonnull
    public FilterBuilder setChangeSetFilter(@Nonnull ChangeSetFilter changeSetFilter) {
        this.changeSetFilter = changeSetFilter;
        return this;
    }

    /**
     * Adds a path to the set of paths whose subtrees include all events of
     * this filter. Does nothing if the paths is already covered by an
     * path added earlier.
     * <p>
     * This is used for optimisation in order to restrict traversal to
     * these sub trees.
     *
     * @param absPath  absolute path
     * @return  this instance
     */
    @Nonnull
    public FilterBuilder addSubTree(@Nonnull String absPath) {
        if (absPath.endsWith("/")) {
            absPath = absPath.substring(0, absPath.length() - 1);
        }
        for (String path : subTrees) {
            if (path.equals(absPath) || isAncestor(path, absPath)) {
                return this;
            }
        }
        subTrees.add(checkNotNull(absPath));
        return this;
    }
    
    /**
     * Adds paths to the FilterConfigMBean's getPaths set
     * @param paths
     * @return
     */
    public FilterBuilder addPathsForMBean(@Nonnull Set<String> paths) {
        pathsForMBean.addAll(paths);
        return this;
    }

    /**
     * A set of paths whose subtrees include all events of this filter.
     * @return  list of paths
     * @see #addSubTree(String)
     */
    @Nonnull
    private Iterable<String> getSubTrees() {
        return subTrees.isEmpty() ? ImmutableList.of("/") : subTrees;
    }
    
    public FilterBuilder aggregator(EventAggregator aggregator) {
        this.aggregator = aggregator;
        return this;
    }

    /**
     * Whether to include session local changes. Defaults to {@code false}.
     * @param include if {@code true} session local changes are included,
     *                otherwise session local changes are not included.
     * @return  this instance
     */
    @Nonnull
    public FilterBuilder includeSessionLocal(boolean include) {
        this.includeSessionLocal = include;
        return this;
    }

    /**
     * Whether to include cluster external changes. Defaults to {@code false}.
     * @param include if {@code true} cluster external changes are included,
     *                otherwise cluster external changes are not included.
     * @return  this instance
     */
    @Nonnull
    public FilterBuilder includeClusterExternal(boolean include) {
        this.includeClusterExternal = include;
        return this;
    }

    /**
     * Whether to include cluster local changes. Defaults to {@code true}.
     * @param include if {@code true} cluster local changes are included,
     *                otherwise cluster local changes are not included.
     * @return  this instance
     */
    @Nonnull
    public FilterBuilder includeClusterLocal(boolean include) {
        this.includeClusterLocal = include;
        return this;
    }

    /**
     * Set the condition of this filter. Conditions are obtained from
     * the various methods on this instance that return a {@code Condition}
     * instance.
     *
     * @param condition  the conditions to apply
     * @return this instance
     */
    @Nonnull
    public FilterBuilder condition(@Nonnull Condition condition) {
        this.condition = checkNotNull(condition);
        return this;
    }

    //------------------------------------------------------------< initial conditions >---

    /**
     * A condition the always holds
     * @return  true condition
     */
    @Nonnull
    public Condition includeAll() {
        return ConstantCondition.INCLUDE_ALL;
    }

    /**
     * A condition that never holds
     * @return  false condition
     */
    @Nonnull
    public Condition excludeAll() {
        return ConstantCondition.EXCLUDE_ALL;
    }

    /**
     * A condition that hold for accessible items as determined by the passed permission
     * provider.
     *
     * @param permissionProviderFactory  permission provider for checking whether an item is accessible.
     * @return  access control condition
     * @see  ACFilter
     */
    @Nonnull
    public Condition accessControl(@Nonnull PermissionProviderFactory permissionProviderFactory) {
        return new ACCondition(checkNotNull(permissionProviderFactory));
    }

    /**
     * A condition that holds on the paths matching a certain pattern.
     * @param pathPattern
     * @return  path condition
     * @see GlobbingPathFilter
     */
    @Nonnull
    public Condition path(@Nonnull String pathPattern) {
        return new PathCondition(checkNotNull(pathPattern));
    }

    /**
     * A condition that holds for matching event types.
     * @param eventTypes
     * @return event type condition
     * @see EventTypeFilter
     */
    @Nonnull
    public Condition eventType(int eventTypes) {
        if ((ALL_EVENTS & eventTypes) == 0) {
            return excludeAll();
        } else if ((ALL_EVENTS & eventTypes) != ALL_EVENTS) {
            return new EventTypeCondition(eventTypes);
        } else {
            return includeAll();
        }
    }

    /**
     * A condition that holds for matching node types.
     * @param selector  selector selecting the node to check the condition on
     * @param ntNames node type names to match. This conditions never matches if {@code null} and
     *                always matches if empty.
     * @return node type condition
     */
    @Nonnull
    public Condition nodeType(@Nonnull Selector selector, @CheckForNull String[] ntNames) {
        if (ntNames == null) {
            return includeAll();
        } else if (ntNames.length == 0) {
            return excludeAll();
        } else {
            return new NodeTypeCondition(checkNotNull(selector), ntNames);
        }
    }

    /**
     * A condition that holds for matching uuids.
     * @param selector  selector selecting the node to check the condition on
     * @param uuids uuids to match. This conditions never matches if {@code null} and
     *                always matches if empty.
     * @return node type condition
     */
    @Nonnull
    public Condition uuid(@Nonnull Selector selector, @CheckForNull String[] uuids) {
        if (uuids == null) {
            return includeAll();
        } else if (uuids.length == 0) {
            return excludeAll();
        } else {
            return new UniversalCondition(checkNotNull(selector), new UuidPredicate(uuids));
        }
    }

    /**
     * A condition that holds when the property predicate matches.
     * @param selector  selector selecting the node to check the condition on
     * @param name      the name of the property to check the predicate on
     * @param predicate the predicate to check on the named property
     * @return property condition
     */
    @Nonnull
    public Condition property(@Nonnull Selector selector, @Nonnull String name,
            @Nonnull Predicate<PropertyState> predicate) {

        return new UniversalCondition(
                checkNotNull(selector),
                new PropertyPredicate(checkNotNull(name), checkNotNull(predicate)));
    }

    /**
     * A condition that holds when the predicate matches.
     * @param selector  selector selecting the node to check the condition on
     * @param predicate the predicate to check on the selected node
     * @return universal condition
     */
    @Nonnull
    public Condition universal(@Nonnull Selector selector, @Nonnull Predicate<NodeState> predicate) {
        return new UniversalCondition(checkNotNull(selector), checkNotNull(predicate));
    }

    /**
     * @return a condition that holds for children of added nodes.
     */
    @Nonnull
    public Condition addSubtree() {
        return new AddSubtreeTreeCondition();
    }

    /**
     * @return a condition that holds for children of deleted nodes.
     */
    @Nonnull
    public Condition deleteSubtree() {
        return new DeleteSubtreeTreeCondition();
    }

    /**
     * @return a condition that holds for children of the target of a moved node
     */
    @Nonnull
    public Condition moveSubtree() {
        return new MoveCondition();
    }

    //------------------------------------------------------------< Compound conditions >---

    /**
     * A compound condition that holds when any of its constituents hold.
     * @param conditions conditions of which any must hold in order for this condition to hold
     * @return  any condition
     */
    @Nonnull
    public Condition any(@Nonnull Condition... conditions) {
        return new AnyCondition(newArrayList(checkNotNull(conditions)));
    }

    /**
     * A compound condition that holds when all of its constituents hold.
     * @param conditions conditions of which all must hold in order for this condition to hold
     * @return  any condition
     */
    @Nonnull
    public Condition all(@Nonnull Condition... conditions) {
        return new AllCondition(newArrayList(checkNotNull(conditions)));
    }

    /**
     * A compound condition that holds when all of its constituents hold.
     * @param conditions conditions of which all must hold in order for this condition to hold
     * @return  any condition
     */
    @Nonnull
    public Condition all(@Nonnull List<Condition> conditions) {
        return new AllCondition(checkNotNull(conditions));
    }

    /**
     * A compound condition that holds when its constituent does not hold.
     * @param condition condition which must not hold in order for this condition to hold
     * @return  not condition
     */
    @Nonnull
    public Condition not(@Nonnull Condition condition) {
        return new NotCondition(checkNotNull(condition));
    }

    /**
     * A compound condition that holds when any of its constituents hold.
     * @param conditions conditions of which any must hold in order for this condition to hold
     * @return  any condition
     */
    @Nonnull
    public Condition any(@Nonnull Iterable<Condition> conditions) {
        return new AnyCondition(checkNotNull(conditions));
    }

    /**
     * A compound condition that holds when all of its constituents hold.
     * @param conditions conditions of which all must hold in order for this condition to hold
     * @return  any condition
     */
    @Nonnull
    public Condition all(@Nonnull Iterable<Condition> conditions) {
        return new AllCondition(checkNotNull(conditions));
    }

    /**
     * Create a {@code FilterProvider} reflecting the current state of this builder.
     * @return  filter provider of the current state of this builder
     */
    @Nonnull
    public FilterProvider build() {
        return new FilterProvider() {
            final boolean includeSessionLocal = FilterBuilder.this.includeSessionLocal;
            final boolean includeClusterExternal = FilterBuilder.this.includeClusterExternal;
            final boolean includeClusterLocal = FilterBuilder.this.includeClusterLocal;
            final EventAggregator aggregator = FilterBuilder.this.aggregator;
            final Iterable<String> subTrees = FilterBuilder.this.getSubTrees();
            final Condition condition = FilterBuilder.this.condition;
            final ChangeSetFilter changeSetFilter = FilterBuilder.this.changeSetFilter;

            @Override
            public String toString() {
                return super.toString() + " [changeSetFilter="+changeSetFilter+"]";
            }
            
            @Override
            public boolean includeCommit(@Nonnull String sessionId, @CheckForNull CommitInfo info) {
                return (includeSessionLocal || !isLocal(checkNotNull(sessionId), info))
                    && (includeClusterExternal || !isExternal(info))
                    && (includeClusterLocal || isExternal(info));
            }

            @Nonnull
            @Override
            public EventFilter getFilter(@Nonnull NodeState before, @Nonnull NodeState after) {
                return condition.createFilter(checkNotNull(before), checkNotNull(after));
            }

            @Nonnull
            @Override
            public Iterable<String> getSubTrees() {
                return subTrees;
            }

            @Override
            public FilterConfigMBean getConfigMBean() {
                return FilterBuilder.this.getConfigMBean();
            }

            private boolean isLocal(String sessionId, CommitInfo info) {
                return info != null && Objects.equal(info.getSessionId(), sessionId);
            }

            private boolean isExternal(CommitInfo info) {
                return info.isExternal();
            }
            
            @Override
            public EventAggregator getEventAggregator() {
                return aggregator;
            }
            
            @Override
            public boolean excludes(ChangeSet changeSet) {
                return changeSetFilter.excludes(changeSet);
            }
        };
    }

    @Nonnull
    private FilterConfigMBean getConfigMBean(){
        return new FilterConfigMBean() {
            @Override
            public String[] getPaths() {
                return Iterables.toArray(pathsForMBean, String.class);
            }

            @Override
            public boolean isIncludeClusterLocal() {
                return FilterBuilder.this.includeClusterLocal;
            }

            @Override
            public boolean isIncludeClusterExternal() {
                return FilterBuilder.this.includeClusterExternal;
            }
        };
    }

    //------------------------------------------------------------< Conditions >---

    private static class ConstantCondition implements Condition {
        public static final ConstantCondition INCLUDE_ALL = new ConstantCondition(true);
        public static final ConstantCondition EXCLUDE_ALL = new ConstantCondition(false);

        private final boolean value;

        public ConstantCondition(boolean value) {
            this.value = value;
        }

        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            return value ? Filters.includeAll() : Filters.excludeAll();
        }
    }

    private static class ACCondition implements Condition {
        private final PermissionProviderFactory permissionProviderFactory;

        public ACCondition(PermissionProviderFactory permissionProviderFactory) {
            this.permissionProviderFactory = permissionProviderFactory;
        }

        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            return new ACFilter(before, after,
                    permissionProviderFactory.create(RootFactory.createReadOnlyRoot(after)));
        }
    }

    private static class PathCondition implements Condition {
        private final String pathGlob;
        private final Map<String,Pattern> patternMap = new HashMap<String,Pattern>();

        public PathCondition(String pathGlob) {
            this.pathGlob = pathGlob;
        }

        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            return new GlobbingPathFilter(pathGlob, patternMap);
        }
    }

    private static class EventTypeCondition implements Condition {
        private final int eventTypes;

        public EventTypeCondition(int eventTypes) {
            this.eventTypes = eventTypes;
        }

        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            return new EventTypeFilter(eventTypes);
        }
    }

    private static class NodeTypeCondition implements Condition {
        private final Selector selector;
        private final String[] ntNames;

        public NodeTypeCondition(Selector selector, String[] ntNames) {
            this.selector = selector;
            this.ntNames = ntNames;
        }

        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            TypePredicate predicate = new TypePredicate(
                    after.exists() ? after : before, ntNames);
            return new UniversalFilter(before, after, selector, predicate);
        }
    }

    private static class UniversalCondition implements Condition {
        private final Selector selector;
        private final Predicate<NodeState> predicate;

        public UniversalCondition(Selector selector, Predicate<NodeState> predicate) {
            this.selector = selector;
            this.predicate = predicate;
        }

        @Nonnull
        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            return new UniversalFilter(before, after, selector, predicate);
        }
    }

    protected static class AddSubtreeTreeCondition implements Condition {
        @Nonnull
        @Override
        public EventFilter createFilter(@Nonnull NodeState before, @Nonnull NodeState after) {
            return AddSubtreeFilter.getInstance();
        }
    }

    protected static class DeleteSubtreeTreeCondition implements Condition {
        @Nonnull
        @Override
        public EventFilter createFilter(@Nonnull NodeState before, @Nonnull NodeState after) {
            return DeleteSubtreeFilter.getInstance();
        }
    }

    protected static class MoveCondition implements Condition {
        @Nonnull
        @Override
        public EventFilter createFilter(@Nonnull NodeState before, @Nonnull NodeState after) {
            return new MoveFilter();
        }
    }

    private static class AnyCondition implements Condition {
        private final Iterable<Condition> conditions;

        public AnyCondition(Iterable<Condition> conditions) {
            this.conditions = conditions;
        }

        public AnyCondition(Condition... conditions) {
            this(newArrayList(conditions));
        }

        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            List<EventFilter> filters = newArrayList();
            for (Condition condition : conditions) {
                if (condition == ConstantCondition.INCLUDE_ALL) {
                    return ConstantFilter.INCLUDE_ALL;
                } else if (condition != ConstantCondition.EXCLUDE_ALL) {
                    filters.add(condition.createFilter(before, after));
                }
            }
            return filters.isEmpty()
                ? ConstantFilter.EXCLUDE_ALL
                : Filters.any(filters);
        }
    }

    private static class AllCondition implements Condition {
        private final Iterable<Condition> conditions;

        public AllCondition(Iterable<Condition> conditions) {
            this.conditions = conditions;
        }

        public AllCondition(Condition... conditions) {
            this(newArrayList(conditions));
        }

        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            List<EventFilter> filters = newArrayList();
            for (Condition condition : conditions) {
                if (condition == ConstantCondition.EXCLUDE_ALL) {
                    return ConstantFilter.EXCLUDE_ALL;
                } else if (condition != ConstantCondition.INCLUDE_ALL) {
                    filters.add(condition.createFilter(before, after));
                }
            }
            return filters.isEmpty()
                ? ConstantFilter.INCLUDE_ALL
                : Filters.all(filters);
        }
    }

    private static class NotCondition implements Condition {
        private final Condition condition;

        public NotCondition(Condition condition) {
            this.condition = condition;
        }

        @Nonnull
        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            if (condition == ConstantCondition.EXCLUDE_ALL) {
                return ConstantFilter.INCLUDE_ALL;
            } else if (condition == ConstantCondition.INCLUDE_ALL) {
                return ConstantFilter.EXCLUDE_ALL;
            } else {
                return Filters.not(condition.createFilter(before, after));
            }
        }
    }

}
