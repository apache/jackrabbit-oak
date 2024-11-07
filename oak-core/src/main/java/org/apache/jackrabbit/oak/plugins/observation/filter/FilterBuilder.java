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

import static java.util.Objects.requireNonNull;
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;
import static org.apache.jackrabbit.oak.commons.PathUtils.isAncestor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.observation.filter.UniversalFilter.Selector;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.observation.ChangeSet;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Builder for {@link FilterProvider} instances.
 */
public final class FilterBuilder {
    private static final int ALL_EVENTS = NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED |
            PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;

    private boolean includeSessionLocal;
    private boolean includeClusterExternal;
    private boolean includeClusterLocal = true;
    private final List<String> subTrees = new ArrayList<>();
    private final Set<String> pathsForMBean = new HashSet<>();
    private Condition condition = includeAll();
    private ChangeSetFilter changeSetFilter = new ChangeSetFilter() {

        @Override
        public boolean excludes(ChangeSet changeSet) {
            return false;
        }
    };

    private EventAggregator aggregator;

    public interface Condition {
        @NotNull
        EventFilter createFilter(@NotNull NodeState before, @NotNull NodeState after);
    }

    @NotNull
    public FilterBuilder setChangeSetFilter(@NotNull ChangeSetFilter changeSetFilter) {
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
    @NotNull
    public FilterBuilder addSubTree(@NotNull String absPath) {
        if (absPath.endsWith("/")) {
            absPath = absPath.substring(0, absPath.length() - 1);
        }
        for (String path : subTrees) {
            if (path.equals(absPath) || isAncestor(path, absPath)) {
                return this;
            }
        }
        subTrees.add(requireNonNull(absPath));
        return this;
    }

    /**
     * Adds paths to the FilterConfigMBean's getPaths set
     * @param paths
     * @return
     */
    public FilterBuilder addPathsForMBean(@NotNull Set<String> paths) {
        pathsForMBean.addAll(paths);
        return this;
    }

    /**
     * A set of paths whose subtrees include all events of this filter.
     * @return  list of paths
     * @see #addSubTree(String)
     */
    @NotNull
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
    @NotNull
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
    @NotNull
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
    @NotNull
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
    @NotNull
    public FilterBuilder condition(@NotNull Condition condition) {
        this.condition = requireNonNull(condition);
        return this;
    }

    //------------------------------------------------------------< initial conditions >---

    /**
     * A condition the always holds
     * @return  true condition
     */
    @NotNull
    public Condition includeAll() {
        return ConstantCondition.INCLUDE_ALL;
    }

    /**
     * A condition that never holds
     * @return  false condition
     */
    @NotNull
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
    @NotNull
    public Condition accessControl(@NotNull PermissionProviderFactory permissionProviderFactory) {
        return new ACCondition(requireNonNull(permissionProviderFactory));
    }

    /**
     * A condition that holds on the paths matching a certain pattern.
     * @param pathPattern
     * @return  path condition
     * @see GlobbingPathFilter
     */
    @NotNull
    public Condition path(@NotNull String pathPattern) {
        return new PathCondition(requireNonNull(pathPattern));
    }

    /**
     * A condition that holds for matching event types.
     * @param eventTypes
     * @return event type condition
     * @see EventTypeFilter
     */
    @NotNull
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
    @NotNull
    public Condition nodeType(@NotNull Selector selector, @Nullable String[] ntNames) {
        if (ntNames == null) {
            return includeAll();
        } else if (ntNames.length == 0) {
            return excludeAll();
        } else {
            return new NodeTypeCondition(requireNonNull(selector), ntNames);
        }
    }

    /**
     * A condition that holds for matching uuids.
     * @param selector  selector selecting the node to check the condition on
     * @param uuids uuids to match. This conditions never matches if {@code null} and
     *                always matches if empty.
     * @return node type condition
     */
    @NotNull
    public Condition uuid(@NotNull Selector selector, @Nullable String[] uuids) {
        if (uuids == null) {
            return includeAll();
        } else if (uuids.length == 0) {
            return excludeAll();
        } else {
            return new UniversalCondition(requireNonNull(selector), new UuidPredicate(uuids));
        }
    }

    /**
     * A condition that holds when the property predicate matches.
     * @param selector  selector selecting the node to check the condition on
     * @param name      the name of the property to check the predicate on
     * @param predicate the predicate to check on the named property
     * @return property condition
     */
    @NotNull
    public Condition property(@NotNull Selector selector, @NotNull String name,
            @NotNull Predicate<PropertyState> predicate) {

        return new UniversalCondition(
                requireNonNull(selector),
                new PropertyPredicate(requireNonNull(name), requireNonNull(predicate)));
    }

    /**
     * A condition that holds when the predicate matches.
     * @param selector  selector selecting the node to check the condition on
     * @param predicate the predicate to check on the selected node
     * @return universal condition
     */
    @NotNull
    public Condition universal(@NotNull Selector selector, @NotNull Predicate<NodeState> predicate) {
        return new UniversalCondition(requireNonNull(selector), requireNonNull(predicate));
    }

    /**
     * @return a condition that holds for children of added nodes.
     */
    @NotNull
    public Condition addSubtree() {
        return new AddSubtreeTreeCondition();
    }

    /**
     * @return a condition that holds for children of deleted nodes.
     */
    @NotNull
    public Condition deleteSubtree() {
        return new DeleteSubtreeTreeCondition();
    }

    /**
     * @return a condition that holds for children of the target of a moved node
     */
    @NotNull
    public Condition moveSubtree() {
        return new MoveCondition();
    }

    //------------------------------------------------------------< Compound conditions >---

    /**
     * A compound condition that holds when any of its constituents hold.
     * @param conditions conditions of which any must hold in order for this condition to hold
     * @return  any condition
     */
    @NotNull
    public Condition any(@NotNull Condition... conditions) {
        return new AnyCondition(new ArrayList<>(Arrays.asList(requireNonNull(conditions))));
    }
    /**
     * A compound condition that holds when all of its constituents hold.

     * @param conditions conditions of which all must hold in order for this condition to hold
     * @return  any condition
     */
    @NotNull
    public Condition all(@NotNull Condition... conditions) {
        return new AllCondition(new ArrayList<>(Arrays.asList(requireNonNull(conditions))));
    }

    /**
     * A compound condition that holds when all of its constituents hold.
     * @param conditions conditions of which all must hold in order for this condition to hold
     * @return  any condition
     */
    @NotNull
    public Condition all(@NotNull List<Condition> conditions) {
        return new AllCondition(requireNonNull(conditions));
    }

    /**
     * A compound condition that holds when its constituent does not hold.
     * @param condition condition which must not hold in order for this condition to hold
     * @return  not condition
     */
    @NotNull
    public Condition not(@NotNull Condition condition) {
        return new NotCondition(requireNonNull(condition));
    }

    /**
     * A compound condition that holds when any of its constituents hold.
     * @param conditions conditions of which any must hold in order for this condition to hold
     * @return  any condition
     */
    @NotNull
    public Condition any(@NotNull Iterable<Condition> conditions) {
        return new AnyCondition(requireNonNull(conditions));
    }

    /**
     * A compound condition that holds when all of its constituents hold.
     * @param conditions conditions of which all must hold in order for this condition to hold
     * @return  any condition
     */
    @NotNull
    public Condition all(@NotNull Iterable<Condition> conditions) {
        return new AllCondition(requireNonNull(conditions));
    }

    /**
     * Create a {@code FilterProvider} reflecting the current state of this builder.
     * @return  filter provider of the current state of this builder
     */
    @NotNull
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
            public boolean includeCommit(@NotNull String sessionId, @Nullable CommitInfo info) {
                return (includeSessionLocal || !isLocal(requireNonNull(sessionId), info))
                    && (includeClusterExternal || !isExternal(info))
                    && (includeClusterLocal || isExternal(info));
            }

            @NotNull
            @Override
            public EventFilter getFilter(@NotNull NodeState before, @NotNull NodeState after) {
                return condition.createFilter(requireNonNull(before), requireNonNull(after));
            }

            @NotNull
            @Override
            public Iterable<String> getSubTrees() {
                return subTrees;
            }

            @Override
            public FilterConfigMBean getConfigMBean() {
                return FilterBuilder.this.getConfigMBean();
            }

            private boolean isLocal(String sessionId, CommitInfo info) {
                return info != null && Objects.equals(info.getSessionId(), sessionId);
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

    @NotNull
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

        @NotNull
        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            return new UniversalFilter(before, after, selector, predicate);
        }
    }

    protected static class AddSubtreeTreeCondition implements Condition {
        @NotNull
        @Override
        public EventFilter createFilter(@NotNull NodeState before, @NotNull NodeState after) {
            return AddSubtreeFilter.getInstance();
        }
    }

    protected static class DeleteSubtreeTreeCondition implements Condition {
        @NotNull
        @Override
        public EventFilter createFilter(@NotNull NodeState before, @NotNull NodeState after) {
            return DeleteSubtreeFilter.getInstance();
        }
    }

    protected static class MoveCondition implements Condition {
        @NotNull
        @Override
        public EventFilter createFilter(@NotNull NodeState before, @NotNull NodeState after) {
            return new MoveFilter();
        }
    }

    private static class AnyCondition implements Condition {
        private final Iterable<Condition> conditions;

        public AnyCondition(Iterable<Condition> conditions) {
            this.conditions = conditions;
        }

        public AnyCondition(Condition... conditions) {
            this(new ArrayList<>(Arrays.asList(conditions)));
        }

        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            List<EventFilter> filters = new ArrayList<>();
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
            this(new ArrayList<>(Arrays.asList(conditions)));
        }

        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            List<EventFilter> filters = new ArrayList<>();
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

        @NotNull
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
