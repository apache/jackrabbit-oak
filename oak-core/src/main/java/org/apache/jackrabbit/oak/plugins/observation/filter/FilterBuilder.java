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
import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;

import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventGenerator.Filter;
import org.apache.jackrabbit.oak.plugins.observation.filter.UniversalFilter.Selector;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Builder for {@link FilterProvider} instances.
 */
public final class FilterBuilder {
    private static final int ALL_EVENTS = NODE_ADDED | NODE_REMOVED | NODE_MOVED | PROPERTY_ADDED |
            PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;

    private boolean includeSessionLocal;
    private boolean includeClusterExternal;
    private String basePath = "/";
    private Condition condition;

    public interface Condition {
        @Nonnull
        Filter createFilter(@Nonnull NodeState before, @Nonnull NodeState after, String basePath);
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
     * The base determines a subtree which contains all filter results.
     * In the most simple case where a filter should include all events
     * at a given path that path is at time same time the base path.
     * <p>
     * The base path is used for optimising the filter implementation by
     * upfront exclusion of all parts of the content tree that are out
     * side of the sub tree designated by the base path.
     *
     * @param absPath  absolute path
     * @return  this instance
     */
    @Nonnull
    public FilterBuilder basePath(@Nonnull String absPath) {
        this.basePath = checkNotNull(absPath);
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
     * @param permissionProvider  permission provider for checking whether an item is accessible.
     * @return  access control condition
     * @see  ACFilter
     */
    @Nonnull
    public Condition accessControl(@Nonnull PermissionProvider permissionProvider) {
        return new ACCondition(checkNotNull(permissionProvider));
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

    //------------------------------------------------------------< Compound conditions >---

    /**
     * A compound condition that holds when any of its constituents hold.
     * @param conditions conditions of which any must hold in order for this condition to hold
     * @return  any condition
     */
    @Nonnull
    public Condition any(@Nonnull Condition... conditions) {
        return new AnyCondition(checkNotNull(conditions));
    }

    /**
     * A compound condition that holds when all of its constituents hold.
     * @param conditions conditions of which all must hold in order for this condition to hold
     * @return  any condition
     */
    @Nonnull
    public Condition all(@Nonnull Condition... conditions) {
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
            final String basePath = FilterBuilder.this.basePath;
            final Condition condition = FilterBuilder.this.condition;

            @Override
            public boolean includeCommit(@Nonnull String sessionId, @CheckForNull CommitInfo info) {
                return (includeSessionLocal || !isLocal(checkNotNull(sessionId), info))
                    && (includeClusterExternal || !isExternal(info));
            }

            @Nonnull
            @Override
            public Filter getFilter(@Nonnull NodeState before, @Nonnull NodeState after) {
                return condition.createFilter(checkNotNull(before), checkNotNull(after), basePath);
            }

            @Nonnull
            @Override
            public String getPath() {
                return basePath;
            }

            private boolean isLocal(String sessionId, CommitInfo info) {
                return info != null && Objects.equal(info.getSessionId(), sessionId);
            }

            private boolean isExternal(CommitInfo info) {
                return info == null;
            }
        };
    }

    //------------------------------------------------------------< Conditions >---

    private static NodeState getChildNode(NodeState node, String path) {
        for (String name : PathUtils.elements(path)) {
            node = node.getChildNode(name);
        }
        return node;
    }

    private static class ConstantCondition implements Condition {
        public static final ConstantCondition INCLUDE_ALL = new ConstantCondition(true);
        public static final ConstantCondition EXCLUDE_ALL = new ConstantCondition(false);

        private final boolean value;

        public ConstantCondition(boolean value) {
            this.value = value;
        }

        @Override
        public Filter createFilter(NodeState before, NodeState after, String basePath) {
            return value ? Filters.includeAll() : Filters.excludeAll();
        }
    }

    private static class ACCondition implements Condition {
        private final PermissionProvider permissionProvider;

        public ACCondition(PermissionProvider permissionProvider) {
            this.permissionProvider = permissionProvider;
        }

        @Override
        public Filter createFilter(NodeState before, NodeState after, String basePath) {
            return new ACFilter(before, after, permissionProvider, basePath);
        }
    }

    private static class PathCondition implements Condition {
        private final String pathGlob;

        public PathCondition(String pathGlob) {
            this.pathGlob = pathGlob;
        }

        @Override
        public Filter createFilter(NodeState before, NodeState after, String basePath) {
            return new GlobbingPathFilter(pathGlob);
        }
    }

    private static class EventTypeCondition implements Condition {
        private final int eventTypes;

        public EventTypeCondition(int eventTypes) {
            this.eventTypes = eventTypes;
        }

        @Override
        public Filter createFilter(NodeState before, NodeState after, String basePath) {
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
        public Filter createFilter(NodeState before, NodeState after, String basePath) {
            TypePredicate predicate = new TypePredicate(
                    after.exists() ? after : before, ntNames);
            return new UniversalFilter(
                    getChildNode(before, basePath),
                    getChildNode(after, basePath),
                    selector, predicate);
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
        public Filter createFilter(NodeState before, NodeState after, String basePath) {
            return new UniversalFilter(
                    getChildNode(before, basePath),
                    getChildNode(after, basePath),
                    selector, predicate);
        }
    }

    private static class AnyCondition implements Condition {
        private final Condition[] conditions;

        public AnyCondition(Condition... conditions) {
            this.conditions = conditions;
        }

        @Override
        public Filter createFilter(NodeState before, NodeState after, String basePath) {
            List<Filter> filters = Lists.newArrayList();
            for (Condition condition : conditions) {
                if (condition == ConstantCondition.INCLUDE_ALL) {
                    return ConstantFilter.INCLUDE_ALL;
                } else if (condition != ConstantCondition.EXCLUDE_ALL) {
                    filters.add(condition.createFilter(before, after, basePath));
                }
            }
            return filters.isEmpty()
                ? ConstantFilter.EXCLUDE_ALL
                : Filters.any(filters);
        }
    }

    private static class AllCondition implements Condition {
        private final Condition[] conditions;

        public AllCondition(Condition... conditions) {
            this.conditions = conditions;
        }

        @Override
        public Filter createFilter(NodeState before, NodeState after, String basePath) {
            List<Filter> filters = Lists.newArrayList();
            for (Condition condition : conditions) {
                if (condition == ConstantCondition.EXCLUDE_ALL) {
                    return ConstantFilter.EXCLUDE_ALL;
                } else if (condition != ConstantCondition.INCLUDE_ALL) {
                    filters.add(condition.createFilter(before, after, basePath));
                }
            }
            return filters.isEmpty()
                ? ConstantFilter.INCLUDE_ALL
                : Filters.all(filters);
        }
    }

}
