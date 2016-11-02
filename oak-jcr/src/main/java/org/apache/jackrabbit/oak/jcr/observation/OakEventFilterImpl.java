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
package org.apache.jackrabbit.oak.jcr.observation;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;
import static javax.jcr.observation.Event.NODE_REMOVED;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.api.observation.JackrabbitEventFilter;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.jcr.observation.filter.OakEventFilter;
import org.apache.jackrabbit.oak.plugins.nodetype.TypePredicate;
import org.apache.jackrabbit.oak.plugins.observation.filter.ConstantFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventAggregator;
import org.apache.jackrabbit.oak.plugins.observation.filter.EventFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterBuilder.Condition;
import org.apache.jackrabbit.oak.plugins.observation.filter.Filters;
import org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathFilter;
import org.apache.jackrabbit.oak.plugins.observation.filter.GlobbingPathHelper;
import org.apache.jackrabbit.oak.plugins.observation.filter.PermissionProviderFactory;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Implements OakEventFilter which is an extension to the JackrabbitEventFilter
 * with features only supported by Oak.
 */
public class OakEventFilterImpl extends OakEventFilter {

    static class NodeTypeAggregationFilter implements EventFilter, Condition {

        private final String[] nodeTypes;
        private final String[] relativeGlobPaths;
        private final boolean includeThis;
        private TypePredicate predicate;

        NodeTypeAggregationFilter(String[] nodeTypes, String[] relativeGlobPaths) {
            this.nodeTypes = nodeTypes;
            this.relativeGlobPaths = relativeGlobPaths;
            boolean includeThis = false;
            for (String aRelativeGlobPath : relativeGlobPaths) {
                if (aRelativeGlobPath.equals("") || aRelativeGlobPath.equals(".") || aRelativeGlobPath.equals("*")
                        || aRelativeGlobPath.equals("**")) {
                    includeThis = true;
                }
            }
            this.includeThis = includeThis;
        }

        @Override
        public boolean includeAdd(PropertyState after) {
            // the AggregateFilter 'waits' for the first hit based on nodeTypes
            // at which point it switches to a GlobbingPathFilter - so property
            // changes will be handled in the GlobbingPathFilter, never here.
            return false;
        }

        @Override
        public boolean includeChange(PropertyState before, PropertyState after) {
            // the AggregateFilter 'waits' for the first hit based on nodeTypes
            // at which point it switches to a GlobbingPathFilter - so property
            // changes will be handled in the GlobbingPathFilter, never here.
            return false;
        }

        @Override
        public boolean includeDelete(PropertyState before) {
            // the AggregateFilter 'waits' for the first hit based on nodeTypes
            // at which point it switches to a GlobbingPathFilter - so property
            // changes will be handled in the GlobbingPathFilter, never here.
            return false;
        }

        @Override
        public boolean includeAdd(String name, NodeState after) {
            return includeThis && predicate.apply(after);
        }

        @Override
        public boolean includeDelete(String name, NodeState before) {
            return includeThis && predicate.apply(before);
        }

        @Override
        public boolean includeMove(String sourcePath, String name, NodeState moved) {
            return includeThis && predicate.apply(moved);
        }

        @Override
        public boolean includeReorder(String destName, String name, NodeState reordered) {
            return includeThis && predicate.apply(reordered);
        }

        @Override
        public EventFilter create(String name, NodeState before, NodeState after) {
            boolean predicateMatches = false;
            if (after.exists()) {
                predicateMatches = predicate.apply(after);
            } else {
                predicateMatches = predicate.apply(before);
            }
            if (predicateMatches) {
                // greedy match - we switch to the globbing path filters
                List<EventFilter> filters = newArrayList();
                for (String relativeGlobPath : relativeGlobPaths) {
                    if (relativeGlobPath.endsWith("*")) {
                        filters.add(new GlobbingPathFilter(relativeGlobPath));
                    } else {
                        filters.add(new GlobbingPathFilter(relativeGlobPath + "/*"));
                    }
                }
                return filters.isEmpty() ? ConstantFilter.EXCLUDE_ALL : Filters.any(filters);
            } else {
                // non-match - we stay with this filter
                return this;
            }
        }

        @Override
        public EventFilter createFilter(NodeState before, NodeState after) {
            if (after.exists()) {
                predicate = new TypePredicate(after, nodeTypes);
            } else {
                predicate = new TypePredicate(before, nodeTypes);
            }
            return this;
        }

    }

    private static final class NodeTypeAggregator implements EventAggregator {
        private final String[] nodeTypes;
        private final Pattern[] relativePathPatterns;

        private NodeTypeAggregator(String[] nodeTypes, Pattern[] relativePathPatterns) {
            this.nodeTypes = nodeTypes;
            this.relativePathPatterns = relativePathPatterns;
        }

        @Override
        public int aggregate(NodeState root, List<ChildNodeEntry> parents, ChildNodeEntry childNodeState) {
            final TypePredicate nodeTypePredicate = new TypePredicate(root, nodeTypes);
            final int depth = parents.size();
            for (int i = 0; i < depth; i++) {
                ChildNodeEntry child = parents.get(i);
                NodeState nodeState = child.getNodeState();
                if (!nodeTypePredicate.apply(nodeState)) {
                    continue;
                }
                if (i + 1 <= depth) {
                    String childPath = asPath(parents.subList(i + 1, depth));
                    for (Pattern pattern : relativePathPatterns) {
                        if (pattern.matcher(childPath).matches()) {
                            return depth - i;
                        }
                    }
                }
            }
            return 0;
        }

        @Override
        public int aggregate(NodeState root, List<ChildNodeEntry> parents, PropertyState propertyState) {
            final TypePredicate nodeTypePredicate = new TypePredicate(root, nodeTypes);
            final int depth = parents.size();
            for (int i = 0; i < depth; i++) {
                ChildNodeEntry child = parents.get(i);
                NodeState nodeState = child.getNodeState();
                if (!nodeTypePredicate.apply(nodeState)) {
                    continue;
                }
                if (i + 1 <= depth) {
                    String childPath = asPath(parents.subList(i + 1, depth));
                    for (Pattern pattern : relativePathPatterns) {
                        if (pattern.matcher(childPath).matches()) {
                            return depth - (i + 1);
                        }
                    }
                }
            }
            return 0;
        }

        private String asPath(List<ChildNodeEntry> children) {
            if (children.isEmpty()) {
                return "";
            }
            StringBuilder sb = new StringBuilder();
            for (ChildNodeEntry child : children) {
                if (sb.length() != 0) {
                    sb.append("/");
                }
                sb.append(child.getName());
            }
            return sb.toString();
        }
    }

    private final JackrabbitEventFilter delegate;
    
    /** whether or not applyNodeTypeOnSelf feature is enabled */
    private boolean applyNodeTypeOnSelf;

    /** whether or not includeAncestorsRemove feature is enabled */
    private boolean includeAncestorRemove;

    /** whether or not includeSubTreeOnRemove feature is enabled */
    private boolean includeSubtreeOnRemove;

    private String[] globPaths;

    private FilterBuilder builder;

    private Condition all;

    private EventAggregator aggregator;

    public OakEventFilterImpl(@Nonnull JackrabbitEventFilter delegate) {
        checkNotNull(delegate);
        this.delegate = delegate;
    }

    @Override
    public String getAbsPath() {
        return delegate.getAbsPath();
    }
    
    @Override
    public JackrabbitEventFilter setAbsPath(String absPath) {
        delegate.setAbsPath(absPath);
        return this;
    }
    
    @Override
    public String[] getAdditionalPaths() {
        return delegate.getAdditionalPaths();
    }

    @Override
    public JackrabbitEventFilter setAdditionalPaths(String... absPaths) {
        delegate.setAdditionalPaths(absPaths);
        return this;
    }

    @Override
    public int getEventTypes() {
        return delegate.getEventTypes();
    }
    
    @Override
    public JackrabbitEventFilter setEventTypes(int eventTypes) {
        delegate.setEventTypes(eventTypes);
        return this;
    }

    @Override
    public String[] getExcludedPaths() {
        return delegate.getExcludedPaths();
    }
    
    @Override
    public JackrabbitEventFilter setExcludedPaths(String... excludedPaths) {
        delegate.setExcludedPaths(excludedPaths);
        return this;
    }
    
    @Override
    public String[] getIdentifiers() {
        return delegate.getIdentifiers();
    }

    @Override
    public JackrabbitEventFilter setIdentifiers(String[] identifiers) {
        delegate.setIdentifiers(identifiers);
        return this;
    }

    @Override
    public boolean getIsDeep() {
        return delegate.getIsDeep();
    }
    
    @Override
    public JackrabbitEventFilter setIsDeep(boolean isDeep) {
        delegate.setIsDeep(isDeep);
        return this;
    }
    
    @Override
    public String[] getNodeTypes() {
        return delegate.getNodeTypes();
    }

    @Override
    public JackrabbitEventFilter setNodeTypes(String[] nodeTypeNames) {
        delegate.setNodeTypes(nodeTypeNames);
        return this;
    }
    
    @Override
    public boolean getNoExternal() {
        return delegate.getNoExternal();
    }

    @Override
    public JackrabbitEventFilter setNoExternal(boolean noExternal) {
        delegate.setNoExternal(noExternal);
        return this;
    }

    @Override
    public boolean getNoInternal() {
        return delegate.getNoInternal();
    }
    
    @Override
    public JackrabbitEventFilter setNoInternal(boolean noInternal) {
        delegate.setNoInternal(noInternal);
        return this;
    }

    @Override
    public boolean getNoLocal() {
        return delegate.getNoLocal();
    }
    
    @Override
    public JackrabbitEventFilter setNoLocal(boolean noLocal) {
        delegate.setNoLocal(noLocal);
        return this;
    }

    @Override
    public OakEventFilter withApplyNodeTypeOnSelf() {
        this.applyNodeTypeOnSelf = true;
        return this;
    }
    
    boolean getApplyNodeTypeOnSelf() {
        return applyNodeTypeOnSelf;
    }

    @Override
    public OakEventFilter withIncludeAncestorsRemove() {
        this.includeAncestorRemove = true;
        return this;
    }

    boolean getIncludeAncestorsRemove() {
        return includeAncestorRemove;
    }

    private void addAncestorsRemoveCondition(Set<String> parentPaths, String globPath) {
        if (globPath == null || !globPath.contains("/")) {
            return;
        }
        // from /a/b/c         => add /a and /a/b
        // from /a/b/**        => add /a
        // from /a             => add nothing
        // from /              => add nothing
        // from /a/b/**/*.html => add /a
        // from /a/b/*/*.html  => add /a

        Iterator<String> it = PathUtils.elements(globPath).iterator();
        StringBuffer sb = new StringBuffer();
        while(it.hasNext()) {
            String element = it.next();
            if (element.contains("*")) {
                if (parentPaths.size() > 0) {
                    parentPaths.remove(parentPaths.size()-1);
                }
                break;
            } else if (!it.hasNext()) {
                break;
            }
            sb.append("/");
            sb.append(element);
            parentPaths.add(sb.toString() + "/*");
        }
    }

    public Condition wrapMainCondition(Condition mainCondition, FilterBuilder filterBuilder, PermissionProviderFactory permissionProviderFactory) {
        if (!includeAncestorRemove || (getEventTypes() & NODE_REMOVED) != NODE_REMOVED) {
            return mainCondition;
        }
        Set<String> parentPaths = new HashSet<String>();
        addAncestorsRemoveCondition(parentPaths, getAbsPath());
        if (getAdditionalPaths() != null) {
            for (String absPath : getAdditionalPaths()) {
                addAncestorsRemoveCondition(parentPaths, absPath);
            }
        }
        if (globPaths != null) {
            for (String globPath : globPaths) {
                addAncestorsRemoveCondition(parentPaths, globPath);
            }
        }
        if (parentPaths.size() == 0) {
            return mainCondition;
        }
        List<Condition> ancestorsRemoveConditions = new LinkedList<Condition>();
        for (String aParentPath : parentPaths) {
            ancestorsRemoveConditions.add(filterBuilder.path(aParentPath));
        }
        return filterBuilder.any(
                        mainCondition,
                        filterBuilder.all(
                                filterBuilder.eventType(NODE_REMOVED),
                                filterBuilder.any(ancestorsRemoveConditions),
                                filterBuilder.deleteSubtree(),
                                filterBuilder.accessControl(permissionProviderFactory)
                                )
                        );
    }

    @Override
    public OakEventFilter withIncludeSubtreeOnRemove() {
        this.includeSubtreeOnRemove = true;
        return this;
    }
    
    boolean getIncludeSubtreeOnRemove() {
        return includeSubtreeOnRemove;
    }

    @Override
    public OakEventFilter withIncludeGlobPaths(String... globPaths) {
        if (this.globPaths == null) {
            this.globPaths = globPaths;
        } else {
            List<String> combo = new LinkedList<String>();
            combo.addAll(Arrays.asList(this.globPaths));
            combo.addAll(Arrays.asList(globPaths));
            this.globPaths = combo.toArray(new String[combo.size()]);
        }
        return this;
    }

    String[] getIncludeGlobPaths() {
        return globPaths;
    }

    public FilterBuilder builder() {
        if (builder == null) {
            builder = new FilterBuilder();
        }
        return builder;
    }

    public OakEventFilterImpl and(Condition... condition) {
        checkNotNull(condition);
        if (all == null) {
            all = builder().all(condition);
        } else {
            all = builder().all(all, builder.all(condition));
        }
        return this;
    }

    public OakEventFilterImpl aggregator(EventAggregator aggregator) {
        checkNotNull(aggregator);
        this.aggregator = aggregator;
        return this;
    }

    public Condition getAnds() {
        return all;
    }

    public EventAggregator getAggregator() {
        return aggregator;
    }

    @Override
    public OakEventFilter withNodeTypeAggregate(String[] nodeTypes, String[] relativeGlobPaths) {
        final Pattern[] relativePathPatterns = new Pattern[relativeGlobPaths.length];
        for (int i = 0; i < relativePathPatterns.length; i++) {
            relativePathPatterns[i] = Pattern.compile(GlobbingPathHelper.globPathAsRegex(relativeGlobPaths[i]));
        }
        aggregator(new NodeTypeAggregator(nodeTypes, relativePathPatterns));

        and(new NodeTypeAggregationFilter(nodeTypes, relativeGlobPaths));

        return this;
    }

}
