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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

        private final Map<String,Pattern> patternMap = new HashMap<String,Pattern>();
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
                        filters.add(new GlobbingPathFilter(relativeGlobPath, patternMap));
                    } else {
                        filters.add(new GlobbingPathFilter(relativeGlobPath + "/*", patternMap));
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

    private EventAggregator aggregator;

    private boolean withNodeTypeAggregate;

    private Set<String> relativeGlobPaths;

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

    /**
     * This helper method goes through the provided globPath and adds
     * each parent (ancestor)'s path to the ancestorPaths set.
     * <p>
     * OAK-5619 : this used to add "${parent}/*" type ancestor paths, however
     * that was wrong: we must only take the actual "${parent}"s to which we want
     * to listen to. Also, the glob case looks slightly different than originally
     * implemented:
     * <ul>
     *  <li>* : we treat this as a normal name, ie as a normal parent and continue normally</li>
     *  <li>**: when a ** is hit, the loop through the elements can be stopped,
     *  as ** includes all children already, so no further paths are needed.</li>
     * </ul>
     * @param ancestorPaths the set to which the ancestors of globPath will
     * be added to
     * @param globPath the input path that may contain globs
     */
    static void addAncestorPaths(Set<String> ancestorPaths, String globPath) {
        if (globPath == null || !globPath.contains("/")) {
            return;
        }
        // from /a/b/c         => add /a, /a/b, /a/b/c
        // from /a/b/**        => add /a, /a/b, /a/b/**
        // from /a             => add /a
        // from /              => add nothing
        // from /a/b/**/*.html => add /a, /a/b, /a/b/**
        // from /a/b/*/*.html  => add /a, /a/b, /a/b/*, /a/b/*/*.html
        // from /a/b/*/d       => add /a, /a/b, /a/b/*, /a/b/*/d
        // from /a/b/*/d/e     => add /a, /a/b, /a/b/*, /a/b/*/d, /a/b/*/d/e

        Iterator<String> it = PathUtils.elements(globPath).iterator();
        StringBuffer sb = new StringBuffer();
        while(it.hasNext()) {
            String element = it.next();
            sb.append("/");
            sb.append(element);
            ancestorPaths.add(sb.toString());
            if (element.equals("**")) {
                // then we can stop as ** contains everything already
                break;
            }
        }
    }

    public Condition wrapMainCondition(Condition mainCondition, FilterBuilder filterBuilder, PermissionProviderFactory permissionProviderFactory) {
        if (!includeAncestorRemove || (getEventTypes() & NODE_REMOVED) != NODE_REMOVED) {
            return mainCondition;
        }
        Set<String> parentPaths = new HashSet<String>();
        addAncestorPaths(parentPaths, getAbsPath());
        if (getAdditionalPaths() != null) {
            for (String absPath : getAdditionalPaths()) {
                addAncestorPaths(parentPaths, absPath);
            }
        }
        if (globPaths != null) {
            for (String globPath : globPaths) {
                addAncestorPaths(parentPaths, globPath);
            }
        }
        if (parentPaths.size() == 0) {
            return mainCondition;
        }
        List<Condition> ancestorsIncludeConditions = new LinkedList<Condition>();
        for (String aParentPath : parentPaths) {
            ancestorsIncludeConditions.add(
                    filterBuilder.all(
                            filterBuilder.path(aParentPath),
                            filterBuilder.deleteSubtree()));
        }
        if (globPaths != null) {
            for (String globPath : globPaths) {
                if (globPath.contains("**") || globPath.contains("/*/")) {
                    ancestorsIncludeConditions.add(filterBuilder.path(globPath));
                    // unlike the known parent case above, this variant doesn't filter out deleteSubtrees
                    // that way it will report the actual file deleted
                }
            }
        }
        return filterBuilder.any(
                        mainCondition,
                        filterBuilder.all(
                                filterBuilder.eventType(NODE_REMOVED),
                                filterBuilder.any(ancestorsIncludeConditions),
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

    public OakEventFilterImpl aggregator(EventAggregator aggregator) {
        checkNotNull(aggregator);
        this.aggregator = aggregator;
        return this;
    }

    public Condition getAdditionalIncludeConditions(Set<String> includePaths) {
        if (!withNodeTypeAggregate) {
            return null;
        }
        List<Condition> additionalIncludeConditions = new LinkedList<Condition>();
        // for nodeTypeAggregation in OR mode we must append
        // the relativeGlobPaths to all includePaths
        for (String includePath : includePaths) {
            if (includePath.equals("**") || includePath.endsWith("/*") || includePath.endsWith("/**")) {
                // this will include anything, so nothing to append in this case
            } else {
                // otherwise append all the relativeGlobPaths, except ""
                for (String relativeGlobPath : relativeGlobPaths) {
                    if (relativeGlobPath.equals("")) {
                        // this corresponds to 'SELF' which is already included, so skip
                        continue;
                    } else {
                        String additionalGlobPath;
                        if (includePath.endsWith("/")) {
                            additionalGlobPath = includePath + relativeGlobPath;
                        } else {
                            additionalGlobPath = includePath + "/" + relativeGlobPath;
                        }
                        additionalIncludeConditions.add(builder().path(additionalGlobPath));
                        additionalIncludeConditions.add(builder().path(additionalGlobPath + "/*"));
                    }
                }
            }
        }
        if (additionalIncludeConditions.size() == 0) {
            return null;
        } else {
            return builder().any(additionalIncludeConditions);
        }
    }

    public EventAggregator getAggregator() {
        return aggregator;
    }

    @Override
    public OakEventFilter withNodeTypeAggregate(String[] nodeTypes, String[] relativeGlobPaths) {
        this.withNodeTypeAggregate = true;
        if (this.relativeGlobPaths == null) {
            this.relativeGlobPaths = new HashSet<String>();
        }
        this.relativeGlobPaths.addAll(Arrays.asList(relativeGlobPaths));
        
        final Pattern[] relativePathPatterns = new Pattern[relativeGlobPaths.length];
        for (int i = 0; i < relativePathPatterns.length; i++) {
            relativePathPatterns[i] = Pattern.compile(GlobbingPathHelper.globPathAsRegex(relativeGlobPaths[i]));
        }
        aggregator(new NodeTypeAggregator(nodeTypes, relativePathPatterns));

        // OAK-5096: the following AND is not necessary:
//        and(new NodeTypeAggregationFilter(nodeTypes, relativeGlobPaths));

        return this;
    }

    /**
     * A hook called by the ObservationManagerImpl before creating the ChangeSetFilterImpl
     * which allows this filter to adjust the includePaths according to its
     * enabled flags.
     * <p>
     * This is used to set the includePath to be '/' in case includeAncestorRemove
     * is set. The reason for this is that we must catch parent removals and can thus
     * not apply the normally applied prefilter paths.
     * @param includePaths the set to adjust depending on filter flags
     */
    Set<String> calcPrefilterIncludePaths(Set<String> includePaths) {
        Set<String> paths = new HashSet<String>();
        if (includeAncestorRemove) {
            for (String includePath : includePaths) {
                addAncestorPaths(paths, includePath);
            }
        }
        if (withNodeTypeAggregate) {
            // ensure that, for prefixing, all includePaths allow additional
            // subpaths for the aggregation - this can be simplified
            // to just allow anything (**) below there, as this is just
            // about prefiltering, not actual (precise) filtering.
            // so the goal is just to ensure nothing is erroneously excluded
            // so more including is fine.
            for (String includePath : includePaths) {
                if (includePath.equals("**") || includePath.endsWith("/**")) {
                    // skip, this is fine
                } else if (includePath.endsWith("/")) {
                    paths.add(includePath + "**");
                } else {
                    paths.add(includePath + "/**");
                }
            }
        }
        return paths;
    }

}
