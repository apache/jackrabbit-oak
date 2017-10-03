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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.annotation.CheckForNull;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.ConfigUtil;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.toArray;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

class Aggregate {
    public static final String MATCH_ALL = "*";

    /**
     * recursive aggregation (for same type nodes) limit default value.
     */
    public static final int RECURSIVE_AGGREGATION_LIMIT_DEFAULT = 5;
    private final String nodeTypeName;
    private final List<? extends Include> includes;
    final int reAggregationLimit;
    private final List<NodeInclude> relativeNodeIncludes;
    private final boolean nodeAggregates;

    Aggregate(String nodeTypeName) {
       this(nodeTypeName, Collections.<Include>emptyList());
    }

    Aggregate(String nodeTypeName, List<? extends Include> includes) {
        this(nodeTypeName, includes, RECURSIVE_AGGREGATION_LIMIT_DEFAULT);
    }

    Aggregate(String nodeTypeName, List<? extends Include> includes,
              int recursionLimit) {
        this.nodeTypeName = nodeTypeName;
        this.includes = ImmutableList.copyOf(includes);
        this.reAggregationLimit = recursionLimit;
        this.relativeNodeIncludes = findRelativeNodeIncludes(includes);
        this.nodeAggregates = hasNodeIncludes(includes);
    }

    public List<? extends Include> getIncludes() {
        return includes;
    }

    public void collectAggregates(NodeState root, ResultCollector collector) {
        if (matchingType(nodeTypeName, root)) {
            List<Matcher> matchers = createMatchers();
            collectAggregates(root, matchers, collector);
        }
    }

    public List<Matcher> createMatchers(AggregateRoot root){
        List<Matcher> matchers = newArrayListWithCapacity(includes.size());
        for (Include include : includes) {
            matchers.add(new Matcher(this, include, root));
        }
        return matchers;
    }

    public boolean hasRelativeNodeInclude(String nodePath) {
        for (NodeInclude ni : relativeNodeIncludes){
            if (ni.matches(nodePath)){
                return true;
            }
        }
        return false;
    }

    public boolean hasNodeAggregates(){
        return nodeAggregates;
    }

    @Override
    public String toString() {
        return nodeTypeName;
    }

    private static boolean matchingType(String nodeTypeName, NodeState nodeState) {
        if (nodeTypeName.equals(ConfigUtil.getPrimaryTypeName(nodeState))) {
            return true;
        }

        for (String mixin : ConfigUtil.getMixinNames(nodeState)) {
            if (nodeTypeName.equals(mixin)) {
                return true;
            }
        }
        return false;
    }

    private static void collectAggregates(NodeState nodeState, List<Matcher> matchers,
                                          ResultCollector collector) {
        if (hasPatternMatcher(matchers)){
            collectAggregatesForPatternMatchers(nodeState, matchers, collector);
        } else {
            collectAggregatesForDirectMatchers(nodeState, matchers, collector);
        }
    }

    private static void collectAggregatesForDirectMatchers(NodeState nodeState, List<Matcher> matchers,
                                          ResultCollector collector) {
        Map<String, ChildNodeEntry> children = Maps.newHashMap();
        //Collect potentially matching child nodestates based on matcher name
        for (Matcher m : matchers){
            String nodeName = m.getNodeName();
            NodeState child = nodeState.getChildNode(nodeName);
            if (child.exists()){
                children.put(nodeName, new MemoryChildNodeEntry(nodeName, child));
            }
        }
        matchChildren(matchers, collector, children.values());
    }

    private static void collectAggregatesForPatternMatchers(NodeState nodeState, List<Matcher> matchers,
                                          ResultCollector collector) {
        matchChildren(matchers, collector, nodeState.getChildNodeEntries());
    }

    private static void matchChildren(List<Matcher> matchers, ResultCollector collector,
                                      Iterable<? extends ChildNodeEntry> children) {
        for (ChildNodeEntry cne : children) {
            List<Matcher> nextSet = newArrayListWithCapacity(matchers.size());
            for (Matcher m : matchers) {
                Matcher result = m.match(cne.getName(), cne.getNodeState());
                if (result.getStatus() == Matcher.Status.MATCH_FOUND){
                    result.collectResults(collector);
                }

                if (result.getStatus() != Matcher.Status.FAIL){
                    nextSet.addAll(result.nextSet());
                }
            }
            if (!nextSet.isEmpty()) {
                collectAggregates(cne.getNodeState(), nextSet, collector);
            }
        }
    }

    private static boolean hasPatternMatcher(List<Matcher> matchers){
        for (Matcher m : matchers){
            if (m.isPatternBased()){
                return true;
            }
        }
        return false;
    }

    private List<Matcher> createMatchers() {
        List<Matcher> matchers = newArrayListWithCapacity(includes.size());
        for (Include include : includes) {
            matchers.add(new Matcher(this, include));
        }
        return matchers;
    }

    private static List<NodeInclude> findRelativeNodeIncludes(List<? extends Include> includes) {
        List<NodeInclude> result = newArrayList();
        for (Include i : includes){
            if (i instanceof NodeInclude){
                NodeInclude ni = (NodeInclude) i;
                if (ni.relativeNode){
                    result.add(ni);
                }
            }
        }
        return ImmutableList.copyOf(result);
    }

    private static boolean hasNodeIncludes(List<? extends Include> includes) {
        return Iterables.any(includes, new Predicate<Include>() {
            @Override
            public boolean apply(Include input) {
                return input instanceof NodeInclude;
            }
        });
    }

    public static interface AggregateMapper {
        @CheckForNull
        Aggregate getAggregate(String nodeTypeName);
    }

    //~-----------------------------------------------------< Includes >

    public static abstract class Include<T> {
        protected final String[] elements;

        public Include(String pattern) {
            this.elements = computeElements(pattern);
        }

        public boolean match(String name, NodeState nodeState, int depth) {
            String element = elements[depth];
            if (MATCH_ALL.equals(element)) {
                return true;
            } else if (element.equals(name)) {
                return true;
            }
            return false;
        }

        public int maxDepth() {
            return elements.length;
        }

        public void collectResults(T rootInclude, String rootIncludePath,
                                   String nodePath, NodeState nodeState,  ResultCollector results) {
            collectResults(nodePath, nodeState, results);
        }

        public void collectResults(String nodePath, NodeState nodeState,
                                            ResultCollector results) {

        }

        public abstract boolean aggregatesProperty(String name);

        @CheckForNull
        public Aggregate getAggregate(NodeState matchedNodeState) {
            return null;
        }

        public boolean isPattern(int depth){
            return MATCH_ALL.equals(elements[depth]);

        }

        public String getElementNameIfNotAPattern(int depth) {
            checkArgument(!isPattern(depth),
                    "Element at %s is pattern instead of specific name in %s", depth, Arrays.toString(elements));
            return elements[depth];
        }
    }

    public static class NodeInclude extends Include<NodeInclude> {
        final String primaryType;
        final boolean relativeNode;
        private final String pattern;
        private final AggregateMapper aggMapper;

        public NodeInclude(AggregateMapper mapper, String pattern) {
            this(mapper, null, pattern, false);
        }

        public NodeInclude(AggregateMapper mapper, String primaryType, String pattern, boolean relativeNode) {
            super(pattern);
            this.pattern = pattern;
            this.primaryType = primaryType;
            this.aggMapper = mapper;
            this.relativeNode = relativeNode;
        }

        @Override
        public boolean match(String name, NodeState nodeState, int depth) {
            //As per JR2 the primaryType is enforced on last element
            //last segment -> add to collector if node type matches
            if (depth == maxDepth() - 1
                    && primaryType != null
                    && !matchingType(primaryType, nodeState)) {
                return false;
            }
            return super.match(name, nodeState, depth);
        }

        @Override
        public void collectResults(NodeInclude rootInclude, String rootIncludePath, String nodePath,
                                   NodeState nodeState, ResultCollector results) {
            //For supporting jcr:contains(jcr:content, 'foo')
            if (rootInclude.relativeNode){
                results.onResult(new NodeIncludeResult(nodePath, rootIncludePath, nodeState));
            }

            //For supporting jcr:contains(., 'foo')
            results.onResult(new NodeIncludeResult(nodePath, nodeState));
        }

        @Override
        public boolean aggregatesProperty(String name) {
            return true;
        }

        @Override
        public Aggregate getAggregate(NodeState matchedNodeState) {
            //Check agg defn for primaryType first
            Aggregate agg = aggMapper.getAggregate(ConfigUtil.getPrimaryTypeName(matchedNodeState));

            //If not found then look for defn for mixins
            if (agg == null) {
                for (String mixin : ConfigUtil.getMixinNames(matchedNodeState)) {
                    agg = aggMapper.getAggregate(mixin);
                    if (agg != null) {
                        break;
                    }
                }
            }
            return agg;
        }

        @Override
        public String toString() {
            return "NodeInclude{" +
                    "primaryType='" + primaryType + '\'' +
                    ", relativeNode=" + relativeNode +
                    ", pattern='" + pattern + '\'' +
                    '}';
        }

        public boolean matches(String nodePath) {
            List<String> pathElements = ImmutableList.copyOf(PathUtils.elements(nodePath));
            if (pathElements.size() != elements.length){
                return false;
            }

            for (int i = 0; i < elements.length; i++){
                String element = elements[i];
                if (MATCH_ALL.equals(element)) {
                    continue;
                }

                if (!element.equals(pathElements.get(i))) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class PropertyInclude extends Include<PropertyInclude> {
        private final PropertyDefinition propertyDefinition;
        private final String propertyName;
        private final Pattern pattern;
        private final String parentPath;

        public PropertyInclude(PropertyDefinition pd) {
            super(getParentPath(pd.name));
            this.propertyDefinition = pd;
            this.propertyName = PathUtils.getName(pd.name);
            this.parentPath = getParentPath(pd.name);

            if (pd.isRegexp) {
                pattern = Pattern.compile(propertyName);
            } else {
                pattern = null;
            }
        }

        @Override
        public void collectResults(String nodePath, NodeState nodeState, ResultCollector results) {
            if (pattern != null) {
                for (PropertyState ps : nodeState.getProperties()) {
                    if (pattern.matcher(ps.getName()).matches()) {
                        results.onResult(new PropertyIncludeResult(ps, propertyDefinition, parentPath));
                    }
                }
            } else {
                PropertyState ps = nodeState.getProperty(propertyName);
                if (ps != null) {
                    results.onResult(new PropertyIncludeResult(ps, propertyDefinition, parentPath));
                }
            }
        }

        @Override
        public boolean aggregatesProperty(String name) {
            if (pattern != null){
                return pattern.matcher(name).matches();
            }
            return propertyName.equals(name);
        }

        @Override
        public String toString() {
            return propertyDefinition.toString();
        }

        public PropertyDefinition getPropertyDefinition() {
            return propertyDefinition;
        }
    }

    public static interface ResultCollector {
        void onResult(NodeIncludeResult result);

        void onResult(PropertyIncludeResult result);
    }

    public static class NodeIncludeResult {
        final NodeState nodeState;
        final String nodePath;
        final String rootIncludePath;

        public NodeIncludeResult(String nodePath, NodeState nodeState) {
            this(nodePath, null, nodeState);
        }

        public NodeIncludeResult(String nodePath, String rootIncludePath, NodeState nodeState) {
            this.nodePath = nodePath;
            this.nodeState = nodeState;
            this.rootIncludePath = rootIncludePath;
        }

        public boolean isRelativeNode(){
            return  rootIncludePath != null;
        }

        @Override
        public String toString() {
            return "NodeIncludeResult{" +
                    "nodePath='" + nodePath + '\'' +
                    ", rootIncludePath='" + rootIncludePath + '\'' +
                    '}';
        }
    }

    public static class PropertyIncludeResult {
        final PropertyState propertyState;
        final PropertyDefinition pd;
        final String propertyPath;
        final String nodePath;

        public PropertyIncludeResult(PropertyState propertyState, PropertyDefinition pd,
                                     String parentPath) {
            this.propertyState = propertyState;
            this.pd = pd;
            this.nodePath = parentPath;
            this.propertyPath = PathUtils.concat(parentPath, propertyState.getName());
        }
    }

    public interface AggregateRoot {
        void markDirty();

        String getPath();
    }

    public static class Matcher {
        public enum Status {CONTINUE, MATCH_FOUND, FAIL}

        private static class RootState {
            final AggregateRoot root;
            final Aggregate rootAggregate;
            final Include rootInclude;

            private RootState(AggregateRoot root, Aggregate rootAggregate, Include rootInclude) {
                this.root = root;
                this.rootAggregate = rootAggregate;
                this.rootInclude = rootInclude;
            }
        }

        private final RootState rootState;
        private final Include currentInclude;
        /**
         * Current depth in the include pattern.
         */
        private final int depth;
        private final Status status;
        private final NodeState matchedNodeState;
        private final String currentPath;

        private final List<String> aggregateStack;

        public Matcher(Aggregate aggregate, Include currentInclude) {
            this(aggregate, currentInclude, null);
        }

        public Matcher(Aggregate aggregate, Include include, AggregateRoot root) {
            this.rootState = new RootState(root, aggregate, include);
            this.depth = 0;
            this.currentInclude = include;
            this.status = Status.CONTINUE;
            this.currentPath = null;
            this.matchedNodeState = null;
            this.aggregateStack = Collections.emptyList();
        }

        private Matcher(Matcher m, Status status, int depth) {
            checkArgument(status == Status.FAIL);
            this.rootState = m.rootState;
            this.depth = depth;
            this.currentInclude = m.currentInclude;
            this.status = status;
            this.currentPath = null;
            this.matchedNodeState = null;
            this.aggregateStack = m.aggregateStack;
        }

        private Matcher(Matcher m, Status status, int depth,
                        NodeState matchedNodeState, String currentPath) {
            checkArgument(status != Status.FAIL);
            this.rootState = m.rootState;
            this.depth = depth;
            this.currentInclude = m.currentInclude;
            this.status = status;
            this.matchedNodeState = matchedNodeState;
            this.currentPath = currentPath;
            this.aggregateStack = m.aggregateStack;
        }

        private Matcher(Matcher m, Include i, String currentPath) {
            checkArgument(m.status == Status.MATCH_FOUND);
            this.rootState = m.rootState;
            this.depth = 0;
            this.currentInclude = i;
            this.status = Status.CONTINUE;
            this.matchedNodeState = null;
            this.currentPath = currentPath;

            List<String> paths = newArrayList(m.aggregateStack);
            paths.add(currentPath);
            this.aggregateStack = ImmutableList.copyOf(paths);
        }

        public boolean isPatternBased() {
            return currentInclude.isPattern(depth);
        }

        /**
         * Returns the nodeName at current depth. This should only be called
         * if and only if #isPatternBased is false otherwise it would throw exception
         */
        public String getNodeName() {
            return currentInclude.getElementNameIfNotAPattern(depth);
        }

        public Matcher match(String name, NodeState nodeState) {
            boolean result = currentInclude.match(name, nodeState, depth);
            if (result){
                if (hasMore()){
                    return new Matcher(this, Status.CONTINUE, depth, nodeState, path(name));
                } else {
                    return new Matcher(this, Status.MATCH_FOUND, depth, nodeState, path(name));
                }
            } else {
                return new Matcher(this, Status.FAIL, depth);
            }
        }

        public Collection<Matcher> nextSet() {
            checkArgument(status != Status.FAIL);

            if (status == Status.MATCH_FOUND){
                Aggregate nextAgg = currentInclude.getAggregate(matchedNodeState);
                if (nextAgg != null){
                    int recursionLevel = aggregateStack.size() + 1;

                    if (recursionLevel >= rootState.rootAggregate.reAggregationLimit){
                        return Collections.emptyList();
                    }

                    List<Matcher> result = Lists.newArrayListWithCapacity(nextAgg.includes.size());
                    for (Include i : nextAgg.includes){
                        result.add(new Matcher(this,  i, currentPath));
                    }
                    return result;
                }
                return Collections.emptyList();
            }

            return Collections.singleton(new Matcher(this, status, depth + 1,
                    null, currentPath));
        }

        public void collectResults(ResultCollector results) {
            checkArgument(status == Status.MATCH_FOUND);

            //If result being collected as part of reaggregation then take path
            //from the stack otherwise its the current path
            String rootIncludePath = aggregateStack.isEmpty() ?  currentPath : aggregateStack.get(0);
            currentInclude.collectResults(rootState.rootInclude, rootIncludePath,
                    currentPath, matchedNodeState, results);
        }

        public void markRootDirty() {
            checkArgument(status == Status.MATCH_FOUND);
            rootState.root.markDirty();
        }

        public String getRootPath() {
            return rootState.root.getPath();
        }

        public String getMatchedPath(){
            checkArgument(status == Status.MATCH_FOUND);
            return currentPath;
        }

        public Include getCurrentInclude(){
            return currentInclude;
        }

        public Status getStatus() {
            return status;
        }

        public boolean aggregatesProperty(String name) {
            checkArgument(status == Status.MATCH_FOUND);
            return currentInclude.aggregatesProperty(name);
        }

        private boolean hasMore() {
            return depth < currentInclude.maxDepth() - 1;
        }

        private String path(String nodeName){
            if (currentPath == null){
                return nodeName;
            } else {
                return PathUtils.concat(currentPath, nodeName);
            }
        }
    }

    //~--------------------------------------------------< utility >

    private static String[] computeElements(String path) {
        return toArray(elements(path), String.class);
    }

}
