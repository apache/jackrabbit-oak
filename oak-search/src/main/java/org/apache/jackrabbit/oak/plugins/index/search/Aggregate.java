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
package org.apache.jackrabbit.oak.plugins.index.search;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.collections.IterableUtils;
import org.apache.jackrabbit.oak.commons.collections.ListUtils;
import org.apache.jackrabbit.oak.plugins.index.search.util.ConfigUtil;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.jackrabbit.oak.commons.PathUtils.elements;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;
import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

/**
 * Aggregates text from child nodes for fulltext queries.
 * <p>
 * Example: let's say node /x is of type 'web page', but the actual content is
 * stored in child nodes; say /x/section1 contains "Hello" and /x/section2
 * contains "World". If index aggregation is configured correctly, it will
 * combine all the text of the child nodes, and index that as /x. When doing a
 * fulltext search for "Hello World", the index will then return /x.
 */
public class Aggregate {

    public static final String MATCH_ALL = "*";

    /**
     * recursive aggregation (for same type nodes) limit default value.
     */
    public static final int RECURSIVE_AGGREGATION_LIMIT_DEFAULT = 5;
    private final String nodeTypeName;
    private final List<? extends Include> includes;
    public final int reAggregationLimit;
    private final NodeInclude[] relativeNodeIncludes;
    private final boolean nodeAggregates;

    public Aggregate(String nodeTypeName, List<? extends Include> includes) {
        this(nodeTypeName, includes, RECURSIVE_AGGREGATION_LIMIT_DEFAULT);
    }

    Aggregate(String nodeTypeName, List<? extends Include> includes, int recursionLimit) {
        this.nodeTypeName = nodeTypeName;
        this.includes = List.copyOf(includes);
        this.reAggregationLimit = recursionLimit;
        this.relativeNodeIncludes = findRelativeNodeIncludes(includes);
        this.nodeAggregates = includes.stream().anyMatch(input -> input instanceof NodeInclude);
    }

    public List<? extends Include> getIncludes() {
        return includes;
    }

    public void collectAggregates(NodeState root, ResultCollector collector) {
        if (matchingType(nodeTypeName, root)) {
            Matcher[] matchers = createMatchers();
            collectAggregates(root, matchers, collector);
        }
    }

    public List<Matcher> createMatchers(AggregateRoot root) {
        Matcher[] matchers = new Matcher[includes.size()];
        for (int i = 0; i < includes.size(); i++) {
            matchers[i] = new Matcher(this, includes.get(i), root);
        }
        // Wrap the array in an ArrayList, this avoids copying the array
        return Arrays.asList(matchers);
    }

    public boolean hasRelativeNodeInclude(String nodePath) {
        for (NodeInclude ni : relativeNodeIncludes) {
            if (ni.matches(nodePath)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasNodeAggregates() {
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

    private static void collectAggregates(NodeState nodeState, Matcher[] matchers, ResultCollector collector) {
        if (hasPatternMatcher(matchers)) {
            collectAggregatesForPatternMatchers(nodeState, matchers, collector);
        } else {
            collectAggregatesForDirectMatchers(nodeState, matchers, collector);
        }
    }

    private static void collectAggregatesForDirectMatchers(NodeState nodeState, Matcher[] matchers,
                                                           ResultCollector collector) {
        Map<String, ChildNodeEntry> children = null;
        //Collect potentially matching child nodestates based on matcher name
        for (Matcher m : matchers) {
            String nodeName = m.getNodeName();
            NodeState child = nodeState.getChildNode(nodeName);
            if (child.exists()) {
                if (children == null) {
                    children = new HashMap<>();
                }
                children.put(nodeName, new MemoryChildNodeEntry(nodeName, child));
            }
        }
        if (children != null) {
            matchChildren(matchers, collector, children.values());
        }
    }

    private static void collectAggregatesForPatternMatchers(NodeState nodeState, Matcher[] matchers,
                                                            ResultCollector collector) {
        matchChildren(matchers, collector, nodeState.getChildNodeEntries());
    }

    private static void matchChildren(Matcher[] matchers, ResultCollector collector,
                                      Iterable<? extends ChildNodeEntry> children) {
        // Performance critical code: create nextSet lazily. And once created, reuse the same instance.
        List<Matcher> nextSet = null;
        for (ChildNodeEntry cne : children) {
            for (Matcher m : matchers) {
                Matcher result = m.match(cne.getName(), cne.getNodeState());
                if (result.getStatus() == Matcher.Status.MATCH_FOUND) {
                    result.collectResults(collector);
                }

                if (result.getStatus() != Matcher.Status.FAIL) {
                    if (nextSet == null) {
                        nextSet = new ArrayList<>();
                    }
                    result.nextSet(nextSet);
                }
            }
            if (nextSet !=null && !nextSet.isEmpty()) {
                collectAggregates(cne.getNodeState(), nextSet.toArray(new Matcher[0]), collector);
                // Clear the set so it can be reused. This reduces object allocation overhead.
                nextSet.clear();
            }
        }
    }

    private static boolean hasPatternMatcher(Matcher[] matchers) {
        for (Matcher m : matchers) {
            if (m.isPatternBased()) {
                return true;
            }
        }
        return false;
    }

    private Matcher[] createMatchers() {
        Matcher[] matchers = new Matcher[includes.size()];
        for (int i = 0; i < includes.size(); i++) {
            matchers[i] = new Matcher(this, includes.get(i));
        }
        return matchers;
    }

    private static NodeInclude[] findRelativeNodeIncludes(List<? extends Include> includes) {
        List<NodeInclude> result = new ArrayList<>();
        for (Include i : includes) {
            if (i instanceof NodeInclude) {
                NodeInclude ni = (NodeInclude) i;
                if (ni.relativeNode) {
                    result.add(ni);
                }
            }
        }
        return result.toArray(new NodeInclude[0]);
    }

    public interface AggregateMapper {
        @Nullable
        Aggregate getAggregate(String nodeTypeName);
    }

    //~-----------------------------------------------------< Includes >

    public static abstract class Include {
        protected final String[] elements;

        public Include(String pattern) {
            this.elements = computeElements(pattern);
        }

        public boolean match(String name, NodeState nodeState, int depth) {
            String element = elements[depth];
            if (MATCH_ALL.equals(element)) {
                return true;
            } else return element.equals(name);
        }

        public int maxDepth() {
            return elements.length;
        }

        public void collectResults(Include rootInclude, String rootIncludePath,
                                   String nodePath, NodeState nodeState, ResultCollector results) {
            collectResults(nodePath, nodeState, results);
        }

        public void collectResults(String nodePath, NodeState nodeState, ResultCollector results) {
        }

        public abstract boolean aggregatesProperty(String name);

        @Nullable
        public Aggregate getAggregate(NodeState matchedNodeState) {
            return null;
        }

        public boolean isPattern(int depth) {
            return MATCH_ALL.equals(elements[depth]);
        }

        public String getElementNameIfNotAPattern(int depth) {
            if (isPattern(depth)) {
                throw new IllegalArgumentException("Element at " + depth + " is pattern instead of specific name in " + Arrays.toString(elements));
            }
            return elements[depth];
        }
    }

    public static class NodeInclude extends Include {
        public final String primaryType;
        public final boolean relativeNode;
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
        public void collectResults(Include include, String rootIncludePath, String nodePath,
                                   NodeState nodeState, ResultCollector results) {
            //For supporting jcr:contains(jcr:content, 'foo')
            if (!(include instanceof NodeInclude)) {
                throw new IllegalArgumentException("" + include);
            }
            NodeInclude rootInclude = (NodeInclude) include;
            if (rootInclude.relativeNode) {
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
            List<String> pathElements = ListUtils.toList(PathUtils.elements(nodePath));
            if (pathElements.size() != elements.length) {
                return false;
            }

            for (int i = 0; i < elements.length; i++) {
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

    public static class PropertyInclude extends Include {
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

        /**
         * Collect the aggregated results ignoring hidden properties
         */
        @Override
        public void collectResults(String nodePath, NodeState nodeState, ResultCollector results) {
            if (pattern != null) {
                for (PropertyState ps : nodeState.getProperties()) {
                    if (!NodeStateUtils.isHidden(ps.getName()) && pattern.matcher(ps.getName()).matches()) {
                        results.onResult(new PropertyIncludeResult(ps, propertyDefinition, parentPath));
                    }
                }
            } else {
                PropertyState ps = nodeState.getProperty(propertyName);
                if (ps != null && !NodeStateUtils.isHidden(ps.getName())) {
                    results.onResult(new PropertyIncludeResult(ps, propertyDefinition, parentPath));
                }
            }
        }

        @Override
        public boolean aggregatesProperty(String name) {
            if (pattern != null) {
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

    public static class FunctionInclude extends PropertyInclude {

        public FunctionInclude(PropertyDefinition pd) {
            super(pd);
        }

        @Override
        public void collectResults(String nodePath, NodeState nodeState, ResultCollector results) {
            // Do Nothing here - Function includes aren't indexed using aggregate of property parameters of the function itself
        }

    }

    public interface ResultCollector {
        void onResult(NodeIncludeResult result);

        void onResult(PropertyIncludeResult result);
    }

    public static class NodeIncludeResult {
        public final NodeState nodeState;
        public final String nodePath;
        public final String rootIncludePath;

        public NodeIncludeResult(String nodePath, NodeState nodeState) {
            this(nodePath, null, nodeState);
        }

        public NodeIncludeResult(String nodePath, String rootIncludePath, NodeState nodeState) {
            this.nodePath = nodePath;
            this.nodeState = nodeState;
            this.rootIncludePath = rootIncludePath;
        }

        public boolean isRelativeNode() {
            return rootIncludePath != null;
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
        public final PropertyState propertyState;
        public final PropertyDefinition pd;
        public final String propertyPath;
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
            this.aggregateStack = List.of();
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

            List<String> paths = new ArrayList<>(m.aggregateStack);
            paths.add(currentPath);
            this.aggregateStack = List.copyOf(paths);
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
            if (result) {
                if (hasMore()) {
                    return new Matcher(this, Status.CONTINUE, depth, nodeState, path(name));
                } else {
                    return new Matcher(this, Status.MATCH_FOUND, depth, nodeState, path(name));
                }
            } else {
                return new Matcher(this, Status.FAIL, depth);
            }
        }

        public void nextSet(List<Matcher> destination) {
            checkArgument(status != Status.FAIL);

            if (status == Status.MATCH_FOUND) {
                Aggregate nextAgg = currentInclude.getAggregate(matchedNodeState);
                if (nextAgg != null) {
                    int recursionLevel = aggregateStack.size() + 1;

                    if (recursionLevel >= rootState.rootAggregate.reAggregationLimit) {
                        return;
                    }

                    for (int i = 0; i < nextAgg.includes.size(); i++) {
                        destination.add(new Matcher(this, nextAgg.includes.get(i), currentPath));
                    }
                }
            } else {
                destination.add(new Matcher(this, status, depth + 1, null, currentPath));
            }
        }

        public void collectResults(ResultCollector results) {
            checkArgument(status == Status.MATCH_FOUND);

            //If result being collected as part of re-aggregation then take path
            //from the stack otherwise it's the current path
            String rootIncludePath = aggregateStack.isEmpty() ? currentPath : aggregateStack.get(0);
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

        public String getMatchedPath() {
            checkArgument(status == Status.MATCH_FOUND);
            return currentPath;
        }

        public Include getCurrentInclude() {
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

        private String path(String nodeName) {
            if (currentPath == null) {
                return nodeName;
            } else {
                return PathUtils.concat(currentPath, nodeName);
            }
        }
    }

    //~--------------------------------------------------< utility >

    private static String[] computeElements(String path) {
        return IterableUtils.toArray(elements(path), String.class);
    }

}
