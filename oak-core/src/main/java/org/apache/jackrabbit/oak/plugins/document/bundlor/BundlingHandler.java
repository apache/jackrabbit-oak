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

package org.apache.jackrabbit.oak.plugins.document.bundlor;

import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.ROOT_PATH;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

public class BundlingHandler {
    /**
     * True property which is used to mark the presence of relative node
     * This needs to be set when a bundled relative node is added
     */
    private static final PropertyState NODE_PRESENCE_MARKER =
            PropertyStates.createProperty(DocumentBundlor.META_PROP_NODE, Boolean.TRUE);
    private final BundledTypesRegistry registry;
    private final String path;
    private final BundlingContext ctx;
    private final NodeState nodeState;

    public BundlingHandler(BundledTypesRegistry registry) {
        this(registry, BundlingContext.NULL, ROOT_PATH, EMPTY_NODE);
    }

    private BundlingHandler(BundledTypesRegistry registry, BundlingContext ctx, String path, NodeState nodeState) {
        this.registry = checkNotNull(registry);
        this.path = path;
        this.ctx = ctx;
        this.nodeState = nodeState;
    }

    /**
     * Returns property path. For non bundling case this is the actual property name
     * while for bundling case this is the relative path from bundling root
     */
    public String getPropertyPath(String propertyName) {
        return ctx.isBundling() ? ctx.getPropertyPath(propertyName) : propertyName;
    }

    /**
     * Returns true if and only if current node is bundled in another node
     */
    public boolean isBundledNode(){
        return ctx.matcher.depth() > 0;
    }

    /**
     * Returns absolute path of the current node
     */
    public String getNodeFullPath() {
        return path;
    }

    public NodeState getNodeState() {
        return nodeState;
    }

    public Set<PropertyState> getMetaProps() {
        return ctx.metaProps;
    }

    /**
     * Returns name of properties which needs to be removed or marked as deleted
     */
    public Set<String> getRemovedProps(){
        return ctx.removedProps;
    }

    public String getRootBundlePath() {
        return ctx.isBundling() ? ctx.bundlingPath : path;
    }

    public BundlingHandler childAdded(String name, NodeState state){
        String childPath = childPath(name);
        BundlingContext childContext;
        Matcher childMatcher = ctx.matcher.next(name);
        if (childMatcher.isMatch()) {
            childContext = createChildContext(childMatcher);
            childContext.addMetaProp(NODE_PRESENCE_MARKER);
        } else {
            DocumentBundlor bundlor = registry.getBundlor(state);
            if (bundlor != null){
                PropertyState bundlorConfig = bundlor.asPropertyState();
                childContext = new BundlingContext(childPath, bundlor.createMatcher());
                childContext.addMetaProp(bundlorConfig);
            } else {
                childContext = BundlingContext.NULL;
            }
        }
        return new BundlingHandler(registry, childContext, childPath, state);
    }

    public BundlingHandler childDeleted(String name, NodeState state){
        String childPath = childPath(name);
        BundlingContext childContext;
        Matcher childMatcher = ctx.matcher.next(name);
        if (childMatcher.isMatch()) {
            childContext = createChildContext(childMatcher);
            removeDeletedChildProperties(state, childContext);
        } else {
            childContext = getBundlorContext(childPath, state);
        }
        return new BundlingHandler(registry, childContext, childPath, state);
    }

    public BundlingHandler childChanged(String name, NodeState state){
        String childPath = childPath(name);
        BundlingContext childContext;
        Matcher childMatcher = ctx.matcher.next(name);
        if (childMatcher.isMatch()) {
            childContext = createChildContext(childMatcher);
        } else {
            childContext = getBundlorContext(childPath, state);
        }

        return new BundlingHandler(registry, childContext,  childPath, state);
    }

    public boolean isBundlingRoot() {
        if (ctx.isBundling()){
            return ctx.bundlingPath.equals(path);
        }
        return true;
    }

    private String childPath(String name){
        return PathUtils.concat(path, name);
    }

    private BundlingContext createChildContext(Matcher childMatcher) {
        return ctx.child(childMatcher);
    }

    private static BundlingContext getBundlorContext(String path, NodeState state) {
        BundlingContext result = BundlingContext.NULL;
        PropertyState bundlorConfig = state.getProperty(DocumentBundlor.META_PROP_PATTERN);
        if (bundlorConfig != null){
            DocumentBundlor bundlor = DocumentBundlor.from(bundlorConfig);
            result = new BundlingContext(path, bundlor.createMatcher());
        }
        return result;
    }

    private static void removeDeletedChildProperties(NodeState state, BundlingContext childContext) {
        childContext.removeProperty(DocumentBundlor.META_PROP_NODE);
        for (PropertyState ps : state.getProperties()){
            String propName = ps.getName();
            if (!propName.startsWith(DocumentBundlor.HAS_CHILD_PROP_PREFIX))
                childContext.removeProperty(ps.getName());
        }
    }

    private static class BundlingContext {
        static final BundlingContext NULL = new BundlingContext("", Matcher.NON_MATCHING);
        final String bundlingPath;
        final Matcher matcher;
        final Set<PropertyState> metaProps = Sets.newHashSet();
        final Set<String> removedProps = Sets.newHashSet();

        public BundlingContext(String bundlingPath, Matcher matcher) {
            this.bundlingPath = bundlingPath;
            this.matcher = matcher;
        }

        public BundlingContext child(Matcher matcher){
            return new BundlingContext(bundlingPath, matcher);
        }

        public boolean isBundling(){
            return matcher.isMatch();
        }

        public String getPropertyPath(String propertyName) {
            return PathUtils.concat(matcher.getMatchedPath(), propertyName);
        }

        public void addMetaProp(PropertyState state){
            metaProps.add(state);
        }

        public void removeProperty(String name){
            removedProps.add(name);
        }
    }
}
