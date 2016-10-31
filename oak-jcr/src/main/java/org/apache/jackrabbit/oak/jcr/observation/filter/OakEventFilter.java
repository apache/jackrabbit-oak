/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr.observation.filter;

import org.apache.jackrabbit.api.observation.JackrabbitEventFilter;

import aQute.bnd.annotation.ConsumerType;

/**
 * Extension of the JackrabbitEventFilter that exposes Oak specific
 * features.
 * <p>
 * Usage:
 * <code>
 * OakEventFilter oakFilter = FilterFactory.wrap(jackrabbitFilter);
 * // then call extensions on OakEventFilters
 * observationManager.addEventListener(listener, oakFilter);  
 * </code>
 */
@ConsumerType
public abstract class OakEventFilter extends JackrabbitEventFilter {

    /**
     * This causes the node type filter to be applied on 'this' node instead of
     * the 'parent' node, thus allows to create a filter which listens on
     * adding/removing/etc on nodes of a particular node type (while the default
     * was that the node type was applicable on the parent).
     * <p>
     * Note that this is an 'either/or' thing: either the node type is applied
     * on the parent (default) or on 'self/this' (via this switch) but not both.
     * @return this filter with the filter change applied
     */
    public abstract OakEventFilter withApplyNodeTypeOnSelf();

    /**
     * This causes the registration of !deep NODE_REMOVED registrations
     * of all parents of the include paths (both normal and glob).
     * <ul>
     * <li>include path /a/b/c/d results in additional !deep NODE_REMOVED
     * filters on /a/b/c, on /a/b and on /a</li>
     * <li>include path /a/b/** results in additional !deep NODE_REMOVED
     * filter on /a</li>
     * </ul>
     * <p>
     * Note that unlike 'normal' include and exclude paths, this variant
     * doesn't apply Oak's NamePathMapper on the ancestors of the
     * registers paths.
     * <p>
     * Also note that this might disable 'observation prefiltering based on paths' 
     * (OAK-4796) on this listener.
     * @return this filter with the filter change applied
     */
    public abstract OakEventFilter withIncludeAncestorsRemove();

    /**
     * This flag causes remove events to be sent for all nodes and properties
     * of an entire subtree (hence use with care!).
     * <p>
     * It is only applied when a parent node is actually removed. For 
     * a parent node move this is not applied.
     * @return this filter with the filter change applied
     */
    public abstract OakEventFilter withIncludeSubtreeOnRemove();

    /**
     * Adds the provided glob paths to the set of include paths.
     * <p>
     * The definition of a glob path is
     * <a href="https://jackrabbit.apache.org/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/observation/filter/GlobbingPathFilter.html">here</a>
     * <p>
     * Note that unlike 'normal' include and exclude paths, this variant
     * doesn't apply Oak's NamePathMapper.
     * <p>
     * This filter property is added in 'or' mode.
     * 
     * @param globPath
     *            glob path that should be added as include path pattern. Note
     *            that the NamePathMapper is not applied on this globPath.
     * @return this filter with the filter change applied
     */
    public abstract OakEventFilter withIncludeGlobPaths(String... globPaths);

    /**
     * Greedy aggregating filter which upon first (hence greedy) hit of provided
     * nodeTypes checks if the child subtree leading to the actual change
     * matches any of the provided relativeGlobPaths.
     * <p>
     * Note that unlike 'normal' include and exclude paths, this variant
     * doesn't apply Oak's NamePathMapper.
     * <p>
     * This filter property is added in 'and' mode.
     * 
     * @param nodeTypes
     *            note that these nodeTypes are not mapped to oak nor validated
     * @param relativeGlobPaths
     *            glob paths that are added to the set of include paths.
     *            Note that Oak's NamePathMapper is not applied to these relativeGlobPaths.
     * @return this filter with the filter change applied
     */
    public abstract OakEventFilter withNodeTypeAggregate(final String[] nodeTypes, final String[] relativeGlobPaths);

}
