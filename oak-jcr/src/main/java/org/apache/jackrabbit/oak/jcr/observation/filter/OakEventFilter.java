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

}
