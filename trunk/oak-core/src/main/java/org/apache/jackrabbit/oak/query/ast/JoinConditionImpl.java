/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query.ast;

import java.util.Set;

import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * The base class for join conditions.
 */
public abstract class JoinConditionImpl extends AstElement {
    
    /**
     * The prefix for known paths.
     */
    public static final String SPECIAL_PATH_PREFIX = "//";
    
    /**
     * A path for a join.
     */
    protected static final String KNOWN_PATH = "//path/from/join";

    /**
     * The parent path of the joined selector
     */
    protected static final String KNOWN_PARENT_PATH = "//parent/of/join";
    
    protected static final String KNOWN_VALUE = "valueFromTheJoinSelector";

    /**
     * Evaluate the result using the currently set values.
     * 
     * @return true if the constraint matches
     */
    public abstract boolean evaluate();
    
    /**
     * Apply the condition to the filter, further restricting the filter if
     * possible. This may also verify the data types are compatible, and that
     * paths are valid.
     * 
     * @param f the filter
     */
    public abstract void restrict(FilterImpl f);

    /**
     * Push as much of the condition down to this selector, further restricting
     * the selector condition if possible.
     * 
     * @param s the selector
     */
    public abstract void restrictPushDown(SelectorImpl s);

    /**
     * Check whether the given source is the parent of the join condition, as
     * selector "[b]" is the parent of the join condition
     * "isdescendantnode([a], [b])".
     * 
     * @param source the source
     * @return true if the source is the parent
     */
    public abstract boolean isParent(SourceImpl source);
    
    /**
     * Whether the join condition can be evaluated if the given selectors are able to retrieve data.
     * 
     * @param available the available selectors
     * @return true if the condition can be evaluated
     */
    public abstract boolean canEvaluate(Set<SourceImpl> available);

}
