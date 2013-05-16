/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.query.ast;

import java.util.Set;

import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * The base class for constraints.
 */
public abstract class ConstraintImpl extends AstElement {

    /**
     * Evaluate the result using the currently set values.
     *
     * @return true if the constraint matches
     */
    public abstract boolean evaluate();
    
    /**
     * Get the set of property existence conditions that can be derived for this
     * condition. For example, for the condition "x=1 or x=2", the property
     * existence condition is "x is not null". For the condition "x=1 or y=2",
     * there is no such condition. For the condition "x=1 and y=1", there are
     * two (x is not null, and y is not null).
     * 
     * @return the common property existence condition (possibly empty)
     */
    public abstract Set<PropertyExistenceImpl> getPropertyExistenceConditions();

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

}
