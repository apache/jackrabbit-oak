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
package org.apache.jackrabbit.oak.query.ast;

import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.api.Result.SizePrecision;
import org.apache.jackrabbit.oak.query.plan.ExecutionPlan;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * The base class of a selector and a join.
 */
public abstract class SourceImpl extends AstElement {
    
    /**
     * Set the complete constraint of the query (the WHERE ... condition).
     *
     * @param queryConstraint the constraint
     */
    public abstract void setQueryConstraint(ConstraintImpl queryConstraint);

    /**
     * Add the join condition (the ON ... condition).
     *
     * @param joinCondition the join condition
     * @param forThisSelector if set, the join condition can only be evaluated
     *        when all previous selectors are executed.
     */
    public abstract void addJoinCondition(JoinConditionImpl joinCondition, boolean forThisSelector);

    /**
     * Set whether this source is the left hand side or right hand side of a left outer join.
     *
     * @param outerJoinLeftHandSide true if yes
     * @param outerJoinRightHandSide true if yes
     */
    public abstract void setOuterJoin(boolean outerJoinLeftHandSide, boolean outerJoinRightHandSide);

    /**
     * Get the selector with the given name, or null if not found.
     *
     * @param selectorName the selector name
     * @return the selector, or null
     */
    public abstract SelectorImpl getSelector(String selectorName);

    /**
     * Get the selector with the given name, or fail if not found.
     *
     * @param selectorName the selector name
     * @return the selector (never null)
     */
    public SelectorImpl getExistingSelector(String selectorName) {
        SelectorImpl s = getSelector(selectorName);
        if (s == null) {
            throw new IllegalArgumentException("Unknown selector: " + selectorName);
        }
        return s;
    }

    /**
     * Get the query plan.
     *
     * @param rootState the root
     * @return the query plan
     */
    public abstract String getPlan(NodeState rootState);

    /**
     * Get the index cost as a JSON string.
     *
     * @param rootState the root
     * @return the cost
     */
    public abstract String getIndexCostInfo(NodeState rootState);

    /**
     * Prepare executing the query (recursively). This will 'wire' the
     * selectors with the join constraints, and decide which index to use.
     * 
     * @return the execution plan
     */
    public abstract ExecutionPlan prepare();
    
    /**
     * Undo a prepare.
     */
    public abstract void unprepare();

    /**
     * Re-apply a previously prepared plan. This will also 're-wire' the
     * selectors with the join constraints
     * 
     * @param p the plan to use
     */
    public abstract void prepare(ExecutionPlan p);
    
    /**
     * Execute the query. The current node is set to before the first row.
     *
     * @param rootState root state of the given revision
     */
    public abstract void execute(NodeState rootState);

    /**
     * Go to the next node for the given source. This will also filter the
     * result for the right node type if required.
     *
     * @return true if there is a next row
     */
    public abstract boolean next();

    /**
     * <b>!Test purpose only! </b>
     * 
     * this creates a filter for the given query
     * 
     * @param preparing whether this this the prepare phase
     * @return a new filter
     */
    public abstract Filter createFilter(boolean preparing);

    /**
     * Get all sources that are joined via inner join. (These can be swapped.)
     * 
     * @return the list of selectors (sorted from left to right)
     */
    public abstract List<SourceImpl> getInnerJoinSelectors();
    
    /**
     * Get the list of inner join conditions. (These match the inner join selectors.)
     * 
     * @return the list of join conditions
     */
    public List<JoinConditionImpl> getInnerJoinConditions() {
        return Collections.emptyList();
    }
    
    /**
     * Whether any selector is the outer-join right hand side.
     * 
     * @return true if there is any
     */
    public abstract boolean isOuterJoinRightHandSide();

    /**
     * Get the size if known.
     * 
     * @param precision the required precision
     * @param max the maximum nodes read (for an exact size)
     * @return the size, or -1 if unknown
     */
    public abstract long getSize(SizePrecision precision, long max);
}
