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

import java.util.ArrayList;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.query.Query;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * The base class of a selector and a join.
 */
public abstract class SourceImpl extends AstElement {

    /**
     * The WHERE clause of the query.
     */
    protected ConstraintImpl queryConstraint;

    /**
     * The join condition of this selector that can be evaluated at execution
     * time. For the query "select * from nt:base as a inner join nt:base as b
     * on a.x = b.x", the join condition "a.x = b.x" is only set for the
     * selector b, as selector a can't evaluate it if it is executed first
     * (until b is executed).
     */
    protected JoinConditionImpl joinCondition;

    /**
     * The list of all join conditions this selector is involved. For the query
     * "select * from nt:base as a inner join nt:base as b on a.x =
     * b.x", the join condition "a.x = b.x" is set for both selectors a and b,
     * so both can check if the property x is set.
     */
    protected ArrayList<JoinConditionImpl> allJoinConditions =
            new ArrayList<JoinConditionImpl>();

    /**
     * Whether this selector is the right hand side of a join.
     */
    protected boolean join;

    /**
     * Whether this selector is the right hand side of a left outer join.
     * Right outer joins are converted to left outer join.
     */
    protected boolean outerJoin;

    /**
     * Set the complete constraint of the query (the WHERE ... condition).
     *
     * @param queryConstraint the constraint
     */
    public void setQueryConstraint(ConstraintImpl queryConstraint) {
        this.queryConstraint = queryConstraint;
    }

    /**
     * Add the join condition (the ON ... condition).
     *
     * @param joinCondition the join condition
     * @param forThisSelector if set, the join condition can only be evaluated
     *        when all previous selectors are executed.
     */
    public void addJoinCondition(JoinConditionImpl joinCondition, boolean forThisSelector) {
        if (forThisSelector) {
            this.joinCondition = joinCondition;
        }
        allJoinConditions.add(joinCondition);
    }

    /**
     * Set whether this source is the right hand side of a left outer join.
     *
     * @param outerJoin true if yes
     */
    public void setOuterJoin(boolean outerJoin) {
        this.outerJoin = outerJoin;
    }

    /**
     * Initialize the query. This will 'wire' the selectors with the
     * constraints.
     *
     * @param query the query
     */
    public abstract void init(Query query);

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
     * @return the query plan
     */
    public abstract String getPlan();

    /**
     * Prepare executing the query. This method will decide which index to use.
     *
     * @param mk the MicroKernel
     */
    public abstract void prepare(MicroKernel mk);

    /**
     * Execute the query. The current node is set to before the first row.
     *
     * @param revisionId the revision to use
     * @param root root state of the given revision
     */
    public abstract void execute(String revisionId, NodeState root);

    /**
     * Go to the next node for the given source. This will also filter the
     * result for the right node type if required.
     *
     * @return true if there is a next row
     */
    public abstract boolean next();

}
