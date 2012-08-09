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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.query.Query;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * The base class of a selector and a join.
 */
public abstract class SourceImpl extends AstElement {

    protected ConstraintImpl queryConstraint;
    protected JoinConditionImpl joinCondition;
    protected boolean join;
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
     * Set the join condition (the ON ... condition).
     *
     * @param joinCondition the join condition
     */
    public void setJoinCondition(JoinConditionImpl joinCondition) {
        this.joinCondition = joinCondition;
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

    /**
     * Get the current absolute path (including workspace name)
     *
     * @return the path
     */
    public abstract String currentPath();

}
