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
import org.apache.jackrabbit.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.query.Query;

public abstract class SourceImpl extends AstElement {

    protected ConstraintImpl queryConstraint;
    protected JoinConditionImpl joinCondition;
    protected boolean join;
    protected boolean outerJoin;

    public void setQueryConstraint(ConstraintImpl queryConstraint) {
        this.queryConstraint = queryConstraint;
    }

    public void setJoinCondition(JoinConditionImpl joinCondition) {
        this.joinCondition = joinCondition;
    }

    public void setOuterJoin(boolean outerJoin) {
        this.outerJoin = outerJoin;
    }

    public abstract void init(Query qom);

    public abstract SelectorImpl getSelector(String selectorName);

    public abstract String getPlan();

    public abstract void prepare(MicroKernel mk);

    public abstract void execute(String revisionId);

    public abstract boolean next();

    public abstract String currentPath();

    public abstract NodeImpl currentNode();

}
