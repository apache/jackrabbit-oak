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

import static com.google.common.collect.Lists.newArrayList;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * A "not" condition.
 */
public class NotImpl extends ConstraintImpl {

    private ConstraintImpl constraint;

    public NotImpl(ConstraintImpl constraint) {
        this.constraint = constraint;
    }

    public ConstraintImpl getConstraint() {
        return constraint;
    }

    /**
     * Apply DeMorgan's Laws to push AND/OR constraints higher.
     */
    @Override
    public ConstraintImpl simplify() {
        ConstraintImpl simple = constraint.simplify();
        if (simple instanceof AndImpl) {
            // not (X and Y) == (not X) or (not Y)
            AndImpl and = (AndImpl) simple;
            List<ConstraintImpl> constraints = newArrayList();
            for (ConstraintImpl constraint : and.getConstraints()) {
                constraints.add(new NotImpl(constraint));
            }
            return new OrImpl(constraints).simplify();
        } else if (simple instanceof OrImpl) {
            // not (X or Y) == (not X) and (not Y)
            OrImpl or = (OrImpl) simple;
            List<ConstraintImpl> constraints = newArrayList();
            for (ConstraintImpl constraint : or.getConstraints()) {
                constraints.add(new NotImpl(constraint));
            }
            return new AndImpl(constraints).simplify();
        } else if (simple instanceof NotImpl) {
            // not not X == X
            return ((NotImpl) simple).constraint;
        } else if (simple != constraint) {
            return new NotImpl(simple);
        } else {
            return this;
        }
    }

    @Override
    public boolean evaluate() {
        return !constraint.evaluate();
    }
    
    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        return Collections.emptySet();
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        return constraint.getSelectors();
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "not " + protect(constraint);
    }

    @Override
    public void restrict(FilterImpl f) {
        if (f.getSelector().isOuterJoinRightHandSide()) {
            // we need to be careful with the condition
            // "NOT (property IS NOT NULL)"
            // (which is the same as "property IS NULL")
            // because this might cause an index
            // to ignore the join condition "property = x"
            // for example in:
            // "select * from a left outer join b on a.x = b.y
            // where not b.y is not null"
            // must not result in the index to check for
            // "b.y is null", because that would alter the
            // result
            return;
        }
        // ignore
        // TODO convert NOT conditions
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        // ignore
        // TODO convert NOT conditions
    }

}
