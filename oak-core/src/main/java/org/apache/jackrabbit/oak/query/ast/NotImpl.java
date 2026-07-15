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

import static org.apache.jackrabbit.oak.query.ast.AstElementFactory.copyElementAndCheckReference;

import java.util.Collections;
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
        ConstraintImpl not = simple.not();
        if (not != null) {
            return not.simplify();
        } else if (simple != constraint) {
            return new NotImpl(simple);
        }
        return this;
    }

    @Override
    ConstraintImpl not() {
        // not not X == X -> X == X
        return constraint;
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

    @Override
    public AstElement copyOf() {
        return new NotImpl((ConstraintImpl) copyElementAndCheckReference(constraint));
    }

    @Override
    public boolean requiresFullTextIndex() {
        return constraint.requiresFullTextIndex();
    }

    @Override
    public boolean containsUnfilteredFullTextCondition() {
        // If the constraint is a fulltext condition,
        // then we can not apply it, as in "not contains(., 'x')".
        // Also, if the constraint _contains_ a unfiltered fulltext condition, as in
        // "not (x=1 or contains(., 'x')".
        return constraint.containsUnfilteredFullTextCondition() || 
                constraint.requiresFullTextIndex();
    }

}
