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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static org.apache.jackrabbit.oak.query.ast.AstElementFactory.copyElementAndCheckReference;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

import com.google.common.collect.Sets;

/**
 * An AND condition.
 */
public class AndImpl extends ConstraintImpl {

    private final List<ConstraintImpl> constraints;

    public AndImpl(List<ConstraintImpl> constraints) {
        checkArgument(!constraints.isEmpty());
        this.constraints = constraints;
    }

    public AndImpl(ConstraintImpl constraint1, ConstraintImpl constraint2) {
        this(Arrays.asList(constraint1, constraint2));
    }

    public List<ConstraintImpl> getConstraints() {
        return constraints;
    }

    @Override
    public ConstraintImpl simplify() {
        // Use LinkedHashSet to eliminate duplicate constraints while keeping
        // the ordering for test cases (and clients?) that depend on it
        LinkedHashSet<ConstraintImpl> simplified = newLinkedHashSet();
        boolean changed = false; // keep track of changes in simplification

        for (ConstraintImpl constraint : constraints) {
            ConstraintImpl simple = constraint.simplify();
            if (simple instanceof AndImpl) {
                // unwind nested AND constraints
                simplified.addAll(((AndImpl) simple).constraints);
                changed = true;
            } else if (simplified.add(simple)) {
                // check if this constraint got simplified
                changed = changed || simple != constraint;
            } else {
                // this constraint was a duplicate of a previous one
                changed = true;
            }
        }

        if (simplified.size() == 1) {
            return simplified.iterator().next();
        } else if (changed) {
            return new AndImpl(newArrayList(simplified));
        } else {
            return this;
        }
    }

    @Override
    ConstraintImpl not() {
        // not (X and Y) == (not X) or (not Y)
        List<ConstraintImpl> list = newArrayList();
        for (ConstraintImpl constraint : constraints) {
            list.add(new NotImpl(constraint));
        }
        return new OrImpl(list).simplify();
    }

    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        Set<PropertyExistenceImpl> result = newHashSet();
        for (ConstraintImpl constraint : constraints) {
            result.addAll(constraint.getPropertyExistenceConditions());
        }
        return result;
    }
    
    @Override
    public FullTextExpression getFullTextConstraint(SelectorImpl s) {
        List<FullTextExpression> list = newArrayList();
        for (ConstraintImpl constraint : constraints) {
            FullTextExpression expression = constraint.getFullTextConstraint(s);
            if (expression != null) {
                list.add(expression);
            }
        }
        switch (list.size()) {
        case 0:
            return null;
        case 1:
            return list.iterator().next();
        default:
            return new FullTextAnd(list);
        }
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        Set<SelectorImpl> result = newHashSet();
        for (ConstraintImpl constraint : constraints) {
            result.addAll(constraint.getSelectors());
        }
        return result;
    }

    @Override
    public boolean evaluate() {
        for (ConstraintImpl constraint : constraints) {
            if (!constraint.evaluate()) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public boolean evaluateStop() {
        // the logic is reversed here:
        // if one of the conditions is to stop, then we stop
        for (ConstraintImpl constraint : constraints) {
            if (constraint.evaluateStop()) {
                return true;
            }
        }
        return false;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public void restrict(FilterImpl f) {
        for (ConstraintImpl constraint : constraints) {
            constraint.restrict(f);
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        for (ConstraintImpl constraint : constraints) {
            constraint.restrictPushDown(s);
        }
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        if (constraints.size() == 1) {
            return constraints.iterator().next().toString();
        } else {
            StringBuilder builder = new StringBuilder();
            for (ConstraintImpl constraint : constraints) {
                if (builder.length() > 0) {
                    builder.append(" and ");
                }
                builder.append(protect(constraint));
            }
            return builder.toString();
        }
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof AndImpl) {
            return constraints.equals(((AndImpl) that).constraints);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return constraints.hashCode();
    }

    @Override
    public AstElement copyOf() {
        List<ConstraintImpl> clone = new ArrayList<ConstraintImpl>(constraints.size());
        for (ConstraintImpl c : constraints) {
            clone.add((ConstraintImpl) copyElementAndCheckReference(c));
        }
        return new AndImpl(clone);
    }

    @Override
    public Set<ConstraintImpl> convertToUnion() {
        Set<ConstraintImpl> union = Sets.newHashSet();
        Set<ConstraintImpl> result = Sets.newHashSet();
        Set<ConstraintImpl> nonUnion = Sets.newHashSet();
        
        for (ConstraintImpl c : constraints) {
            Set<ConstraintImpl> converted = c.convertToUnion();
            if (converted.isEmpty()) {
                nonUnion.add(c);
            } else {
                union.addAll(converted);
            }
        }
        if (!union.isEmpty() && nonUnion.size() == 1) {
            // this is the simplest case where, for example, out of the two AND operands at least
            // one is a non-union. For example WHERE (a OR b OR c) AND d
            ConstraintImpl right = nonUnion.iterator().next();
            for (ConstraintImpl c : union) {
                result.add(new AndImpl(c, right));
            }
        } else {
            // in this case prefer to be conservative and don't optimize. This could happen when for
            // example: WHERE (a OR b) AND (c OR d).
            // This should be translated into a AND c, a AND d, b AND c, b AND d.
        }
        
        return result;
    }
    
    @Override
    public boolean requiresFullTextIndex() {
        for (ConstraintImpl c : constraints) {
            if (c.requiresFullTextIndex()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsUnfilteredFullTextCondition() {
        for (ConstraintImpl c : constraints) {
            if (c.containsUnfilteredFullTextCondition()) {
                return true;
            }
        }
        return false;
    }

}
