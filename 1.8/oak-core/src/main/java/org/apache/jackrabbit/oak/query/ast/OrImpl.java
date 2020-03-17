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
import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newLinkedHashSet;
import static org.apache.jackrabbit.oak.query.ast.AstElementFactory.copyElementAndCheckReference;
import static org.apache.jackrabbit.oak.query.ast.Operator.EQUAL;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

import com.google.common.collect.Sets;

/**
 * An "or" condition.
 */
public class OrImpl extends ConstraintImpl {

    private final List<ConstraintImpl> constraints;

    public OrImpl(List<ConstraintImpl> constraints) {
        checkArgument(!constraints.isEmpty());
        this.constraints = constraints;
    }

    public OrImpl(ConstraintImpl constraint1, ConstraintImpl constraint2) {
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
            if (simple instanceof OrImpl) {
                // unwind nested OR constraints
                simplified.addAll(((OrImpl) simple).constraints);
                changed = true;
            } else if (simplified.add(simple)) {
                // check if this constraint got simplified
                changed = changed || simple != constraint;
            } else {
                // this constraint was a duplicate of a previous one
                changed = true;
            }
        }

        LinkedHashMap<DynamicOperandImpl, LinkedHashSet<StaticOperandImpl>> in =
                newLinkedHashMap();
        Iterator<ConstraintImpl> iterator = simplified.iterator();
        while (iterator.hasNext()) {
            ConstraintImpl simple = iterator.next();
            if (simple instanceof ComparisonImpl
                    && ((ComparisonImpl) simple).getOperator() == EQUAL) {
                DynamicOperandImpl o = ((ComparisonImpl) simple).getOperand1();
                LinkedHashSet<StaticOperandImpl> values = in.get(o);
                if (values == null) {
                    values = newLinkedHashSet();
                    in.put(o, values);
                }
                values.add(((ComparisonImpl) simple).getOperand2());
                iterator.remove();
                changed = true;
            } else if (simple instanceof InImpl) {
                DynamicOperandImpl o = ((InImpl) simple).getOperand1();
                LinkedHashSet<StaticOperandImpl> values = in.get(o);
                if (values == null) {
                    values = newLinkedHashSet();
                    in.put(o, values);
                }
                values.addAll(((InImpl) simple).getOperand2());
                iterator.remove();
                changed = true;
            }
        }
        for (Entry<DynamicOperandImpl, LinkedHashSet<StaticOperandImpl>> entry
                : in.entrySet()) {
            LinkedHashSet<StaticOperandImpl> values = entry.getValue();
            if (values.size() == 1) {
                simplified.add(new ComparisonImpl(
                        entry.getKey(), EQUAL, values.iterator().next()));
            } else {
                simplified.add(new InImpl(
                        entry.getKey(), newArrayList(values)));
            }
        }

        if (simplified.size() == 1) {
            return simplified.iterator().next();
        } else if (changed) {
            return new OrImpl(newArrayList(simplified));
        } else {
            return this;
        }
    }

    @Override
    ConstraintImpl not() {
        // not (X or Y) == (not X) and (not Y)
        List<ConstraintImpl> list = newArrayList();
        for (ConstraintImpl constraint : getConstraints()) {
            list.add(new NotImpl(constraint));
        }
        return new AndImpl(list).simplify();
    }

    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        // for the condition "x=1 or x=2", the existence condition
        // "x is not null" be be derived
        Set<PropertyExistenceImpl> result = null;
        for (ConstraintImpl constraint : constraints) {
            if (result == null) {
                result = newHashSet(constraint.getPropertyExistenceConditions());
            } else {
                result.retainAll(constraint.getPropertyExistenceConditions());
            }
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
            } else {
                // the full-text index can not be used for conditions
                // of the form "contains(a, 'x') or b=123"
                return null;
            }
        }
        return new FullTextOr(list);
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
            if (constraint.evaluate()) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public boolean evaluateStop() {
        // the logic is reversed here:
        // we stop only if both conditions say we need to
        for (ConstraintImpl constraint : constraints) {
            if (!constraint.evaluateStop()) {
                return false;
            }
        }
        return true;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public void restrict(FilterImpl f) {
        Set<PropertyExistenceImpl> set = getPropertyExistenceConditions();
        if (!set.isEmpty()) {
            for (PropertyExistenceImpl p : set) {
                p.restrict(f);
            }
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        restrictPushDownNotExists(s);
        restrictPushDownInList(s);
    }

    /**
     * Push down the "not exists" conditions to the selector.
     * 
     * @param s the selector
     */
    private void restrictPushDownNotExists(SelectorImpl s) {
        Set<PropertyExistenceImpl> set = getPropertyExistenceConditions();
        if (set.isEmpty()) {
            return;
        }
        for (PropertyExistenceImpl p : set) {
            p.restrictPushDown(s);
        }
    }

    /**
     * Push down the "property in(1, 2, 3)" conditions to the selector, if there
     * are any that can be derived.
     * 
     * @param s the selector
     */
    private void restrictPushDownInList(SelectorImpl s) {
        DynamicOperandImpl operand = null;
        LinkedHashSet<StaticOperandImpl> values = newLinkedHashSet();
 
        boolean multiPropertyOr = false;
        List<AndImpl> ands = newArrayList();
        for (ConstraintImpl constraint : constraints) {
            Set<SelectorImpl> selectors = constraint.getSelectors();
            if (selectors.size() != 1 || !selectors.contains(s)) {
                return;
            } else if (constraint instanceof AndImpl) {
                ands.add((AndImpl) constraint);
            } else if (constraint instanceof InImpl) {
                InImpl in = (InImpl) constraint;
                DynamicOperandImpl o = in.getOperand1();
                if (operand == null || operand.equals(o)) {
                    operand = o;
                    values.addAll(in.getOperand2());
                } else {
                    multiPropertyOr = true;
                }
            } else if (constraint instanceof ComparisonImpl
                    && ((ComparisonImpl) constraint).getOperator() == EQUAL) {
                ComparisonImpl comparison = (ComparisonImpl) constraint;
                DynamicOperandImpl o = comparison.getOperand1();
                if (operand == null || operand.equals(o)) {
                    operand = o;
                    values.add(comparison.getOperand2());
                } else {
                    multiPropertyOr = true;
                }
            } else {
                return;
            }
        }

        if (multiPropertyOr && ands.isEmpty()) {
            s.restrictSelector(this);
            return;
        } else if (operand == null) {
            return;
        }

        for (AndImpl and : ands) {
            boolean found = false;
            for (ConstraintImpl constraint : and.getConstraints()) {
                if (constraint instanceof InImpl) {
                    InImpl in = (InImpl) constraint;
                    if (operand.equals(in.getOperand1())) {
                        values.addAll(in.getOperand2());
                        found = true;
                        break;
                    }
                } else if (constraint instanceof ComparisonImpl
                        && ((ComparisonImpl) constraint).getOperator() == EQUAL) {
                    ComparisonImpl comparison = (ComparisonImpl) constraint;
                    if (operand.equals(comparison.getOperand1())) {
                        values.add(comparison.getOperand2());
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                return;
            }
        }

        InImpl in = new InImpl(operand, newArrayList(values));
        in.setQuery(query);
        in.restrictPushDown(s);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (ConstraintImpl constraint : constraints) {
            if (builder.length() > 0) {
                builder.append(" or ");
            }
            builder.append(protect(constraint));
        }
        return builder.toString();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        } else if (that instanceof OrImpl) {
            return constraints.equals(((OrImpl) that).constraints);
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
        List<ConstraintImpl> clone = newArrayList();
        for (ConstraintImpl c : constraints) {
            clone.add((ConstraintImpl) copyElementAndCheckReference(c));
        }
        return new OrImpl(clone);
    }

    @Override
    public Set<ConstraintImpl> convertToUnion() {
        Set<ConstraintImpl> result = Sets.newHashSet();
        for (ConstraintImpl c : getConstraints()) {
            Set<ConstraintImpl> converted = c.convertToUnion(); 
            if (converted.isEmpty()) {
                result.add(c);
            } else {
                result.addAll(converted);
            }
        }
        return result;
    }

    @Override
    public boolean requiresFullTextIndex() {
        for (ConstraintImpl c : getConstraints()) {
            if (c.requiresFullTextIndex()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsUnfilteredFullTextCondition() {
        boolean fulltext = false;
        boolean plain = false;
        for (ConstraintImpl c : constraints) {
            // this part of the condition already contains an unfiltered
            // condition, so we don't need to check further
            if (c.containsUnfilteredFullTextCondition()) {
                return true;
            }
            if (c.requiresFullTextIndex()) {
                // for example "contains(a, 'x')"
                fulltext = true;
            } else {
                // for example "b=123"
                plain = true;
            }
            // the full-text index contains both typescan not be used for conditions
            // of the form "contains(a, 'x') or b=123"
            if (fulltext && plain) {
                return true;
            }
        }
        return false;
    }    

}
