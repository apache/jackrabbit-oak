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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.fulltext.FullTextOr;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An "or" condition.
 */
public class OrImpl extends ConstraintImpl {

    private ConstraintImpl constraint1, constraint2;

    public OrImpl(ConstraintImpl constraint1, ConstraintImpl constraint2) {
        this.constraint1 = constraint1;
        this.constraint2 = constraint2;
    }

    public ConstraintImpl getConstraint1() {
        return constraint1;
    }

    public ConstraintImpl getConstraint2() {
        return constraint2;
    }
    
    @Override
    public ConstraintImpl simplify() {
        constraint1 = constraint1.simplify();
        constraint2 = constraint2.simplify();
        if (constraint1.equals(constraint2)) {
            return constraint1;
        }
        return this;
    }
    
    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        // for the condition "x=1 or x=2", the existence condition
        // "x is not null" be be derived
        Set<PropertyExistenceImpl> s1 = constraint1.getPropertyExistenceConditions();
        if (s1.isEmpty()) {
            return s1;
        }
        Set<PropertyExistenceImpl> s2 = constraint2.getPropertyExistenceConditions();
        if (s2.isEmpty()) {
            return s2;
        }
        return Sets.intersection(s1, s2);
    }
    
    @Override
    public FullTextExpression getFullTextConstraint(SelectorImpl s) {
        FullTextExpression f1 = constraint1.getFullTextConstraint(s);
        FullTextExpression f2 = constraint2.getFullTextConstraint(s);
        if (f1 == null || f2 == null) {
            // the full-text index can not be used for conditions of the form
            // "contains(a, 'x') or b=123"
            return null;
        }
        ArrayList<FullTextExpression> list = new ArrayList<FullTextExpression>();
        list.add(f1);
        list.add(f2);
        return new FullTextOr(list);
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        Set<SelectorImpl> s1 = constraint1.getSelectors();
        Set<SelectorImpl> s2 = constraint2.getSelectors();
        if (s1.isEmpty()) {
            return s2;
        } else if (s2.isEmpty()) {
            return s1;
        }
        return Sets.union(s1, s2);
    }
    
    @Override 
    public Map<DynamicOperandImpl, Set<StaticOperandImpl>> getInMap() {
        Map<DynamicOperandImpl, Set<StaticOperandImpl>> m1 = constraint1.getInMap();
        Map<DynamicOperandImpl, Set<StaticOperandImpl>> m2 = constraint2.getInMap();
        if (m1.isEmpty()) {
            return m1;
        } else if (m2.isEmpty()) {
            return m2;
        }
        Map<DynamicOperandImpl, Set<StaticOperandImpl>> result = Maps.newHashMap();
        for (Entry<DynamicOperandImpl, Set<StaticOperandImpl>> e2 : m2.entrySet()) {
            Set<StaticOperandImpl> l2 = e2.getValue();
            Set<StaticOperandImpl> l1 = m1.get(e2.getKey());
            if (l1 != null && !l1.isEmpty() && !l2.isEmpty()) {
                // ensure the same order is used
                LinkedHashSet<StaticOperandImpl> set = new LinkedHashSet<StaticOperandImpl>();
                set.addAll(l1);
                set.addAll(l2);
                result.put(e2.getKey(), set);
            }
        }
        return result;
    }    

    @Override
    public boolean evaluate() {
        return constraint1.evaluate() || constraint2.evaluate();
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }
    
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (!(other instanceof OrImpl)) {
            return false;
        }
        OrImpl o = (OrImpl) other;
        return constraint1.equals(o.constraint1) &&
                constraint2.equals(o.constraint2);
    }

    @Override
    public String toString() {
        return protect(constraint1) + " or " + protect(constraint2);
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
     * Push down the "property in(1, 2, 3)" conditions to the selector, if there
     * are any that can be derived.
     * 
     * @param s the selector
     */
    private void restrictPushDownInList(SelectorImpl s) {
        if (isOnlySelector(s)) {
            Map<DynamicOperandImpl, Set<StaticOperandImpl>> m = getInMap();
            for (Entry<DynamicOperandImpl, Set<StaticOperandImpl>> e : m.entrySet()) {
                Set<StaticOperandImpl> set = e.getValue();
                if (set.size() > 1) {
                    InImpl in = new InImpl(e.getKey(), Lists.newArrayList(set));
                    in.setQuery(query);
                    in.restrictPushDown(s);
                }
            }
        }
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
     * Check whether there are no other selectors in this "or" condition.
     * 
     * @param s the selector
     * @return true if there are no other selectors
     */
    private boolean isOnlySelector(SelectorImpl s) {
        Set<SelectorImpl> set = getSelectors();
        if (set.size() == 0) {
            // conditions without selectors, for example "1=0":
            // the condition can be pushed down; 
            // (currently there are no such conditions,
            // but in the future we might add them)
            return true;
        } else if (set.size() > 1) {
            // "x.a=1 or y.a=2" can't be pushed down to either "x" or "y"
            return false;
        } else {
            // exactly one selector: check if it's the right one
            SelectorImpl s2 = set.iterator().next();
            if (!s2.equals(s)) {
                // a different selector
                return false;
            }
        }
        return true;
    }

}
