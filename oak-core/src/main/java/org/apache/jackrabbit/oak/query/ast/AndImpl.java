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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jackrabbit.oak.query.fulltext.FullTextAnd;
import org.apache.jackrabbit.oak.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An AND condition.
 */
public class AndImpl extends ConstraintImpl {

    private ConstraintImpl constraint1, constraint2;

    public AndImpl(ConstraintImpl constraint1, ConstraintImpl constraint2) {
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
        Set<PropertyExistenceImpl> s1 = constraint1.getPropertyExistenceConditions();
        Set<PropertyExistenceImpl> s2 = constraint2.getPropertyExistenceConditions();
        Set<PropertyExistenceImpl> result = Sets.newHashSet(s1);
        result.addAll(s2);
        return result;
    }
    
    @Override
    public FullTextExpression getFullTextConstraint(SelectorImpl s) {
        FullTextExpression f1 = constraint1.getFullTextConstraint(s);
        FullTextExpression f2 = constraint2.getFullTextConstraint(s);
        if (f1 == null) {
            return f2;
        } else if (f2 == null) {
            return f1;
        }
        ArrayList<FullTextExpression> list = new ArrayList<FullTextExpression>();
        list.add(f1);
        list.add(f2);
        return new FullTextAnd(list);
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        Set<SelectorImpl> s1 = constraint1.getSelectors();
        Set<SelectorImpl> s2 = constraint1.getSelectors();
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
            return m2;
        } else if (m2.isEmpty()) {
            return m1;
        }
        Map<DynamicOperandImpl, Set<StaticOperandImpl>> result = Maps.newHashMap();
        result.putAll(m1);
        for (Entry<DynamicOperandImpl, Set<StaticOperandImpl>> e2 : m2.entrySet()) {
            Set<StaticOperandImpl> s = result.get(e2.getKey());
            if (s != null) {
                s.retainAll(e2.getValue());
            } else {
                result.put(e2.getKey(), e2.getValue());
            }
        }
        return result;
    }

    @Override
    public boolean evaluate() {
        return constraint1.evaluate() && constraint2.evaluate();
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return protect(constraint1) + " and " + protect(constraint2);
    }

    @Override
    public void restrict(FilterImpl f) {
        constraint1.restrict(f);
        constraint2.restrict(f);
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        constraint1.restrictPushDown(s);
        constraint2.restrictPushDown(s);
    }

}

