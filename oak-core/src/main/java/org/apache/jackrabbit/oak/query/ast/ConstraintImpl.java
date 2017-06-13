/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.query.ast;

import java.util.Collections;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * The base class for constraints.
 */
public abstract class ConstraintImpl extends AstElement {
    
    /**
     * Simplify the expression if possible, for example by removing duplicate expressions.
     * For example, "x=1 or x=1" should be simplified to "x=1".
     * 
     * @return the simplified constraint, or "this" if it is not possible to simplify
     */
    public ConstraintImpl simplify() {
        return this;
    }

    /**
     * Get the negative constraint, if it is simpler, or null. For example,
     * "not x = 1" returns "x = 1", but "x = 1" returns null.
     * 
     * @return the negative constraint, or null
     */
    ConstraintImpl not() {
        return null;
    }

    /**
     * Evaluate the result using the currently set values.
     *
     * @return true if the constraint matches
     */
    public abstract boolean evaluate();
    
    /**
     * Whether this condition will, from now on, always evaluate to false. This
     * is the case for example for full-text constraints if there is no
     * full-text index (unless FullTextComparisonWithoutIndex is enabled). This
     * will also allow is to add conditions that stop further processing for
     * other reasons, similar to {@code "WHERE ROWNUM < 10"} in Oracle.
     * 
     * @return true if further processing should be stopped
     */
    public boolean evaluateStop() {
        return false;
    }
    
    /**
     * Get the set of property existence conditions that can be derived for this
     * condition. For example, for the condition "x=1 or x=2", the property
     * existence condition is "x is not null". For the condition "x=1 or y=2",
     * there is no such condition. For the condition "x=1 and y=1", there are
     * two (x is not null, and y is not null).
     * 
     * @return the common property existence condition (possibly empty)
     */
    public abstract Set<PropertyExistenceImpl> getPropertyExistenceConditions();
    
    /**
     * Get the (combined) full-text constraint. For constraints of the form
     * "contains(*, 'x') or contains(*, 'y')", the combined expression is
     * returned. If there is none, null is returned. For constraints of the form
     * "contains(*, 'x') or z=1", null is returned as the full-text index cannot
     * be used in this case for filtering (as it might filter out the z=1
     * nodes).
     * 
     * @param s the selector
     * @return the full-text constraint, if there is any, or null if not
     */
    public FullTextExpression getFullTextConstraint(SelectorImpl s) {
        return null;
    }
    
    /**
     * Get the set of selectors for the given condition.
     * 
     * @return the set of selectors (possibly empty)
     */
    public abstract Set<SelectorImpl> getSelectors();

    /**
     * Apply the condition to the filter, further restricting the filter if
     * possible. This may also verify the data types are compatible, and that
     * paths are valid.
     *
     * @param f the filter
     */
    public abstract void restrict(FilterImpl f);

    /**
     * Push as much of the condition down to this selector, further restricting
     * the selector condition if possible. This is important for a join: for
     * example, the query
     * "select * from a inner join b on a.x=b.x where a.y=1 and b.y=1", the
     * condition "a.y=1" can be pushed down to "a", and the condition "b.y=1"
     * can be pushed down to "b". That means it is possible to use an index in
     * this case.
     *
     * @param s the selector
     */
    public abstract void restrictPushDown(SelectorImpl s);
    
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        } else if (!(other instanceof ConstraintImpl)) {
            return false;
        }
        return toString().equals(other.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }
    
    /**
     * Whether the constraint contains a fulltext condition that requires
     * using a fulltext index, because the condition can only be evaluated there.
     * 
     * @return true if yes
     */
    public boolean requiresFullTextIndex() {
        return false;
    }
    
    /**
     * Whether the condition contains a fulltext condition that can not be 
     * applied to the filter, for example because it is part of an "or" condition
     * of the form "where a=1 or contains(., 'x')".
     * 
     * @return true if yes
     */
    public boolean containsUnfilteredFullTextCondition() {
        return false;
    }
    
    /**
     * Compute a set of sub-constraints that could be used for composing UNION
     * statements. For example in case of "c=1 or c=2", it will return to the
     * caller {@code [c=1, c=2]}. Those can be later on used for re-composing
     * conditions.
     * <p>
     * If it is not possible to convert to a union, it must return an empty set.
     * <p>
     * The default implementation in {@link ConstraintImpl#convertToUnion()}
     * always return an empty set.
     * 
     * @return the set of union constraints, if available, or an empty set if
     *         conversion is not possible
     */
    @Nonnull
    public Set<ConstraintImpl> convertToUnion() {
        return Collections.emptySet();
    }
}
