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

import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

/**
 * The base class for dynamic operands (such as a function or property).
 */
public abstract class DynamicOperandImpl extends AstElement {

    public abstract PropertyValue currentProperty();

    /**
     * Apply a restriction of type "this = value" to the given filter.
     * 
     * @param f the filter where the restriction is applied.
     * @param operator the operator (for example "=").
     * @param v the value
     */
    public abstract void restrict(FilterImpl f, Operator operator, PropertyValue v);

    /**
     * Apply a restriction of type "this in (list)" to the given filter.
     * 
     * @param f the filter where the restriction is applied.
     * @param list the list of values
     */
    public abstract void restrictList(FilterImpl f, List<PropertyValue> list);
    
    /**
     * Get the function of a function-based index, in Polish notation.
     * 
     * @param s the selector
     * @return the function, or null if not supported
     */
    public abstract String getFunction(SelectorImpl s);

    /**
     * Check whether the condition can be applied to a selector (to restrict the
     * selector). The method may return true if the operand can be evaluated
     * when the given selector and all previous selectors in the join can be
     * evaluated.
     *
     * @param s the selector
     * @return true if the condition can be applied
     */
    public abstract boolean canRestrictSelector(SelectorImpl s);

    public boolean supportsRangeConditions() {
        return true;
    }

    abstract int getPropertyType();

    /**
     * Get the property existence condition for this operand, if this operand is
     * used as part of a condition.
     * 
     * @return the property existence condition, or null if none
     */
    public abstract PropertyExistenceImpl getPropertyExistence();

    /**
     * Get the set of selectors for this operand.
     * 
     * @return the set of selectors
     */
    public abstract Set<SelectorImpl> getSelectors();
    
    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (!(other instanceof DynamicOperandImpl)) {
            return false;
        }
        DynamicOperandImpl o = (DynamicOperandImpl) other;
        return o.toString().equals(toString());
    }
    
    @Override
    public int hashCode() {
        return toString().hashCode();
    }
    
    public abstract DynamicOperandImpl createCopy();
    
    /**
     * Create an entry for the "order by" list for a given filter.
     * 
     * @param s the selector
     * @param o the ordering
     * @return the entry
     */
    public abstract OrderEntry getOrderEntry(SelectorImpl s, OrderingImpl o);

}
