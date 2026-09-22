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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

/**
 * A literal used as a dynamic operand, for example the operator argument of
 * op(a, operator, b), or the "null" literal, for example used in
 * if(a, b, null).
 */
public class LiteralOperandImpl extends DynamicOperandImpl {

    private final PropertyValue value;
    private final String functionToken;

    /**
     * @param value the literal value, or null to represent the "null" literal
     * @param functionToken the Polish notation token, either "null" or
     *            "'text'" (matching FunctionIndexProcessor's format)
     */
    public LiteralOperandImpl(PropertyValue value, String functionToken) {
        this.value = value;
        this.functionToken = functionToken;
    }

    public PropertyValue getLiteralValue() {
        return value;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return functionToken;
    }

    @Override
    public PropertyExistenceImpl getPropertyExistence() {
        return null;
    }

    @Override
    public Set<SelectorImpl> getSelectors() {
        return Collections.emptySet();
    }

    @Override
    public PropertyValue currentProperty() {
        return value;
    }

    @Override
    public void restrict(FilterImpl f, Operator operator, PropertyValue v) {
        // a literal is never meaningfully the top-level compared operand
    }

    @Override
    public void restrictList(FilterImpl f, List<PropertyValue> list) {
        // see restrict()
    }

    @Override
    public String getFunction(SelectorImpl s) {
        return functionToken;
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return true;
    }

    @Override
    int getPropertyType() {
        return PropertyType.STRING;
    }

    @Override
    public DynamicOperandImpl createCopy() {
        return new LiteralOperandImpl(value, functionToken);
    }

    @Override
    public OrderEntry getOrderEntry(SelectorImpl s, OrderingImpl o) {
        return null;
    }

}
