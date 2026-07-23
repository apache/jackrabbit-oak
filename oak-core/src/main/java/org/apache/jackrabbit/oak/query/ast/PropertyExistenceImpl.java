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
import java.util.Objects;
import java.util.Set;

import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * A condition to check if the property exists ("is not null").
 */
public class PropertyExistenceImpl extends ConstraintImpl {

    private final PropertyValueImpl propertyValue;

    public PropertyExistenceImpl(PropertyValueImpl propertyValue) {
        this.propertyValue = propertyValue;
    }

    @Override
    public boolean evaluate() {
        return propertyValue.currentProperty() != null;
    }

    @Override
    public Set<PropertyExistenceImpl> getPropertyExistenceConditions() {
        return Collections.singleton(this);
    }

    @Override
    public Set<SelectorImpl> getSelectors() {
        return propertyValue.getSelectors();
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return propertyValue + " is not null";
    }

    public void bindSelector(SourceImpl source) {
        propertyValue.bindSelector(source);
    }

    @Override
    public void restrict(FilterImpl f) {
        propertyValue.restrict(f, Operator.NOT_EQUAL, null);
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        if (propertyValue.canRestrictSelector(s)) {
            s.restrictSelector(this);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass().getName(), propertyValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (!(obj instanceof PropertyExistenceImpl)) {
            return false;
        }
        PropertyExistenceImpl other = (PropertyExistenceImpl) obj;
        return propertyValue.equals(other.propertyValue);
    }

    @Override
    public AstElement copyOf() {
        return new PropertyExistenceImpl(propertyValue.createCopy());
    }
}
