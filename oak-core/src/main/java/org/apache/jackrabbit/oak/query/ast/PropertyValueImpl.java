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
import java.util.Locale;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.SQL2Parser;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * A property expression.
 */
public class PropertyValueImpl extends DynamicOperandImpl {

    private final String selectorName;
    private final String propertyName;
    private final int propertyType;
    private SelectorImpl selector;

    public PropertyValueImpl(String selectorName, String propertyName) {
        this(selectorName, propertyName, null);
    }

    public PropertyValueImpl(String selectorName, String propertyName, String propertyType) {
        this.selectorName = selectorName;
        this.propertyName = propertyName;
        this.propertyType = propertyType == null ?
                PropertyType.UNDEFINED :
                SQL2Parser.getPropertyTypeFromName(propertyType);
    }

    public String getSelectorName() {
        return selectorName;
    }

    public String getPropertyName() {
        return propertyName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        String s = quote(selectorName) + '.' + quote(propertyName);
        if (propertyType != PropertyType.UNDEFINED) {
            s = "property(" + s + ", '" +
                    PropertyType.nameFromValue(propertyType).toLowerCase(Locale.ENGLISH) +
                    "')";
        }
        return s;
    }
    
    @Override
    public boolean supportsRangeConditions() {
        // the jcr:path pseudo-property doesn't support LIKE conditions,
        // because the path doesn't might be escaped, and possibly contain
        // expressions that would result in incorrect results (/test[1] for example)
        return !propertyName.equals(QueryImpl.JCR_PATH);
    }
    
    @Override
    public PropertyExistenceImpl getPropertyExistence() {
        if (propertyName.equals("*")) {
            return null;
        }
        return new PropertyExistenceImpl(selector, selectorName, propertyName);
    }
    
    @Override
    public Set<SelectorImpl> getSelectors() {
        return Collections.singleton(selector);
    }

    @Override
    public PropertyValue currentProperty() {
        PropertyValue p;
        if (propertyType == PropertyType.UNDEFINED) {
            p = selector.currentProperty(propertyName);
        } else {
            p = selector.currentProperty(propertyName, propertyType);
        }
        return p;        
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public void restrict(FilterImpl f, Operator operator, PropertyValue v) {
        if (f.getSelector() == selector) {
            if (operator == Operator.NOT_EQUAL && v != null) {
                // not supported
                return;
            }
            String pn = normalizePropertyName(propertyName);            
            f.restrictProperty(pn, operator, v);
            if (propertyType != PropertyType.UNDEFINED) {
                f.restrictPropertyType(pn, operator, propertyType);
            }
        }
    }
    
    @Override
    public void restrictList(FilterImpl f, List<PropertyValue> list) {
        if (f.getSelector() == selector) {
            String pn = normalizePropertyName(propertyName);            
            f.restrictPropertyAsList(pn, list);
        }
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return s == selector;
    }
    
    @Override
    int getPropertyType() {
        return propertyType;
    }
    
    @Override
    public PropertyValueImpl createCopy() {
        return new PropertyValueImpl(selectorName, propertyName);
    }

}
