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
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.query.SQL2Parser;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Filter.PathRestriction;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

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
        return !propertyName.equals(QueryConstants.JCR_PATH);
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
        if (f.getSelector().equals(selector)) {
            String pn = normalizePropertyName(propertyName);
            if (pn.equals(QueryConstants.JCR_PATH)) {
                if (operator == Operator.EQUAL) {
                    f.restrictPath(v.getValue(Type.STRING), PathRestriction.EXACT);
                }
            } else {
                // "x <> 1" also means "x is not null" and "x <> 1"
                if (operator == Operator.NOT_EQUAL && v != null) {
                    // This handles "x is not null" part
                    // Details -
                    // This is being used to add the x is not null query for inequality queries on property x
                    // Without this index implementations will return entries with null/no value for x for query like (x!=some_value)
                    // and QE will need to do extra work to remove these entries.
                    // We can handle x is not null from index implementations of lucene and elastic also but
                    // this Restriction also needs to be added to support existing behaviour of PropertyIndex
                    f.restrictProperty(pn, Operator.NOT_EQUAL, null, propertyType);
                }
                // For NOT_EQUAL operator this will be the second restriction that will be added.
                // This will be used by index implementations (Lucene and Elastic) to serve the not equal condition ("x <> 1") from index itself
                f.restrictProperty(pn, operator, v, propertyType);
            }
        }
    }
    
    @Override
    public void restrictList(FilterImpl f, List<PropertyValue> list) {
        if (f.getSelector().equals(selector)) {
            String pn = normalizePropertyName(propertyName);            
            f.restrictPropertyAsList(pn, list);
        }
    }
    
    @Override
    public String getFunction(SelectorImpl s) {
        if (!s.equals(selector)) {
            return null;
        }
        String pn = normalizePropertyName(propertyName);
        return "@" + pn;
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return s.equals(selector);
    }
    
    @Override
    int getPropertyType() {
        return propertyType;
    }
    
    @Override
    public PropertyValueImpl createCopy() {
        return new PropertyValueImpl(selectorName, propertyName);
    }

    @Override
    public OrderEntry getOrderEntry(SelectorImpl s, OrderingImpl o) {
        if (!s.equals(selector)) {
            // ordered by a different selector
            return null;
        }
        String pn = normalizePropertyName(propertyName);
        return new OrderEntry(
            pn, 
            Type.UNDEFINED, 
            o.isDescending() ? 
            OrderEntry.Order.DESCENDING : OrderEntry.Order.ASCENDING);
    }

    @Override
    public String getOrderEntryPropertyName(SelectorImpl s) {
        if (!s.equals(selector)) {
            // ordered by a different selector
            return null;
        }
        return normalizePropertyName(propertyName);
    }

}
