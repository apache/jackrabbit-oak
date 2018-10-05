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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.jackrabbit.oak.api.PropertyValue;

/**
 * A result column expression.
 */
public class ColumnImpl extends AstElement {

    private final String selectorName, propertyName, columnName;
    private SelectorImpl selector;

    public ColumnImpl(String selectorName, String propertyName, String columnName) {
        this.selectorName = checkNotNull(selectorName);
        this.propertyName = checkNotNull(propertyName);
        this.columnName = checkNotNull(columnName);
    }
    
    public String getColumnName() {
        return columnName;
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
        return quote(selectorName) + '.' + quote(propertyName)
                + " as " + quote(columnName);
    }

    public PropertyValue currentProperty() {
        return selector.currentProperty(propertyName);
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }
    
    public SelectorImpl getSelector() {
        return selector;
    }

    @Override
    public AstElement copyOf() {
        return new ColumnImpl(selectorName, propertyName, columnName);
    }
}
