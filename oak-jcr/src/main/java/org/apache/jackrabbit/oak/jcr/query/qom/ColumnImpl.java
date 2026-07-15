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
package org.apache.jackrabbit.oak.jcr.query.qom;

import javax.jcr.query.qom.Column;

/**
 * The implementation of the corresponding JCR interface.
 */
public class ColumnImpl extends QOMNode implements Column {

    private final String selectorName, propertyName, columnName;

    public ColumnImpl(String selectorName, String propertyName, String columnName) {
        this.selectorName = selectorName;
        this.propertyName = propertyName;
        this.columnName = columnName;
    }

    @Override
    public String getColumnName() {
        return columnName;
    }

    @Override
    public String getPropertyName() {
        return propertyName;
    }

    @Override
    public String getSelectorName() {
        return selectorName;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (selectorName != null) {
            buff.append(quoteSelectorName(selectorName));
            buff.append('.');
        }
        if (propertyName != null) {
            buff.append(quotePropertyName(propertyName));
        } else {
            buff.append('*');
        }
        if (columnName != null) {
            buff.append(" AS ").append(quoteColumnName(columnName));
        }
        return buff.toString();
    }

}
