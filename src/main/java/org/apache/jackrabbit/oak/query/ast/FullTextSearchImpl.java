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

import org.apache.jackrabbit.oak.query.index.Filter;

public class FullTextSearchImpl extends ConstraintImpl {

    private final String selectorName;
    private final String propertyName;
    private final StaticOperandImpl fullTextSearchExpression;

    public FullTextSearchImpl(String selectorName, String propertyName,
            StaticOperandImpl fullTextSearchExpression) {
        this.selectorName = selectorName;
        this.propertyName = propertyName;
        this.fullTextSearchExpression = fullTextSearchExpression;
    }

    public StaticOperandImpl getFullTextSearchExpression() {
        return fullTextSearchExpression;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getSelectorName() {
        return selectorName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        // TODO quote property names?
        StringBuilder builder = new StringBuilder();
        builder.append("CONTAINS(");
        builder.append(getSelectorName());
        if (propertyName != null) {
            builder.append('.');
            builder.append(propertyName);
            builder.append(", ");
        } else {
            builder.append(".*, ");
        }
        builder.append(getFullTextSearchExpression());
        builder.append(')');
        return builder.toString();
    }

    @Override
    public boolean evaluate() {
        // TODO support evaluating fulltext conditions
        return false;
    }

    @Override
    public void apply(Filter f) {
        // TODO support fulltext index conditions
    }

}
