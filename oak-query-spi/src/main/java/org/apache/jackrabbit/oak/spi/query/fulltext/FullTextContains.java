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
package org.apache.jackrabbit.oak.spi.query.fulltext;

/**
 * A group of full-text expressions that reflects a "contains(...)" expression,
 * and allows to access the original (unparsed) full text term.
 */
public class FullTextContains extends FullTextExpression {
    
    private final String propertyName;
    private final String rawText;
    private final FullTextExpression base;
    
    public FullTextContains(String propertyName, String rawText, FullTextExpression base) {
        this.propertyName = propertyName;
        this.rawText = rawText;
        this.base = base;
    }

    @Override
    public int getPrecedence() {
        return base.getPrecedence();
    }

    @Override
    public boolean evaluate(String value) {
        return base.evaluate(value);
    }

    @Override
    FullTextExpression simplify() {
        FullTextExpression s = base.simplify();
        if (s == base) {
            return this;
        }
        return new FullTextContains(propertyName, rawText, s);
    }

    @Override
    public String toString() {
        return base.toString();
    }

    @Override
    public boolean accept(FullTextVisitor v) {
        return v.visit(this);
    }
    
    public FullTextExpression getBase() {
        return base;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getRawText() {
        return rawText;
    }

    @Override
    public boolean isNot() {
        return base.isNot();
    }
}
