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

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

public class EquiJoinConditionImpl extends JoinConditionImpl {

    private final String property1Name;
    private final String property2Name;
    private final String selector1Name;
    private final String selector2Name;
    private SelectorImpl selector1;
    private SelectorImpl selector2;

    public EquiJoinConditionImpl(String selector1Name, String property1Name, String selector2Name,
            String property2Name) {
        this.selector1Name = selector1Name;
        this.property1Name = property1Name;
        this.selector2Name = selector2Name;
        this.property2Name = property2Name;
    }

    public String getSelector1Name() {
        return selector1Name;
    }

    public String getProperty1Name() {
        return property1Name;
    }

    public String getSelector2Name() {
        return selector2Name;
    }

    public String getProperty2Name() {
        return property2Name;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        // TODO quote property names?
        return getSelector1Name() + '.' + getProperty1Name()
                + " = " + getSelector2Name() + '.' + getProperty2Name();
    }

    public void bindSelector(SourceImpl source) {
        selector1 = source.getSelector(selector1Name);
        if (selector1 == null) {
            throw new IllegalArgumentException("Unknown selector: " + selector1Name);
        }
        selector2 = source.getSelector(selector2Name);
        if (selector2 == null) {
            throw new IllegalArgumentException("Unknown selector: " + selector2Name);
        }
    }

    @Override
    public boolean evaluate() {
        CoreValue v1 = selector1.currentProperty(property1Name);
        if (v1 == null) {
            return false;
        }
        // TODO data type mapping
        CoreValue v2 = selector2.currentProperty(property2Name);
        return v2 != null && v1.equals(v2);
    }

    @Override
    public void apply(FilterImpl f) {
        CoreValue v1 = selector1.currentProperty(property1Name);
        CoreValue v2 = selector2.currentProperty(property2Name);
        if (f.getSelector() == selector1 && v2 != null) {
            f.restrictProperty(property1Name, Operator.EQUAL, v2);
        }
        if (f.getSelector() == selector2 && v1 != null) {
            f.restrictProperty(property2Name, Operator.EQUAL, v1);
        }
    }

}
