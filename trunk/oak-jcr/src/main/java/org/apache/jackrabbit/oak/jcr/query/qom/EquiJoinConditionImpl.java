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

import javax.jcr.query.qom.EquiJoinCondition;

/**
 * The implementation of the corresponding JCR interface.
 */
public class EquiJoinConditionImpl extends JoinConditionImpl implements EquiJoinCondition {

    private final String property1Name;
    private final String property2Name;
    private final String selector1Name;
    private final String selector2Name;

    public EquiJoinConditionImpl(String selector1Name, String property1Name, String selector2Name,
            String property2Name) {
        this.selector1Name = selector1Name;
        this.property1Name = property1Name;
        this.selector2Name = selector2Name;
        this.property2Name = property2Name;
    }

    @Override
    public String getSelector1Name() {
        return selector1Name;
    }

    @Override
    public String getProperty1Name() {
        return property1Name;
    }

    @Override
    public String getSelector2Name() {
        return selector2Name;
    }

    @Override
    public String getProperty2Name() {
        return property2Name;
    }

    @Override
    public String toString() {
        return quoteSelectorName(selector1Name) + '.' +
                quotePropertyName(property1Name) + " = " +
                quoteSelectorName(selector2Name) + '.' +
                quotePropertyName(property2Name);
    }

}
