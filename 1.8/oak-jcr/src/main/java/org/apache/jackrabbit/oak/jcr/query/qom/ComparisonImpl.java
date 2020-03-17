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

import javax.jcr.query.qom.Comparison;

/**
 * The implementation of the corresponding JCR interface.
 */
public class ComparisonImpl extends ConstraintImpl implements Comparison {

    private final DynamicOperandImpl operand1;
    private final Operator operator;
    private final StaticOperandImpl operand2;

    public ComparisonImpl(DynamicOperandImpl operand1, Operator operator, StaticOperandImpl operand2) {
        this.operand1 = operand1;
        this.operator = operator;
        this.operand2 = operand2;
    }

    @Override
    public DynamicOperandImpl getOperand1() {
        return operand1;
    }

    @Override
    public String getOperator() {
        return operator.toString();
    }

    @Override
    public StaticOperandImpl getOperand2() {
        return operand2;
    }

    @Override
    public String toString() {
        return operator.formatSql(operand1.toString(), operand2.toString());
    }

    @Override
    public void bindVariables(QueryObjectModelImpl qom) {
        operand2.bindVariables(qom);
    }

}
