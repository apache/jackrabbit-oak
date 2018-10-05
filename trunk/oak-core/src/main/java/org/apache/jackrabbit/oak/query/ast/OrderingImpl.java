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

/**
 * An element of an "order by" list. This includes whether this element should
 * be sorted in ascending or descending order.
 */
public class OrderingImpl extends AstElement {

    private final DynamicOperandImpl operand;
    private final Order order;

    public OrderingImpl(DynamicOperandImpl operand, Order order) {
        this.operand = operand;
        this.order = order;
    }

    public DynamicOperandImpl getOperand() {
        return operand;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return operand + " " + order.name();
    }

    public boolean isDescending() {
        return order == Order.DESCENDING;
    }

    public OrderingImpl createCopy() {
        return new OrderingImpl(operand.createCopy(), order);
    }

}
