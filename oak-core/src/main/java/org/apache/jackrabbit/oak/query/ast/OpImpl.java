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

import java.util.List;
import java.util.Set;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.SetUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.OrderEntry;

/**
 * The function "op(a, operator, b)". The operator is itself a dynamic
 * operand, typically a quoted string literal, but it can also be a property
 * or another function. Supported operators:
 * <ul>
 * <li>=, &lt;&gt;, &gt;, &gt;=, &lt;, &lt;= : comparisons (null if a or b is
 * null), return true / false</li>
 * <li>+, -, *, / : math (null if a or b is null, or not numeric)</li>
 * <li>is, is not : same as = and &lt;&gt;, but consider null equal to
 * null</li>
 * <li>and, or : logical and / or, using three-valued logic</li>
 * </ul>
 * Whether this function is available in queries is controlled by
 * {@link org.apache.jackrabbit.oak.query.QueryEngineSettings#isOpFunctionEnabled()}
 * (checked by the parser, not here).
 */
public class OpImpl extends DynamicOperandImpl {

    private final DynamicOperandImpl operand1;
    private final DynamicOperandImpl operator;
    private final DynamicOperandImpl operand2;

    public OpImpl(DynamicOperandImpl operand1, DynamicOperandImpl operator, DynamicOperandImpl operand2) {
        this.operand1 = operand1;
        this.operator = operator;
        this.operand2 = operand2;
    }

    public DynamicOperandImpl getOperand1() {
        return operand1;
    }

    public DynamicOperandImpl getOperator() {
        return operator;
    }

    public DynamicOperandImpl getOperand2() {
        return operand2;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "op(" + operand1 + ", " + operator + ", " + operand2 + ')';
    }

    @Override
    public PropertyExistenceImpl getPropertyExistence() {
        return null;
    }

    @Override
    public Set<SelectorImpl> getSelectors() {
        return SetUtils.union(
                SetUtils.union(operand1.getSelectors(), operator.getSelectors()),
                operand2.getSelectors());
    }

    @Override
    public PropertyValue currentProperty() {
        PropertyValue op = operator.currentProperty();
        if (op == null) {
            return null;
        }
        return calculateOp(operand1.currentProperty(), op.getValue(Type.STRING), operand2.currentProperty());
    }

    private static PropertyValue calculateOp(PropertyValue a, String op, PropertyValue b) {
        switch (op) {
        case "is":
            return PropertyValues.newBoolean(FunctionOperatorUtils.isEqual(a, b));
        case "is not":
            return PropertyValues.newBoolean(!FunctionOperatorUtils.isEqual(a, b));
        case "and": {
            Boolean result = FunctionOperatorUtils.and3(
                    FunctionOperatorUtils.toBoolean3(a), FunctionOperatorUtils.toBoolean3(b));
            return result == null ? null : PropertyValues.newBoolean(result);
        }
        case "or": {
            Boolean result = FunctionOperatorUtils.or3(
                    FunctionOperatorUtils.toBoolean3(a), FunctionOperatorUtils.toBoolean3(b));
            return result == null ? null : PropertyValues.newBoolean(result);
        }
        default:
            break;
        }
        if (a == null || b == null) {
            return null;
        }
        switch (op) {
        case "=":
            return PropertyValues.newBoolean(FunctionOperatorUtils.isEqual(a, b));
        case "<>":
            return PropertyValues.newBoolean(!FunctionOperatorUtils.isEqual(a, b));
        case ">":
            return PropertyValues.newBoolean(FunctionOperatorUtils.compareOperands(a, b) > 0);
        case ">=":
            return PropertyValues.newBoolean(FunctionOperatorUtils.compareOperands(a, b) >= 0);
        case "<":
            return PropertyValues.newBoolean(FunctionOperatorUtils.compareOperands(a, b) < 0);
        case "<=":
            return PropertyValues.newBoolean(FunctionOperatorUtils.compareOperands(a, b) <= 0);
        case "+":
        case "-":
        case "*":
        case "/": {
            Double da = FunctionOperatorUtils.toDouble(a);
            Double db = FunctionOperatorUtils.toDouble(b);
            if (da == null || db == null) {
                return null;
            }
            double result;
            switch (op) {
            case "+": result = da + db; break;
            case "-": result = da - db; break;
            case "*": result = da * db; break;
            default: result = da / db; break;
            }
            return PropertyValues.newDouble(result);
        }
        default:
            throw new IllegalArgumentException("Unknown operator for op(): " + op);
        }
    }

    @Override
    public void restrict(FilterImpl f, Operator filterOperator, PropertyValue v) {
        if (filterOperator == Operator.NOT_EQUAL && v != null) {
            // not supported
            return;
        }
        String fn = getFunction(f.getSelector());
        if (fn != null) {
            f.restrictProperty(QueryConstants.FUNCTION_RESTRICTION_PREFIX + fn,
                    filterOperator, v, PropertyType.STRING);
        }
    }

    @Override
    public void restrictList(FilterImpl f, List<PropertyValue> list) {
        String fn = getFunction(f.getSelector());
        f.restrictPropertyAsList(QueryConstants.FUNCTION_RESTRICTION_PREFIX + fn, list);
    }

    @Override
    public String getFunction(SelectorImpl s) {
        String f1 = operand1.getFunction(s);
        if (f1 == null) {
            return null;
        }
        String fo = operator.getFunction(s);
        if (fo == null) {
            return null;
        }
        String f2 = operand2.getFunction(s);
        if (f2 == null) {
            return null;
        }
        return "op*" + f1 + "*" + fo + "*" + f2;
    }

    @Override
    public boolean canRestrictSelector(SelectorImpl s) {
        return operand1.canRestrictSelector(s) && operator.canRestrictSelector(s)
                && operand2.canRestrictSelector(s);
    }

    @Override
    int getPropertyType() {
        return PropertyType.STRING;
    }

    @Override
    public DynamicOperandImpl createCopy() {
        return new OpImpl(operand1.createCopy(), operator.createCopy(), operand2.createCopy());
    }

    @Override
    public OrderEntry getOrderEntry(SelectorImpl s, OrderingImpl o) {
        String fn = getFunction(s);
        if (fn != null) {
            return new OrderEntry(
                QueryConstants.FUNCTION_RESTRICTION_PREFIX + fn,
                Type.STRING,
                o.isDescending() ?
                OrderEntry.Order.DESCENDING : OrderEntry.Order.ASCENDING);
        }
        return null;
    }

}
