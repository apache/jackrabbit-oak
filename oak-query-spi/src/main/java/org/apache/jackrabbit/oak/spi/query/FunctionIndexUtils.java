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
package org.apache.jackrabbit.oak.spi.query;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;

/**
 * Shared evaluation helpers for the "if(condition, trueValue, falseValue)" and
 * "op(a, operator, b)" functions, where we need the same behavior at query
 * time and index time.
 * <p>
 * These helpers intentionally do <em>not</em> perform the kind of type
 * coercion the query engine's {@code ComparisonImpl} does for regular
 * {@code WHERE} clause conditions. Instead,
 * comparisons are delegated directly to {@link PropertyValue#compareTo}: if
 * the two operands have the same type, they are compared according to that
 * type's natural ordering; if the types differ, the comparison falls back to
 * an arbitrary but consistent type-based ordering.
 */
public class FunctionIndexUtils {

    private FunctionIndexUtils() {
    }

    /**
     * Whether the given value is "truthy": neither missing, nor the number
     * 0, nor the boolean false. Used for the condition of "if(...)", and for
     * the "and" / "or" operators of "op(...)".
     *
     * @param v the value, or null if missing
     * @return true, unless the value is missing, the boolean false, or the
     *         number (long, double, or decimal) zero
     */
    public static boolean isTruthy(PropertyValue v) {
        if (v == null) {
            return false;
        }
        switch (v.getType().tag()) {
        case PropertyType.BOOLEAN:
            return v.getValue(Type.BOOLEAN);
        case PropertyType.LONG:
            return v.getValue(Type.LONG) != 0;
        case PropertyType.DOUBLE:
            return v.getValue(Type.DOUBLE) != 0;
        case PropertyType.DECIMAL:
            return v.getValue(Type.DECIMAL).signum() != 0;
        default:
            return true;
        }
    }

    /**
     * Evaluate "op(a, operator, b)".
     *
     * @param a the first operand, or null if missing
     * @param operator the operator, never null
     * @param b the second operand, or null if missing
     * @return the result of the operation, or null if the result is
     *         undefined (for example, a comparison or math operation with a
     *         missing or non-numeric operand)
     * @throws IllegalArgumentException if the operator is not one of the
     *         operators listed above
     */
    public static PropertyValue processOp(PropertyValue a, PropertyValue operator, PropertyValue b) {
        String op = operator.getValue(Type.STRING);
        switch (op) {
        case "is":
            return PropertyValues.newBoolean(isSame(a, b));
        case "is not":
            return PropertyValues.newBoolean(!isSame(a, b));
        case "and": {
            Boolean result = and3(toBoolean3(a), toBoolean3(b));
            return result == null ? null : PropertyValues.newBoolean(result);
        }
        case "or": {
            Boolean result = or3(toBoolean3(a), toBoolean3(b));
            return result == null ? null : PropertyValues.newBoolean(result);
        }
        }
        if (a == null || b == null) {
            return null;
        }
        switch (op) {
        case "=":
            return PropertyValues.newBoolean(a.compareTo(b) == 0);
        case "<>":
            return PropertyValues.newBoolean(a.compareTo(b) != 0);
        case ">":
            return PropertyValues.newBoolean(a.compareTo(b) > 0);
        case ">=":
            return PropertyValues.newBoolean(a.compareTo(b) >= 0);
        case "<":
            return PropertyValues.newBoolean(a.compareTo(b) < 0);
        case "<=":
            return PropertyValues.newBoolean(a.compareTo(b) <= 0);
        case "+":
        case "-":
        case "*":
        case "/": {
            Double da = toDoubleOrNull(a);
            Double db = toDoubleOrNull(b);
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

    /**
     * Whether the two operands are the same, for the "is" / "is not"
     * operators of "op(...)": unlike {@code =} / {@code <>}, this considers
     * two missing operands to be the same, so the result of this method is
     * never itself "unknown".
     *
     * @param a the first operand, or null if missing
     * @param b the second operand, or null if missing
     * @return true if both are missing, or if neither is missing and
     *         {@code a.compareTo(b) == 0}
     */
    private static boolean isSame(PropertyValue a, PropertyValue b) {
        if (a == null || b == null) {
            return a == null && b == null;
        }
        return a.compareTo(b) == 0;
    }

    /**
     * Three-valued (null meaning "unknown") coercion to boolean.
     *
     * @param v the value, or null if missing
     * @return {@link #isTruthy}, or null if the value is missing
     */
    private static Boolean toBoolean3(PropertyValue v) {
        if (v == null) {
            return null;
        }
        return isTruthy(v);
    }

    /**
     * Three-valued (SQL-style) logical "and".
     */
    private static Boolean and3(Boolean a, Boolean b) {
        if (Boolean.FALSE.equals(a) || Boolean.FALSE.equals(b)) {
            return false;
        }
        if (a == null || b == null) {
            return null;
        }
        return true;
    }

    /**
     * Three-valued (SQL-style) logical "or".
     */
    private static Boolean or3(Boolean a, Boolean b) {
        if (Boolean.TRUE.equals(a) || Boolean.TRUE.equals(b)) {
            return true;
        }
        if (a == null || b == null) {
            return null;
        }
        return false;
    }

    /**
     * Try to convert the value to a number.
     *
     * @param v the value, or null if missing
     * @return the number, or null if the value is missing or not a valid number
     */
    public static Double toDoubleOrNull(PropertyValue v) {
        if (v == null) {
            return null;
        }
        try {
            return Double.parseDouble(v.getValue(Type.STRING));
        } catch (NumberFormatException e) {
            return null;
        }
    }

}
