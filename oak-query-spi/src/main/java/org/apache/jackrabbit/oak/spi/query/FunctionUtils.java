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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;

/**
 * Shared evaluation helpers for functions (upper, lower, etc), were we need the
 * same behavior at index time (query engine) and index time
 * (FunctionIndexProcessor).
 */
public class FunctionUtils {

    private static final PropertyState EMPTY_PROPERTY_STATE = EmptyPropertyState.emptyProperty("empty", Type.STRINGS);

    private FunctionUtils() {
    }

    public static PropertyState processIf(PropertyState condition, PropertyState trueValue, PropertyState falseValue) {
        return isTruthy(condition) ? trueValue : falseValue;
    }

    /**
     * Whether the given value is "truthy": neither missing, nor the number 0,
     * nor the boolean false.
     */
    private static boolean isTruthy(PropertyState ps) {
        return isTruthy(PropertyValues.create(ps));
    }

    /**
     * Whether the given value is "truthy": neither missing, nor the number
     * 0, nor the boolean false.
     */
    public static boolean isTruthy(PropertyValue v) {
        if (v == null) {
            return false;
        }
        int tag = v.getType().tag();
        switch (tag) {
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

    public static PropertyValue calculateOp(PropertyValue a, PropertyValue operator, PropertyValue b) {
        String op = operator.getValue(Type.STRING);
        switch (op) {
        case "is":
            return PropertyValues.newBoolean(isEqual(a, b));
        case "is not":
            return PropertyValues.newBoolean(!isEqual(a, b));
        case "and": {
            Boolean result = and3(
                    toBoolean3(a), toBoolean3(b));
            return result == null ? null : PropertyValues.newBoolean(result);
        }
        case "or": {
            Boolean result = or3(
                    toBoolean3(a), toBoolean3(b));
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
            return PropertyValues.newBoolean(isEqual(a, b));
        case "<>":
            return PropertyValues.newBoolean(!isEqual(a, b));
        case ">":
            return PropertyValues.newBoolean(compareOperands(a, b) > 0);
        case ">=":
            return PropertyValues.newBoolean(compareOperands(a, b) >= 0);
        case "<":
            return PropertyValues.newBoolean(compareOperands(a, b) < 0);
        case "<=":
            return PropertyValues.newBoolean(compareOperands(a, b) <= 0);
        case "+":
        case "-":
        case "*":
        case "/": {
            Double da = toDouble(a);
            Double db = toDouble(b);
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
     * Try to convert the value to a number, for op()'s math and comparison
     * operators.
     *
     * @return the number, or null if the value is missing or not numeric
     */
    static Double toDouble(PropertyValue v) {
        if (v == null) {
            return null;
        }
        try {
            return Double.parseDouble(v.getValue(Type.STRING));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Compare two (non-null) operands: numerically if both can be parsed as
     * a number, otherwise as strings.
     */
    static int compareOperands(PropertyValue a, PropertyValue b) {
        Double da = toDouble(a);
        Double db = toDouble(b);
        if (da != null && db != null) {
            return Double.compare(da, db);
        }
        return a.getValue(Type.STRING).compareTo(b.getValue(Type.STRING));
    }

    /**
     * Whether the two operands are equal. Unlike "=" / "&lt;&gt;", this
     * considers two missing operands to be equal (used for "is" / "is not").
     */
    static boolean isEqual(PropertyValue a, PropertyValue b) {
        if (a == null || b == null) {
            return a == null && b == null;
        }
        Double da = toDouble(a);
        Double db = toDouble(b);
        if (da != null && db != null) {
            return da.doubleValue() == db.doubleValue();
        }
        return a.getValue(Type.STRING).equals(b.getValue(Type.STRING));
    }

    /**
     * Three-valued (null = unknown) coercion to boolean, for "and" / "or".
     */
    static Boolean toBoolean3(PropertyValue v) {
        if (v == null) {
            return null;
        }
        return isTruthy(v);
    }

    static Boolean and3(Boolean a, Boolean b) {
        if (Boolean.FALSE.equals(a) || Boolean.FALSE.equals(b)) {
            return false;
        }
        if (a == null || b == null) {
            return null;
        }
        return true;
    }

    static Boolean or3(Boolean a, Boolean b) {
        if (Boolean.TRUE.equals(a) || Boolean.TRUE.equals(b)) {
            return true;
        }
        if (a == null || b == null) {
            return null;
        }
        return false;
    }

    /**
     * Evaluate op(a, operator, b). Comparisons return null if either operand is
     * missing (except "is" / "is not", which treat missing as a comparable
     * value); math operators return null if either operand is missing or not
     * numeric; "and" / "or" use standard SQL three-valued logic.
     */
    public static PropertyState calculateOp(PropertyState a, PropertyState operatorLiteral, PropertyState b) {
        String operator = operatorLiteral.getValue(Type.STRING);
        boolean aMissing = a == EMPTY_PROPERTY_STATE;
        boolean bMissing = b == EMPTY_PROPERTY_STATE;
        switch (operator) {
            case "is":
                return PropertyStates.createProperty("value", isEqual(a, b), Type.BOOLEAN);
            case "is not":
                return PropertyStates.createProperty("value", !isEqual(a, b), Type.BOOLEAN);
            case "and": {
                Boolean result = and3(toBoolean3(a), toBoolean3(b));
                return result == null ? null : PropertyStates.createProperty("value", result, Type.BOOLEAN);
            }
            case "or": {
                Boolean result = or3(toBoolean3(a), toBoolean3(b));
                return result == null ? null : PropertyStates.createProperty("value", result, Type.BOOLEAN);
            }
            default:
                break;
        }
        if (aMissing || bMissing) {
            return null;
        }
        switch (operator) {
            case "=":
                return PropertyStates.createProperty("value", isEqual(a, b), Type.BOOLEAN);
            case "<>":
                return PropertyStates.createProperty("value", !isEqual(a, b), Type.BOOLEAN);
            case ">":
                return PropertyStates.createProperty("value", compareOperands(a, b) > 0, Type.BOOLEAN);
            case ">=":
                return PropertyStates.createProperty("value", compareOperands(a, b) >= 0, Type.BOOLEAN);
            case "<":
                return PropertyStates.createProperty("value", compareOperands(a, b) < 0, Type.BOOLEAN);
            case "<=":
                return PropertyStates.createProperty("value", compareOperands(a, b) <= 0, Type.BOOLEAN);
            case "+":
            case "-":
            case "*":
            case "/": {
                Double da = toDouble(a);
                Double db = toDouble(b);
                if (da == null || db == null) {
                    return null;
                }
                double result;
                switch (operator) {
                    case "+": result = da + db; break;
                    case "-": result = da - db; break;
                    case "*": result = da * db; break;
                    default: result = da / db; break;
                }
                return PropertyStates.createProperty("value", result, Type.DOUBLE);
            }
            default:
                throw new IllegalArgumentException("Unknown operator for op(): " + operator);
        }
    }


    /**
     * Three-valued (null = unknown) coercion to boolean, for "and" / "or".
     */
    private static Boolean toBoolean3(PropertyState ps) {
        if (ps == null || ps == EMPTY_PROPERTY_STATE) {
            return null;
        }
        return isTruthy(ps);
    }


    /**
     * Compare two (non-null) operands: numerically if both can be parsed as a
     * number, otherwise as strings.
     */
    private static int compareOperands(PropertyState a, PropertyState b) {
        Double da = toDouble(a);
        Double db = toDouble(b);
        if (da != null && db != null) {
            return Double.compare(da, db);
        }
        return a.getValue(Type.STRING).compareTo(b.getValue(Type.STRING));
    }

    /**
     * Whether the two operands are equal. Unlike "=" / "&lt;&gt;", this
     * considers two missing operands to be equal (used for "is" / "is not").
     */
    private static boolean isEqual(PropertyState a, PropertyState b) {
        boolean aMissing = a == null || a == EMPTY_PROPERTY_STATE;
        boolean bMissing = b == null || b == EMPTY_PROPERTY_STATE;
        if (aMissing || bMissing) {
            return aMissing && bMissing;
        }
        Double da = toDouble(a);
        Double db = toDouble(b);
        if (da != null && db != null) {
            return da.doubleValue() == db.doubleValue();
        }
        return a.getValue(Type.STRING).equals(b.getValue(Type.STRING));
    }

    /**
     * Try to convert the value to a number, for use with op()'s math and
     * comparison operators.
     *
     * @return the number, or null if the value is missing or not numeric
     */
    private static Double toDouble(PropertyState ps) {
        if (ps == null || ps == EMPTY_PROPERTY_STATE) {
            return null;
        }
        try {
            return Double.parseDouble(ps.getValue(Type.STRING));
        } catch (NumberFormatException e) {
            return null;
        }
    }

}
