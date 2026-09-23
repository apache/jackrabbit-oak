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
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;

/**
 * Shared evaluation helpers for functions (upper, lower, etc), were we need the
 * same behavior at index time (query engine) and index time
 * (FunctionIndexProcessor).
 */
public class FunctionIndexUtils {

    private FunctionIndexUtils() {
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

    public static PropertyValue calculateOp(PropertyValue a, PropertyValue operator, PropertyValue b) {
        String op = operator.getValue(Type.STRING);
        switch (op) {
        case "is":
            return PropertyValues.newBoolean(a == null || b == null || a.compareTo(b) == 0);
        case "is not":
            return PropertyValues.newBoolean(!(a == null || b == null || a.compareTo(b) == 0));
        case "and":
            return PropertyValues.newBoolean(isTruthy(a) && isTruthy(b));
        case "or":
            return PropertyValues.newBoolean(isTruthy(a) || isTruthy(b));
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
     * Try to convert the value to a number, for op()'s math and comparison
     * operators.
     *
     * @return the number, or null if the value is missing or not numeric
     */
    static Double toDoubleOrNull(PropertyValue v) {
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
