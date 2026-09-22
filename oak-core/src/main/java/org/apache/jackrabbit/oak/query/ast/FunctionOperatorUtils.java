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

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;

/**
 * Shared evaluation helpers for the if(...) and op(...) dynamic operands.
 * Mirrors the semantics implemented (independently, for index-time
 * evaluation) in FunctionIndexProcessor (oak-search).
 */
final class FunctionOperatorUtils {

    private FunctionOperatorUtils() {
    }

    /**
     * Whether the given value is "truthy": neither missing, nor the number
     * 0, nor the boolean false.
     */
    static boolean isTruthy(PropertyValue v) {
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

}
