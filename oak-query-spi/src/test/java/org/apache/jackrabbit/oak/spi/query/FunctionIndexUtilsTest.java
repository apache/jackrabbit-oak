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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;

import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.junit.Test;

/**
 * Tests for {@link FunctionIndexUtils}.
 */
public class FunctionIndexUtilsTest {

    // ---------- isTruthy ----------

    @Test
    public void isTruthy_missing() {
        assertFalse(FunctionIndexUtils.isTruthy(null));
    }

    @Test
    public void isTruthy_boolean() {
        assertTrue(FunctionIndexUtils.isTruthy(PropertyValues.newBoolean(true)));
        assertFalse(FunctionIndexUtils.isTruthy(PropertyValues.newBoolean(false)));
    }

    @Test
    public void isTruthy_long() {
        assertFalse(FunctionIndexUtils.isTruthy(PropertyValues.newLong(0L)));
        assertTrue(FunctionIndexUtils.isTruthy(PropertyValues.newLong(1L)));
        assertTrue(FunctionIndexUtils.isTruthy(PropertyValues.newLong(-1L)));
    }

    @Test
    public void isTruthy_double() {
        assertFalse(FunctionIndexUtils.isTruthy(PropertyValues.newDouble(0.0)));
        assertTrue(FunctionIndexUtils.isTruthy(PropertyValues.newDouble(0.5)));
    }

    @Test
    public void isTruthy_decimal() {
        assertFalse(FunctionIndexUtils.isTruthy(PropertyValues.newDecimal(BigDecimal.ZERO)));
        assertTrue(FunctionIndexUtils.isTruthy(PropertyValues.newDecimal(BigDecimal.ONE)));
    }

    @Test
    public void isTruthy_string() {
        // strings are always truthy, even an empty one -- only missing, 0,
        // and false are falsy
        assertTrue(FunctionIndexUtils.isTruthy(PropertyValues.newString("")));
        assertTrue(FunctionIndexUtils.isTruthy(PropertyValues.newString("false")));
        assertTrue(FunctionIndexUtils.isTruthy(PropertyValues.newString("0")));
        assertTrue(FunctionIndexUtils.isTruthy(PropertyValues.newString("anything")));
    }

    // ---------- processOp: comparisons ----------

    @Test
    public void processOp_equals() {
        assertTrue(op(PropertyValues.newLong(2L), "=", PropertyValues.newLong(2L)));
        assertFalse(op(PropertyValues.newLong(2L), "=", PropertyValues.newLong(3L)));
        assertFalse(op(PropertyValues.newLong(2L), "<>", PropertyValues.newLong(2L)));
        assertTrue(op(PropertyValues.newLong(2L), "<>", PropertyValues.newLong(3L)));
    }

    @Test
    public void processOp_orderComparisons() {
        assertTrue(op(PropertyValues.newLong(1L), "<", PropertyValues.newLong(2L)));
        assertFalse(op(PropertyValues.newLong(2L), "<", PropertyValues.newLong(2L)));
        assertTrue(op(PropertyValues.newLong(2L), "<=", PropertyValues.newLong(2L)));
        assertTrue(op(PropertyValues.newLong(2L), ">", PropertyValues.newLong(1L)));
        assertFalse(op(PropertyValues.newLong(1L), ">", PropertyValues.newLong(2L)));
        assertTrue(op(PropertyValues.newLong(2L), ">=", PropertyValues.newLong(2L)));
    }

    @Test
    public void processOp_stringComparison() {
        assertTrue(op(PropertyValues.newString("apple"), "<", PropertyValues.newString("banana")));
        assertTrue(op(PropertyValues.newString("banana"), ">", PropertyValues.newString("apple")));
    }

    @Test
    public void processOp_comparisonWithMissingOperandIsNull() {
        assertNull(FunctionIndexUtils.processOp(null, PropertyValues.newString("="), PropertyValues.newLong(1L)));
        assertNull(FunctionIndexUtils.processOp(PropertyValues.newLong(1L), PropertyValues.newString("="), null));
        assertNull(FunctionIndexUtils.processOp(null, PropertyValues.newString("<"), null));
    }

    @Test
    public void processOp_noTypeCoercion() {
        // by design, comparisons use PropertyValue.compareTo() directly, with
        // no type coercion: comparing a LONG to a DOUBLE does not compare the
        // numeric values, since the types differ. This is a deliberate
        // limitation, see the class-level documentation.
        assertFalse(op(PropertyValues.newLong(2L), "=", PropertyValues.newDouble(2.0)));
    }

    // ---------- processOp: is / is not ----------

    @Test
    public void processOp_is_bothMissing() {
        assertTrue(op(null, "is", null));
        assertFalse(op(null, "is not", null));
    }

    @Test
    public void processOp_is_oneMissing() {
        // unlike "=", "is" is never null: one missing and one present is
        // simply "not the same", in both directions
        assertFalse(op(null, "is", PropertyValues.newLong(2L)));
        assertFalse(op(PropertyValues.newLong(2L), "is", null));
        assertTrue(op(null, "is not", PropertyValues.newLong(2L)));
        assertTrue(op(PropertyValues.newLong(2L), "is not", null));
    }

    @Test
    public void processOp_is_bothPresent() {
        assertTrue(op(PropertyValues.newLong(2L), "is", PropertyValues.newLong(2L)));
        assertFalse(op(PropertyValues.newLong(2L), "is", PropertyValues.newLong(3L)));
        assertFalse(op(PropertyValues.newLong(2L), "is not", PropertyValues.newLong(2L)));
        assertTrue(op(PropertyValues.newLong(2L), "is not", PropertyValues.newLong(3L)));
    }

    // ---------- processOp: and / or (three-valued logic) ----------

    @Test
    public void processOp_and_determinate() {
        assertTrue(op(PropertyValues.newBoolean(true), "and", PropertyValues.newBoolean(true)));
        assertFalse(op(PropertyValues.newBoolean(true), "and", PropertyValues.newBoolean(false)));
        assertFalse(op(PropertyValues.newBoolean(false), "and", PropertyValues.newBoolean(true)));
        assertFalse(op(PropertyValues.newBoolean(false), "and", PropertyValues.newBoolean(false)));
    }

    @Test
    public void processOp_and_withMissingOperand() {
        // false and unknown -> false (determinate, regardless of the unknown side)
        assertFalse(op(PropertyValues.newBoolean(false), "and", null));
        assertFalse(op(null, "and", PropertyValues.newBoolean(false)));
        // true and unknown -> unknown (null)
        assertNull(FunctionIndexUtils.processOp(PropertyValues.newBoolean(true),
                PropertyValues.newString("and"), null));
        assertNull(FunctionIndexUtils.processOp(null,
                PropertyValues.newString("and"), PropertyValues.newBoolean(true)));
        // unknown and unknown -> unknown
        assertNull(FunctionIndexUtils.processOp(null, PropertyValues.newString("and"), null));
    }

    @Test
    public void processOp_or_determinate() {
        assertTrue(op(PropertyValues.newBoolean(true), "or", PropertyValues.newBoolean(true)));
        assertTrue(op(PropertyValues.newBoolean(true), "or", PropertyValues.newBoolean(false)));
        assertTrue(op(PropertyValues.newBoolean(false), "or", PropertyValues.newBoolean(true)));
        assertFalse(op(PropertyValues.newBoolean(false), "or", PropertyValues.newBoolean(false)));
    }

    @Test
    public void processOp_or_withMissingOperand() {
        // true or unknown -> true (determinate, regardless of the unknown side)
        assertTrue(op(PropertyValues.newBoolean(true), "or", null));
        assertTrue(op(null, "or", PropertyValues.newBoolean(true)));
        // false or unknown -> unknown (null)
        assertNull(FunctionIndexUtils.processOp(PropertyValues.newBoolean(false),
                PropertyValues.newString("or"), null));
        assertNull(FunctionIndexUtils.processOp(null,
                PropertyValues.newString("or"), PropertyValues.newBoolean(false)));
        // unknown or unknown -> unknown
        assertNull(FunctionIndexUtils.processOp(null, PropertyValues.newString("or"), null));
    }

    // ---------- processOp: math ----------

    @Test
    public void processOp_math() {
        assertEquals(3.0, FunctionIndexUtils.processOp(PropertyValues.newLong(1L),
                PropertyValues.newString("+"), PropertyValues.newLong(2L)).getValue(Type.DOUBLE), 0.0001);
        assertEquals(-1.0, FunctionIndexUtils.processOp(PropertyValues.newLong(1L),
                PropertyValues.newString("-"), PropertyValues.newLong(2L)).getValue(Type.DOUBLE), 0.0001);
        assertEquals(6.0, FunctionIndexUtils.processOp(PropertyValues.newLong(2L),
                PropertyValues.newString("*"), PropertyValues.newLong(3L)).getValue(Type.DOUBLE), 0.0001);
        assertEquals(2.0, FunctionIndexUtils.processOp(PropertyValues.newLong(4L),
                PropertyValues.newString("/"), PropertyValues.newLong(2L)).getValue(Type.DOUBLE), 0.0001);
    }

    @Test
    public void processOp_math_withMissingOrNonNumericOperandIsNull() {
        assertNull(FunctionIndexUtils.processOp(null, PropertyValues.newString("+"), PropertyValues.newLong(2L)));
        assertNull(FunctionIndexUtils.processOp(PropertyValues.newString("abc"),
                PropertyValues.newString("+"), PropertyValues.newLong(2L)));
    }

    @Test(expected = IllegalArgumentException.class)
    public void processOp_unknownOperator() {
        FunctionIndexUtils.processOp(PropertyValues.newLong(1L), PropertyValues.newString("%"), PropertyValues.newLong(2L));
    }

    private static boolean op(PropertyValue a, String operator, PropertyValue b) {
        PropertyValue result = FunctionIndexUtils.processOp(a, PropertyValues.newString(operator), b);
        return result.getValue(Type.BOOLEAN);
    }

    // ---------- toDoubleOrNull ----------

    @Test
    public void toDoubleOrNull_missing() {
        assertNull(FunctionIndexUtils.toDoubleOrNull(null));
    }

    @Test
    public void toDoubleOrNull_numericString() {
        assertEquals(3.5, FunctionIndexUtils.toDoubleOrNull(PropertyValues.newString("3.5")), 0.0001);
    }

    @Test
    public void toDoubleOrNull_nonNumericString() {
        assertNull(FunctionIndexUtils.toDoubleOrNull(PropertyValues.newString("abc")));
    }

    @Test
    public void toDoubleOrNull_longAndDouble() {
        assertEquals(2.0, FunctionIndexUtils.toDoubleOrNull(PropertyValues.newLong(2L)), 0.0001);
        assertEquals(2.5, FunctionIndexUtils.toDoubleOrNull(PropertyValues.newDouble(2.5)), 0.0001);
    }

}
