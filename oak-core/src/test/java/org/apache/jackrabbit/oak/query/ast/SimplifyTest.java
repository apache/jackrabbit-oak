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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.junit.Test;

/**
 * Tests constraint simplification
 */
public class SimplifyTest {

    private static final StaticOperandImpl A =
            new LiteralImpl(PropertyValues.newString("A"));

    private static final StaticOperandImpl B =
            new LiteralImpl(PropertyValues.newString("B"));

    private static final StaticOperandImpl C =
            new LiteralImpl(PropertyValues.newString("C"));

    private static final PropertyValueImpl foo =
            new PropertyValueImpl("a", "foo");

    private static final ComparisonImpl fooIsA =
            new ComparisonImpl(foo, Operator.EQUAL, A);

    private static final ComparisonImpl fooIsB =
            new ComparisonImpl(foo, Operator.EQUAL, B);

    private static final ComparisonImpl fooIsC =
            new ComparisonImpl(foo, Operator.EQUAL, C);

    private static final InImpl fooInAB = new InImpl(foo, Arrays.asList(A, B));

    private static final InImpl fooInBC = new InImpl(foo, Arrays.asList(B, C));

    private static final InImpl fooInABC = new InImpl(foo, Arrays.asList(A, B, C));

    @Test
    public void simplifyIn() {
        assertEquals(fooIsA, new InImpl(foo, Arrays.asList(A)).simplify());
        assertEquals(fooIsA, new InImpl(foo, Arrays.asList(A, A)).simplify());
        assertEquals(fooInAB, new InImpl(foo, Arrays.asList(A, A, B)).simplify());
    }

    @Test
    public void simplifyOr() {
        assertEquals(fooIsA, new OrImpl(fooIsA, fooIsA).simplify());
        assertEquals(fooInAB, new OrImpl(fooIsA, fooIsB).simplify());
        assertEquals(fooInBC, new OrImpl(fooIsB, fooIsC).simplify());
        assertEquals(fooInABC, new OrImpl(fooInAB, fooIsC).simplify());
        assertEquals(fooInABC, new OrImpl(fooIsA, fooInBC).simplify());
        assertEquals(fooInABC, new OrImpl(fooInAB, fooInBC).simplify());
        assertEquals(fooInABC, new OrImpl(fooIsA, fooInABC).simplify());
        assertEquals(fooInABC, new OrImpl(fooIsA, new OrImpl(fooIsB, fooIsC)).simplify());
    }

}
