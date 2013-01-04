/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.model;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class IdTest {

    @Test
    public void idCompareToEqualsTest() {
        final Id id1 = Id.fromString("00000000000002cc");
        final Id id2 = Id.fromString("00000000000002cc");

        assertTrue(id1 + " should be equals to " + id2, id1.compareTo(id2) == 0);
    }

    @Test
    public void idCompareToGreaterThanOneDigitTest() {
        final Id id1 = Id.fromString("0000000000000007");
        final Id id2 = Id.fromString("000000000000000c");

        assertTrue(id1 + " should be less than " + id2, id1.compareTo(id2) < 0);
    }

    @Test
    public void idCompareToGreaterThanOneByteTest() {
        final Id id1 = Id.fromString("0000000000000070");
        final Id id2 = Id.fromString("00000000000000c0");

        assertTrue(id1 + " should be less than " + id2, id1.compareTo(id2) < 0);
    }

    @Test
    public void idCompareToGreaterThanTwoBytesTest() {
        final Id id1 = Id.fromString("0000000000000270");
        final Id id2 = Id.fromString("00000000000002c0");

        assertTrue(id1 + " should be less than " + id2, id1.compareTo(id2) < 0);
    }
}