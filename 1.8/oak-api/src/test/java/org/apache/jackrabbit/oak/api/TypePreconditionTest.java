/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.api;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TypePreconditionTest {

    @Parameters(name = "{0}/{1}")
    public static Object[][] getTypes() {
        return new Object[][]{
                {Type.BINARIES, Type.BINARY},
                {Type.BOOLEANS, Type.BOOLEAN},
                {Type.DATES, Type.DATE},
                {Type.DECIMALS, Type.DECIMAL},
                {Type.DOUBLES, Type.DOUBLE},
                {Type.LONGS, Type.LONG},
                {Type.NAMES, Type.NAME},
                {Type.PATHS, Type.PATH},
                {Type.REFERENCES, Type.REFERENCE},
                {Type.STRINGS, Type.STRING},
                {Type.UNDEFINEDS, Type.UNDEFINED},
                {Type.URIS, Type.URI},
                {Type.WEAKREFERENCES, Type.WEAKREFERENCE}
        };
    }

    private final Type multi;

    private final Type single;

    public TypePreconditionTest(Type multi, Type single) {
        this.multi = multi;
        this.single = single;
    }

    @Test
    public void testBaseTypeOnMultiValueType() {
        assertEquals(single, multi.getBaseType());
    }

    @Test(expected = IllegalStateException.class)
    public void testBaseTypeOnSingleValueType() {
        single.getBaseType();
    }

    @Test(expected = IllegalStateException.class)
    public void testArrayTypeOnMultiValueType() {
        multi.getArrayType();
    }

    @Test
    public void testArrayTypeOnSingleValueType() {
        assertEquals(multi, single.getArrayType());
    }

}
