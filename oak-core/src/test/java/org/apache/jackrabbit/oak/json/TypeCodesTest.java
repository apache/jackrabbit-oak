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

package org.apache.jackrabbit.oak.json;

import javax.jcr.PropertyType;

import org.apache.jackrabbit.oak.json.TypeCodes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TypeCodesTest {

    @Test
    public void testEncode() {
        assertEquals("str:foo", TypeCodes.encode(PropertyType.STRING, "foo"));
        assertEquals(":blobId:", TypeCodes.encode(PropertyType.BINARY, ""));
        assertEquals("und:foo", TypeCodes.encode(PropertyType.UNDEFINED, "foo"));
    }

    @Test
    public void testSplit() {
        assertEquals(3, TypeCodes.split("str:foo"));
        assertEquals(3, TypeCodes.split("str:"));
        assertEquals(-1, TypeCodes.split("foo"));
        assertEquals(-1, TypeCodes.split(""));
    }

    @Test
    public void testDecodeType() {
        String strFoo = "str:foo";
        assertEquals(PropertyType.STRING, TypeCodes.decodeType(TypeCodes.split(strFoo), strFoo));

        String str = "str:";
        assertEquals(PropertyType.STRING, TypeCodes.decodeType(TypeCodes.split(str), str));

        String any = "any";
        assertEquals(PropertyType.UNDEFINED, TypeCodes.decodeType(TypeCodes.split(any), any));

        String empty = "";
        assertEquals(PropertyType.UNDEFINED, TypeCodes.decodeType(TypeCodes.split(empty), empty));
    }

    @Test
    public void testDecodeName() {
        String strFoo = "str:foo";
        assertEquals("foo", TypeCodes.decodeName(TypeCodes.split(strFoo), strFoo));

        String str = "str:";
        assertEquals("", TypeCodes.decodeName(TypeCodes.split(str), str));

        String any = "any";
        assertEquals("any", TypeCodes.decodeName(TypeCodes.split(any), any));

        String empty = "";
        assertEquals("", TypeCodes.decodeName(TypeCodes.split(empty), empty));
    }
}
