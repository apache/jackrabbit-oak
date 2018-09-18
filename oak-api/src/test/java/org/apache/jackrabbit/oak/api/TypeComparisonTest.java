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

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TypeComparisonTest {

    @Test
    public void compareTypes() {
        Type[] types = new Type[]{
                Type.BINARIES,
                Type.BINARY,
                Type.BOOLEANS,
                Type.BOOLEAN,
                Type.DATES,
                Type.DATE,
                Type.DECIMALS,
                Type.DECIMAL,
                Type.DOUBLES,
                Type.DOUBLE,
                Type.LONGS,
                Type.LONG,
                Type.NAMES,
                Type.NAME,
                Type.PATHS,
                Type.PATH,
                Type.REFERENCES,
                Type.REFERENCE,
                Type.STRINGS,
                Type.STRING,
                Type.UNDEFINEDS,
                Type.UNDEFINED,
                Type.URIS,
                Type.URI,
                Type.WEAKREFERENCES,
                Type.WEAKREFERENCE,

        };

        Arrays.sort(types);

        for (int i = 0; i < types.length; i++) {
            for (int j = 0; j < types.length; j++) {
                if (i < j) {
                    assertTypeLessThan(types[i], types[j]);
                }

                if (j < i) {
                    assertTypeLessThan(types[j], types[i]);
                }

                if (i == j) {
                    assertTypeEqual(types[i], types[j]);
                }
            }
        }
    }

    private void assertTypeLessThan(Type<?> a, Type<?> b) {
        assertTrue(a.compareTo(b) < 0);
        assertTrue(b.compareTo(a) > 0);

        if (a.tag() > b.tag()) {
            fail("Types should be ordered by increasing tag value");
        }

        if (b.tag() == a.tag() && a.isArray() && !b.isArray()) {
            fail("If their tag is the same, types should be ordered by multiplicity");
        }
    }

    private void assertTypeEqual(Type<?> a, Type<?> b) {
        assertTrue(a.compareTo(b) == 0);
        assertTrue(a.tag() == b.tag());
        assertTrue(a.isArray() == b.isArray());
    }

}
