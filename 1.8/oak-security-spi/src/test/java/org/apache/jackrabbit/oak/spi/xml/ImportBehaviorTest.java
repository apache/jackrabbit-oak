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
package org.apache.jackrabbit.oak.spi.xml;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ImportBehaviorTest {

    @Test
    public void testValueFromString() {
       assertEquals(ImportBehavior.ABORT, ImportBehavior.valueFromString(ImportBehavior.NAME_ABORT));
       assertEquals(ImportBehavior.IGNORE, ImportBehavior.valueFromString(ImportBehavior.NAME_IGNORE));
       assertEquals(ImportBehavior.BESTEFFORT, ImportBehavior.valueFromString(ImportBehavior.NAME_BESTEFFORT));
       assertEquals(ImportBehavior.ABORT, ImportBehavior.valueFromString("invalid"));
    }


    @Test
    public void testNameFromValue() {
        assertEquals(ImportBehavior.NAME_ABORT, ImportBehavior.nameFromValue(ImportBehavior.ABORT));
        assertEquals(ImportBehavior.NAME_IGNORE, ImportBehavior.nameFromValue(ImportBehavior.IGNORE));
        assertEquals(ImportBehavior.NAME_BESTEFFORT, ImportBehavior.nameFromValue(ImportBehavior.BESTEFFORT));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNameFromInvalidValue() {
        ImportBehavior.nameFromValue(Integer.MAX_VALUE);
    }
}