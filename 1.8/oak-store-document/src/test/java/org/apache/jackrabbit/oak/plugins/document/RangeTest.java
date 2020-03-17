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
package org.apache.jackrabbit.oak.plugins.document;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeTest {

    @Test
    public void includes() {
        Revision lower = new Revision(0xff, 0, 1);
        Revision low = new Revision(0x100, 0, 1);
        Revision high = new Revision(0x300, 0, 1);
        Revision higher = new Revision(0x301, 0, 1);
        Revision r = new Revision(0x200, 0, 1);
        Revision other = new Revision(0x200, 0, 2);

        Range range = new Range(high, low, 0);

        // bounds are inclusive
        assertTrue(range.includes(low));
        assertTrue(range.includes(high));
        // within bounds
        assertTrue(range.includes(r));
        // outside bounds
        assertFalse(range.includes(lower));
        assertFalse(range.includes(higher));
        // other cluster id
        assertFalse(range.includes(other));
    }

    @Test
    public void parse() {
        Revision low = Revision.fromString("r1-0-1");
        Revision high = Revision.fromString("r2-0-1");
        Range r = new Range(high, low, 0);
        assertEquals("r1-0-1/0", r.getLowValue());
        assertEquals(r, Range.fromEntry(high, r.getLowValue()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidRange() throws Exception{
        Revision low = new Revision(0x100, 0, 1);
        Revision high = new Revision(0x300, 0, 1);

        Range range = new Range(low, high, 0);

    }
}
