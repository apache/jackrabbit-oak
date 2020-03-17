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
package org.apache.jackrabbit.oak.query;

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Test;

public class UtilsTest {

    @Test
    public void saturatedAdd() {
        assertEquals(3, QueryImpl.saturatedAdd(1, 2));
        assertEquals(Long.MAX_VALUE, QueryImpl.saturatedAdd(1, Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, QueryImpl.saturatedAdd(Long.MAX_VALUE, 1));
        assertEquals(Long.MAX_VALUE, QueryImpl.saturatedAdd(Long.MAX_VALUE, Long.MAX_VALUE));
        long[] test = {Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 10, 
            -1000, -10, -1, 0, 1, 3, 10000,
            Long.MAX_VALUE - 20, Long.MAX_VALUE - 1, Long.MAX_VALUE};
        Random r = new Random(1);
        for (int i = 0; i < 10000; i++) {
            long x = r.nextBoolean() ? test[r.nextInt(test.length)] : r
                    .nextLong();
            long y = r.nextBoolean() ? test[r.nextInt(test.length)] : r
                    .nextLong();
            long alt = altSaturatedAdd(x, y);
            long got = QueryImpl.saturatedAdd(x, y);
            assertEquals(x + "+" + y, alt, got);
        }
    }

    private static long altSaturatedAdd(long x, long y) {
        // see also http://stackoverflow.com/questions/2632392/saturated-addition-of-two-signed-java-long-values
        if (x > 0 != y > 0) {
            // different signs
            return x + y;
        } else if (x > 0) {
            // can overflow
            return Long.MAX_VALUE - x < y ? Long.MAX_VALUE : x + y;
        } else {
            // can underflow
            return Long.MIN_VALUE - x > y ? Long.MIN_VALUE : x + y;
        }
    }
}
