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
package org.apache.jackrabbit.oak.commons;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class LongUtilsTest {

    @Test
    public void tryParse() {
        assertEquals(-1, LongUtils.tryParse("-1").longValue());
        assertEquals(0, LongUtils.tryParse("0").longValue());
        assertEquals(1, LongUtils.tryParse("1").longValue());
        assertEquals(Long.MAX_VALUE, LongUtils.tryParse(String.valueOf(Long.MAX_VALUE)).longValue());
        assertEquals(Long.MIN_VALUE, LongUtils.tryParse(String.valueOf(Long.MIN_VALUE)).longValue());
        assertEquals(Integer.MIN_VALUE, LongUtils.tryParse(String.valueOf(Integer.MIN_VALUE)).longValue());
        assertEquals(Integer.MAX_VALUE, LongUtils.tryParse(String.valueOf(Integer.MAX_VALUE)).longValue());
        assertNull(LongUtils.tryParse("0.1"));
        assertNull(LongUtils.tryParse("1.1"));
        assertNull(LongUtils.tryParse("-1.1"));
        assertNull(LongUtils.tryParse("1.1e3"));
        assertNull(LongUtils.tryParse("foo"));
    }
}