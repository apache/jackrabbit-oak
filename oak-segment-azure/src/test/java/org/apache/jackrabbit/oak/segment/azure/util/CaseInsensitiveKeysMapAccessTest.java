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
package org.apache.jackrabbit.oak.segment.azure.util;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class CaseInsensitiveKeysMapAccessTest {

    @Test
    public void convert() {
        Map<String, String> map = CaseInsensitiveKeysMapAccess.convert(
            Collections.singletonMap("hello", "world"));

        assertEquals("world", map.get("hello"));
        assertEquals("world", map.get("Hello"));
        assertEquals("world", map.get("hELLO"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void assertImmutable() {
        Map<String, String> map = CaseInsensitiveKeysMapAccess.convert(
            Collections.singletonMap("hello", "world"));
        map.put("foo", "bar");
    }
}
