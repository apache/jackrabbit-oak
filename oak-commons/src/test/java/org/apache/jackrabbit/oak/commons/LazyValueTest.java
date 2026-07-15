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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LazyValueTest {

    private static LazyValue<String> create() {
        return new LazyValue<String>() {
            @Override
            protected String createValue() {
                return "lazyValue";
            }
        };
    }

    @Test
    public void testHasValue() {
        LazyValue<String> lazyValue = create();
        assertFalse(lazyValue.hasValue());

        lazyValue.get();
        assertTrue(lazyValue.hasValue());
    }

    @Test
    public void testGet() {
        LazyValue<String> lazyValue = create();

        String get1 = lazyValue.get();
        String get2 = lazyValue.get();

        assertSame(get1, get2);
    }
}