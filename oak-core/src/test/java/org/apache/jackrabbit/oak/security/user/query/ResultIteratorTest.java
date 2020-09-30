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
package org.apache.jackrabbit.oak.security.user.query;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

public class ResultIteratorTest {

    @Test(expected = IllegalArgumentException.class)
    public void createWithNegativeOffset() {
        ResultIterator.create(-1, ResultIterator.MAX_ALL, Collections.emptyIterator());
    }

    @Test
    public void testCreateWithoutLimitation() {
        Iterator<String> it = ImmutableList.of("str").iterator();
        assertSame(it, ResultIterator.create(ResultIterator.OFFSET_NONE, ResultIterator.MAX_ALL, it));
    }

    @Test
    public void testCreateMaxZero() {
        assertFalse(ResultIterator.create(ResultIterator.OFFSET_NONE, 0, Iterators.singletonIterator("str")).hasNext());
    }

    @Test
    public void testCreateOffsetEqualsSize() {
        assertFalse(ResultIterator.create(1, ResultIterator.MAX_ALL,  Iterators.singletonIterator("str")).hasNext());
    }

    @Test
    public void testCreateOffsetGtSize() {
        assertFalse(ResultIterator.create(2, ResultIterator.MAX_ALL,  Iterators.singletonIterator("str")).hasNext());
    }

    @Test
    public void testCreateOffsetLtSize() {
        assertEquals(1, Iterators.size(ResultIterator.create(1, ResultIterator.MAX_ALL,  ImmutableList.of("str", "str").iterator())));
    }

    @Test
    public void testCreateOffsetEqualsMax() {
        assertEquals(1, Iterators.size(ResultIterator.create(1, 1,  ImmutableList.of("str", "str").iterator())));
    }

    @Test
    public void testCreateOffsetGtMax() {
        assertEquals(1, Iterators.size(ResultIterator.create(2, 1,  ImmutableList.of("str", "str", "str").iterator())));
    }

    @Test
    public void testCreateOffsetLtMax() {
        Iterator resultIt = ResultIterator.create(1, 3,  ImmutableList.of("str", "str", "str", "str").iterator());
        assertEquals(3, Iterators.size(resultIt));
    }

    @Test(expected = NoSuchElementException.class)
    public void testNextNoElements() {
        Iterator<String> it = ResultIterator.create(1, ResultIterator.MAX_ALL,  Iterators.singletonIterator("str"));
        it.next();
    }

    @Test
    public void testNextWithOffset() {
        Iterator<String> it = ResultIterator.create(1, ResultIterator.MAX_ALL, ImmutableList.of("str", "str2").iterator());
        assertEquals("str2", it.next());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemove() {
        Iterator<String> it = ResultIterator.create(ResultIterator.OFFSET_NONE, 1, Iterators.singletonIterator("value"));
        it.remove();
    }
}