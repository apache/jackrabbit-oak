/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.commons.collections;

import org.junit.Assert;
import org.junit.Test;
import java.util.NoSuchElementException;

public class AbstractIteratorTest {

    @Test
    public void testIteration() {
        AbstractIterator<Integer> it = new RangeIterator(0, 3);
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals(Integer.valueOf(0), it.next());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals(Integer.valueOf(1), it.next());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals(Integer.valueOf(2), it.next());
        Assert.assertFalse(it.hasNext());
    }

    @Test
    public void testNoSuchElementException() {
        AbstractIterator<Integer> it = new RangeIterator(0, 1);
        it.next(); // 0
        Assert.assertThrows(NoSuchElementException.class, it::next); // should throw
    }

    @Test
    public void testHasNextDoesNotAdvance() {
        AbstractIterator<Integer> it = new RangeIterator(0, 2);
        Assert.assertTrue(it.hasNext());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals(Integer.valueOf(0), it.next());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals(Integer.valueOf(1), it.next());
        Assert.assertFalse(it.hasNext());
    }

    // Simple concrete implementation for testing
    private static class RangeIterator extends AbstractIterator<Integer> {
        private int current;
        private final int end;
        public RangeIterator(int start, int end) { this.current = start; this.end = end; }

        @Override
        protected Integer computeNext() {
            if (current < end) return current++;
            return endOfData();
        }
    }
}