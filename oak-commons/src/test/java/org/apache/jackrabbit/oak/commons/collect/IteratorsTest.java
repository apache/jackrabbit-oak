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
package org.apache.jackrabbit.oak.commons.collect;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IteratorsTest {
    
    @Test
    public void testRemove() {
        Iterator<String> it = Iterators.concat(Collections.singletonList("a").iterator(), Collections.singletonList("b").iterator());
        while (it.hasNext()) {
            it.next();
            try {
                it.remove();
                fail("Iterator.remove is not supported");
            } catch (UnsupportedOperationException e) {
                // success
            }
        }
    }
    
    @Test
    public void testZeroIterator() {
        assertSame(Collections.emptyIterator(), Iterators.concat());
    }

    @Test
    public void testSingleIterator() {
        Iterator<String> it = Collections.singletonList("a").iterator();
        assertSame(it, Iterators.concat(it));
    }

    @Test
    public void testTwoIterators() {
        Iterator<String> it1 = Collections.singletonList("a").iterator();
        Iterator<String> it2 = Collections.singletonList("b").iterator();
        
        assertEquals(ImmutableList.of("a", "b"), ImmutableList.copyOf(Iterators.concat(it1, it2)));
    }

    @Test
    public void testRepeatedHasNext() {
        Iterator<String> it1 = Collections.singletonList("a").iterator();
        Iterator<String> it2 = Collections.singletonList("b").iterator();
        Iterator<String> it3 = Collections.singletonList("c").iterator();
        
        Iterator<String> it = Iterators.concat(it1, it2, it3);
        List<String> result = new ArrayList<>();
        while (it.hasNext()) {
            assertTrue(it.hasNext());
            String s = it.next();
            assertNotNull(s);
            result.add(s);
        }
        assertEquals(Lists.newArrayList("a", "b", "c"), result);
    }
}