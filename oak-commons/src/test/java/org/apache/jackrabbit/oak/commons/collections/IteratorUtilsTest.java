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

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.fail;

public class IteratorUtilsTest {

    @Test
    public void iteratorToIIterable() {
        Iterator<String> iterator = List.of("a", "b", "c").iterator();
        iterator.next();
        Iterable<String> iterable = IteratorUtils.toIterable(iterator);
        Iterator<String> testit = iterable.iterator();
        Assert.assertEquals("b", testit.next());
        Assert.assertEquals("c", testit.next());
        Assert.assertFalse(testit.hasNext());
        try {
            iterable.iterator();
            fail("should only work once");
        } catch (IllegalStateException expected) {
            // that's what we want
        }
    }
}
