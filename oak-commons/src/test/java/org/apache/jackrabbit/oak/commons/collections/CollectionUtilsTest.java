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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CollectionUtilsTest {

    final List<String> data = Arrays.asList("one", "two", "three", null);

    @Test
    public void iterableToList() {
        // create an iterable
        final Iterable<String> iterable = new SimpleIterable<>(data);

        Assert.assertEquals(data, CollectionUtils.toList(iterable));

    }

    @Test
    public void iteratorToList() {
        // create an iterator
        final Iterable<String> iterable = new SimpleIterable<>(data);

        Assert.assertEquals(data, CollectionUtils.toList(iterable.iterator()));
    }

    @Test
    public void iterableToSet() {
        // create an iterable
        final Set<String> s = new HashSet<>(data);
        final Iterable<String> iterable = new SimpleIterable<>(s);

        Assert.assertEquals(s, CollectionUtils.toSet(iterable));
    }

    @Test
    public void iteratorToSet() {
        // create an iterable
        final Set<String> s = new HashSet<>(data);
        final Iterable<String> iterable = new SimpleIterable<>(s);

        Assert.assertEquals(s, CollectionUtils.toSet(iterable.iterator()));
    }

    @Test
    public void arrayToSet() {
        final Set<String> s = CollectionUtils.toSet(data);
        Assert.assertEquals(s, CollectionUtils.toSet(data.toArray()));
    }
}