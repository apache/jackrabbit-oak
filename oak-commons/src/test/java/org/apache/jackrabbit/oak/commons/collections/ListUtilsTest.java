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
import java.util.List;

import static org.junit.Assert.fail;

public class ListUtilsTest {

    final List<String> data = Arrays.asList("one", "two", "three", null);

    @Test
    public void iterableToList() {
        // create an iterable
        final Iterable<String> iterable = new SimpleIterable<>(data);

        Assert.assertEquals(data, ListUtils.toList(iterable));

    }

    @Test
    public void iterableToLinkedList() {
        // create an iterable
        final Iterable<String> iterable = new SimpleIterable<>(data);

        Assert.assertEquals(data, ListUtils.toLinkedList(iterable));
    }

    @Test
    public void iteratorToList() {
        // create an iterator
        final Iterable<String> iterable = new SimpleIterable<>(data);

        Assert.assertEquals(data, ListUtils.toList(iterable.iterator()));
    }

    @Test
    public void partitionList() {
        final List<String> list = List.of("a", "b", "c", "d", "e", "f", "g");
        final List<List<String>> partitions = ListUtils.partitionList(list, 3);
        Assert.assertEquals(3, partitions.size());
        Assert.assertEquals(List.of("a", "b", "c"), partitions.get(0));
        Assert.assertEquals(List.of("d", "e", "f"), partitions.get(1));
        Assert.assertEquals(List.of("g"), partitions.get(2));
    }

    @Test
    public void partitionListWhenListIsSmallerThanPartitionSize() {
        final List<String> list = List.of("a");
        final List<List<String>> partitions = ListUtils.partitionList(list, 3);
        Assert.assertEquals(1, partitions.size());
        Assert.assertEquals(List.of("a"), partitions.get(0));
    }

    @Test
    public void testReverseWithNonEmptyList() {
        List<String> list = List.of("a", "b", "c");
        List<String> result = ListUtils.reverse(list);
        List<String> expected = List.of("c", "b", "a");
        Assert.assertEquals(expected, result);
    }

    @Test
    public void testReverseWithEmptyList() {
        List<String> list = List.of();
        List<String> result = ListUtils.reverse(list);
        List<String> expected = List.of();
        Assert.assertEquals(expected, result);
    }

    @Test(expected = NullPointerException.class)
    public void testReverseWithNullList() {
        List<String> list = null;
        ListUtils.reverse(list);
        fail("Shouldn't reach here");
    }

    @Test
    public void testReverseWithSingleElementList() {
        List<String> list = List.of("a");
        List<String> result = ListUtils.reverse(list);
        List<String> expected = List.of("a");
        Assert.assertEquals(expected, result);
    }
}