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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Unit tests for the {@link IterableUtils} class.
 * <p>
 * This class contains test cases to verify the functionality of the methods
 * in the {@link IteratorUtils} class.
 */
public class IterableUtilsTest {


    @Test
    public void testAddAllWithNonEmptyIterable() {
        List<String> target = new ArrayList<>();
        Iterable<String> source = Arrays.asList("one", "two", "three");
        IterableUtils.addAll(target, source);
        Assert.assertEquals(Arrays.asList("one", "two", "three"), target);
    }

    @Test
    public void testAddAllWithEmptyIterable() {
        List<String> target = new ArrayList<>();
        Iterable<String> source = Collections.emptyList();
        IterableUtils.addAll(target, source);
        Assert.assertTrue(target.isEmpty());
    }

    @Test
    public void testAddAllWithNullCollection() {
        Iterable<String> source = Arrays.asList("one", "two", "three");
        Assert.assertThrows(NullPointerException.class, () -> IterableUtils.addAll(null, source));
    }

    @Test
    public void testAddAllWithNullIterable() {
        List<String> target = new ArrayList<>();
        Assert.assertThrows(NullPointerException.class, () -> IterableUtils.addAll(target, null));
    }

    @Test
    public void testAddAllWithCollectionAsIterable() {
        List<String> target = new ArrayList<>(List.of("zero"));
        List<String> source = Arrays.asList("one", "two", "three");
        IterableUtils.addAll(target, source);
        Assert.assertEquals(Arrays.asList("zero", "one", "two", "three"), target);
    }

}