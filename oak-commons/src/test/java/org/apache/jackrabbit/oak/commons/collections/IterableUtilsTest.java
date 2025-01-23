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

import java.util.Collections;
import java.util.List;

/**
 * Unit tests for the {@link IterableUtils} class.
 * <p>
 * This class contains test cases to verify the functionality of the methods
 * in the {@link IterableUtils} class.
 */
public class IterableUtilsTest {

    @Test
    public void testIsEmptyWithEmptyList() {
        List<String> emptyList = Collections.emptyList();
        Assert.assertTrue(IterableUtils.isEmpty(emptyList));
    }

    @Test
    public void testIsEmptyWithNonEmptyList() {
        List<String> nonEmptyList = List.of("one", "two", "three");
        Assert.assertFalse(IterableUtils.isEmpty(nonEmptyList));
    }

    @Test
    public void testIsEmptyWithEmptyIterable() {
        Iterable<String> emptyIterable = Collections::emptyIterator;
        Assert.assertTrue(IterableUtils.isEmpty(emptyIterable));
    }

    @Test
    public void testIsEmptyWithNonEmptyIterable() {
        Iterable<String> nonEmptyIterable = List.of("one", "two", "three");
        Assert.assertFalse(IterableUtils.isEmpty(nonEmptyIterable));
    }

    @Test
    public void testIsEmptyWithNullIterable() {
        Assert.assertThrows(NullPointerException.class, () -> IterableUtils.isEmpty(null));
    }

}