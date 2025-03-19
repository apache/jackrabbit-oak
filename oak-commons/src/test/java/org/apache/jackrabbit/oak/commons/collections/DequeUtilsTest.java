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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Unit tests for the {@link DequeUtils} class.
 * <p>
 * This class contains test cases to verify the functionality of the methods
 * in the {@link DequeUtils} class.
 */
public class DequeUtilsTest {

    @Test
    public void toArrayDequeWithNonEmptyIterable() {
        List<String> list = Arrays.asList("one", "two", "three");
        ArrayDeque<String> result = DequeUtils.toArrayDeque(list);

        Assert.assertNotNull(result);
        Assert.assertEquals(3, result.size());
        Assert.assertEquals("one", result.peekFirst());
        Assert.assertEquals("three", result.peekLast());
    }

    @Test
    public void toArrayDequeWithEmptyIterable() {
        List<String> emptyList = Collections.emptyList();
        ArrayDeque<String> result = DequeUtils.toArrayDeque(emptyList);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.isEmpty());
    }

    @Test
    public void testToArrayDequeWithNullIterable() {
        Assert.assertThrows(NullPointerException.class, () -> DequeUtils.toArrayDeque(null));
    }

}