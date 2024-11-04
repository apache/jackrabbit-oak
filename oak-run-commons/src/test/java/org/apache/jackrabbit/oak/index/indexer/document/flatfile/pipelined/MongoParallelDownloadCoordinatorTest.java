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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongoParallelDownloadCoordinatorTest {

    @Test
    public void increaseLowerRangeOnly() {
        var m = new MongoParallelDownloadCoordinator();
        assertFalse(m.increaseLowerRange(1));
        assertFalse(m.increaseLowerRange(2));
        assertFalse(m.increaseLowerRange(10));
        // The updates can be out of order
        assertFalse(m.increaseLowerRange(5));
        assertFalse(m.increaseLowerRange(Integer.MAX_VALUE));
        assertFalse(m.increaseLowerRange(Long.MAX_VALUE));
    }


    @Test
    public void decreaseUpperRangeOnly() {
        var m = new MongoParallelDownloadCoordinator();
        assertFalse(m.decreaseUpperRange(Long.MAX_VALUE - 1));
        assertFalse(m.decreaseUpperRange(Integer.MAX_VALUE));
        assertFalse(m.decreaseUpperRange(10));
        // The updates can be out of order
        assertFalse(m.decreaseUpperRange(15));
        assertFalse(m.decreaseUpperRange(0));
    }

    @Test
    public void sameValuesShouldNotCross() {
        var m = new MongoParallelDownloadCoordinator();
        assertFalse(m.increaseLowerRange(10));
        assertFalse(m.decreaseUpperRange(10));
    }

    @Test
    public void crossByOne() {
        var m = new MongoParallelDownloadCoordinator();
        assertFalse(m.increaseLowerRange(10));
        assertTrue(m.decreaseUpperRange(9));
        // Once the ranges crossed, it must always return true
        assertTrue(m.decreaseUpperRange(11));
        assertTrue(m.increaseLowerRange(20));
    }
}
