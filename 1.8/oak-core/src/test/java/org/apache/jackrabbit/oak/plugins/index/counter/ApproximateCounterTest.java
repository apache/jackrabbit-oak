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
package org.apache.jackrabbit.oak.plugins.index.counter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.jackrabbit.oak.plugins.index.counter.ApproximateCounter;
import org.junit.Test;

public class ApproximateCounterTest {

    @Test
    public void veryHighResolution() {
        for (int i = -100; i < 100; i++) {
            assertEquals(i, ApproximateCounter.calculateOffset(i, -1));
            assertEquals(i, ApproximateCounter.calculateOffset(i, 0));
            assertEquals(i, ApproximateCounter.calculateOffset(i, 1));
        }
    }
    
    @Test
    public void regularResolution() {
        ApproximateCounter.setSeed(0);
        long result = 0;
        long count = 100000;
        int nonZero = 0;
        for (long i = 0; i < count; i++) {
            long offset = ApproximateCounter.calculateOffset(1, 1000);
            if (offset != 0) {
                nonZero++;
                result += offset; 
            }
        }
        // most of the time, 0 needs to be returned
        assertTrue(nonZero < count / 500);
        // the expected result is within a certain range
        assertTrue(Math.abs(result - count) < count / 10);
    }
    
    @Test
    public void addRemove() {
        ApproximateCounter.setSeed(0);
        Random r = new Random(1);
        long result = 0;
        long exactResult = 0;
        long count = 100000;
        long sumChange = 0;
        int nonZero = 0;
        for (long i = 0; i < count; i++) {
            int o = r.nextInt(20) - 10;
            exactResult += o;
            sumChange += Math.abs(o);
            long offset = ApproximateCounter.calculateOffset(o, 1000);
            if (offset != 0) {
                nonZero++;
                result += offset; 
            }
        }
        // most of the time, 0 needs to be returned
        assertTrue(nonZero < count / 50);
        // the expected result is within a certain range
        assertTrue(Math.abs(result - exactResult) < sumChange / 10);
    }
    
    @Test
    public void lowResolution() {
        ApproximateCounter.setSeed(0);
        long result = 0;
        long count = 100000;
        int nonZero = 0;
        for (long i = 0; i < count; i++) {
            long offset = ApproximateCounter.calculateOffset(1, 100);
            if (offset != 0) {
                offset = ApproximateCounter.adjustOffset(result, offset, 100);
            }
            if (offset != 0) {
                nonZero++;
                result += offset; 
            }
        }
        // most of the time, 0 needs to be returned
        assertTrue(nonZero < count / 500);
        // the expected result is within a certain range
        assertTrue(Math.abs(result - count) < count / 10);
    }
    
    @Test
    public void keepAboveZero() {
        // adjustOffset ensures that the resulting count is larger or equal to 0
        assertEquals(1234, ApproximateCounter.adjustOffset(-1234, -100, 10));
    }
    
    @Test
    public void highResolutionAdjust() {
        // adjustOffset with resolution of 1 should not affect the result
        for (int i = 0; i < 10; i++) {
            assertEquals(123, ApproximateCounter.adjustOffset(i, 123, 1));
        }
    }
    
}
