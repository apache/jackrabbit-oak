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

import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class BloomFilterUtilsTest {

    @Test
    public void testCreateFilterWithValidParameters() {
        int entries = 1000;
        double fpp = 0.01;
        BloomFilter<SimpleBloomFilter> filter = BloomFilterUtils.createFilter(entries, fpp);

        Assert.assertNotNull(filter);
        Assert.assertTrue(filter instanceof SimpleBloomFilter);
    }

    @Test
    public void testFilterFunctionality() {
        BloomFilter<SimpleBloomFilter> filter = BloomFilterUtils.createFilter(100, 0.01);
        String testValue = "test-value";

        // Initially should not contain anything
        Assert.assertFalse(filter.contains(BloomFilterUtils.hasher(testValue)));

        // Add the item and verify it's found
        filter.merge(BloomFilterUtils.hasher(testValue));
        Assert.assertTrue(filter.contains(BloomFilterUtils.hasher(testValue)));

        // Verify another value is not found
        Assert.assertFalse(filter.contains(BloomFilterUtils.hasher("different-value")));
    }

    @Test
    public void testFilterWithMultipleEntries() {
        BloomFilter<SimpleBloomFilter> filter = BloomFilterUtils.createFilter(1000, 0.01);

        // Add multiple entries
        for (int i = 0; i < 100; i++) {
            filter.merge(BloomFilterUtils.hasher("value-" + i));
        }

        // Verify all entries are found
        for (int i = 0; i < 100; i++) {
            Assert.assertTrue(filter.contains(BloomFilterUtils.hasher("value-" + i)));
        }
    }

    @Test
    public void testFalsePositiveProbability() {
        // Create a filter with high false positive probability for testing
        double fpp = 0.3;
        BloomFilter<SimpleBloomFilter> filter = BloomFilterUtils.createFilter(100, fpp);

        // Fill the filter to capacity
        for (int i = 0; i < 100; i++) {
            filter.merge(BloomFilterUtils.hasher("existing-" + i));
        }

        // Test with values not in the filter
        int falsePositives = 0;
        int trials = 1000;

        for (int i = 0; i < trials; i++) {
            if (filter.contains(BloomFilterUtils.hasher("nonexistent-" + i))) {
                falsePositives++;
            }
        }

        // The false positive rate should be approximately fpp
        double actualFpp = (double) falsePositives / trials;
        Assert.assertTrue("False positive rate should be close to expected", Math.abs(actualFpp - fpp) < 0.15);
    }

    @Test
    public void testInvalidEntries() {
        // Should throw exception for entries < 1
        Assert.assertThrows(IllegalArgumentException.class,() -> BloomFilterUtils.createFilter(0, 0.01));
    }

    @Test
    public void testInvalidFppZero() {
        // Should throw exception for fpp <= 0
        Assert.assertThrows(IllegalArgumentException.class,() -> BloomFilterUtils.createFilter(100, 0.0));
    }

    @Test
    public void testInvalidFppOne() {
        // Should throw exception for fpp >= 1
        Assert.assertThrows(IllegalArgumentException.class,() -> BloomFilterUtils.createFilter(100, 1.0));
    }

    @Test
    public void testHasherWithNormalString() {
        // Create a hasher from a string
        Hasher hasher = BloomFilterUtils.hasher("test string");

        // Verify the hasher is not null and correct type
        Assert.assertNotNull(hasher);
        Assert.assertTrue(hasher instanceof EnhancedDoubleHasher);
    }

    @Test
    public void testHasherWithEmptyString() {
        // Empty strings should also work
        Hasher hasher = BloomFilterUtils.hasher("");

        Assert.assertNotNull(hasher);
        Assert.assertTrue(hasher instanceof EnhancedDoubleHasher);
    }

    @Test
    public void testConsistentHashing() throws ReflectiveOperationException {
        // Create two hashers from the same string
        Hasher hasher1 = BloomFilterUtils.hasher("consistent");
        Hasher hasher2 = BloomFilterUtils.hasher("consistent");

        // Same string should produce identical hash values
        Assert.assertTrue(compareHasherContents(hasher1, hasher2));
    }

    @Test
    public void testDifferentStringsProduceDifferentHashes() throws ReflectiveOperationException {
        Hasher hasher1 = BloomFilterUtils.hasher("string1");
        Hasher hasher2 = BloomFilterUtils.hasher("string2");

        // Different strings should produce different hashers
        Assert.assertFalse(compareHasherContents(hasher1, hasher2));
    }

    @Test
    public void testHasherWithNullThrowsNPE() {
        // Method should throw NullPointerException when given null
        Assert.assertThrows(NullPointerException.class, () -> BloomFilterUtils.hasher(null));
    }

    @Test
    public void testHasherWithSpecialCharacters() {
        // Special characters should be handled properly
        Hasher hasher = BloomFilterUtils.hasher("!@#$%^&*()_+");
        Assert.assertNotNull(hasher);
    }

    // util mehtods
    /**
     * Compares two EnhancedDoubleHasher instances by examining their internal hash values.
     *
     * @param h1 first Hasher to compare, must be EnhancedDoubleHasher
     * @param h2 second Hasher to compare, must be EnhancedDoubleHasher
     * @return true if both hashers have identical internal hash values
     * @throws IllegalArgumentException if either hasher is not EnhancedDoubleHasher
     * @throws ReflectiveOperationException if reflection fails
     */
    private boolean compareHasherContents(final Hasher h1, final Hasher h2)
            throws ReflectiveOperationException {

        if (!(h1 instanceof EnhancedDoubleHasher) ||
                !(h2 instanceof EnhancedDoubleHasher)) {
            throw new IllegalArgumentException("Both hashers must be EnhancedDoubleHasher instances");
        }

        // Use reflection to access the private fields
        Field value1Field = EnhancedDoubleHasher.class.getDeclaredField("initial");
        Field value2Field = EnhancedDoubleHasher.class.getDeclaredField("increment");

        value1Field.setAccessible(true);
        value2Field.setAccessible(true);

        long value1A = (long) value1Field.get(h1);
        long value2A = (long) value2Field.get(h1);
        long value1B = (long) value1Field.get(h2);
        long value2B = (long) value2Field.get(h2);

        return (value1A == value1B) && (value2A == value2B);
    }

}