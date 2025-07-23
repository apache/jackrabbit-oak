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

import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.commons.collections4.bloomfilter.BloomFilter;
import org.apache.commons.collections4.bloomfilter.EnhancedDoubleHasher;
import org.apache.commons.collections4.bloomfilter.Hasher;
import org.apache.commons.collections4.bloomfilter.Shape;
import org.apache.commons.collections4.bloomfilter.SimpleBloomFilter;
import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;

public class BloomFilterUtils {

    private BloomFilterUtils() {
        // no instances for you
    }

    /**
     * Creates a new Bloom filter with the specified expected entries and false positive probability.
     * <p>
     * The method constructs a properly configured Bloom filter by first calculating the optimal
     * filter shape (bit size and hash functions) based on the provided parameters, then instantiating
     * a SimpleBloomFilter with that shape.
     * <p>
     * The resulting Bloom filter provides efficient membership testing with a predictable
     * false positive rate.
     *
     * @param entries the expected number of entries to be inserted into the filter
     * @param fpp the desired false positive probability (between 0 and 1 exclusive)
     * @return a new empty BloomFilter instance configured with the specified parameters
     * @throws IllegalArgumentException if entries is less than 1 or if fpp is not between 0 and 1 exclusive
     */
    public static BloomFilter<SimpleBloomFilter> createFilter(final int entries, final double fpp) {
        final Shape shape = Shape.fromNP(entries, fpp);
        return new SimpleBloomFilter(shape);
    }


    /**
     * Creates a Bloom filter-compatible {@link Hasher} for the provided string value.
     *
     * @param value the string value to hash must not be null
     * @return a non-null {@link Hasher} implementation based on the hash of the input string
     * @throws NullPointerException if the input value is null
     */

    // This method generates a 128-bit MurmurHash3 hash from the UTF-8 representation
    // of the input string, then creates an {@link EnhancedDoubleHasher} using the
    // two 64-bit components of the hash. The resulting hasher can be used with Bloom
    // filters from the Apache Commons Collections.

    public static @NotNull Hasher hasher(final @NotNull String value) {
        final long[] hashed128 = MurmurHash3.hash128(value.getBytes(StandardCharsets.UTF_8));
        return new EnhancedDoubleHasher(hashed128[0], hashed128[1]);
    }


}
