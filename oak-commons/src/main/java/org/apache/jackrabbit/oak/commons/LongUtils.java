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
package org.apache.jackrabbit.oak.commons;

import java.util.Arrays;
import java.util.Date;

public final class LongUtils {

    private LongUtils() {}

    /**
     * Sums {@code a} and {@code b} and verifies that it doesn't overflow in
     * signed long arithmetic, in which case {@link Long#MAX_VALUE} will be
     * returned instead of the result.
     *
     * Note: this method is a variant of {@link org.apache.jackrabbit.guava.common.math.LongMath#checkedAdd(long, long)}
     * that returns {@link Long#MAX_VALUE} instead of throwing {@code ArithmeticException}.
     *
     * @see org.apache.jackrabbit.guava.common.math.LongMath#checkedAdd(long, long)
     */
    public static long safeAdd(long a, long b) {
        long result = a + b;
        if ((a ^ b) < 0 | (a ^ result) >= 0) {
            return result;
        } else {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Calculate an expiration time based on {@code new Date().getTime()} and
     * the specified {@code expiration} in number of milliseconds.
     *
     * @param expiration The expiration in milliseconds.
     * @return The expiration time.
     */
    public static long calculateExpirationTime(long expiration) {
        return LongUtils.safeAdd(expiration, new Date().getTime());
    }

    /*
     * Taken from https://github.com/google/guava/blob/0c33dd12b193402cdf6962d43d69743521aa2f76/guava/src/com/google/common/primitives/Longs.java#L334
     */
    private static final class AsciiDigits {
        private AsciiDigits() {
        }

        private static final byte[] asciiDigits;

        static {
            byte[] result = new byte[128];
            Arrays.fill(result, (byte) -1);
            for (int i = 0; i < 10; i++) {
                result['0' + i] = (byte) i;
            }
            for (int i = 0; i < 26; i++) {
                result['A' + i] = (byte) (10 + i);
                result['a' + i] = (byte) (10 + i);
            }
            asciiDigits = result;
        }

        static int digit(char c) {
            return (c < 128) ? asciiDigits[c] : -1;
        }
    }

    private static final int RADIX = 10;

    /**
     * Parses the specified string as a signed decimal long value. Unlike {@link Long#parseLong(String)}, this method
     * returns {@code null} instead of throwing an exception if parsing fails. This can be significantly more efficient
     * when the string to be parsed is often invalid, as raising an exception is an expensive operation.
     * <p>
     * This is a simplified version of
     * <a href="https://github.com/google/guava/blob/0c33dd12b193402cdf6962d43d69743521aa2f76/guava/src/com/google/common/primitives/Longs.java#L400">Longs.tryParse()</a>
     * in Guava. This version is hardcoded to only support radix 10.
     * <p>
     *
     * @see org.apache.jackrabbit.guava.common.primitives.Longs#tryParse(String)
     */
    public static Long tryParse(String string) {
        if (string == null || string.isEmpty()) {
            return null;
        }
        boolean negative = string.charAt(0) == '-';
        int index = negative ? 1 : 0;
        if (index == string.length()) {
            return null;
        }
        int digit = AsciiDigits.digit(string.charAt(index++));
        if (digit < 0 || digit >= RADIX) {
            return null;
        }
        long accum = -digit;

        long cap = Long.MIN_VALUE / RADIX;

        while (index < string.length()) {
            digit = AsciiDigits.digit(string.charAt(index++));
            if (digit < 0 || digit >= RADIX || accum < cap) {
                return null;
            }
            accum *= RADIX;
            if (accum < Long.MIN_VALUE + digit) {
                return null;
            }
            accum -= digit;
        }

        if (negative) {
            return accum;
        } else if (accum == Long.MIN_VALUE) {
            return null;
        } else {
            return -accum;
        }
    }

}
