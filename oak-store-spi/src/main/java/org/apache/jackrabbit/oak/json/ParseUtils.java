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
package org.apache.jackrabbit.oak.json;

import java.util.Arrays;

public class ParseUtils {
    /*
     * Taken from https://github.com/google/guava/blob/v33.2.1/guava/src/com/google/common/primitives/Longs.java
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

    /*
     * Simplified version of https://github.com/google/guava/blob/v33.2.1/guava/src/com/google/common/primitives/Longs.java
     * This version is hardcoded to support only radix 10
     *
     * Compared to Long.parseLong(), this version does not throw an exception when it fails to parse the value, instead
     * it returns null. This is more efficient when the input is expected to often be invalid.
     */
    public static Long tryParse(String string) {
        if (string == null) {
            return null;
        }
        int radix = 10;
        boolean negative = string.charAt(0) == '-';
        int index = negative ? 1 : 0;
        if (index == string.length()) {
            return null;
        }
        int digit = AsciiDigits.digit(string.charAt(index++));
        if (digit < 0 || digit >= radix) {
            return null;
        }
        long accum = -digit;

        long cap = Long.MIN_VALUE / radix;

        while (index < string.length()) {
            digit = AsciiDigits.digit(string.charAt(index++));
            if (digit < 0 || digit >= radix || accum < cap) {
                return null;
            }
            accum *= radix;
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
