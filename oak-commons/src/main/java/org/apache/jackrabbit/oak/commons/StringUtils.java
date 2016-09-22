/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.commons;

/**
 * Some string utility methods.
 */
public class StringUtils {

    private static final char[] HEX = "0123456789abcdef".toCharArray();

    /**
     * Convert a byte array to a hex encoded string.
     *
     * @param value the byte array
     * @return the hex encoded string
     */
    public static String convertBytesToHex(byte[] value) {
        int len = value.length;
        char[] buff = new char[len + len];
        char[] hex = HEX;
        for (int i = 0; i < len; i++) {
            int c = value[i] & 0xff;
            buff[i + i] = hex[c >> 4];
            buff[i + i + 1] = hex[c & 0xf];
        }
        return new String(buff);
    }

    /**
     * Convert a hex encoded string to a byte array.
     *
     * @param s the hex encoded string
     * @return the byte array
     */
    public static byte[] convertHexToBytes(String s) {
        int len = s.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException(s);
        }
        len /= 2;
        byte[] buff = new byte[len];
        for (int i = 0; i < len; i++) {
            buff[i] = (byte) ((getHexDigit(s, i + i) << 4) | getHexDigit(s, i + i + 1));
        }
        return buff;
    }

    /**
     * Convert the digit at the given position to a hex number.
     *
     * @param s the hex encoded string
     * @param i the index
     * @return the number
     */
    private static int getHexDigit(String s, int i) {
        char c = s.charAt(i);
        if (c >= '0' && c <= '9') {
            return c - '0';
        } else if (c >= 'a' && c <= 'f') {
            return c - 'a' + 0xa;
        } else if (c >= 'A' && c <= 'F') {
            return c - 'A' + 0xa;
        } else {
            throw new IllegalArgumentException(s);
        }
    }

    /**
     * Estimates the memory usage of the given string.
     *
     * @param s the string to estimate.
     * @return the estimated memory usage.
     */
    public static int estimateMemoryUsage(String s) {
        return s == null ? 0 : 48 + s.length() * 2;
    }
}
