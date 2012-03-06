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
package org.apache.jackrabbit.mk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.math.BigInteger;
import java.util.HashSet;
import java.util.Random;
import org.junit.Test;

/**
 * Test the bloom filter utility class.
 */
public class BloomFilterUtilsTest {

    /**
     * Program to calculate the best shift and multiply constants.
     */
    public static void main(String... args) {
        Random random = new Random(1);
        HashSet<String> inSet = new HashSet<String>();
        while (inSet.size() < 100) {
            inSet.add(randomString(random));
        }
        Object[] in = inSet.toArray();
        HashSet<String> notSet = new HashSet<String>();
        while (notSet.size() < 10000) {
            String k = randomString(random);
            if (!inSet.contains(k)) {
                notSet.add(k);
            }
        }
        Object[] not = notSet.toArray();
        int best = Integer.MAX_VALUE;
        for (int mul = 1; mul < 100000; mul += 2) {
            if (!BigInteger.valueOf(mul).isProbablePrime(10)) {
                continue;
            }
            for (int shift = 0; shift < 32; shift++) {
                byte[] bloom = BloomFilterUtils.createFilter(100, 64);
                for (Object k : in) {
                    int h1 = hash(k.hashCode(), mul, shift), h2 = hash(h1, mul, shift);
                    add(bloom, h1, h2);
                }
                int falsePositives = 0;
                for (Object k : not) {
                    int h1 = hash(k.hashCode(), mul, shift), h2 = hash(h1, mul, shift);
                    if (probablyContains(bloom, h1, h2)) {
                        falsePositives++;
                        // short false positives are bad
                        if (k.toString().length() < 4) {
                            falsePositives += 5;
                        }
                        if (falsePositives > best) {
                            break;
                        }
                    }
                }
                if (falsePositives < best) {
                    best = falsePositives;
                    System.out.println("mul: " + mul + " shift: "
                            + shift + " falsePositives: " + best);
                }
            }
        }
    }

    private static String randomString(Random r) {
        if (r.nextInt(5) == 0) {
            return randomName(r);
        }
        int length = 1 + Math.abs((int) r.nextGaussian() * 5);
        if (r.nextBoolean()) {
            length += r.nextInt(10);
        }
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = randomChar(r);
        }
        return new String(chars);
    }

    private static char randomChar(Random r) {
        switch (r.nextInt(101) / 100) {
        case 0:
        case 1:
            // 20% ascii
            return (char) (32 + r.nextInt(127 - 32));
        case 2:
        case 3:
        case 4:
        case 5:
            // 40% a-z
            return (char) ('a' + r.nextInt('z' - 'a'));
        case 6:
            // 10% A-Z
            return (char) ('A' + r.nextInt('Z' - 'A'));
        case 7:
        case 8:
            // 20% 0-9
            return (char) ('0' + r.nextInt('9' - '0'));
        case 9:
            // 10% aeiou
            return "aeiou".charAt(r.nextInt("aeiou".length()));
        }
        // 1% unicode
        return (char) r.nextInt(65535);
    }

    private static String randomName(Random r) {
        int i = r.nextInt(1000);
        // like TPC-C lastName, but lowercase
        String[] n = {
                "bar", "ought", "able", "pri", "pres", "ese", "anti",
                "cally", "ation", "eing" };
        StringBuilder buff = new StringBuilder();
        buff.append(n[i / 100]);
        buff.append(n[(i / 10) % 10]);
        buff.append(n[i % 10]);
        return buff.toString();
    }


    private static int hash(int oldHash, int mul, int shift) {
        return oldHash ^ ((oldHash * mul) >> shift);
    }

    private static void add(byte[] bloom, int h1, int h2) {
        int len = bloom.length;
        if (len > 0) {
            bloom[(h1 >>> 3) % len] |= 1 << (h1 & 7);
            bloom[(h2 >>> 3) % len] |= 1 << (h2 & 7);
        }
    }

    private static boolean probablyContains(byte[] bloom, int h1, int h2) {
        int len = bloom.length;
        if (len == 0) {
            return true;
        }
        int x = bloom[(h1 >>> 3) % len] & (1 << (h1 & 7));
        if (x != 0) {
            x = bloom[(h2 >>> 3) % len] & (1 << (h2 & 7));
        }
        return x != 0;
    }

    @Test
    public void size() {
        byte[] bloom = BloomFilterUtils.createFilter(100, 64);
        assertEquals(64, bloom.length);
        bloom = BloomFilterUtils.createFilter(10, 64);
        assertEquals(11, bloom.length);
        bloom = BloomFilterUtils.createFilter(0, 64);
        assertEquals(0, bloom.length);
        bloom = BloomFilterUtils.createFilter(1, 64);
        assertEquals(1, bloom.length);
    }

    @Test
    public void probability() {
        byte[] bloom = BloomFilterUtils.createFilter(20, 64);
        for (int i = 0; i < 20; i++) {
            BloomFilterUtils.add(bloom, String.valueOf(i));
        }
        for (int i = 0; i < 20; i++) {
            assertTrue(BloomFilterUtils.probablyContains(bloom, String.valueOf(i)));
        }
        int falsePositives = 0;
        for (int i = 20; i < 100000; i++) {
            if (BloomFilterUtils.probablyContains(bloom, String.valueOf(i))) {
                falsePositives++;
            }
        }
        assertEquals(1101, falsePositives);
    }
    @Test
    public void negativeHashCode() {
        BloomFilterUtils.add(new byte[0], new Object() {
            public int hashCode() {
                return -1;
            }
        });
    }

}
