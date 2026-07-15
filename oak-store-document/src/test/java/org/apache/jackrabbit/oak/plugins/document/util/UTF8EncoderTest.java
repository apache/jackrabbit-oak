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
package org.apache.jackrabbit.oak.plugins.document.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.junit.Test;

public class UTF8EncoderTest {

    private static char[] SURROGATE_PAIR = Character.toChars(0x1f4a9);

    @Test
    public void encodeValid() throws IOException {

        // a a-umlaut euro plane-1-char
        String test = "a \u00E4 \u20ac " + new String(SURROGATE_PAIR);

        byte[] withStringClass = test.getBytes(StandardCharsets.UTF_8);
        byte[] withUtilsClass = UTF8Encoder.encodeAsByteArray(test);
        assertArrayEquals(withStringClass, withUtilsClass);
        assertEquals(test, new String(withUtilsClass, StandardCharsets.UTF_8));
    }

    @Test
    public void encodeInValid() {

        // a a-umlaut euro plane-1-char, second char in surrogate pair missing
        String test = "a \u00E4 \u20ac " + SURROGATE_PAIR[0];

        try {
            UTF8Encoder.encodeAsByteArray(test);
            fail("expected encoding to fail");
        } catch (IOException expected) {
            // expected
        }
    }

    @Test
    public void encodeMultiThreaded() throws InterruptedException {

        int tc = 20;

        Thread[] threads = new Thread[tc];

        for (int i = 0; i < tc; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    // encode and decode 100 random UUID strings
                    for (int j = 0; j < 100; j++) {
                        String test = UUID.randomUUID().toString();
                        String roundtripped = null;
                        try {
                            byte[] bytes = UTF8Encoder.encodeAsByteArray(test);
                            roundtripped = new String(bytes, StandardCharsets.UTF_8);
                        } catch (IOException exignored) {
                        }
                        assertEquals(test, roundtripped);
                    }

                }
            });
        }
        for (int i = 0; i < tc; i++) {
            threads[i].start();
        }
        for (int i = 0; i < tc; i++) {
            threads[i].join();
        }
    }
}
