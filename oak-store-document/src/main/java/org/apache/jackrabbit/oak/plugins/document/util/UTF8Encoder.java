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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import java.nio.charset.StandardCharsets;

/**
 * Utility class related to encoding characters into (UTF-8) byte sequences.
 */
public class UTF8Encoder {

    private UTF8Encoder() {
    }

    /**
     * Get an encoder suitable for UTF-8, using the proper error handling flags.
     * 
     * Note that the instance is kept in a thread local to avoid the cost of
     * constructing an encoder for every call. This actually mimics the
     * internals of {@link String#getBytes(String)}.
     */
    private static ThreadLocal<CharsetEncoder> CSE = new ThreadLocal<CharsetEncoder>() {
        @Override
        protected CharsetEncoder initialValue() {
            CharsetEncoder e = StandardCharsets.UTF_8.newEncoder();
            e.onUnmappableCharacter(CodingErrorAction.REPORT);
            e.onMalformedInput(CodingErrorAction.REPORT);
            return e;
        }
    };

    /**
     * Like {@link String#getBytes(java.nio.charset.Charset)} (with "UTF-8"),
     * except that invalid character sequences (such as unpaired surrogates) are
     * reported as exceptions (see {@link CodingErrorAction#REPORT}, instead of
     * being silently replaced by a replacement character as it would happen
     * otherwise.
     * 
     * @param input
     *            String to encode
     * @return String encoded using {@link StandardCharsets#UTF_8}
     * @throws IOException
     *             on encoding error
     */
    public static byte[] encodeAsByteArray(String input) throws IOException {
        CharsetEncoder e = CSE.get();
        e.reset();
        return bytes(e.encode(CharBuffer.wrap(input.toCharArray())));
    }

    /**
     * See {@link CharsetEncoder#canEncode(CharSequence)}.
     */
    public static boolean canEncode(CharSequence input) {
        CharsetEncoder e = CSE.get();
        e.reset();
        return e.canEncode(input);
    }

    private static byte[] bytes(ByteBuffer b) {
        byte[] a = new byte[b.remaining()];
        b.get(a);
        return a;
    }
}
