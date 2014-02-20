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

import junit.framework.TestCase;

/**
 * Test the string utilities.
 */
public class StringUtilsTest extends TestCase {

    public void testHex() {
        assertEquals("0123", StringUtils.convertBytesToHex(new byte[]{(byte) 0x01, (byte) 0x23}));
        assertEquals("89bd", StringUtils.convertBytesToHex(new byte[]{(byte) 0x89, (byte) 0xbd}));
        assertEquals("face", StringUtils.convertBytesToHex(new byte[]{(byte) 0xfa, (byte) 0xce}));
        IOUtilsTest.assertEquals(new byte[]{(byte) 0xfa, (byte) 0xce}, StringUtils.convertHexToBytes("face"));
        IOUtilsTest.assertEquals(new byte[]{(byte) 0xfa, (byte) 0xce}, StringUtils.convertHexToBytes("fAcE"));
        IOUtilsTest.assertEquals(new byte[]{(byte) 0xfa, (byte) 0xce}, StringUtils.convertHexToBytes("FaCe"));
        IOUtilsTest.assertEquals(new byte[]{(byte) 0x09, (byte) 0xaf}, StringUtils.convertHexToBytes("09af"));
        for (String s : new String[]{"120", "1/", "9:", "fast", "a`", "ag", "0@", "aG"}) {
            try {
                StringUtils.convertHexToBytes(s);
                fail();
            } catch (IllegalArgumentException e) {
                // expected
            }
        }
    }

}
