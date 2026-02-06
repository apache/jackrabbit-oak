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
package org.apache.jackrabbit.oak.jcr.xml;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Calendar;

import static org.junit.Assert.assertEquals;

public class BufferedStringValueTest {

    private BufferedStringValue bufferedStringValue;

    // minimal for test usage
    private ValueFactory vf = new ValueFactory() {
        @Override
        public Value createValue(String s) {
            return null;
        }

        @Override
        public Value createValue(String s, int i) {
            return null;
        }

        @Override
        public Value createValue(long l) {
            return null;
        }

        @Override
        public Value createValue(double v) {
            return null;
        }

        @Override
        public Value createValue(BigDecimal bigDecimal) {
            return null;
        }

        @Override
        public Value createValue(boolean b) {
            return null;
        }

        @Override
        public Value createValue(Calendar calendar) {
            return null;
        }

        @Override
        public Value createValue(InputStream inputStream) {
            try {
                byte[] data = inputStream.readAllBytes();
                return new Value() {
                    @Override
                    public String getString() throws IllegalStateException {
                        return new String(data, StandardCharsets.UTF_8);
                    }

                    @Override
                    public InputStream getStream() {
                        return new ByteArrayInputStream(data);
                    }

                    @Override
                    public Binary getBinary() {
                        return null;
                    }

                    @Override
                    public long getLong() {
                        return 0;
                    }

                    @Override
                    public double getDouble() {
                        return 0;
                    }

                    @Override
                    public BigDecimal getDecimal() {
                        return null;
                    }

                    @Override
                    public Calendar getDate() {
                        return null;
                    }

                    @Override
                    public boolean getBoolean() {
                        return false;
                    }

                    @Override
                    public int getType() {
                        return 0;
                    }
                };
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Value createValue(Binary binary) {
            return null;
        }

        @Override
        public Value createValue(Node node) {
            return null;
        }

        @Override
        public Value createValue(Node node, boolean b) {
            return null;
        }

        @Override
        public Binary createBinary(InputStream inputStream) {
            return null;
        }
    };


    @Before
    public void setUp() throws Exception {
        bufferedStringValue = new BufferedStringValue(vf, null, false);
    }

    @After
    public void shutdown() throws Exception {
        bufferedStringValue.dispose();
    }

    @Test
    public void length() throws IOException {
        String s = "Hello, World!";
        bufferedStringValue.append(s.toCharArray(), 0, s.length());
        assertEquals(s.length(), bufferedStringValue.length());
    }

    @Test
    public void getString() throws IOException {
        String s = "foobar";
        bufferedStringValue.append(s.toCharArray(), 0, s.length());
        assertEquals(s, bufferedStringValue.getString());
    }

    @Test
    public void getStringBase64() throws IOException {
        bufferedStringValue.dispose();
        // with base64
        bufferedStringValue = new BufferedStringValue(null, null, true);
        String s = "Zm9vYg==";
        bufferedStringValue.append(s.toCharArray(), 0, s.length());
        assertEquals("foob", bufferedStringValue.getString());
    }

    @Test
    public void getStringBase64NoPadding() throws IOException {
        bufferedStringValue.dispose();
        // with base64
        bufferedStringValue = new BufferedStringValue(null, null, true);
        String s = "Zm9vYg";
        bufferedStringValue.append(s.toCharArray(), 0, s.length());
        // https://datatracker.ietf.org/doc/html/rfc4648#section-10
        // BUG in Base64 class - this should be "foob".
        // Behavior will change when Jackrabbit version with fix
        // https://issues.apache.org/jira/browse/JCR-5226 is used.
        assertEquals("foo", bufferedStringValue.getString());
    }

    @Test
    public void reader() throws IOException {
        String s = "Hello, World!";
        bufferedStringValue.append(s.toCharArray(), 0, s.length());
        Reader reader = bufferedStringValue.reader();
        char[] chars = new char[s.length()];
        reader.read(chars);
        assertEquals(s, String.valueOf(chars));
    }

    @Test
    public void testBigAsString() throws IOException {
        StringBuilder sb = new StringBuilder();
        String s = "Hello, World!";
        int limit = 1014 * 1024; // 1M
        while (sb.length() < limit) {
            sb.append(s);
        }
        String test = sb.toString();
        bufferedStringValue.append(test.toCharArray(), 0, test.length());
        assertEquals(test, bufferedStringValue.getString());
    }

    @Test
    public void testBigAsReader() throws IOException {
        StringBuilder sb = new StringBuilder();
        String s = "Hello, World!";
        int limit = 1014 * 1024; // 1M
        while (sb.length() < limit) {
            sb.append(s);
        }
        String test = sb.toString();
        bufferedStringValue.append(test.toCharArray(), 0, test.length());
        assertEquals(test, bufferedStringValue.getString());

        Reader reader = bufferedStringValue.reader();
        char[] chars = new char[test.length()];
        reader.read(chars);
        assertEquals(test, String.valueOf(chars));
    }

    @Test
    public void testBase64Big() throws IOException {
        bufferedStringValue.dispose();
        // with base64
        bufferedStringValue = new BufferedStringValue(null, null, true);
        StringBuilder sb = new StringBuilder();
        String s = "Foo Bar Qux";
        int limit = 1014 * 1024; // 1M
        while (sb.length() < limit) {
            sb.append(s);
        }
        String test = java.util.Base64.getEncoder().encodeToString(sb.toString().getBytes());
        bufferedStringValue.append(test.toCharArray(), 0, test.length());
        assertEquals(sb.toString(), bufferedStringValue.getString());
    }

    @Test
    public void testBigAsValue() throws IOException, RepositoryException {
        // with base64
        bufferedStringValue = new BufferedStringValue(vf, null, true);
        StringBuilder sb = new StringBuilder();
        String s = "Foo Bar Qux";
        int limit = 1014 * 1024; // 1M
        while (sb.length() < limit) {
            sb.append(s);
        }
        String test = java.util.Base64.getEncoder().encodeToString(sb.toString().getBytes());
        bufferedStringValue.append(test.toCharArray(), 0, test.length());
        Value x = bufferedStringValue.getValue(PropertyType.BINARY);
        assertEquals(sb.toString(), x.getString());
    }
}