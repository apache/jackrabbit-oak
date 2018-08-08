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
package org.apache.jackrabbit.oak.jcr.binary.util;

import static org.apache.jackrabbit.oak.jcr.binary.util.BinaryAccessTestUtils.httpPut;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;

public class Content {
    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private final String content;
    private final byte[] bytes;

    private Content(String content) {
        this.content = content;
        this.bytes = content.getBytes(CHARSET);
    }

    public static Content createRandom(long size) {
          return new Content(getRandomString(size));
    }

    public long size() {
        return bytes.length;
    }

    public InputStream getStream() {
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public String toString() {
        return content;
    }

    public void assertEqualsWith(InputStream stream) throws IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(stream, writer, CHARSET);
        // converting to string gives us a diff in assertEquals() if it's not equal
        Assert.assertEquals(content, writer.toString());
    }

    /** Uploads this data via HTTP PUT to the provided URI and returns the HTTP status code */
    public int httpPUT(URI uri) throws IOException {
        return httpPut(uri, size(), getStream());
    }

    /** Uploads a sub range of this data via HTTP PUT. */
    public int httpPUT(URI uri, long offset, long length) throws IOException {
        ByteArrayInputStream partStream = new ByteArrayInputStream(bytes, (int) offset, (int) length);
        return httpPut(uri, length, partStream);
    }

    private static String getRandomString(long size) {
        //noinspection SpellCheckingInspection
        String base = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ+/";
        StringWriter writer = new StringWriter();
        Random random = new Random();
        if (size > 256) {
            final String str256 = getRandomString(255);
            // add newlines for better diffs in failing junit test results
            while (size >= 256) {
                writer.write(str256);
                writer.write('\n');
                size -= 256;
            }
            if (size > 0) {
                writer.write(str256.substring(0, (int)size));
            }
        }
        else {
            for (long i = 0; i < size; i++) {
                writer.append(base.charAt(random.nextInt(base.length())));
            }
        }
        return writer.toString();
    }

}
