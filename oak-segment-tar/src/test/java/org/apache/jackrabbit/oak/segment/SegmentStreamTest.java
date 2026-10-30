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
package org.apache.jackrabbit.oak.segment;

import static org.apache.jackrabbit.oak.segment.SegmentStore.EMPTY_STORE;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

public class SegmentStreamTest {

    private static final String VALUE = "hello world";

    private static SegmentStream newInlineStream() {
        byte[] bytes = VALUE.getBytes(StandardCharsets.UTF_8);
        RecordId id = new RecordId(new SegmentId(EMPTY_STORE, 1, 2), 0);
        return new SegmentStream(id, ByteBuffer.wrap(bytes), bytes.length);
    }

    @Test
    public void getStringIsIdempotent() {
        SegmentStream stream = newInlineStream();
        assertEquals(VALUE, stream.getString());
        assertEquals(VALUE, stream.getString());
    }

    @Test
    public void getStringReturnsFullValueAfterRead() throws IOException {
        SegmentStream stream = newInlineStream();
        byte[] prefix = new byte[5];
        assertEquals(5, stream.read(prefix));
        assertEquals("hello", new String(prefix, StandardCharsets.UTF_8));
        assertEquals(VALUE, stream.getString());
    }
}
