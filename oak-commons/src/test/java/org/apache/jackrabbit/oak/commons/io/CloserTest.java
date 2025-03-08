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
package org.apache.jackrabbit.oak.commons.io;

import com.google.common.io.Closer;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CloserTest {

    // tests below confirm what Guava actually does

    @Test
    public void testGuavaCloserOrder() throws IOException {
        // shows that Guava closes in reverse order

        int cnt = 2;
        List<Integer> order = new ArrayList<>();

        Closer closer = Closer.create();
        for (int i = 0; i < cnt; i++) {
            Integer val = i;
            Closeable c = new Closeable() {

                final Integer c = val;

                @Override
                public void close() {
                    order.add(c);
                }
            };
            closer.register(c);
        }
        closer.close();
        assertEquals(1, (int)order.get(0));
        assertEquals(0, (int)order.get(1));
    }

    @Test
    public void testGuavaWhatThrows() throws IOException {
        // shows which exception is not suppressed

        int cnt = 2;

        Closer closer = Closer.create();
        for (int i = 0; i < cnt; i++) {
            Integer val = i;
            Closeable c = new Closeable() {

                final Integer c = val;

                @Override
                public void close() throws IOException {
                    throw new IOException("" + c);
                }
            };
            closer.register(c);
        }

        try {
            closer.close();
            fail("should throw");
        } catch (IOException ex) {
            assertEquals("1", ex.getMessage());
        }
    }
}
