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
package org.apache.jackrabbit.oak.commons.pio;

import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CloserTest {

    @Test
    public void testCloserOrder() throws IOException {
        // shows closes in reverse order

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
    public void testCloserWithTryWithResources() throws IOException {
        // check cloesable behavior of Closer

        AtomicBoolean wasClosed = new AtomicBoolean(false);

        try (Closer closer = Closer.create()) {
            closer.register(() -> wasClosed.set(true));
        }

        assertTrue("closeable should be closed by try-w-resources", wasClosed.get());
    }

    @Test
    public void testCloseableThrowsRuntimeException() {
        Closer closer = Closer.create();
        closer.register(() -> {
            throw new RuntimeException();
        });
        assertThrows(RuntimeException.class, closer::close);
    }

    @Test
    public void testWhichThrows() throws IOException {
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

    @Test
    public void testRethrowRuntime() {
        try {
            Closer closer = Closer.create();
            try {
                closer.register(() -> {
                    throw new IOException("checked");
                });
                throw new RuntimeException("unchecked");
            } catch (Throwable t) {
                throw closer.rethrow(t);
            } finally {
                closer.close();
            }
        } catch (Exception ex) {
            assertTrue(
                    "should throw the (wrapped) unchecked exception, but got " +
                    ex.getMessage(),
                    ex.getMessage().contains("unchecked"));
        }
    }

    @Test
    public void testRethrowChecked() throws IOException {
        try {
            Closer closer = Closer.create();
            try {
                closer.register(() -> {
                    throw new IOException("checked");
                });
                throw new InterruptedException("interrupted");
            } catch (Throwable t) {
                throw closer.rethrow(t);
            } finally {
                closer.close();
            }
        } catch (RuntimeException ex) {
            assertTrue("should throw the (wrapped) exception",
                    ex.getCause() instanceof InterruptedException);
        }
    }

    @Test
    public void compareClosers() {
        // when rethrow was called, IOExceptions that happened upon close will be swallowed

        com.google.common.io.Closer guavaCloser = com.google.common.io.Closer.create();
        Closer oakCloser = Closer.create();

        try {
            throw oakCloser.rethrow(new InterruptedException());
        } catch (Exception e) {}

        try {
            throw guavaCloser.rethrow(new InterruptedException());
        } catch (Exception e) {}

        try {
            oakCloser.close();
        } catch (Exception e) {
            fail("should not throw but got: " + e);
        }

        try {
            guavaCloser.close();
        } catch (Exception e) {
            fail("should not throw but got: " + e);
        }
    }

    @Test
    public void compareClosers2() {
        // when rethrow was called, Exceptions that happened upon close will be swallowed

        com.google.common.io.Closer guavaCloser = com.google.common.io.Closer.create();
        Closer oakCloser = Closer.create();

        try {
            throw oakCloser.rethrow(new InterruptedException());
        } catch (Exception e) {}

        try {
            throw guavaCloser.rethrow(new InterruptedException());
        } catch (Exception e) {}

        try {
            oakCloser.register(() -> { throw new RuntimeException(); });
            oakCloser.close();
        } catch (Exception e) {
            fail("should not throw but got: " + e);
        }

        try {
            guavaCloser.register(() -> { throw new RuntimeException(); });
            guavaCloser.close();
        } catch (Exception e) {
            fail("should not throw but got: " + e);
        }
    }
}
