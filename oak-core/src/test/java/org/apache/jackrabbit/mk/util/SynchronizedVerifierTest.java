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

import java.util.concurrent.atomic.AtomicInteger;
import junit.framework.TestCase;
import org.apache.jackrabbit.mk.util.Concurrent.Task;

/**
 * Tests the SynchronizedVerifier
 */
public class SynchronizedVerifierTest extends TestCase {

    public void testReadRead() throws Exception {
        final AtomicInteger x = new AtomicInteger();
        SynchronizedVerifier.setDetect(AtomicInteger.class, true);
        Concurrent.run("read", new Task() {
            public void call() throws Exception {
                SynchronizedVerifier.check(x, false);
                x.get();
            }
        });
        SynchronizedVerifier.setDetect(AtomicInteger.class, false);
    }

    public void testReadWrite() throws Exception {
        final AtomicInteger x = new AtomicInteger();
        SynchronizedVerifier.setDetect(AtomicInteger.class, true);
        try {
            Concurrent.run("readWrite", new Task() {
                public void call() throws Exception {
                    if (Thread.currentThread().getName().endsWith("1")) {
                        SynchronizedVerifier.check(x, true);
                        x.set(1);
                    } else {
                        SynchronizedVerifier.check(x, false);
                        x.get();
                    }
                }
            });
            fail();
        } catch (AssertionError e) {
            // expected
        } finally {
            SynchronizedVerifier.setDetect(AtomicInteger.class, false);
        }
    }

    public void testWriteWrite() throws Exception {
        final AtomicInteger x = new AtomicInteger();
        SynchronizedVerifier.setDetect(AtomicInteger.class, true);
        try {
            Concurrent.run("write", new Task() {
                public void call() throws Exception {
                    SynchronizedVerifier.check(x, true);
                    x.set(1);
                }
            });
            fail();
        } catch (AssertionError e) {
            // expected
        } finally {
            SynchronizedVerifier.setDetect(AtomicInteger.class, false);
        }
    }

}
