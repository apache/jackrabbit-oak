/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.mk.concurrent;

import junit.framework.Assert;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test concurrent access to nodes, the journal, and revision.
 */
public class ConcurrentTest {

    final MicroKernel mk = new MicroKernelImpl();

    @Test
    public void test() throws Exception {
        final AtomicInteger id = new AtomicInteger();
        Concurrent.run("MicroKernel", new Concurrent.Task() {
            @Override
            public void call() throws Exception {
                long start = System.currentTimeMillis();
                String rev = mk.getHeadRevision();
                int i = id.getAndIncrement();
                String newRev = mk.commit("/", "+\"" + i + "\":{\"x\": " + i + "}", rev, "");
                Assert.assertTrue(!newRev.equals(rev));
                mk.getJournal(rev, newRev, null);
                mk.getRevisionHistory(start, 100, null);
                mk.getNodes("/" + i, newRev);
                mk.getNodes("/" + i, newRev, 0, 0, 0, null);
                Assert.assertFalse(mk.nodeExists("/" + i, rev));
                Assert.assertTrue(mk.nodeExists("/" + i, newRev));
                rev = newRev;
                newRev = mk.commit("/", "-\"" + i + "\"", rev, "");
                Assert.assertTrue(mk.nodeExists("/" + i, rev));
                Assert.assertFalse(mk.nodeExists("/" + i, newRev));
            }
        });
    }

}
