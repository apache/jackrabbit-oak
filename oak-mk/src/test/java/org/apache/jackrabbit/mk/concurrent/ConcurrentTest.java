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

import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
                mk.getNodes("/" + i, newRev, 1, 0, -1, null);
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

    @Test
    @Ignore("OAK-532")  // FIXME OAK-532
    public void journalConsistency() throws Exception {
        while (true) {
            final MicroKernel mk1 = new MicroKernelImpl();
            final String rev = mk1.commit("", "+\"/a\":{}", null, null);

            Thread t1 = new Thread("t1") {
                @Override
                public void run() {
                    try {
                        String r2 = mk1.commit("", "-\"/a\"+\"/c\":{}", rev, null);
                    }
                    catch (MicroKernelException ignore) { }
                }
            };
            Thread t2 = new Thread("t2") {
                @Override
                public void run() {
                    try {
                        String r2 = mk1.commit("", "-\"/a\"+\"/b\":{}", rev, null);
                    }
                    catch (MicroKernelException ignore) { }
                }
            };

            t1.start();
            t2.start();

            t1.join();
            t2.join();

            String journal = mk1.getJournal(rev, null, null);
            int c = count("-\\\"/a\\", journal);
            assertEquals(1, c);
        }
    }

    private static int count(String subStr, String str) {
        int lastIndex = 0;
        int count =0;

        while(lastIndex != -1){
            lastIndex = str.indexOf(subStr, lastIndex);

            if( lastIndex != -1) {
                count ++;
                lastIndex += subStr.length();
            }
        }
        return count;
    }

}
