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
package org.apache.jackrabbit.mk;

import java.util.Random;
import junit.framework.TestCase;
import org.apache.jackrabbit.mk.api.MicroKernel;

public class ConcurrentWriteTest extends TestCase {

    protected static final String TEST_PATH = "/" + ConcurrentWriteTest.class.getName();

    private static final String URL = "fs:{homeDir}/target;clean";
    // private static final String URL = "fs:{homeDir}/target";
    // private static final String URL = "simple:";
    //private static final String URL = "simple:fs:target/temp;clean";

    private static final int NUM_THREADS = 20;
    private static final int NUM_CHILDNODES = 1000;

    MicroKernel mk;

    public void setUp() throws Exception {
        mk = MicroKernelFactory.getInstance(URL);
        mk.commit("/", "+ \"" + TEST_PATH.substring(1) + "\": {\"jcr:primaryType\":\"nt:unstructured\"}", mk.getHeadRevision(), null);
    }

    public void tearDown() throws InterruptedException {
        String head = mk.commit("/", "- \"" + TEST_PATH.substring(1) + "\"", mk.getHeadRevision(), null);
        System.out.println("new HEAD: " + head);
        mk.dispose();
    }

    /**
     * Runs the test.
     */
    public void testConcurrentWriting() throws Exception {

        Profiler prof = new Profiler();
        prof.depth = 8;
        prof.interval = 1;
        // prof.startCollecting();

        String oldHead = mk.getHeadRevision();

        long t0 = System.currentTimeMillis();

        TestThread[] threads = new TestThread[NUM_THREADS];
        for (int i = 0; i < threads.length; i++) {
            TestThread thread = new TestThread(oldHead, "t" + i);
            thread.start();
            threads[i] = thread;
        }

        for (TestThread t : threads) {
            if (t != null) {
                t.join();
            }
        }

        long t1 = System.currentTimeMillis();

        System.out.println("duration: " + (t1 - t0) + "ms");

        String head = mk.getHeadRevision();
        mk.getNodes("/", head, Integer.MAX_VALUE, 0, -1);
        // System.out.println(json);
        System.out.println("new HEAD: " + head);
        System.out.println();

        String history = mk.getRevisions(t0, -1);
        System.out.println("History:");
        System.out.println(history);
        System.out.println();

        mk.getJournal(oldHead, head);
        // System.out.println("Journal:");
        // System.out.println(journal);
        // System.out.println();

        // System.out.println(prof.getTop(5));
    }

    class TestThread extends Thread {
        String revId;
        Random rand;

        TestThread(String revId, String name) {
            super(name);
            this.revId = revId;
            rand = new Random();
        }

        public void run() {
            StringBuilder sb = new StringBuilder();
            sb.append("+\"");
            sb.append(getName());
            sb.append("\" : {\"jcr:primaryType\":\"nt:unstructured\",\n");
            for (int i = 0; i < NUM_CHILDNODES; i++) {
                sb.append("\"sub" + i + "\" : {\"jcr:primaryType\":\"nt:unstructured\", \"prop\":\"" + rand.nextLong() + "\"}");
                if (i == NUM_CHILDNODES - 1) {
                    sb.append('\n');
                } else {
                    sb.append(",\n");
                }
            }
            sb.append('}');
            revId = mk.commit(TEST_PATH, sb.toString(), revId, null);
        }
    }
}
