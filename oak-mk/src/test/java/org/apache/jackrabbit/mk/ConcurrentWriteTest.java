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
import org.apache.jackrabbit.mk.core.MicroKernelImpl;

public class ConcurrentWriteTest extends TestCase {

    protected static final String TEST_PATH = "/" + ConcurrentWriteTest.class.getName();

    private static final int NUM_THREADS = 20;
    private static final int NUM_CHILDNODES = 1000;

    MicroKernel mk;

    public void setUp() throws Exception {
        String homeDir = System.getProperty("homeDir", "target");
        mk = new MicroKernelImpl(homeDir);
        mk.commit("/", "+ \"" + TEST_PATH.substring(1) + "\": {\"jcr:primaryType\":\"nt:unstructured\"}", mk.getHeadRevision(), null);
    }

    public void tearDown() throws InterruptedException {
        String head = mk.commit("/", "- \"" + TEST_PATH.substring(1) + "\"", mk.getHeadRevision(), null);
        mk.dispose();
    }

    /**
     * Runs the test.
     */
    public void testConcurrentWriting() throws Exception {

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

        String head = mk.getHeadRevision();
        mk.getNodes("/", head, Integer.MAX_VALUE, 0, -1, null);

        String history = mk.getRevisions(t0, -1);

        mk.getJournal(oldHead, head, null);
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
