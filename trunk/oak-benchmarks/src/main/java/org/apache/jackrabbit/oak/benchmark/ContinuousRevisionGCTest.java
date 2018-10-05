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
package org.apache.jackrabbit.oak.benchmark;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.Session;

import com.google.common.base.Stopwatch;

import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * Similar to {@link RevisionGCTest} but runs continuously and performs
 * periodic RevisionGC (every 10 seconds).
 */
public class ContinuousRevisionGCTest extends RevisionGCTest {

    @Override
    protected void run(Repository repository, NodeStore nodeStore)
            throws Exception {
        Thread t = new Thread(new Writer(createSession(repository)));
        t.start();

        for (;;) {
            Thread.sleep(TimeUnit.SECONDS.toMillis(10));
            Stopwatch sw = Stopwatch.createStarted();
            String result = revisionGC(nodeStore);
            sw.stop();
            System.out.println("\nPerformed RevisionGC in " + sw + " (" + result + ")");
        }
    }

    private class Writer implements Runnable {

        private final Session s;

        public Writer(Session s) {
            this.s = s;
        }

        @Override
        public void run() {
            Random rand = new Random();
            try {
                System.out.print("Creating garbage ");
                for (int i = 0; ; i++) {
                    Node n = s.getRootNode().addNode("node-" + i);
                    for (int j = 0; j < 1000; j++) {
                        n.addNode("child-" + j, NODE_TYPE);
                    }
                    s.save();
                    if (rand.nextFloat() <= GARBAGE_RATIO) {
                        n.remove();
                        s.save();
                    }
                    System.out.print(".");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                s.logout();
            }
        }
    }
}
