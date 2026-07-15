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
package org.apache.jackrabbit.oak.jcr;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Test;

/**
 * Concurrently adds and removes the same node. Either the operation must
 * succeed or throw an InvalidItemStateException.
 */
public class ConcurrentAddRemoveIT extends AbstractRepositoryTest {

    private static final int NUM_WORKERS = 10;

    private static final long RUN_TIME = TimeUnit.SECONDS.toMillis(10);

    public ConcurrentAddRemoveIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void concurrent() throws Exception {
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());
        Node test = getAdminSession().getRootNode().addNode("test");
        List<Thread> worker = new ArrayList<Thread>();
        for (int i = 0; i < NUM_WORKERS; i++) {
            worker.add(new Thread(new Worker(
                    createAdminSession(), test.getPath(), exceptions)));
        }
        getAdminSession().save();
        for (Thread t : worker) {
            t.start();
        }
        for (Thread t : worker) {
            t.join();
        }
        for (Exception e : exceptions) {
            fail(e.toString());
        }
    }

    private static final class Worker implements Runnable {

        private final Session s;
        private final String path;
        private final List<Exception> exceptions;

        Worker(Session s, String path, List<Exception> exceptions) {
            this.s = s;
            this.path = path;
            this.exceptions = exceptions;
        }

        @Override
        public void run() {
            try {
                long end = System.currentTimeMillis() + RUN_TIME;
                while (end > System.currentTimeMillis()) {
                    s.refresh(false);
                    Node n = s.getNode(path);
                    if (n.hasNode("child")) {
                        n.getNode("child").remove();
                    } else {
                        n.addNode("child");
                    }
                    try {
                        s.save();
                    } catch (InvalidItemStateException e) {
                        // refresh and retry
                    }
                }
            } catch (RepositoryException e) {
                exceptions.add(e);
            } finally {
                s.logout();
            }
        }
    }
}
