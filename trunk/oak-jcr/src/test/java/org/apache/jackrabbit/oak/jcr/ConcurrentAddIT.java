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
package org.apache.jackrabbit.oak.jcr;

import static org.apache.jackrabbit.commons.JcrUtils.in;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.collect.Iterators;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * <code>ConcurrentAddIT</code> adds nodes with multiple sessions in separate
 * locations of the repository and under the same parent.
 */
@RunWith(Parameterized.class) // OAK-2704
public class ConcurrentAddIT extends AbstractRepositoryTest {

    private static final int NUM_WORKERS = 10;

    private static final int NODES_PER_WORKER = 100;

    public ConcurrentAddIT(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test @SuppressWarnings("unchecked")
    public void addNodes() throws Exception {
        List<Exception> exceptions = Collections.synchronizedList(
                new ArrayList<Exception>());
        Node test = getAdminSession().getRootNode().addNode("test");
        List<Thread> worker = new ArrayList<Thread>();
        for (int i = 0; i < NUM_WORKERS; i++) {
            String path = test.addNode("node" + i).getPath();
            worker.add(new Thread(new Worker(
                    createAdminSession(), path, exceptions)));
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
        getAdminSession().refresh(false);
        for (Node n : in((Iterator<Node>) test.getNodes())) {
            assertEquals(NODES_PER_WORKER, Iterators.size(n.getNodes()));
        }
    }

    @Test @SuppressWarnings("unchecked")
    public void addNodesSameParent() throws Exception {
        List<Exception> exceptions = Collections.synchronizedList(
                new ArrayList<Exception>());
        // use nt:unstructured to force conflicts on :childOrder property
        Node test = getAdminSession().getRootNode().addNode("test", "nt:unstructured");
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
        getAdminSession().refresh(false);
        assertEquals(NODES_PER_WORKER * NUM_WORKERS, Iterators.size(test.getNodes()));
    }

    private static final class Worker implements Runnable {

        private static final AtomicInteger WORKER_ID = new AtomicInteger();
        private final Session s;
        private final String path;
        private final List<Exception> exceptions;
        private final String id = "worker-" + WORKER_ID.incrementAndGet();

        Worker(Session s, String path, List<Exception> exceptions) {
            this.s = s;
            this.path = path;
            this.exceptions = exceptions;
        }

        @Override
        public void run() {
            try {
                s.refresh(false);
                Node n = s.getNode(path);
                for (int i = 0; i < NODES_PER_WORKER; i++) {
                    n.addNode(id + "-node-" + i);
                    s.save();
                }
            } catch (RepositoryException e) {
                exceptions.add(e);
            } finally {
                s.logout();
            }
        }
    }
}
