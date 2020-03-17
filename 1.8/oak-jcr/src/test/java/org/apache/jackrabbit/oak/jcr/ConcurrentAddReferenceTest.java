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

import static org.apache.jackrabbit.commons.JcrUtils.in;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeType;

import com.google.common.collect.Iterators;

import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * <code>ConcurrentAddReferenceTest</code> adds nodes with multiple sessions in separate
 * locations of the repository and creates references to a single node.
 */
public class ConcurrentAddReferenceTest extends AbstractRepositoryTest {

    private static final int NUM_WORKERS = 10;

    private static final int NODES_PER_WORKER = 100;

    private String refPath;

    private Node testRoot;

    public ConcurrentAddReferenceTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setup() throws RepositoryException {
        Session session = getAdminSession();
        Node root = session.getRootNode();
        Node testNode = root.addNode("test_referenceable");
        testNode.addMixin(NodeType.MIX_REFERENCEABLE);
        testRoot = getAdminSession().getRootNode().addNode("test");
        session.save();
        refPath = testNode.getPath();
    }

    @After
    public void tearDown() throws RepositoryException {
        Session session = getAdminSession();
        testRoot.remove();
        session.removeItem(refPath);
        session.save();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void addReferences() throws Exception {
        List<Exception> exceptions = Collections.synchronizedList(new ArrayList<Exception>());
        List<Thread> worker = new ArrayList<Thread>();
        for (int i = 0; i < NUM_WORKERS; i++) {
            String path = testRoot.addNode("node" + i).getPath();
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
        for (Node n : in((Iterator<Node>) testRoot.getNodes())) {
            assertEquals(NODES_PER_WORKER, Iterators.size(n.getNodes()));
        }
    }

    private final class Worker implements Runnable {

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
                s.refresh(false);
                Node n = s.getNode(path);
                Node refNode = s.getNode(refPath);
                for (int i = 0; i < NODES_PER_WORKER; i++) {
                    Node n1 = n.addNode("node" + i);
                    n1.setProperty("myRef", refNode);
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
