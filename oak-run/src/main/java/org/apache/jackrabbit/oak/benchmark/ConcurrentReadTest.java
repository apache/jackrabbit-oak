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

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

/**
 * Test case that traverses 10k unstructured nodes (100x100) while 50 concurrent
 * readers randomly access nodes from within this tree.
 */
public class ConcurrentReadTest extends AbstractTest {

    protected static final int NODE_COUNT = 100;

    private static final int READER_COUNT = getScale(20);

    private Session session;

    protected Node root;

    @Override
    public void beforeSuite() throws Exception {
        session = getRepository().login(
                new SimpleCredentials("admin", "admin".toCharArray()));
        root = session.getRootNode().addNode("testroot", "nt:unstructured");
        for (int i = 0; i < NODE_COUNT; i++) {
            Node node = root.addNode("node" + i, "nt:unstructured");
            for (int j = 0; j < NODE_COUNT; j++) {
                node.addNode("node" + j, "nt:unstructured");
            }
            session.save();
        }

        for (int i = 0; i < READER_COUNT; i++) {
            addBackgroundJob(new Reader());
        }
    }

    class Reader implements Runnable {

        private final Random random = new Random();

        public void run() {
            try {
                Session session = getRepository().login(
                        new SimpleCredentials("admin", "admin".toCharArray()));
                try {
                    int i = random.nextInt(NODE_COUNT);
                    int j = random.nextInt(NODE_COUNT);
                    session.getRootNode()
                        .getNode("testroot/node" + i + "/node" + j);
                } finally {
                    session.logout();
                }
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void runTest() throws Exception {
        Reader reader = new Reader();
        for (int i = 0; i < 1000; i++) {
            reader.run();
        }
    }

    @Override
    public void afterSuite() throws Exception {
        for (int i = 0; i < NODE_COUNT; i++) {
            root.getNode("node" + i).remove();
            session.save();
        }

        root.remove();
        session.save();
    }

}
