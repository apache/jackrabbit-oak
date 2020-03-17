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

import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

/**
 * Test case that traverses 10k unstructured nodes (100x100) while 50 concurrent
 * readers randomly access nodes from within this tree.
 */
public class ConcurrentReadTest extends AbstractTest {

    protected static final int NODE_COUNT = 100;
    
    protected static final String ROOT_NODE_NAME = "test" + TEST_ID;

    private final int backgroundReaderCount;

    private final int backgroundWriterCount;

    private final boolean foregroundIsReader;

    private Runnable foregroundTask;

    protected ConcurrentReadTest(
            int backgroundReaderCount, int backgroundWriterCount,
            boolean foregroundIsReader) {
        this.backgroundReaderCount = backgroundReaderCount;
        this.backgroundWriterCount = backgroundWriterCount;
        this.foregroundIsReader = foregroundIsReader;
    }

    public ConcurrentReadTest() {
        this(getScale(20), 0, true);
    }

    @Override
    public void beforeSuite() throws Exception {
        Session session = loginWriter();
        Node root = session.getRootNode().addNode(ROOT_NODE_NAME, "nt:unstructured");
        for (int i = 0; i < NODE_COUNT; i++) {
            Node node = root.addNode("node" + i, "nt:unstructured");
            for (int j = 0; j < NODE_COUNT; j++) {
                node.addNode("node" + j, "nt:unstructured");
            }
            session.save();
        }

        if (foregroundIsReader) {
            foregroundTask = new Reader();
        } else {
            foregroundTask = new Writer();
        }

        for (int i = 0; i < backgroundReaderCount; i++) {
            addBackgroundJob(new Reader());
        }
        for (int i = 0; i < backgroundWriterCount; i++) {
            addBackgroundJob(new Writer());
        }
    }

    private class Reader implements Runnable {

        private final Random random = new Random();

        private final Session session = loginWriter(); // TODO: anonymous is slow

        public void run() {
            try {
                session.refresh(false);
                for (int i = 0; i < 10000; i++) {
                    int a = random.nextInt(NODE_COUNT);
                    int b = random.nextInt(NODE_COUNT);
                    session.getRootNode().getNode(ROOT_NODE_NAME + "/node" + a + "/node" + b);
                }
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private class Writer implements Runnable {

        private final Random random = new Random();

        private final Session session = loginWriter();

        private long count;

        public void run() {
            try {
                session.refresh(false);
                for (int i = 0; i < 10; i++) {
                    int a = random.nextInt(NODE_COUNT);
                    int b = random.nextInt(NODE_COUNT);
                    Node node = session.getRootNode().getNode(
                            ROOT_NODE_NAME + "/node" + a + "/node" + b);
                    boolean done = false;
                    while (!done) {
                        try {
                            node.setProperty("count", count++);
                            session.save();
                            done = true;
                        } catch (InvalidItemStateException e) {
                            // retry with a fresh session
                            session.refresh(false);
                        }
                    }
                }
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void runTest() throws Exception {
        foregroundTask.run();
    }

    @Override
    public void afterSuite() throws Exception {
        Session session = loginWriter();
        Node root = session.getRootNode().getNode(ROOT_NODE_NAME);
        for (int i = 0; i < NODE_COUNT; i++) {
            root.getNode("node" + i).remove();
            session.save();
        }
        root.remove();
        session.save();
    }

}
