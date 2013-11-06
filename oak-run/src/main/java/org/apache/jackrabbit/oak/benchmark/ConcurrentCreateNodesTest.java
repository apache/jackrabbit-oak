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

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

public class ConcurrentCreateNodesTest extends AbstractTest {

    protected static final String ROOT_NODE_NAME = "test" + TEST_ID;
    private static final int WORKER_COUNT = 20;
    private static final int NODE_COUNT_LEVEL2 = 50;
    private Writer writer;

    @Override
    protected void beforeSuite() throws Exception {
        Session session = loginWriter();
        Node rootNode = session.getRootNode();
        if (rootNode.hasNode(ROOT_NODE_NAME)) {
            Node root = rootNode.getNode(ROOT_NODE_NAME);
            root.remove();
        }
        rootNode = session.getRootNode().addNode(ROOT_NODE_NAME, "nt:unstructured");
        for (int i = 0; i < WORKER_COUNT; i++) {
            rootNode.addNode("node" + i);
        }
        session.save();
        for (int i = 1; i < WORKER_COUNT; i++) {
            addBackgroundJob(new Writer(rootNode.getPath() + "/node" + i));
        }
        writer = new Writer(rootNode.getPath() + "/node" + 0);
    }

    private class Writer implements Runnable {

        private final Session session = loginWriter();
        private final String path;
        private int count = 0;

        private Writer(String path) {
            this.path = path;
        }

        @Override
        public void run() {
            try {
                session.refresh(false);

                Node root = session.getNode(path);
                Node node = root.addNode("node" + count++);
                for (int j = 0; j < NODE_COUNT_LEVEL2; j++) {
                    node.addNode("node" + j);
                    session.save();
                }
            } catch (RepositoryException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

    }
    
    @Override
    public void runTest() throws Exception {
        writer.run();
    }
}
