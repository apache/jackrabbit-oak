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

/**
 * Test for measuring the performance of creating a nodes (with a low fanout,
 * that is, with few child nodes for each node).
 */
public class CreateManyNodesTest extends AbstractTest {
    
    protected static final String ROOT_NODE_NAME = "many" + TEST_ID;

    private static final int FANOUT = 5;
    private static final int LEVELS = 4;
    private static final int SAVE_EVERY = 150;

    private Session session;
    private Node testRootNode;
    private int nodeCount;

    @Override
    public void beforeSuite() throws RepositoryException {
        session = loginWriter();
        testRootNode = session.getRootNode().addNode(ROOT_NODE_NAME, "nt:unstructured");
    }
    
    @Override
    public void afterSuite() throws RepositoryException {
        testRootNode.remove();
    }

    @Override
    public void runTest() throws Exception {
        nodeCount = 0;
        nodeCount++;
        Node node = testRootNode.addNode(nextNodeName(), "nt:unstructured");
        addChildNodes(node, LEVELS);
        session.save();
    }
    
    private void addChildNodes(Node parent, int level) throws Exception {
        for (int i = 0; i < FANOUT; i++) {
            nodeCount++;
            Node n = parent.addNode("l" + level + "n" + i, "nt:unstructured");
            if (nodeCount % SAVE_EVERY == 0) {
                n.getSession().save();
            }
            if (level > 0) {
                addChildNodes(n, level - 1);
            }
        }
    }

}
