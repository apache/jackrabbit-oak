/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.benchmark;

import java.io.ByteArrayInputStream;
import java.util.UUID;
import javax.jcr.Binary;
import javax.jcr.Node;
import javax.jcr.NodeIterator;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.time.StopWatch;

public class ReadBinaryPropertiesTest extends AbstractTest<Void> {

    private static final int BINARY_SIZE = Integer.getInteger("binaryValueSize", 32000);
    private static final int MAX_NODES = Integer.getInteger("maxNodes", 500);
    private static final int MAX_CHILDREN_PER_NODE = 10;

    @Override
    protected void beforeTest() throws Exception {
        // To prevent Oak caching to affect the performance stats, we create a new tree of nodes with binary properties for each test execution pass.
        Session session = loginWriter();
        Node rootNode = session.getRootNode();
        System.out.printf("Creating tree with %d nodes. Binary size: %d%n", MAX_NODES, BINARY_SIZE);
        int maxDepth = computeDepth(MAX_NODES);
        StopWatch watch = StopWatch.createStarted();
        int nodeCount = createTree(rootNode, maxDepth, 0);
        watch.stop();
        System.out.printf("%d nodes created in %dms%n", nodeCount, watch.getTime());
    }

    /**
     * Compute depth necessary for the tree to contain {@code maxNodes}, given
     * {@link #MAX_CHILDREN_PER_NODE} children for each node.
     */
    private static int computeDepth(int maxNodes) {
        double nodes = 0;
        int depth = 1;
        while (true) {
            nodes += Math.pow(MAX_CHILDREN_PER_NODE, depth);
            if (maxNodes / nodes <= 1) {
                return depth;
            }
            depth++;
        }
    }

    private static int createTree(Node rootNode, int depth, int nodeCount) throws Exception {
        Session session = rootNode.getSession();
        for (int i = 0; i < MAX_CHILDREN_PER_NODE && nodeCount < MAX_NODES; i++) {
            Node child = rootNode.addNode(UUID.randomUUID().toString(), "oak:Unstructured");
            byte[] b = RandomUtils.nextBytes(BINARY_SIZE);
            Binary binary = session.getValueFactory().createBinary(new ByteArrayInputStream(b));
            child.setProperty("bin", binary);
            nodeCount++;
            if (depth > 0 && nodeCount < MAX_NODES) {
                nodeCount = createTree(child, depth - 1, nodeCount);
            }
        }
        session.save();
        return nodeCount;
    }

    @Override
    protected void runTest() throws Exception {
        Session session = loginWriter();
        Node rootNode = session.getRootNode();
        traverseTree(rootNode);
        System.out.println("Traversal done");
    }

    private static void traverseTree(Node rootNode) throws Exception {
        NodeIterator nodes = rootNode.getNodes();
        while (nodes.hasNext()) {
            Node child = nodes.nextNode();
            if (!child.hasProperty("bin")) {
                continue; // ignore nodes auto-generated by Oak
            }
            // read binary property
            Property property = child.getProperty("bin");
            byte[] bytes = IOUtils.toByteArray(property.getBinary().getStream());
            if (bytes.length != BINARY_SIZE) {
                System.out.println("Invalid binary property in node " + child.getPath());
            }
            if (child.hasNodes()) {
                traverseTree(child);
            }
        }
    }

    @Override
    protected void afterTest() throws Exception {
        Session session = loginWriter();
        Node rootNode = session.getRootNode();
        deleteTree(session, rootNode);
    }

    private static void deleteTree(Session session, Node rootNode) throws RepositoryException {
        NodeIterator nodes = rootNode.getNodes();
        while (nodes.hasNext()) {
            Node child = nodes.nextNode();
            if (child.hasProperty("bin")) {
                child.remove();
            }
        }
        session.save();
    }
}
