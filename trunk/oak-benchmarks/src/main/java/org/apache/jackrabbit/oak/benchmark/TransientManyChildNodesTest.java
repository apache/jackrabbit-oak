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
 * Test for measuring the performance of {@value #ITERATIONS} iterations of
 * transiently adding and removing a child node to a node that already has
 * {@value #CHILD_COUNT} existing child nodes.
 */
public class TransientManyChildNodesTest extends AbstractTest {

    protected static final String ROOT_NODE_NAME = "test" + TEST_ID;

    protected static final int CHILD_COUNT = 10 * 1000;

    protected static final int ITERATIONS = 10;

    private Session session;

    private Node node;

    @Override
    public void beforeSuite() throws RepositoryException {
        session = getRepository().login(getCredentials());
        node = session.getRootNode().addNode(ROOT_NODE_NAME, "nt:unstructured");
        for (int i = 0; i < CHILD_COUNT; i++) {
            node.addNode("node" + i, "nt:unstructured");
        }
    }

    @Override
    public void beforeTest() throws RepositoryException {
    }

    @Override
    public void runTest() throws Exception {
        for (int i = 0; i < ITERATIONS; i++) {
            node.addNode("onemore", "nt:unstructured").remove();
        }
    }

    @Override
    public void afterTest() throws RepositoryException {
    }

    @Override
    public void afterSuite() throws RepositoryException {
        session.getRootNode().getNode(ROOT_NODE_NAME).remove();
        session.save();
        session.logout();
    }

}
