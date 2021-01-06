/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.benchmark;

import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import static javax.jcr.security.Privilege.JCR_READ;
import static org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils.addAccessControlEntry;

/**
 * {@code GetParentNodeTest} implements a performance test, that reads
 * node from the repository, and after that reads parent node using one of the two methods:
 * 1) node.getParentNode()
 * 2) session.getNode("parent_abs_path").
 */
public abstract class GetParentNodeTest extends AbstractTest {
    private final String name;

    private static final String ROOT_NODE_PATH = GetParentNodeTest.class.getSimpleName() + TEST_ID;
    protected String parentNodePath;
    private String childNodePath;

    public static Benchmark withNodeAPI() {
        return new GetParentNodeTest("GetParentNodeWithNodeAPI") {
            @Override
            protected Node getParentNode(Node childNode) throws RepositoryException {
                return childNode.getParent();
            }
        };
    }

    public static Benchmark withSessionAPI() {
        return new GetParentNodeTest("GetParentNodeWithSessionAPI") {
            @Override
            protected Node getParentNode(Node childNode) throws RepositoryException {
                return childNode.getSession().getNode(parentNodePath);
            }
        };
    }

    protected GetParentNodeTest(String name) {
        this.name = name;
    }

    protected abstract Node getParentNode(Node childNode) throws RepositoryException;

    @Override
    public String toString() {
        return name;
    }

    @Override
    protected void beforeSuite() throws Exception {
        Session session = loginWriter();
        Node testRoot = session.getRootNode().addNode(
                ROOT_NODE_PATH, "nt:unstructured");
        Node parent = testRoot.addNode("a").addNode("b").addNode("c").addNode("d").addNode("e").addNode("f");
        Node child = parent.addNode("g");

        parentNodePath = parent.getPath();
        childNodePath = child.getPath();

        addAccessControlEntry(session, testRoot.getPath(), EveryonePrincipal.getInstance(),
                new String[] {JCR_READ}, true);
        session.save();

        session.logout();
    }

    @Override
    protected void runTest() throws Exception {
        Session session = loginAnonymous();
        Node child = session.getNode(childNodePath);
        for (int i = 0; i < 10000; i++) {
            getParentNode(child);
        }
        session.logout();
    }

    @Override
    protected void afterSuite() throws Exception {
        Session session = loginWriter();
        session.getRootNode().getNode(ROOT_NODE_PATH).remove();
        session.save();
        session.logout();
    }
}
