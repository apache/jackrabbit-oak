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

    private boolean runAsAdmin;

    public static Benchmark withNodeAPIAndParentVisible(boolean runAsAdmin) {
        return new GetParentNodeTest("GetParentVisibleNodeAPI", runAsAdmin) {
            @Override
            protected Node getParentNode(Node childNode) throws RepositoryException {
                return childNode.getParent();
            }

            @Override
            protected boolean isParentNodeVisible() {
                return true;
            }
        };
    }

    public static Benchmark withSessionAPIAndParentVisible(boolean runAsAdmin) {
        return new GetParentNodeTest("GetParentVisibleSessionAPI", runAsAdmin) {
            @Override
            protected Node getParentNode(Node childNode) throws RepositoryException {
                return childNode.getSession().getNode(parentNodePath);
            }

            @Override
            protected boolean isParentNodeVisible() {
                return true;
            }
        };
    }

    public static Benchmark withNodeAPIAndParentNotVisible(boolean runAsAdmin) {
        return new GetParentNodeTest("GetParentNotVisibleNodeAPI", runAsAdmin) {
            @Override
            protected Node getParentNode(Node childNode) throws RepositoryException {
                return childNode.getParent();
            }

            @Override
            protected boolean isParentNodeVisible() {
                return false;
            }
        };
    }

    public static Benchmark withSessionAPIAndParentNotVisible(boolean runAsAdmin) {
        return new GetParentNodeTest("GetParentNotVisibleSessionAPI", runAsAdmin) {
            @Override
            protected Node getParentNode(Node childNode) throws RepositoryException {
                return childNode.getSession().getNode(parentNodePath);
            }

            @Override
            protected boolean isParentNodeVisible() {
                return false;
            }
        };
    }

    protected GetParentNodeTest(String name, boolean runAsAdmin) {
        this.name = name;
        this.runAsAdmin = runAsAdmin;
    }

    protected abstract Node getParentNode(Node childNode) throws RepositoryException;

    protected Session login() {
        if (runAsAdmin) {
            return loginAdministrative();
        } else {
            return loginAnonymous();
        }
    }

    protected abstract boolean isParentNodeVisible();

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

        if (!isParentNodeVisible()) {
            addAccessControlEntry(session, parentNodePath, EveryonePrincipal.getInstance(),
                    new String[]{JCR_READ}, false);
            addAccessControlEntry(session, childNodePath, EveryonePrincipal.getInstance(),
                    new String[]{JCR_READ}, true);
        }
        session.save();

        session.logout();
    }

    @Override
    protected void runTest() throws Exception {
        Session session = login();
        Node child = session.getNode(childNodePath);
        for (int i = 0; i < 10000; i++) {
            try {
                getParentNode(child);
            } catch (RepositoryException e){
                //If parent node is not visible
            }

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
