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

import static javax.jcr.security.Privilege.JCR_READ;
import static org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils.addAccessControlEntry;

import javax.jcr.Node;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

/**
 * {@code GetNodeTest} implements a performance test, which reads
 * nodes from the repository. To determine the effect of access control
 * evaluation the test can either run with anonymous or with admin.
 */
public abstract class GetNodeTest extends AbstractTest {
    private final String name;

    private Node testRoot;

    public static Benchmark withAdmin() {
        return new GetNodeTest("GetNodeWithAdmin") {
            @Override
            protected Session login() {
                return loginWriter();
            }
        };
    }

    public static Benchmark withAnonymous() {
        return new GetNodeTest("GetNodeWithAnonymous") {
            @Override
            protected Session login() {
                return loginAnonymous();
            }
        };
    }

    protected GetNodeTest(String name) {
        this.name = name;
    }

    protected abstract Session login();

    @Override
    public String toString() {
        return name;
    }

    @Override
    protected void beforeSuite() throws Exception {
        Session session = loginWriter();
        testRoot = session.getRootNode().addNode(
                getClass().getSimpleName() + TEST_ID, "nt:unstructured");
        testRoot.addNode("node1").addNode("node2");

        addAccessControlEntry(session, testRoot.getPath(), EveryonePrincipal.getInstance(),
                new String[] {JCR_READ}, true);
        session.save();

        testRoot = login().getNode(testRoot.getPath());
        session.logout();
    }

    @Override
    protected void runTest() throws Exception {
        for (int i = 0; i < 10000; i++) {
            testRoot.getNode("node1").getNode("node2");
            testRoot.getNode("node1/node2");
            testRoot.hasNode("node-does-not-exist");
        }
    }

    @Override
    protected void afterSuite() throws Exception {
        Session session = loginWriter();
        session.getNode(testRoot.getPath()).remove();
        testRoot.getSession().logout();
        session.save();
        session.logout();
    }
}
