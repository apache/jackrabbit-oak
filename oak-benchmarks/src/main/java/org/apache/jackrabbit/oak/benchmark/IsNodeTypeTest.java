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

import static javax.jcr.security.Privilege.JCR_READ;
import static org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils.addAccessControlEntry;
import static org.junit.Assert.assertTrue;

import javax.jcr.Node;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

/**
 * Benchmark for Node.isNodeType(String).
 */
public class IsNodeTypeTest extends AbstractTest<Node> {

    private static final String NT_FOLDER = "nt:folder";

    private final boolean runAsAdmin;

    private String testNodeName = "test" + TEST_ID;

    private Node testNode;

    public IsNodeTypeTest(boolean runAsAdmin) {
        this.runAsAdmin = runAsAdmin;
    }

    @Override
    public void beforeSuite() throws Exception {
        Session session = getRepository().login(getCredentials());
        session.getRootNode().addNode(testNodeName, NT_FOLDER);
        addAccessControlEntry(session, "/", EveryonePrincipal.getInstance(), new String[] { JCR_READ }, true);
        session.save();
        session.logout();
        testNode = prepareThreadExecutionContext();
    }

    @Override
    public void afterSuite() throws Exception {
        Session session = getRepository().login(getCredentials());
        session.getRootNode().getNode(testNodeName).remove();
        session.save();
        session.logout();
        disposeThreadExecutionContext(testNode);
    }

    @Override
    protected Node prepareThreadExecutionContext() throws Exception {
        return getTestSession().getRootNode().getNode(testNodeName);
    }

    private Session getTestSession() {
        if (runAsAdmin) {
            return loginWriter();
        } else {
            return loginAnonymous();
        }
    }

    @Override
    protected void disposeThreadExecutionContext(Node context)
            throws Exception {
        context.getSession().logout();
    }

    @Override
    protected void runTest(Node executionContext) throws Exception {
        for (int i = 0; i < 100000; i++) {
            assertTrue(executionContext.isNodeType(NT_FOLDER));
        }
    }

    @Override
    protected void runTest() throws Exception {
        runTest(testNode);
    }
}
