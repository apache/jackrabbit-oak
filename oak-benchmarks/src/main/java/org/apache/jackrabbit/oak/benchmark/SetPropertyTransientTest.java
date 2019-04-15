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
 * Benchmark for transient Node.setProperty(String)
 */
public class SetPropertyTransientTest extends AbstractTest<Node> {

    private static final String NT_UNSTRUCTURED = "nt:unstructured";

    private String testNodeName = "test" + TEST_ID;

    private Node testNode;

    @Override
    public void beforeSuite() throws Exception {
        Session session = getRepository().login(getCredentials());
        session.getRootNode().addNode(testNodeName, NT_UNSTRUCTURED);
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
        return loginWriter().getRootNode().getNode(testNodeName);
    }

    @Override
    protected void disposeThreadExecutionContext(Node context)
            throws Exception {
        context.getSession().logout();
    }

    @Override
    protected void beforeTest(Node executionContext) {
        try {
            executionContext.getSession().refresh(false);
        } catch (RepositoryException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void runTest(Node executionContext) throws Exception {
        for (int i = 0; i < 100000; i++) {
            executionContext.setProperty("p", i);
        }
    }

    @Override
    protected void beforeTest() throws Exception {
        testNode.getSession().refresh(false);
    }

    @Override
    protected void runTest() throws Exception {
        runTest(testNode);
    }
}
