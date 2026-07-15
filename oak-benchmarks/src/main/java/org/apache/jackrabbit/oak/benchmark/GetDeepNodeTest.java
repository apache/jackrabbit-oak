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

import javax.jcr.Node;
import javax.jcr.Session;

/**
 * {@code GetDeepNodeTest} implements a performance test, which reads
 * a node deep down in the hierarchy.
 */
public class GetDeepNodeTest extends AbstractTest {
    private static final int DEPTH = 20;

    private Session session;
    private Node testRoot;
    private String testPath;

    @Override
    protected void beforeSuite() throws Exception {
        session = loginWriter();
        testRoot = session.getRootNode().addNode(
                getClass().getSimpleName() + TEST_ID, "nt:unstructured");
        Node node = testRoot;
        testPath = "";
        for (int k = 0; k < DEPTH; k++) {
            node = node.addNode("node" + k);
        }

        testPath = node.getPath();
        session.save();
    }

    @Override
    protected void runTest() throws Exception {
        for (int i = 0; i < 10000; i++) {
            session.getNode(testPath);
        }
    }

    @Override
    protected void afterSuite() throws Exception {
        testRoot.remove();
        session.logout();
    }
}
