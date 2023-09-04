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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.jcr.Node;
import javax.jcr.Session;

/**
 * Performs a move and then measures how long it takes to access {@code Node}
 * objects with pending move operations.
 */
public class AccessAfterMoveTest extends AbstractTest<Object> {

    private Session session;

    private Node root;

    private Node testNode;

    private final List<Node> nodes = new ArrayList<>();

    @Override
    protected void beforeSuite() throws Exception {
        session = getRepository().login(getCredentials());
        root = session.getRootNode().addNode(
                getClass().getSimpleName() + TEST_ID, "nt:unstructured");
        testNode = root.addNode(UUID.randomUUID().toString());
        session.save();
        for (int i = 0; i < 10_000; i++) {
            nodes.add(root.getNode(testNode.getName()));
        }
    }

    @Override
    protected void beforeTest() throws Exception {
        session.move(testNode.getPath(), root.getPath() + "/" + UUID.randomUUID());
        session.save();
    }

    @Override
    protected void runTest() throws Exception {
        for (Node n : nodes) {
            n.getName();
        }
    }

    @Override
    protected void afterSuite() throws Exception {
        root.remove();
        session.save();
        session.logout();
    }
}
