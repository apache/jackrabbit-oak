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
 * Benchmark for OAK-1866
 */
public class FlatTreeUpdateTest extends AbstractTest {

    protected static final String ROOT_NODE_NAME = "test" + TEST_ID;

    protected static final int CHILD_COUNT = 10 * 1000;

    private Session session;

    @Override
    public void beforeSuite() throws RepositoryException {
        session = loginWriter();
        System.out.println("start");
        Node node = session.getRootNode().addNode(ROOT_NODE_NAME, "oak:Unstructured");
        for (int i = 0; i < CHILD_COUNT; i++) {
            node.addNode("node" + i, "oak:Unstructured");
        }
        session.save();
        System.out.println("end");
    }

    @Override
    public void runTest() throws Exception {
        Node node = session.getRootNode().getNode(ROOT_NODE_NAME);
        for (int i = 1; i < CHILD_COUNT; i++) {
            node.getNode("node" + i).setProperty("foo", "bar");
            session.save();
            node.getNode("node" + i).getProperty("foo").remove();
            node.getNode("node0").setProperty("foo", i);
            session.save();
        }
    }

}
