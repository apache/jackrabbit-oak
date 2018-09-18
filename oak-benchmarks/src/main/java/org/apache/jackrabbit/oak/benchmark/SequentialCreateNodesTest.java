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
import javax.jcr.Session;

public class SequentialCreateNodesTest extends AbstractTest {

    protected static final String ROOT_NODE_NAME = "test" + TEST_ID;
	private static final int NODE_COUNT = 25;

    @Override
    public void beforeTest() throws Exception {
        Session session = loginWriter();
        Node root = session.getRootNode().addNode(ROOT_NODE_NAME, "nt:unstructured");
        for (int i = 0; i < NODE_COUNT; i++) {
            Node node = root.addNode("node" + i, "nt:unstructured");
            session.save();
        }
    }

    @Override
	protected void runTest() throws Exception {
        
    	final Session session = loginWriter(); // TODO: anonymous is slow
        
    	Node root = session.getRootNode().getNode(ROOT_NODE_NAME);
        for (int i = 0; i < NODE_COUNT; i++) {
            Node node = root.getNode("node" + i);
            for (int j = 0; j < NODE_COUNT; j++) {
                Node newNode = node.addNode("node" + j, "nt:unstructured");
                session.save();
            }
        }
	}
    
    @Override
    public void afterTest() throws Exception {
        Session session = loginWriter();
        Node root = session.getRootNode().getNode(ROOT_NODE_NAME);
        root.remove();
        session.save();
    }

}
