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

public class ConcurrentCreateNodesTest extends AbstractTest {

    protected static final String ROOT_NODE_NAME = "test" + TEST_ID;
	private static final int WORKER_COUNT = 20;
	private static final int NODE_COUNT_LEVEL2 = 50;

	@Override
	protected void beforeSuite() throws Exception {
        Session session = loginWriter();
        Node rootNode = session.getRootNode();
        if (rootNode.hasNode(ROOT_NODE_NAME)) {
        	Node root = rootNode.getNode(ROOT_NODE_NAME);
        	root.remove();
        }
        Node root = session.getRootNode().addNode(ROOT_NODE_NAME, "nt:unstructured");
        session.save();
        for (int i = 1; i < WORKER_COUNT; i++) {
            addBackgroundJob(new Writer(i));
        }
    }

    private class Writer implements Runnable {

        private final Session session = loginWriter();
		private final int id;

        private Writer(int id) {
        	this.id = id;
        }
        
        @Override
        public void run() {
            try {
                session.refresh(false);
                
            	Node root = session.getRootNode().getNode(ROOT_NODE_NAME);
                Node node = root.addNode("node" + id, "nt:unstructured");
                for (int j = 0; j < NODE_COUNT_LEVEL2; j++) {
                    Node newNode = node.addNode("node" + j, "nt:unstructured");
                    session.save();
                }
                node.remove();
                session.save();
            } catch (RepositoryException e) {
            	e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

    }
    
    @Override
    public void runTest() throws Exception {
    	new Writer(0).run();
    }
    
    @Override
    protected void afterSuite() throws Exception {
        Session session = loginWriter();
        Node root = session.getRootNode().getNode(ROOT_NODE_NAME);
        root.remove();
        session.save();
    }

}
