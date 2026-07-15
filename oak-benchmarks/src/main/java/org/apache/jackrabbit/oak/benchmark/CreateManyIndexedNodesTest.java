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

import java.util.Random;
import java.util.UUID;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils;
import org.apache.jackrabbit.oak.benchmark.util.OakIndexUtils.PropertyIndex;

/**
 * Test for measuring the performance of creating many nodes
 * which contain a property that is indexed
 * <p>
 * This test does not want to replicate the many-child-nodes test, where
 * all the child nodes are created from the same parent. instead, this test
 * spreads out the nodes over a number of parent.
 */
public class CreateManyIndexedNodesTest extends AbstractTest {
    
    private static final String NODE_TYPE = "oak:Unstructured";

    protected static final String ROOT_NODE_NAME = "test" + TEST_ID;

    private static final int PARENT_COUNT = 100;
    
    private static final int THREAD_COUNT = 1;
    private static final int NODES_PER_THREAD = 50;

    private Session session;

    private Writer foregroundJob;

    @Override
    public void beforeSuite() throws RepositoryException {
        session = loginWriter();
        Node rootNode = session.getRootNode().addNode(ROOT_NODE_NAME, NODE_TYPE);
        Node testNode = rootNode.addNode("testNode", NODE_TYPE);
        for(int i=0; i<PARENT_COUNT; i++) {
            Node level1Node = testNode.addNode("level1_"+i, NODE_TYPE);
        }
        session.save();
        PropertyIndex index = new OakIndexUtils.PropertyIndex();
        index.property("indexedProperty");
        index.create(session);
        session.save();
        
        for(int i=0; i<THREAD_COUNT-1; i++) {
            addBackgroundJob(new Writer());
        }
        
        foregroundJob = new Writer();
    }

    private class Writer implements Runnable {

        private final Random random = new Random();

        private final Session session = loginWriter();

        private long count;

        public void run() {
            try {
                for(int i=0; i<NODES_PER_THREAD; i++) {
                    session.refresh(false);
                    int level1 = random.nextInt(PARENT_COUNT);
                    Node level1Node = session.getNode("/"+ROOT_NODE_NAME+"/testNode/level1_"+level1);
                    String randomName = UUID.randomUUID().toString();
                    Node newNode = level1Node.addNode(randomName);
                    newNode.setProperty("indexedProperty", randomName);
                    session.save();
                }
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void beforeTest() throws RepositoryException {
        // nothing so far
    }

    @Override
    public void runTest() throws Exception {
        foregroundJob.run();
    }

    @Override
    public void afterTest() throws RepositoryException {
        // nothing so far
    }
    
    @Override
    protected void afterSuite() throws Exception {
//        session.getRootNode().getNode(ROOT_NODE_NAME).remove();
        session.save();
    }
    
}
