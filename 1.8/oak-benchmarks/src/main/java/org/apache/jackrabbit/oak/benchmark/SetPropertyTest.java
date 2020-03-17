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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.NodeTypeManager;

import org.apache.jackrabbit.oak.fixture.RepositoryFixture;

import com.google.common.collect.Maps;

/**
 * Test for measuring the performance of setting a single property and
 * saving the change.
 */
public class SetPropertyTest extends AbstractTest {

    /**
     * Map holding node to be tested (below testNodeName) per current running fixture, per thread.
     * A simpler structure holding nodes on a per thread basis is not enough, as runs with concurrencyLevel==1 
     * and warm-up in {@link AbstractTest} are done from the same thread (re-using it between fixtures).
     */
    private Map<String, Map<Thread,Node>> map = new HashMap<>();
    
    String testNodeName = "test" + TEST_ID;

    @Override
    public void beforeSuite() throws RepositoryException {
        Session session = getRepository().login(getCredentials());
        session.getRootNode().addNode(testNodeName, getUnstructuredNodeType(session));
        session.save();
        session.logout();
    }

    @Override
    public void beforeTest() throws RepositoryException {
        Map<Thread, Node> nodes = getOrCreateNodesMap();
        
        Thread t = Thread.currentThread();
        Node node = nodes.get(t);
        if (node == null) {
            Session s = getRepository().login(getCredentials());
            node = s.getRootNode().getNode(testNodeName).addNode(UUID.randomUUID().toString());
            node.setProperty("count", -1);
            s.save();
            nodes.put(t, node);
            map.put(getCurrentFixture().toString(), nodes);
        }
    }

    @Override
    public void runTest() throws Exception {
        Map<Thread, Node> nodes = getOrCreateNodesMap();
        
        Node node = nodes.get(Thread.currentThread());
        Session session = node.getSession();
        for (int i = 0; i < 1000; i++) {
            node.setProperty("count", i);
            session.save();
        }
    }

    @Override
    public void afterSuite() throws RepositoryException {
        Session session = getRepository().login(getCredentials());
        session.getRootNode().getNode(testNodeName).remove();
        session.save();
        session.logout();
    }

    private String getUnstructuredNodeType(Session s)
            throws RepositoryException {
        NodeTypeManager ntMgr = s.getWorkspace().getNodeTypeManager();
        if (ntMgr.hasNodeType("oak:Unstructured")) {
            return "oak:Unstructured";
        } else {
            return "nt:unstructured";
        }
    }
    
    private Map<Thread, Node> getOrCreateNodesMap() {
        RepositoryFixture currentFixture = getCurrentFixture();
        if (currentFixture == null) {
            throw new RuntimeException("Current running fixture was not correctly set!");
        }

        String currentFixtureName = currentFixture.toString();
        
        Map<Thread, Node> nodes = map.get(currentFixtureName);
        if (nodes == null) {
            nodes = Maps.newIdentityHashMap();
        }
        return nodes;
    }
}
