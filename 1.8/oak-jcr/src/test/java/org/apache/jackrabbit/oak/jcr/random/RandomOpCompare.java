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
package org.apache.jackrabbit.oak.jcr.random;

import static org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest.dispose;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;

import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.NodeStoreFixtures;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.mongodb.DB;

/**
 * A randomized test that writes to two repositories (using different storage
 * backends), and compares the results. The test uses low cache sizes, and low
 * thresholds / limits so that as much of the code as possible is used (high
 * code coverage).
 */
public class RandomOpCompare {
    
    private static final int SESSION_COUNT = 3;
    
    protected NodeStoreFixture f1;
    protected NodeStoreFixture f2;
    protected NodeStore ns1, ns2;
    protected Repository r1, r2;
    protected Session session1, session2;
    protected Session[] sessionList1 = new Session[SESSION_COUNT];
    protected Session[] sessionList2 = new Session[SESSION_COUNT];
    
    private Random random;
    
    public static void main(String... args) throws Exception {
        RandomOpCompare app = new RandomOpCompare();
        app.login();
        app.test();
        app.logout();
    }
    
    static {
        
        // TODO changes to system properties are not picked up if other tests
        // are run first

        // code coverage oak.plugins.document: 48.1%

        // code coverage 46.3% (8829) - with
        // DocumentRootBuilder
        System.setProperty("update.limit", "2");
        // System.setProperty("oak.documentMK.childrenCacheLimit", "1024");

        // code coverage 39.8% (7583) - without
    }

    // @Before
    public void login() throws RepositoryException {
        f1 = NodeStoreFixtures.SEGMENT_TAR;
        f2 = getMongo();
        
        ns1 = f1.createNodeStore();
        r1  = new Jcr(ns1).createRepository();
        session1 = r1.login(new SimpleCredentials("admin", "admin".toCharArray()));
        for (int i = 0; i < SESSION_COUNT; i++) {
            sessionList1[i] = r1.login(new SimpleCredentials("admin", "admin"
                    .toCharArray()));
        }

        ns2 = f2.createNodeStore();
        r2 = new Jcr(ns2).createRepository();
        session2 = r2.login(new SimpleCredentials("admin", "admin".toCharArray()));
        for (int i = 0; i < SESSION_COUNT; i++) {
            sessionList2[i] = r2.login(new SimpleCredentials("admin", "admin"
                    .toCharArray()));
        }
    }
    
    // @After
    public void logout() {
        if (session1 != null) {
            session1.logout();
            session1 = null;
        }
        for (Session s : sessionList1) {
            s.logout();
        }
        if (session2 != null) {
            session2.logout();
            session2 = null;
        }
        for (Session s : sessionList2) {
            s.logout();
        }
        r1 = dispose(r1);
        r2 = dispose(r2);
        if (ns1 != null) {
            f1.dispose(ns1);
        }
        if (ns2 != null) {
            f2.dispose(ns2);
        }
    }
    
    private static NodeStoreFixture getMongo() {
        return new NodeStoreFixture() {
            @Override
            public NodeStore createNodeStore() {
                MongoConnection connection;
                try {
                    connection = new MongoConnection("mongodb://localhost:27017/oak");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                DB mongoDB = connection.getDB();                
                return new DocumentMK.Builder().
                            memoryCacheSize(0).
                            setMongoDB(mongoDB, 16).
                            setPersistentCache("target/persistentCache,time").
                            getNodeStore();
            }
    
            @Override
            public NodeStore createNodeStore(int clusterNodeId) {
                return null;
            }
    
            @Override
            public void dispose(NodeStore nodeStore) {
                if (nodeStore instanceof Closeable) {
                    try {
                        ((Closeable) nodeStore).close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        };
    }

    // @Test
    public void test() throws RepositoryException {
        random = new Random(1);
        String longName = longNodeName(0);
        Node root1 = session1.getRootNode().addNode("testNodeRoot").
                addNode(longName).addNode(longName).addNode(longName);
        session1.save();
        Node root2 = session2.getRootNode().addNode("testNodeRoot").
                addNode(longName).addNode(longName).addNode(longName);
        session2.save();
        System.out.println("len: " + root1.getPath().length());
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            long now = System.currentTimeMillis();
            if (now - start > 1000) {
                System.out.println("i: " + i);
                start = now;
            }
            String nodeName = randomNodeName();
            String propertyName = "p" + random.nextInt(2);
            String value = "x" + random.nextInt(10);
            int sessionId = random.nextInt(SESSION_COUNT);
            Session s1 = sessionList1[sessionId];
            Session s2 = sessionList1[sessionId];
            switch (random.nextInt(3)) {
            case 0:
                if (root1.hasNode(nodeName)) {
                    assertTrue(root2.hasNode(nodeName));
                    root1.getNode(nodeName).remove();
                    root2.getNode(nodeName).remove();
                } else {
                    assertFalse(root2.hasNode(nodeName));
                    root1.addNode(nodeName);
                    root2.addNode(nodeName);
                }
                break;
            case 1:
                if (root1.hasNode(nodeName)) {
                    assertTrue(root2.hasNode(nodeName));
                    Node n1 = root1.getNode(nodeName);
                    Node n2 = root1.getNode(nodeName);
                    if (n1.hasProperty(propertyName)) {
                        assertTrue(n2.hasProperty(propertyName));
                        assertEquals(
                                n1.getProperty(propertyName).
                                getValue().getString(),
                                n2.getProperty(propertyName).
                                getValue().getString());
                    } else {
                        assertFalse(n2.hasProperty(propertyName));
                    }
                    n1.setProperty(propertyName, value);
                    n2.setProperty(propertyName, value);
                }                
                break;
            case 2:
                s1.save();
                s2.save();
                break;
            }
        }
    }
    
    private String randomNodeName() {
        if (random.nextInt(50) == 0) {
            return longNodeName(random.nextInt(5));
        }
        return "n" + random.nextInt(50);
        
    }
    
    private static String longNodeName(int x) {
        StringBuilder buff = new StringBuilder("n");
        for (int i = 0; i < 140; i++) {
            buff.append('x');
        }
        buff.append(x);
        return buff.toString();
    }
    
    
}
