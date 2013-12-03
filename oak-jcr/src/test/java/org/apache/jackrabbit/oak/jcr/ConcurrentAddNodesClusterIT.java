/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.mongomk.MongoMK;
import org.apache.jackrabbit.oak.plugins.mongomk.util.MongoConnection;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Concurrently add nodes with multiple sessions on multiple cluster nodes.
 */
@Ignore("OAK-1254")
public class ConcurrentAddNodesClusterIT {

    private static final int NUM_CLUSTER_NODES = 3;
    private static final int NODE_COUNT = 100;
    private static final String PROP_NAME = "testcount";

    private List<MongoMK> mks = new ArrayList<MongoMK>();
    private List<Thread> workers = new ArrayList<Thread>();

    @BeforeClass
    public static void mongoDBAvailable() {
        Assume.assumeTrue(OakMongoMKRepositoryStub.isMongoDBAvailable());
    }

    @Before
    public void before() throws Exception {
        dropDB();
        initRepository();
        for (int i = 0; i < NUM_CLUSTER_NODES; i++) {
            MongoMK mk = new MongoMK.Builder()
                    .setMongoDB(createConnection().getDB())
                    .setClusterId(i + 1).open();
            mks.add(mk);
        }
    }

    @After
    public void after() throws Exception {
        for (MongoMK mk : mks) {
            mk.dispose();
        }
        dropDB();
    }

    @Test
    public void addNodes() throws Exception {
        Map<String, Exception> exceptions = Collections.synchronizedMap(new HashMap<String, Exception>());
        for (int i = 0; i < mks.size(); i++) {
            MongoMK mk = mks.get(i);
            Repository repo = new Jcr(mk).createRepository();
            workers.add(new Thread(new Worker(repo, exceptions), "Worker-" + (i + 1)));
        }
        for (Thread t : workers) {
            t.start();
        }
        for (Thread t : workers) {
            t.join();
        }
        for (Map.Entry<String, Exception> entry : exceptions.entrySet()) {
            // System.out.println("exception in thread " + entry.getKey());
            throw entry.getValue();
        }
    }

    private static MongoConnection createConnection() throws Exception {
        return OakMongoMKRepositoryStub.createConnection(
                ConcurrentAddNodesClusterIT.class.getSimpleName());
    }

    private static void dropDB() throws Exception {
        MongoConnection con = createConnection();
        try {
            con.getDB().dropDatabase();
        } finally {
            con.close();
        }
    }

    private static void initRepository() throws Exception {
        MongoConnection con = createConnection();
        MongoMK mk = new MongoMK.Builder()
                .setMongoDB(con.getDB())
                .setClusterId(1).open();
        Session session = new Jcr(mk).createRepository().login(
                new SimpleCredentials("admin", "admin".toCharArray()));
        session.logout();
        mk.dispose(); // closes connection as well
    }


    private static void ensureIndex(Node root, String propertyName)
            throws RepositoryException {
        Node indexDef = root.getNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        if (indexDef.hasNode(propertyName)) {
            return;
        }
        Node index = indexDef.addNode(propertyName, IndexConstants.INDEX_DEFINITIONS_NODE_TYPE);
        index.setProperty(IndexConstants.TYPE_PROPERTY_NAME,
                PropertyIndexEditorProvider.TYPE);
        index.setProperty(IndexConstants.REINDEX_PROPERTY_NAME,
                true);
        index.setProperty(IndexConstants.PROPERTY_NAMES,
                new String[] { propertyName }, PropertyType.NAME);
        try {
            root.getSession().save();
        } catch (RepositoryException e) {
            // created by other thread -> ignore
        }
    }

    private static final class Worker implements Runnable {

        private final Repository repo;
        private final Map<String, Exception> exceptions;

        Worker(Repository repo, Map<String, Exception> exceptions) {
            this.repo = repo;
            this.exceptions = exceptions;
        }

        @Override
        public void run() {
            try {
                Session session = repo.login(new SimpleCredentials(
                        "admin", "admin".toCharArray()));
                ensureIndex(session.getRootNode(), PROP_NAME);

                String nodeName = "testroot-" + Thread.currentThread().getName();
                Node root = session.getRootNode().addNode(nodeName, "nt:unstructured");
                for (int i = 0; i < NODE_COUNT; i++) {
                    Node node = root.addNode(PROP_NAME + i, "nt:unstructured");
                    for (int j = 0; j < NODE_COUNT; j++) {
                        Node child = node.addNode("node" + j, "nt:unstructured");
                        child.setProperty(PROP_NAME, j);
                    }
                    if (!exceptions.isEmpty()) {
                        break;
                    }
                    session.save();
                }
            } catch (Exception e) {
                exceptions.put(Thread.currentThread().getName(), e);
            }
        }
    }
}
