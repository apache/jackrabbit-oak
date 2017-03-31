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

import java.util.concurrent.atomic.AtomicInteger;

import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.observation.EventIterator;
import javax.jcr.observation.EventListener;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

import static javax.jcr.observation.Event.NODE_ADDED;
import static javax.jcr.observation.Event.NODE_MOVED;
import static javax.jcr.observation.Event.NODE_REMOVED;
import static javax.jcr.observation.Event.PERSIST;
import static javax.jcr.observation.Event.PROPERTY_ADDED;
import static javax.jcr.observation.Event.PROPERTY_CHANGED;
import static javax.jcr.observation.Event.PROPERTY_REMOVED;

public class ConcurrentCreateNodesTest extends AbstractTest {

    public static final int EVENT_TYPES = NODE_ADDED | NODE_REMOVED | NODE_MOVED |
            PROPERTY_ADDED | PROPERTY_REMOVED | PROPERTY_CHANGED | PERSIST;
    protected static final String ROOT_NODE_NAME = "test" + TEST_ID;
    private static final int WORKER_COUNT = Integer.getInteger("workerCount", 20);
    private static final int LISTENER_COUNT = Integer.getInteger("listenerCount", 0);
    private static final boolean NON_ADMIN_LISTENER = Boolean.getBoolean("nonAdminListener");
    private static final String LISTENER_PATH = System.getProperty("listenerPath", "/");
    private static final int ACL_COUNT = Integer.getInteger("aclCount", 0);
    private static final int NODE_COUNT_LEVEL2 = 50;
    private static final String NODE_TYPE = System.getProperty("nodeType", "nt:unstructured");
    private static final boolean DISABLE_INDEX = Boolean.getBoolean("disableIndex");
    private static final boolean VERBOSE = Boolean.getBoolean("verbose");
    private Writer writer;
    private final AtomicInteger NODE_COUNT = new AtomicInteger();

    @Override
    protected void beforeSuite() throws Exception {
        Session session = loginWriter();
        if (DISABLE_INDEX) {
            disableNodeTypeIndex(session);
        }
        Node rootNode = session.getRootNode();
        if (rootNode.hasNode(ROOT_NODE_NAME)) {
            Node root = rootNode.getNode(ROOT_NODE_NAME);
            root.remove();
        }
        rootNode = session.getRootNode().addNode(ROOT_NODE_NAME, NODE_TYPE);
        for (int i = 0; i < WORKER_COUNT; i++) {
            rootNode.addNode("node" + i);
        }
        session.save();
        for (int i = 1; i < WORKER_COUNT; i++) {
            addBackgroundJob(new Writer(rootNode.getPath() + "/node" + i));
        }
        UserManager uMgr = ((JackrabbitSession) session).getUserManager();
        String userId;
        String password;
        if (NON_ADMIN_LISTENER) {
            userId = "user-" + System.currentTimeMillis();
            password = "secret";
            uMgr.createUser(userId, password);
            session.save();
        } else {
            userId = "admin";
            password = "admin";
        }
        createACLsForEveryone(session, ACL_COUNT);

        for (int i = 0; i < LISTENER_COUNT; i++) {
            Session s = login(new SimpleCredentials(userId, password.toCharArray()));
            s.getWorkspace().getObservationManager().addEventListener(
                    new Listener(), EVENT_TYPES, LISTENER_PATH, true, null, null, false);
        }
        writer = new Writer(rootNode.getPath() + "/node" + 0);
    }

    private void createACLsForEveryone(Session session, int numACLs)
            throws RepositoryException {
        AccessControlManager acMgr = session.getAccessControlManager();
        Node listenHere = session.getRootNode().addNode("nodes-with-acl");
        for (int i = 0; i < numACLs; i++) {
            String path = listenHere.addNode("node-" + i).getPath();
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(session, path);
            if (acl.isEmpty()) {
                Privilege[] privileges = new Privilege[] {
                        acMgr.privilegeFromName(Privilege.JCR_READ)
                };
                if (acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privileges)) {
                    acMgr.setPolicy(path, acl);
                }
            }
        }
        session.save();
    }

    private class Writer implements Runnable {

        private final Session session = loginWriter();
        private final String path;
        private int count = 0;

        private Writer(String path) {
            this.path = path;
        }

        @Override
        public void run() {
            try {
                int numNodes = NODE_COUNT.get();
                long time = System.currentTimeMillis();
                session.refresh(false);

                Node root = session.getNode(path);
                Node node = root.addNode("node" + count++);
                for (int j = 0; j < NODE_COUNT_LEVEL2; j++) {
                    node.addNode("node" + j);
                    session.save();
                    NODE_COUNT.incrementAndGet();
                }
                numNodes = NODE_COUNT.get() - numNodes;
                time = System.currentTimeMillis() - time;
                if (this == writer && VERBOSE) {
                    long perSecond = numNodes * 1000 / time;
                    System.out.println("Created " + numNodes + " in " + time + " ms. (" + perSecond + " nodes/sec)");
                }
            } catch (RepositoryException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }

    }

    private class Listener implements EventListener {

        @Override
        public void onEvent(EventIterator events) {
            try {
                while (events.hasNext()) {
                    events.nextEvent().getPath();
                }
            } catch (RepositoryException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }
    
    @Override
    public void runTest() throws Exception {
        writer.run();
    }

    private void disableNodeTypeIndex(Session session) throws RepositoryException {
        if (!session.nodeExists("/oak:index/nodetype")) {
            return;
        }
        Node ntIndex = session.getNode("/oak:index/nodetype");
        ntIndex.setProperty(IndexConstants.REINDEX_PROPERTY_NAME, true);
        ntIndex.setProperty(IndexConstants.DECLARING_NODE_TYPES, new String[0], PropertyType.NAME);
        session.save();
    }
}
