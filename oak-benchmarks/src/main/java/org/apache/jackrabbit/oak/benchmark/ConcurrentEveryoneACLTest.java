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

import javax.jcr.ItemVisitor;
import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;
import javax.jcr.util.TraversingItemVisitor;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

/**
 * Test case that randomly reads from 10k unstructured nodes that are all access controlled with an everyone ACL.
 */
public class ConcurrentEveryoneACLTest extends AbstractTest {

    private final Random random = new Random();

    protected static final int NODE_COUNT = 100;

    protected static final String ROOT_NODE_NAME = "test" + TEST_ID;

    private final boolean runAsAdmin;

    private final int itemsToRead;

    public ConcurrentEveryoneACLTest(boolean runAsAdmin, int itemsToRead) {
        this.runAsAdmin = runAsAdmin;
        this.itemsToRead = itemsToRead;
    }

    @Override
    public void beforeSuite() throws Exception {
        Session session = loginWriter();
        AccessControlManager acMgr = session.getAccessControlManager();
        Privilege[] privileges = new Privilege[] {
                acMgr.privilegeFromName(Privilege.JCR_READ),
                acMgr.privilegeFromName(Privilege.JCR_READ_ACCESS_CONTROL)
        };
        final Node root = session.getRootNode().addNode(ROOT_NODE_NAME, "nt:unstructured");
        for (int i = 0; i < NODE_COUNT; i++) {
            Node node = root.addNode("node" + i, "nt:unstructured");
            for (int j = 0; j < NODE_COUNT; j++) {
                Node newNode = node.addNode("node" + j, "nt:unstructured");
                JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(session, newNode.getPath());
                acl.addEntry(EveryonePrincipal.getInstance(), privileges, true);
                acMgr.setPolicy(newNode.getPath(), acl);
            }
            session.save();
        }
        // deny everyone on root node
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(session, root.getPath());
        acl.addEntry(EveryonePrincipal.getInstance(), privileges, false);
        acMgr.setPolicy(root.getPath(), acl);
        session.save();

        final int[] numACEs = new int[1];
        ItemVisitor v = new TraversingItemVisitor.Default() {
            @Override
            protected void entering(Node node, int i) throws RepositoryException {
                if (node.isNodeType(AccessControlConstants.NT_REP_ACE)) {
                    numACEs[0]++;
                }
                super.entering(node, i);
            }
            @Override
            protected void entering(Property prop, int i) throws RepositoryException {
                super.entering(prop, i);
            }
        };
        v.visit(root);
        System.out.println("Num ACEs: " + numACEs[0]);

        session.logout();
    }

    @Override
    public void afterSuite() throws Exception {
        Session session = loginWriter();
        Node root = session.getRootNode().getNode(ROOT_NODE_NAME);
        root.remove();
        session.logout();
    }

    @Override
    public void runTest() throws Exception {
        Session session = null;
        try {
            session = runAsAdmin ? loginWriter() : loginAnonymous();
            for (int i=0; i<itemsToRead; i++) {
                session.refresh(false);
                int a = random.nextInt(NODE_COUNT);
                int b = random.nextInt(NODE_COUNT);
                String path = "/" + ROOT_NODE_NAME + "/node" + a + "/node" + b + "/jcr:primaryType";
                session.getProperty(path).getString();
            }
        } finally {
            if (session != null) {
                session.logout();
            }
        }
    }


}
