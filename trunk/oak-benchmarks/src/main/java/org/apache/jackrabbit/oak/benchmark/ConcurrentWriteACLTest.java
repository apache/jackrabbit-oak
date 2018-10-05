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

import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;

/**
 * Test case that traverses 10k unstructured nodes (100x100) while 50 concurrent
 * readers randomly access nodes from within this tree.
 */
public class ConcurrentWriteACLTest extends AbstractTest {

    private final Random random = new Random();

    protected static final int NODE_COUNT = 200;

    protected static final String ROOT_NODE_NAME = "test" + TEST_ID;

    private int numItems;

    public ConcurrentWriteACLTest(int numItems) {
        this.numItems = numItems;
    }

    @Override
    public void beforeSuite() throws Exception {
        Session session = loginWriter();
        Node root = session.getRootNode().addNode(ROOT_NODE_NAME, "nt:unstructured");
        for (int i = 0; i < NODE_COUNT; i++) {
            Node node = root.addNode("node" + i, "nt:unstructured");
            for (int j = 0; j < NODE_COUNT; j++) {
                Node newNode = node.addNode("node" + j, "nt:unstructured");
                newNode.addMixin(AccessControlConstants.MIX_REP_ACCESS_CONTROLLABLE);
            }
            session.save();
        }
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
            session = loginWriter();
            for (int i=0; i<numItems; i++) {
                session.refresh(false);
                int a = random.nextInt(NODE_COUNT);
                int b = random.nextInt(NODE_COUNT);
                String path = "/" + ROOT_NODE_NAME + "/node" + a + "/node" + b;
                AccessControlManager acMgr = session.getAccessControlManager();
                JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(session, path);
                if (acl.isEmpty()) {
                    Privilege[] privileges = new Privilege[] {
                            acMgr.privilegeFromName(Privilege.JCR_READ),
                            acMgr.privilegeFromName(Privilege.JCR_READ_ACCESS_CONTROL)
                    };
                    if (acl.addAccessControlEntry(EveryonePrincipal.getInstance(), privileges)) {
                        acMgr.setPolicy(path, acl);
                    }
                } else {
                    for (AccessControlEntry ace: acl.getAccessControlEntries()) {
                        acl.removeAccessControlEntry(ace);
                    }
                    acMgr.setPolicy(path, acl);
                }
                session.save();
            }
        } catch (InvalidItemStateException e) {
            System.out.printf("error: %s%n", e);
            // ignore
        } finally {
            if (session != null) {
                session.logout();
            }
        }
    }


}
