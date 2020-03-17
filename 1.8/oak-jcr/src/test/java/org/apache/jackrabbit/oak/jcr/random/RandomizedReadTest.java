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


import java.security.Principal;
import java.util.Random;

import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Session;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Assert;
import org.junit.Test;

public class RandomizedReadTest extends AbstractRandomizedTest {

    private static final int depth = 4;
    private static final Table<Integer, Integer, String> tree = HashBasedTable.create();
    static {
        tree.put(0, 0, "/");
        tree.put(1, 0, "/n1");
        tree.put(1, 1, "/n2");
        tree.put(2, 0, "/n1/n3");
        tree.put(2, 1, "/n1/n4");
        tree.put(2, 2, "/n1/n5");
        tree.put(3, 0, "/n1/n3/n6");
        tree.put(3, 1, "/n1/n3/n7");
        tree.put(3, 2, "/n1/n3/n8");
        tree.put(3, 3, "/n1/n3/n9");
    }

    @Override
    protected void setupContent() throws Exception {
        for (JackrabbitSession session : writeSessions) {
            Node root = session.getRootNode();
            Node n1 = root.addNode("n1");
            Node n3 = n1.addNode("n3");
            n1.addNode("n4");
            n1.addNode("n5");
            n3.addNode("n6");
            n3.addNode("n7");
            n3.addNode("n8");
            n3.addNode("n9");
            root.addNode("n2");

            Principal principal = getTestPrincipal(session);
            AccessControlManager acm = session.getAccessControlManager();
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acm, "/");
            acl.addEntry(principal, AccessControlUtils.privilegesFromNames(acm, PrivilegeConstants.JCR_READ), true);
            acm.setPolicy("/", acl);

            session.save();
        }
    }

    @Override
    protected void clearContent() throws Exception {
        for (JackrabbitSession session : writeSessions) {
            Node root = session.getRootNode();
            if (root.hasNode("n1")) {
                root.getNode("n1").remove();
            }
            if (root.hasNode("n2")) {
                root.getNode("n2").remove();
            }

            AccessControlList acl = AccessControlUtils.getAccessControlList(session, "/");
            if (acl != null) {
                boolean modified = false;
                for (AccessControlEntry ace : acl.getAccessControlEntries()) {
                    if (getTestPrincipal(session).equals(ace.getPrincipal())) {
                        acl.removeAccessControlEntry(ace);
                        modified = true;
                    }
                }
                if (modified) {
                    session.getAccessControlManager().setPolicy("/", acl);
                }
            }
            session.save();
        }
    }

    @Test
    public void testReadAcl() throws Exception {
        for (int j = 0; j < 1; j++) {
            Random r = new Random(j);
            int operations = 1000;
            int depthToApply;
            int index;
            boolean allow;
            int principalIndex;

            for (int i = 0; i < operations; i++) {
                allow = r.nextBoolean();
                depthToApply = r.nextInt(depth);
                principalIndex = r.nextInt(ids.length);

                if (depthToApply > 0) {
                    index = r.nextInt(depthToApply + 1);
                    String path = getPath(depthToApply, index);
                    setupPermissions(principalIndex, path, allow, PrivilegeConstants.JCR_READ);
                    check();
                }
            }
        }
    }

    private static String getPath(int depth, int index) throws Exception {
        if (depth == 0) {
            return "/";
        }
        return tree.get(depth, index);
    }

    public void check() throws Exception {
        boolean mustThrow;
        try {
            for (String path : tree.values()) {
                mustThrow = false;

                Session s1 = readSessions.get(0);
                try {
                    Node n = s1.getNode(path);
                    if (!path.equals(n.getPath())) {
                        Assert.fail("did not resolved the same node");
                    }
                } catch (PathNotFoundException pnf) {
                    mustThrow = true;
                }

                for (int i = 1; i < readSessions.size(); i++) {
                    try {
                        Node n = readSessions.get(i).getNode(path);
                        if (mustThrow) {
                            Assert.fail("did not throw for path " + path);
                        }
                        if (!path.equals(n.getPath())) {
                            Assert.fail("did not resolved the same node");
                        }
                    } catch (PathNotFoundException pnf) {
                        if (!mustThrow) {
                            Assert.fail("did throw for path " + path);
                        }
                    }
                }
            }

        } catch (Exception e) {
            throw new Exception(e);
        }
    }

    private void setupPermissions(int principalIndex, String path, boolean allow, String... privilegeNames) throws Exception {
        for (JackrabbitSession session : writeSessions) {
            Principal principal = getPrincipal(session, principalIndex);
            AccessControlManager acm = session.getAccessControlManager();
            JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acm, path);
            acl.addEntry(principal, AccessControlUtils.privilegesFromNames(acm, privilegeNames), allow);
            acm.setPolicy(path, acl);
            session.save();
        }
    }

}
