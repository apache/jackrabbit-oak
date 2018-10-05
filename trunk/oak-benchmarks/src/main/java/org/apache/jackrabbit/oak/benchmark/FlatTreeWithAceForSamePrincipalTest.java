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

import java.security.Principal;

import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;

public class FlatTreeWithAceForSamePrincipalTest extends AbstractTest {

    private static final String TEST_USER_ID = "test" + TEST_ID;
    private static final String ROOT_NODE_NAME = "test" + TEST_ID;
    private static final String ROOT_PATH = "/" + ROOT_NODE_NAME;

    private UserManager userManager;

    private Session admin;
    private Session reader;

    @Override
    protected void beforeSuite() throws Exception {

        long start = System.currentTimeMillis();

        admin = loginWriter();
        userManager = ((JackrabbitSession) admin).getUserManager();
        Principal userPrincipal = userManager.createUser(TEST_USER_ID, TEST_USER_ID).getPrincipal();

        AccessControlManager acm = admin.getAccessControlManager();
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acm, "/");
        acl.addEntry(userPrincipal, AccessControlUtils.privilegesFromNames(acm, PrivilegeConstants.JCR_READ), true);
        acm.setPolicy("/", acl);

        Node a = admin.getRootNode().addNode(ROOT_NODE_NAME, "nt:folder");
        for (int i = 1; i < 10000; i++) {
            a.addNode("node" + i, "nt:folder");
            acl = AccessControlUtils.getAccessControlList(acm, ROOT_PATH + "/node" + i);
            acl.addEntry(userPrincipal, AccessControlUtils.privilegesFromNames(acm, PrivilegeConstants.JCR_READ), true);
            acm.setPolicy(ROOT_PATH + "/node" + i, acl);
        }

        admin.save();
        reader = login(new SimpleCredentials(TEST_USER_ID, TEST_USER_ID.toCharArray()));

        long end = System.currentTimeMillis();
        System.out.println("setup time " + (end - start));
    }

    @Override
    protected void runTest() throws Exception {
        Node n = reader.getNode(ROOT_PATH);
        for (int i = 1; i < 10000; i++) {
            n.getNode("node" + i);
        }
    }

    protected void afterSuite() throws Exception {
        Node root = admin.getRootNode();
        if (root.hasNode(ROOT_NODE_NAME)) {
            root.getNode(ROOT_NODE_NAME).remove();
        }
        if (userManager != null) {
            userManager.getAuthorizable(TEST_USER_ID).remove();
        }
        admin.save();
    }
}
