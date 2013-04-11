/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.security.authorization;
 
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.Principal;

import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlManager;
import javax.security.auth.login.LoginException;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Before;
import org.junit.Test;

public class ShadowInvisibleContentTest extends AbstractSecurityTest {
	private static final String USER_ID = "test";

	private Principal userPrincipal;
  
    @Before
    @Override
    public void before() throws Exception {
        super.before();
        
        User user = getUserManager().createUser(USER_ID, USER_ID);
        userPrincipal = user.getPrincipal();

        NodeUtil a = new NodeUtil(root.getTree("/")).addChild("a", NT_UNSTRUCTURED);
        a.setString("x", "xValue");
        NodeUtil b = a.addChild("b", NT_UNSTRUCTURED);
        b.setString("y", "yValue");
        NodeUtil c = b.addChild("c", NT_UNSTRUCTURED);
        c.setString("propName3", "strValue");
    }
     
    private void setupPermission(Principal principal, String path, boolean isAllow, String privilegeName)
            throws CommitFailedException, RepositoryException {

        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        acl.addEntry(principal,AccessControlUtils.privilegesFromNames(acMgr, privilegeName) , isAllow);
        acMgr.setPolicy(path, acl);
        root.commit();
    }

    private Root getLatestRoot() throws LoginException, NoSuchWorkspaceException {
        ContentSession contentSession = login(new SimpleCredentials(USER_ID, USER_ID.toCharArray()));
        return contentSession.getLatestRoot();
    }

    @Test
    public void testShadowInvisibleNode() throws CommitFailedException, RepositoryException, LoginException {
        setupPermission(userPrincipal, "/a", true, PrivilegeConstants.JCR_ALL);
        setupPermission(userPrincipal, "/a/b", false, PrivilegeConstants.JCR_ALL);
        setupPermission(userPrincipal, "/a/b/c", true, PrivilegeConstants.JCR_ALL);

        Root root = getLatestRoot();
        Tree a = root.getTree("/a");
        Tree b = a.addChild("b");
        assertFalse(b.hasChild("c"));

        try {
            root.commit();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
        }
    }

}
