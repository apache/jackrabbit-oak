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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import java.security.Principal;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.SimpleCredentials;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Before;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;

/**
 * Base class for all classes that attempt to test OAK API and OAK core functionality
 * in combination with permission evaluation
 */
public abstract class AbstractOakCoreTest extends AbstractSecurityTest {

    protected static final String TEST_USER_ID = "test";

	protected Principal testPrincipal;

    private ContentSession testSession;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        User user = getUserManager().createUser(TEST_USER_ID, TEST_USER_ID);
        testPrincipal = user.getPrincipal();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"));
        NodeUtil a = rootNode.addChild("a", NT_UNSTRUCTURED);
        a.setString("aProp", "aValue");

        NodeUtil b = a.addChild("b", NT_UNSTRUCTURED);
        b.setString("bProp", "bValue");
        // sibling
        NodeUtil bb = a.addChild("bb", NT_UNSTRUCTURED);
        bb.setString("bbProp", "bbValue");

        NodeUtil c = b.addChild("c", NT_UNSTRUCTURED);
        c.setString("cProp", "cValue");
        root.commit();
    }

    @After
    @Override
    public void after() throws Exception {
        try {
            Authorizable testUser = getUserManager().getAuthorizable(TEST_USER_ID);
            if (testUser != null) {
                testUser.remove();
                root.commit();
            }

            if (testSession != null) {
                testSession.close();
            }
        } finally {
            super.after();
        }
    }

    @Nonnull
    protected ContentSession getTestSession() throws Exception {
        if (testSession == null) {
            testSession = login(new SimpleCredentials(TEST_USER_ID, TEST_USER_ID.toCharArray()));
        }
        return testSession;
    }

    @Nonnull
    protected Root getTestRoot() throws Exception {
        return getTestSession().getLatestRoot();
    }

    /**
     * Setup simple allow/deny permissions (without restrictions).
     *
     * @param path
     * @param isAllow
     * @param privilegeNames
     * @throws Exception
     */
    protected void setupPermission(@Nullable String path,
                                   @Nonnull Principal principal,
                                   boolean isAllow,
                                   @Nonnull String... privilegeNames) throws Exception {
    	AccessControlManager acMgr = getAccessControlManager(root);
    	JackrabbitAccessControlList acl = checkNotNull(AccessControlUtils.getAccessControlList(acMgr, path));
      	acl.addEntry(principal, AccessControlUtils.privilegesFromNames(acMgr, privilegeNames), isAllow);
     	acMgr.setPolicy(path, acl);

        root.commit();
    }
}