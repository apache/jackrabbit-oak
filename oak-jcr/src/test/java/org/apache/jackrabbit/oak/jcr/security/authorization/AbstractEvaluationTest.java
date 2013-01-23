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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.test.api.security.AbstractAccessControlTest;
import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.assertArrayEquals;

/**
 * Base class for testing access control evaluation using JCR API.
 */
public abstract class AbstractEvaluationTest extends AbstractAccessControlTest {

    private static final Map<String, Value> EMPTY_RESTRICTIONS = Collections.emptyMap();

    protected static final String REP_WRITE = "rep:write";

    protected User testUser;
    protected Credentials creds;

    protected Group testGroup;

    private Session testSession;
    private AccessControlManager testAccessControlManager;
    private Node trn;
    private Set<String> toClear = new HashSet<String>();

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        try {
            UserManager uMgr = getUserManager(superuser);
            // create the testUser
            String uid = "testUser" + UUID.randomUUID();
            creds = new SimpleCredentials(uid, uid.toCharArray());

            testUser = uMgr.createUser(uid, uid);
            superuser.save();
        } catch (Exception e) {
            superuser.logout();
            throw e;
        }
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        try {
            if (testSession != null && testSession.isLive()) {
                testSession.logout();
            }
            for (String path : toClear) {
                AccessControlPolicy[] policies = acMgr.getPolicies(path);
                for (AccessControlPolicy policy : policies) {
                    acMgr.removePolicy(path, policy);
                }
            }
            if (testGroup != null) {
                testGroup.remove();
            }
            if (testUser != null) {
                testUser.remove();
            }
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    protected static UserManager getUserManager(Session session) throws
            NotExecutableException {
        if (!(session instanceof JackrabbitSession)) {
            throw new NotExecutableException();
        }
        try {
            return ((JackrabbitSession) session).getUserManager();
        } catch (RepositoryException e) {
            throw new NotExecutableException();
        }
    }

    protected Session getTestSession() throws RepositoryException {
        if (testSession == null) {
            testSession = getHelper().getRepository().login(creds);
        }
        return testSession;
    }

    protected AccessControlManager getTestAccessControlManager() throws Exception {
        if (testAccessControlManager == null) {
            testAccessControlManager = getAccessControlManager(getTestSession());
        }
        return testAccessControlManager;
    }

    protected Group getTestGroup() throws Exception {
        if (testGroup == null) {
            UserManager umgr = getUserManager(superuser);
            testGroup = umgr.createGroup("testGroup" + UUID.randomUUID());
            testGroup.addMember(testUser);
            superuser.save();
        }
        return testGroup;
    }

    protected Node getTestNode() throws RepositoryException {
        if (trn == null) {
            trn = getTestSession().getNode(testRootNode.getPath());
        }
        return trn;
    }

    protected void assertPrivilege(String path, String privName, boolean isAllow) throws Exception {
        Privilege[] privs = privilegesFromName(privName.toString());
        assertEquals(isAllow, getTestAccessControlManager().hasPrivileges(path, privs));
    }

    protected void checkReadOnly(String path) throws Exception {
        Privilege[] privs = getTestAccessControlManager().getPrivileges(path);
        assertArrayEquals(privilegesFromName(Privilege.JCR_READ), privs);
    }

    protected JackrabbitAccessControlList modify(String path, String privilege, boolean isAllow) throws Exception {
        return modify(path, testUser.getPrincipal(), privilegesFromName(privilege), isAllow, EMPTY_RESTRICTIONS);
    }

    private JackrabbitAccessControlList modify(String path, Principal principal, Privilege[] privileges, boolean isAllow, Map<String, Value> restrictions) throws Exception {
        JackrabbitAccessControlList tmpl = AccessControlUtils.getAccessControlList(acMgr, path);
        tmpl.addEntry(principal, privileges, isAllow, restrictions);

        acMgr.setPolicy(tmpl.getPath(), tmpl);
        superuser.save();

        // remember for clean up during tearDown
        toClear.add(tmpl.getPath());
        return tmpl;
    }

    protected JackrabbitAccessControlList allow(String nPath, Privilege[] privileges)
            throws Exception {
        return modify(nPath, testUser.getPrincipal(), privileges, true, EMPTY_RESTRICTIONS);
    }

    protected JackrabbitAccessControlList allow(String nPath, Privilege[] privileges,
                                                Map<String, Value> restrictions)
            throws Exception {
        return modify(nPath, testUser.getPrincipal(), privileges, true, restrictions);
    }

    protected JackrabbitAccessControlList allow(String nPath, Principal principal,
                                                Privilege[] privileges, Map<String, Value> restrictions)
            throws Exception {
        return modify(nPath, principal, privileges, true, restrictions);
    }

    protected JackrabbitAccessControlList deny(String nPath, Privilege[] privileges)
            throws Exception {
        return modify(nPath, testUser.getPrincipal(), privileges, false, EMPTY_RESTRICTIONS);
    }

    protected JackrabbitAccessControlList deny(String nPath, Privilege[] privileges, Map<String, Value> restrictions)
            throws Exception {
        return modify(nPath, testUser.getPrincipal(), privileges, false, restrictions);
    }

    protected JackrabbitAccessControlList deny(String nPath, Principal principal, Privilege[] privileges, Map<String, Value> restrictions)
            throws Exception {
        return modify(nPath, principal, privileges, false, restrictions);
    }
}