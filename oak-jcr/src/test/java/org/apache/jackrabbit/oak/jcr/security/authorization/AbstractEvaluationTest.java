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
import javax.jcr.Property;
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

    protected Privilege[] readPrivileges;
    protected Privilege[] modPropPrivileges;
    protected Privilege[] readWritePrivileges;
    protected Privilege[] repWritePrivileges;

    protected String path;
    protected String childNPath;
    protected String childNPath2;
    protected String childPPath;
    protected String childchildPPath;
    protected String siblingPath;

    protected User testUser;
    protected Credentials creds;
    protected Group testGroup;

    protected Session testSession;
    protected AccessControlManager testAcMgr;

    private Node trn;
    private Set<String> toClear = new HashSet<String>();

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        readPrivileges = privilegesFromName(Privilege.JCR_READ);
        modPropPrivileges = privilegesFromName(Privilege.JCR_MODIFY_PROPERTIES);
        readWritePrivileges = privilegesFromNames(new String[]{Privilege.JCR_READ, REP_WRITE});
        repWritePrivileges = privilegesFromName(REP_WRITE);

        UserManager uMgr = getUserManager(superuser);
        // create the testUser
        String uid = "testUser" + UUID.randomUUID();
        creds = new SimpleCredentials(uid, uid.toCharArray());
        testUser = uMgr.createUser(uid, uid);

        testSession = getTestSession();
        testAcMgr = getAccessControlManager(testSession);

        // create some nodes below the test root in order to apply ac-stuff
        Node node = testRootNode.addNode(nodeName1, testNodeType);
        Node cn1 = node.addNode(nodeName2, testNodeType);
        Property cp1 = node.setProperty(propertyName1, "anyValue");
        Node cn2 = node.addNode(nodeName3, testNodeType);
        Property ccp1 = cn1.setProperty(propertyName1, "childNodeProperty");
        Node n2 = testRootNode.addNode(nodeName2, testNodeType);
        superuser.save();

        path = node.getPath();
        childNPath = cn1.getPath();
        childNPath2 = cn2.getPath();
        childPPath = cp1.getPath();
        childchildPPath = ccp1.getPath();
        siblingPath = n2.getPath();

        /*
        precondition:
        testuser must have READ-only permission on test-node and below
        */
        assertReadOnly(path);
        assertReadOnly(childNPath);
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

    private Session getTestSession() throws RepositoryException {
        if (testSession == null) {
            testSession = getHelper().getRepository().login(creds);
        }
        return testSession;
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
            trn = testSession.getNode(testRootNode.getPath());
        }
        return trn;
    }

    protected String getActions(String... actions) {
        StringBuilder sb = new StringBuilder();
        for (String action : actions) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(action);
        }
        return sb.toString();
    }
    protected Map<String, Value> createGlobRestriction(String value) throws RepositoryException {
        return Collections.singletonMap("rep:glob", testSession.getValueFactory().createValue(value));
    }

    protected void assertHasPrivilege(String path, String privName, boolean isAllow) throws Exception {
        Privilege[] privs = privilegesFromName(privName.toString());
        assertEquals(isAllow, testAcMgr.hasPrivileges(path, privs));
    }

    protected void assertReadOnly(String path) throws Exception {
        Privilege[] privs = testAcMgr.getPrivileges(path);
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
                                                Privilege[] privileges)
            throws Exception {
        return modify(nPath, principal, privileges, true, EMPTY_RESTRICTIONS);
    }

    protected JackrabbitAccessControlList deny(String nPath, Privilege[] privileges)
            throws Exception {
        return modify(nPath, testUser.getPrincipal(), privileges, false, EMPTY_RESTRICTIONS);
    }

    protected JackrabbitAccessControlList deny(String nPath, Privilege[] privileges, Map<String, Value> restrictions)
            throws Exception {
        return modify(nPath, testUser.getPrincipal(), privileges, false, restrictions);
    }

    protected JackrabbitAccessControlList deny(String nPath, Principal principal, Privilege[] privileges)
            throws Exception {
        return modify(nPath, principal, privileges, false, EMPTY_RESTRICTIONS);
    }
}