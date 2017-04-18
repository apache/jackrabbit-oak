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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.Node;
import javax.jcr.PathNotFoundException;
import javax.jcr.Property;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import com.google.common.collect.Maps;
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

import com.google.common.collect.Sets;

import static org.junit.Assert.assertArrayEquals;

/**
 * Base class for testing permission evaluation using JCR API.
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

    private Map<String, ACL> toRestore = Maps.newHashMap();

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
        String uid = generateId("testUser");
        creds = new SimpleCredentials(uid, uid.toCharArray());
        testUser = uMgr.createUser(uid, uid);

        UserManager umgr = getUserManager(superuser);
        testGroup = umgr.createGroup(generateId("testGroup"));
        testGroup.addMember(testUser);

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

        superuser.save();

        testSession = createTestSession();
        testAcMgr = getAccessControlManager(testSession);

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
            superuser.refresh(false);
            // restore in reverse order
            for (String path : toRestore.keySet()) {
                toRestore.get(path).restore();
            }
            toRestore.clear();
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

    protected Session createTestSession() throws RepositoryException {
        return getHelper().getRepository().login(creds);
    }

    protected static UserManager getUserManager(Session session) throws NotExecutableException {
        if (!(session instanceof JackrabbitSession)) {
            throw new NotExecutableException();
        }
        try {
            return ((JackrabbitSession) session).getUserManager();
        } catch (RepositoryException e) {
            throw new NotExecutableException(e.getMessage());
        }
    }

    protected static String generateId(@Nonnull String hint) {
        return hint + UUID.randomUUID();
    }

    protected static boolean canReadNode(Session session, String nodePath) throws RepositoryException {
        try {
            session.getNode(nodePath);
            return session.nodeExists(nodePath);
        } catch (PathNotFoundException e) {
            return session.nodeExists(nodePath);
        }
    }

    protected Group getTestGroup() throws Exception {
        return testGroup;
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

    protected void assertHasRepoPrivilege(@Nonnull String privName, boolean isAllow) throws Exception {
        Privilege[] privs = privilegesFromName(privName.toString());
        assertEquals(isAllow, testAcMgr.hasPrivileges(null, privs));
    }

    protected void assertHasPrivilege(@Nonnull String path, @Nonnull String privName, boolean isAllow) throws Exception {
        assertHasPrivileges(path, privilegesFromName(privName), isAllow);
    }

    protected void assertHasPrivileges(@Nonnull String path, @Nonnull Privilege[] privileges, boolean isAllow) throws Exception {
        if (testSession.nodeExists(path)) {
            assertEquals(isAllow, testAcMgr.hasPrivileges(path, privileges));
        } else {
            try {
                testAcMgr.hasPrivileges(path, privileges);
                fail("PathNotFoundException expected");
            } catch (PathNotFoundException e) {
                // success
            }
        }
    }

    protected void assertReadOnly(String path) throws Exception {
        Privilege[] privs = testAcMgr.getPrivileges(path);
        assertArrayEquals(privilegesFromName(Privilege.JCR_READ), privs);
    }

    protected JackrabbitAccessControlList modify(@Nullable String path, @Nonnull String privilege, boolean isAllow) throws Exception {
        return modify(path, testUser.getPrincipal(), privilegesFromName(privilege), isAllow, EMPTY_RESTRICTIONS);
    }

    protected JackrabbitAccessControlList modify(String path, Principal principal, Privilege[] privileges, boolean isAllow, Map<String, Value> restrictions) throws Exception {
        return modify(path, principal, privileges, isAllow, restrictions, Collections.<String, Value[]>emptyMap());
    }

    protected JackrabbitAccessControlList modify(String path, Principal principal,
                                                 Privilege[] privileges, boolean isAllow,
                                                 Map<String, Value> restrictions,
                                                 Map<String, Value[]> mvRestrictions) throws Exception {
        // remember for restore during tearDown
        rememberForRestore(path);

        JackrabbitAccessControlList tmpl = AccessControlUtils.getAccessControlList(acMgr, path);
        tmpl.addEntry(principal, privileges, isAllow, restrictions, mvRestrictions);

        acMgr.setPolicy(tmpl.getPath(), tmpl);
        superuser.save();
        testSession.refresh(false);

        return tmpl;
    }

    protected JackrabbitAccessControlList allow(@Nullable String nPath,
                                                @Nonnull Privilege[] privileges)
            throws Exception {
        return modify(nPath, testUser.getPrincipal(), privileges, true, EMPTY_RESTRICTIONS);
    }

    protected JackrabbitAccessControlList allow(@Nullable String nPath,
                                                @Nonnull Privilege[] privileges,
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

    private void rememberForRestore(@Nullable String path) throws RepositoryException {
        if (!toRestore.containsKey(path)) {
            toRestore.put(path, new ACL(path));
        }
    }

    private final class ACL {

        private final String path;
        private final boolean remove;
        private final Set<AccessControlEntry> entries = Sets.newHashSet();

        private ACL(String path) throws RepositoryException {
            this.path = path;

            AccessControlList list = getList(path);
            remove = (list == null);
            if (list != null) {
                Collections.addAll(entries, list.getAccessControlEntries());
            }
        }

        private void restore() throws RepositoryException {
            AccessControlList list = getList(path);
            if (list != null) {
                if (remove) {
                    acMgr.removePolicy(path, list);
                } else {
                    for (AccessControlEntry ace : list.getAccessControlEntries()) {
                        list.removeAccessControlEntry(ace);
                    }
                    for (AccessControlEntry ace : entries) {
                        list.addAccessControlEntry(ace.getPrincipal(), ace.getPrivileges());
                    }
                    acMgr.setPolicy(path, list);
                }
            }
            superuser.save();
        }

        @CheckForNull
        private AccessControlList getList(@Nullable String path) throws RepositoryException {
            if (path == null || superuser.nodeExists(path)) {
                for (AccessControlPolicy policy : acMgr.getPolicies(path)) {
                    if (policy instanceof AccessControlList) {
                        return (AccessControlList) policy;
                    }
                }
            }
            return null;
        }
    }
}